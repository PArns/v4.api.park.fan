import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  Logger,
  NotFoundException,
  OnModuleInit,
  UnauthorizedException,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { randomBytes } from "crypto";
import { isIP } from "net";
import {
  ADMIN_ROLES,
  AdminRole,
  AdminUser,
} from "./entities/admin-user.entity";
import {
  hashPassword,
  needsRehash,
  validatePasswordStrength,
  verifyPassword,
} from "./password.util";
import { generateTotpSecret, totpStep, totpUri, verifyTotp } from "./totp.util";
import { AdminSessionStore, type AdminSession } from "./admin-session.store";
import { AdminLoginRateLimitService } from "./admin-login-rate-limit.service";
import {
  getBootstrapAdmin,
  getLoginLockoutMinutes,
  getLoginLockoutThreshold,
  isTotpRequired,
} from "../../config/admin-auth.config";

export interface LoginRequest {
  email: string;
  password: string;
  totpCode?: string;
  ip?: string | null;
  userAgent?: string | null;
}

export type LoginResult =
  | { outcome: "ok"; token: string; session: AdminSession }
  | { outcome: "totp-required" }
  | { outcome: "locked"; retryAfterSeconds: number }
  | { outcome: "rate-limited"; retryAfterSeconds: number };

export interface PublicAdminUser {
  id: string;
  email: string;
  displayName: string;
  role: AdminRole;
  isActive: boolean;
  mustChangePassword: boolean;
  totpEnabled: boolean;
  lastLoginAt: string | null;
  createdAt: string;
}

/**
 * Truncate an address before it is stored.
 *
 * The useful question an audit row answers is "did this session come from
 * somewhere new", not "where was this person". A /24 or /48 answers that and
 * stops the log from being a movement history of the three people who use it.
 */
export function truncateIp(ip: string | null | undefined): string | null {
  if (!ip) return null;
  const clean = ip.startsWith("::ffff:") ? ip.slice(7) : ip;
  if (isIP(clean) === 4) {
    const parts = clean.split(".");
    return `${parts[0]}.${parts[1]}.${parts[2]}.0/24`;
  }
  if (isIP(clean) === 6) {
    return `${expandIpv6(clean).slice(0, 3).join(":")}::/48`;
  }
  return null;
}

/**
 * The eight hextets of an IPv6 address, with `::` written out.
 *
 * Needed because almost every real address is compressed, and slicing the
 * first three groups off the compressed text does not give the first three
 * hextets: `2001:db8::1` splits into `["2001","db8","","1"]`, so the naive
 * version produced `2001:db8:::/48` — not a prefix, not comparable to the
 * next login's, and stored as though it were.
 */
function expandIpv6(address: string): string[] {
  const [head, tail] = address.split("::");
  const headGroups = head ? head.split(":").filter(Boolean) : [];
  if (tail === undefined) return headGroups;

  const tailGroups = tail ? tail.split(":").filter(Boolean) : [];
  const missing = Math.max(0, 8 - headGroups.length - tailGroups.length);
  return [...headGroups, ...Array<string>(missing).fill("0"), ...tailGroups];
}

export function toPublicUser(user: AdminUser): PublicAdminUser {
  return {
    id: user.id,
    email: user.email,
    displayName: user.displayName,
    role: user.role,
    isActive: user.isActive,
    mustChangePassword: user.mustChangePassword,
    totpEnabled: user.totpEnabled,
    lastLoginAt: user.lastLoginAt ? user.lastLoginAt.toISOString() : null,
    createdAt: user.createdAt.toISOString(),
  };
}

/**
 * Accounts, logins, and the lifecycle of both.
 *
 * The login path is written to leak as little as it can: a wrong password, an
 * unknown address and a deactivated account all take the same visible route
 * out (`UnauthorizedException`, generic message), and an unknown address still
 * pays for a password verification so the response time does not answer the
 * question the message refuses to.
 */
@Injectable()
export class AdminAuthService implements OnModuleInit {
  private readonly logger = new Logger(AdminAuthService.name);

  /**
   * A hash to verify against when the address is unknown.
   *
   * scrypt at these parameters costs ~100 ms, which is plenty to tell "no such
   * account" (instant) from "wrong password" (slow) by stopwatch. Verifying a
   * throwaway hash makes both paths cost the same.
   */
  private decoyHash: string | null = null;

  constructor(
    @InjectRepository(AdminUser)
    private readonly users: Repository<AdminUser>,
    private readonly sessions: AdminSessionStore,
    private readonly loginLimiter: AdminLoginRateLimitService,
  ) {}

  async onModuleInit(): Promise<void> {
    // randomBytes rather than Math.random: this value is only ever hashed and
    // thrown away, so its unpredictability does not matter — but it is a
    // password reaching a password hasher, and a scanner cannot tell that from
    // one that does. Using the CSPRNG costs nothing and keeps the security
    // path free of a "this one is fine" exception nobody will re-evaluate.
    this.decoyHash = await hashPassword(
      `decoy-never-matches-${randomBytes(16).toString("hex")}`,
    );
    try {
      await this.bootstrapFirstOwner();
    } catch (error) {
      // This is the only thing in the login path that touches the database at
      // module init, and module init also runs during `pnpm build`'s Swagger
      // generation — which points at DB_HOST=127.0.0.1 DB_PORT=1 on purpose.
      // A failure here means "no bootstrap account was created", never "the
      // build is broken".
      this.logger.error(
        `Bootstrap owner check failed: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  /**
   * Create the first owner from env when the table is empty.
   *
   * Only when it is empty: re-running this on every boot would let a stale
   * environment variable resurrect a deleted account or reset a rotated
   * password, which is exactly the kind of surprise an admin credential must
   * never spring.
   */
  private async bootstrapFirstOwner(): Promise<void> {
    const bootstrap = getBootstrapAdmin();
    if (!bootstrap) return;

    const existing = await this.users.count();
    if (existing > 0) return;

    const complaint = validatePasswordStrength(bootstrap.password);
    if (complaint) {
      this.logger.error(
        `ADMIN_BOOTSTRAP_PASSWORD rejected: ${complaint}. No owner account created.`,
      );
      return;
    }

    await this.users.save(
      this.users.create({
        email: bootstrap.email,
        displayName: bootstrap.email.split("@")[0],
        passwordHash: await hashPassword(bootstrap.password),
        role: "owner",
        isActive: true,
        // The bootstrap password lives in an environment variable, which means
        // it lives in a deployment config, a shell history and probably a
        // password manager note. It gets the account created and nothing else.
        mustChangePassword: true,
      }),
    );
    this.logger.warn(
      `👤 Created bootstrap admin owner ${bootstrap.email} — it must change its password at first login`,
    );
  }

  // ── login ─────────────────────────────────────────────────────────────────

  async login(request: LoginRequest): Promise<LoginResult> {
    const email = request.email?.trim().toLowerCase() ?? "";

    // Before anything expensive: a rejected attempt must not cost 100 ms of
    // scrypt, or the login endpoint becomes its own denial-of-service surface.
    const verdict = await this.loginLimiter.check(request.ip ?? null, email);
    if (!verdict.allowed) {
      return {
        outcome: "rate-limited",
        retryAfterSeconds: verdict.retryAfterSeconds,
      };
    }

    const user = await this.users.findOne({
      where: { email },
      select: {
        id: true,
        email: true,
        displayName: true,
        passwordHash: true,
        role: true,
        isActive: true,
        mustChangePassword: true,
        failedLoginCount: true,
        lockedUntil: true,
        totpSecret: true,
        totpEnabled: true,
        totpLastStep: true,
        createdAt: true,
        lastLoginAt: true,
        updatedAt: true,
        lastLoginIp: true,
      },
    });

    if (!user) {
      // Same cost as a real verification — see decoyHash.
      if (this.decoyHash)
        await verifyPassword(request.password ?? "", this.decoyHash);
      await this.loginLimiter.recordFailure(request.ip ?? null, email);
      throw new UnauthorizedException("Invalid credentials");
    }

    const lockedUntil = user.lockedUntil?.getTime() ?? 0;
    const locked = lockedUntil > Date.now();

    // The password is verified even while locked, and the lockout is reported
    // only to somebody who got it right. Answering "locked" before checking
    // made the endpoint an account-existence oracle: eight wrong guesses at an
    // address that HAS an account produce a distinctive ninth answer, while an
    // address that has none produces nine identical 401s. Verifying first
    // costs the attacker ~100 ms of scrypt per probe and tells them nothing.
    const passwordOk = await verifyPassword(
      request.password ?? "",
      user.passwordHash,
    );

    if (locked) {
      if (!passwordOk) {
        await this.loginLimiter.recordFailure(request.ip ?? null, email);
        throw new UnauthorizedException("Invalid credentials");
      }
      return {
        outcome: "locked",
        retryAfterSeconds: Math.ceil((lockedUntil - Date.now()) / 1000),
      };
    }

    // A lockout that has expired must take its counter with it. Leaving the
    // count at the threshold meant the next single wrong password re-locked the
    // account immediately — one bad guess every fifteen minutes, 96 a day, and
    // the account is shut forever with neither rate-limit bucket ever firing.
    if (lockedUntil > 0 && !locked && user.failedLoginCount > 0) {
      user.failedLoginCount = 0;
      user.lockedUntil = null;
      await this.users.update(
        { id: user.id },
        { failedLoginCount: 0, lockedUntil: null },
      );
    }

    if (!passwordOk) {
      await this.recordFailedLogin(user);
      await this.loginLimiter.recordFailure(request.ip ?? null, email);
      throw new UnauthorizedException("Invalid credentials");
    }

    // Deactivation is checked AFTER the password so that a wrong password
    // against a deactivated account behaves like a wrong password against any
    // other — the login form must not become a way to enumerate who still
    // works here.
    if (!user.isActive) {
      await this.loginLimiter.recordFailure(request.ip ?? null, email);
      throw new UnauthorizedException("Invalid credentials");
    }

    // Mandatory two-factor, enforced where it has to be: at the login. It used
    // to gate only the "turn it off again" endpoint, which means an operator
    // could set ADMIN_REQUIRE_TOTP=true, reasonably conclude two-factor was
    // mandatory, and have every account that never enrolled keep signing in
    // with a password alone. The session is still issued — refusing it outright
    // would lock out the very accounts that need to enrol — but it carries the
    // debt, and the guard lets such a session reach only the enrolment
    // endpoints, exactly like a pending password change.
    const owesEnrolment = isTotpRequired() && !user.totpEnabled;

    if (user.totpEnabled) {
      if (!request.totpCode) return { outcome: "totp-required" };
      const lastStep = user.totpLastStep ? Number(user.totpLastStep) : null;
      const check = verifyTotp(
        user.totpSecret ?? "",
        request.totpCode,
        lastStep,
      );
      if (!check.valid) {
        await this.recordFailedLogin(user);
        await this.loginLimiter.recordFailure(request.ip ?? null, email);
        throw new UnauthorizedException("Invalid credentials");
      }
      await this.users.update(
        { id: user.id },
        { totpLastStep: String(check.step) },
      );
    }

    await this.loginLimiter.recordSuccess(email);

    const ip = truncateIp(request.ip);
    await this.users.update(
      { id: user.id },
      {
        failedLoginCount: 0,
        lockedUntil: null,
        lastLoginAt: new Date(),
        lastLoginIp: ip,
        // Upgrade the stored hash while the plaintext is in hand — the only
        // moment it can be done — if the cost parameters have since risen.
        ...(needsRehash(user.passwordHash)
          ? { passwordHash: await hashPassword(request.password) }
          : {}),
      },
    );

    const issued = await this.sessions.create({
      userId: user.id,
      email: user.email,
      displayName: user.displayName,
      role: user.role,
      ip,
      userAgent: request.userAgent?.slice(0, 300) ?? null,
      mustChangePassword: user.mustChangePassword,
      mustEnrolTotp: owesEnrolment,
    });

    return { outcome: "ok", token: issued.token, session: issued.session };
  }

  private async recordFailedLogin(user: AdminUser): Promise<void> {
    const failed = (user.failedLoginCount ?? 0) + 1;
    const threshold = getLoginLockoutThreshold();
    const lockedUntil =
      failed >= threshold
        ? new Date(Date.now() + getLoginLockoutMinutes() * 60_000)
        : null;

    await this.users.update(
      { id: user.id },
      { failedLoginCount: failed, lockedUntil },
    );

    if (lockedUntil) {
      this.logger.warn(
        `🔒 Locked admin account ${user.email} after ${failed} failed logins, until ${lockedUntil.toISOString()}`,
      );
    }
  }

  async logout(token: string): Promise<void> {
    await this.sessions.destroy(token);
  }

  // ── accounts ──────────────────────────────────────────────────────────────

  async findById(id: string): Promise<AdminUser | null> {
    return this.users.findOne({ where: { id } });
  }

  async list(): Promise<PublicAdminUser[]> {
    const rows = await this.users.find({ order: { createdAt: "ASC" } });
    return rows.map(toPublicUser);
  }

  async create(input: {
    email: string;
    displayName?: string;
    role: AdminRole;
    password: string;
  }): Promise<PublicAdminUser> {
    const email = input.email.trim().toLowerCase();
    if (!looksLikeEmailAddress(email)) {
      throw new BadRequestException("A valid email address is required");
    }
    if (!ADMIN_ROLES.includes(input.role)) {
      throw new BadRequestException(`Unknown role "${input.role}"`);
    }
    const complaint = validatePasswordStrength(input.password);
    if (complaint) throw new BadRequestException(complaint);

    const existing = await this.users.findOne({ where: { email } });
    if (existing)
      throw new BadRequestException("That email already has an account");

    const saved = await this.users.save(
      this.users.create({
        email,
        displayName: input.displayName?.trim() || email.split("@")[0],
        passwordHash: await hashPassword(input.password),
        role: input.role,
        isActive: true,
        mustChangePassword: true,
      }),
    );
    return toPublicUser(saved);
  }

  /**
   * Change role or activation.
   *
   * Both changes take effect on live sessions immediately rather than at the
   * next login: a demotion that only applies tomorrow is not a demotion, and
   * this is the endpoint somebody reaches for when an account is compromised.
   */
  async update(
    id: string,
    patch: { displayName?: string; role?: AdminRole; isActive?: boolean },
  ): Promise<PublicAdminUser> {
    const user = await this.users.findOne({ where: { id } });
    if (!user) throw new NotFoundException("No such account");

    if (patch.role && !ADMIN_ROLES.includes(patch.role)) {
      throw new BadRequestException(`Unknown role "${patch.role}"`);
    }

    // An account demoted or deactivated out of the last owner slot would leave
    // nobody able to manage accounts at all, and the only way back would be to
    // edit the database by hand.
    const losingOwner =
      user.role === "owner" &&
      ((patch.role && patch.role !== "owner") || patch.isActive === false);

    const changes = {
      ...(patch.displayName !== undefined
        ? { displayName: patch.displayName.trim() }
        : {}),
      ...(patch.role !== undefined ? { role: patch.role } : {}),
      ...(patch.isActive !== undefined ? { isActive: patch.isActive } : {}),
    };

    // Counted and written in one transaction, with the other owners locked for
    // its duration. As a plain read-then-write, two requests demoting the two
    // remaining owners in the same tick both see one other owner, both pass,
    // and the deployment ends with none — every owner-only endpoint unreachable
    // and no way back through the API.
    const saved = losingOwner
      ? await this.users.manager.transaction(async (manager) => {
          const otherOwners = await manager
            .createQueryBuilder(AdminUser, "u")
            .setLock("pessimistic_write")
            .where("u.role = :role", { role: "owner" })
            .andWhere("u.is_active = true")
            .andWhere("u.id != :id", { id: user.id })
            .getCount();
          if (otherOwners === 0) {
            throw new BadRequestException(
              "This is the last active owner — promote another account first",
            );
          }
          Object.assign(user, changes);
          return manager.save(user);
        })
      : await this.users.save(Object.assign(user, changes));

    if (patch.isActive === false) {
      await this.sessions.destroyAllForUser(id);
    } else {
      await this.sessions.patchUserSessions(id, {
        role: saved.role,
        displayName: saved.displayName,
      });
    }

    return toPublicUser(saved);
  }

  /**
   * Change one's own password.
   *
   * Every other session of the account is dropped, because the most common
   * reason to change a password is that somebody else may know the old one.
   * The session doing the changing survives — being logged out of the tab you
   * just used correctly is a bug, not a security measure.
   */
  async changeOwnPassword(
    userId: string,
    currentPassword: string,
    newPassword: string,
    keepToken: string | null,
  ): Promise<{ reissuedToken: string | null; expiresAt: string | null }> {
    const user = await this.users.findOne({
      where: { id: userId },
      select: {
        id: true,
        email: true,
        passwordHash: true,
        role: true,
        displayName: true,
        // Needed to re-derive the enrolment debt below. Without it TypeORM
        // omits the property, `!user.totpEnabled` is true for everybody, and
        // every enrolled admin lands in an enrolment screen after a password
        // change.
        totpEnabled: true,
      },
    });
    if (!user) throw new NotFoundException("No such account");

    const limit = await this.loginLimiter.checkAction(
      "change-password",
      userId,
    );
    if (!limit.allowed) {
      throw new UnauthorizedException(
        `Too many attempts. Try again in ${Math.ceil(limit.retryAfterSeconds / 60)} minutes.`,
      );
    }

    const ok = await verifyPassword(currentPassword ?? "", user.passwordHash);
    if (!ok) {
      await this.loginLimiter.recordActionFailure("change-password", userId);
      throw new UnauthorizedException("Current password is incorrect");
    }
    await this.loginLimiter.recordActionSuccess("change-password", userId);

    const complaint = validatePasswordStrength(newPassword);
    if (complaint) throw new BadRequestException(complaint);
    if (await verifyPassword(newPassword, user.passwordHash)) {
      throw new BadRequestException(
        "The new password must differ from the old one",
      );
    }

    await this.users.update(
      { id: userId },
      {
        passwordHash: await hashPassword(newPassword),
        mustChangePassword: false,
        failedLoginCount: 0,
        lockedUntil: null,
      },
    );

    const kept = keepToken ? await this.sessions.resolve(keepToken) : null;
    await this.sessions.destroyAllForUser(userId);
    if (!kept) return { reissuedToken: null, expiresAt: null };

    // Re-issuing rather than sparing the old token: a password change is a
    // good moment for the session identifier to change too. It is returned
    // rather than stashed on the service, because a service is a singleton and
    // two people changing their password in the same second would otherwise
    // hand one of them the other's session.
    const issued = await this.sessions.create({
      userId: kept.userId,
      email: kept.email,
      displayName: kept.displayName,
      role: kept.role,
      ip: kept.ip,
      userAgent: kept.userAgent,
      mustChangePassword: false,
      // Derived from the row, not carried from `kept`: a session must never
      // outrank the account it belongs to. Left out entirely, the new session
      // read as "enrolled" — and since change-password is one of the two
      // endpoints a debt-carrying session may reach, changing a password was a
      // way to walk out of a mandatory second factor.
      mustEnrolTotp: isTotpRequired() && !user.totpEnabled,
    });
    // The new session's ceiling travels with the token. The caller writes a
    // cookie from it, and without the date it had to guess: the frontend used
    // the 12 h idle window, which slides server-side but not in a Max-Age, so
    // an admin who changed their password was signed out that evening while the
    // session behind the cookie still had six days left.
    return {
      reissuedToken: issued.token,
      expiresAt: new Date(issued.session.expiresAt).toISOString(),
    };
  }

  /**
   * An owner resets somebody else's password.
   *
   * The new password is returned once, in the response, and never stored in
   * readable form — the owner reads it out and the account is required to
   * replace it at the next login.
   */
  async resetPassword(
    id: string,
    newPassword: string,
  ): Promise<{ mustChangePassword: true }> {
    const user = await this.users.findOne({ where: { id } });
    if (!user) throw new NotFoundException("No such account");

    const complaint = validatePasswordStrength(newPassword);
    if (complaint) throw new BadRequestException(complaint);

    await this.users.update(
      { id },
      {
        passwordHash: await hashPassword(newPassword),
        mustChangePassword: true,
        failedLoginCount: 0,
        lockedUntil: null,
      },
    );
    await this.sessions.destroyAllForUser(id);
    return { mustChangePassword: true };
  }

  /**
   * An owner clears another account's second factor.
   *
   * The recovery path, and there has to be one. A phone that is lost or wiped
   * leaves its account answering `totp-required` on every attempt forever:
   * `totp/disable` cannot be reached without a code, `reset-password` does not
   * touch the flag, and nothing else did either. On a deployment with one owner
   * that is the whole admin surface, permanently.
   *
   * Every session of the account goes with it — if the second factor is being
   * cleared because a device is gone, any session on that device is gone too.
   */
  async clearTotp(id: string): Promise<void> {
    const user = await this.users.findOne({ where: { id } });
    if (!user) throw new NotFoundException("No such account");
    await this.users.update(
      { id },
      { totpEnabled: false, totpSecret: null, totpLastStep: null },
    );
    await this.sessions.destroyAllForUser(id);
  }

  // ── TOTP enrolment ────────────────────────────────────────────────────────

  /**
   * Start enrolment: hand back a secret and the otpauth URI to scan.
   *
   * The secret is stored immediately but `totpEnabled` stays false until a
   * code proves the app actually has it — enabling first is how people lock
   * themselves out with a mistyped QR scan.
   */
  async beginTotpEnrolment(
    userId: string,
    password: string,
  ): Promise<{ secret: string; uri: string }> {
    const user = await this.users.findOne({
      where: { id: userId },
      select: { id: true, email: true, passwordHash: true, totpEnabled: true },
    });
    if (!user) throw new NotFoundException("No such account");
    if (user.totpEnabled) {
      throw new BadRequestException("Two-factor is already enabled");
    }
    // The password, for the same reason disabling needs one. Without it a
    // stolen session could enrol the attacker's own authenticator on the
    // victim's account — and since only an owner can clear the flag, that is a
    // lockout, not an inconvenience.
    const limit = await this.loginLimiter.checkAction("totp-begin", userId);
    if (!limit.allowed) {
      throw new UnauthorizedException(
        `Too many attempts. Try again in ${Math.ceil(limit.retryAfterSeconds / 60)} minutes.`,
      );
    }
    if (!(await verifyPassword(password ?? "", user.passwordHash))) {
      await this.loginLimiter.recordActionFailure("totp-begin", userId);
      throw new UnauthorizedException("Password is incorrect");
    }
    await this.loginLimiter.recordActionSuccess("totp-begin", userId);

    const secret = generateTotpSecret();
    await this.users.update({ id: userId }, { totpSecret: secret });
    return { secret, uri: totpUri(secret, user.email) };
  }

  async confirmTotpEnrolment(userId: string, code: string): Promise<void> {
    const user = await this.users.findOne({
      where: { id: userId },
      select: {
        id: true,
        totpSecret: true,
        totpEnabled: true,
        email: true,
        displayName: true,
        role: true,
      },
    });
    if (!user?.totpSecret) {
      throw new BadRequestException("Start enrolment first");
    }
    const check = verifyTotp(user.totpSecret, code, null);
    if (!check.valid) throw new BadRequestException("That code did not match");

    await this.users.update(
      { id: userId },
      { totpEnabled: true, totpLastStep: String(check.step ?? totpStep()) },
    );

    // Clear the enrolment debt on the sessions that are already open. Without
    // this, an account signing in under ADMIN_REQUIRE_TOTP would enrol and then
    // still be refused everything, because the session it enrolled with still
    // says it owes one — it would have to sign out and back in to use the admin
    // it just unlocked.
    await this.sessions.patchUserSessions(userId, { mustEnrolTotp: false });
  }

  /**
   * Turn two-factor off again — password AND a current code, both.
   *
   * A stolen session must not be able to remove the second factor; a stolen
   * password must not either. Requiring both means disabling it needs the same
   * evidence as logging in with it.
   */
  async disableTotp(
    userId: string,
    password: string,
    code: string,
  ): Promise<void> {
    if (isTotpRequired()) {
      throw new ForbiddenException(
        "Two-factor is mandatory on this deployment (ADMIN_REQUIRE_TOTP)",
      );
    }

    // The code is six digits and three of them are valid at once, so this is
    // the one endpoint in the admin where an attacker can realistically
    // exhaust the secret. The global throttler will not help: it skips every
    // request carrying the frontend's bypass key, which is every request the
    // admin UI makes.
    const limit = await this.loginLimiter.checkAction("totp-disable", userId);
    if (!limit.allowed) {
      throw new BadRequestException(
        `Too many attempts. Try again in ${Math.ceil(limit.retryAfterSeconds / 60)} minutes.`,
      );
    }
    const user = await this.users.findOne({
      where: { id: userId },
      select: {
        id: true,
        passwordHash: true,
        totpSecret: true,
        totpEnabled: true,
      },
    });
    if (!user?.totpEnabled)
      throw new BadRequestException("Two-factor is not enabled");
    if (!(await verifyPassword(password ?? "", user.passwordHash))) {
      await this.loginLimiter.recordActionFailure("totp-disable", userId);
      throw new UnauthorizedException("Password is incorrect");
    }
    const check = verifyTotp(user.totpSecret ?? "", code, null);
    if (!check.valid) {
      await this.loginLimiter.recordActionFailure("totp-disable", userId);
      throw new BadRequestException("That code did not match");
    }

    await this.loginLimiter.recordActionSuccess("totp-disable", userId);
    await this.users.update(
      { id: userId },
      { totpEnabled: false, totpSecret: null, totpLastStep: null },
    );
  }
}

/**
 * A shape check for an email address, without a regex.
 *
 * The obvious `/^[^@\s]+@[^@\s]+\.[^@\s]+$/` is a backtracking trap: the two
 * adjacent `[^@\s]+` around the dot make matching super-linear in the number
 * of dots, so `a@` followed by a few thousand `!.` pairs burns CPU on a request
 * that was always going to be rejected. The DTO caps the length, which bounds
 * it — but the service is also callable from code that has no DTO in front of
 * it, and a bound is not the same as a fix.
 *
 * This does the same job in one pass. It is deliberately not RFC 5322: nothing
 * here needs to accept a quoted local part, and this is a shape check in front
 * of a uniqueness constraint, not a claim that the address exists.
 */
export function looksLikeEmailAddress(value: string): boolean {
  if (value.length === 0 || value.length > 320) return false;

  const at = value.indexOf("@");
  // Exactly one "@", with something on each side.
  if (at <= 0 || at !== value.lastIndexOf("@")) return false;

  const local = value.slice(0, at);
  const domain = value.slice(at + 1);
  if (domain.length === 0) return false;

  // A single character class with no quantifier — linear, whatever the input.
  if (/\s/.test(local) || /\s/.test(domain)) return false;

  const dot = domain.lastIndexOf(".");
  return dot > 0 && dot < domain.length - 1;
}
