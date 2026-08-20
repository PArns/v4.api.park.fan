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
import { Not, Repository } from "typeorm";
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
    const groups = clean.split(":");
    return `${groups.slice(0, 3).join(":")}::/48`;
  }
  return null;
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
    this.decoyHash = await hashPassword(
      "decoy-never-matches-" + Math.random().toString(36),
    );
    await this.bootstrapFirstOwner();
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

    if (user.lockedUntil && user.lockedUntil.getTime() > Date.now()) {
      const retryAfterSeconds = Math.ceil(
        (user.lockedUntil.getTime() - Date.now()) / 1000,
      );
      return { outcome: "locked", retryAfterSeconds };
    }

    const passwordOk = await verifyPassword(
      request.password ?? "",
      user.passwordHash,
    );

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
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) {
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
    if (losingOwner) {
      const otherOwners = await this.users.count({
        where: { role: "owner", isActive: true, id: Not(user.id) },
      });
      if (otherOwners === 0) {
        throw new BadRequestException(
          "This is the last active owner — promote another account first",
        );
      }
    }

    Object.assign(user, {
      ...(patch.displayName !== undefined
        ? { displayName: patch.displayName.trim() }
        : {}),
      ...(patch.role !== undefined ? { role: patch.role } : {}),
      ...(patch.isActive !== undefined ? { isActive: patch.isActive } : {}),
    });
    const saved = await this.users.save(user);

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
  ): Promise<{ reissuedToken: string | null }> {
    const user = await this.users.findOne({
      where: { id: userId },
      select: {
        id: true,
        email: true,
        passwordHash: true,
        role: true,
        displayName: true,
      },
    });
    if (!user) throw new NotFoundException("No such account");

    const ok = await verifyPassword(currentPassword ?? "", user.passwordHash);
    if (!ok) throw new UnauthorizedException("Current password is incorrect");

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
    if (!kept) return { reissuedToken: null };

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
    });
    return { reissuedToken: issued.token };
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
  ): Promise<{ secret: string; uri: string }> {
    const user = await this.users.findOne({ where: { id: userId } });
    if (!user) throw new NotFoundException("No such account");
    if (user.totpEnabled) {
      throw new BadRequestException("Two-factor is already enabled");
    }

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
      throw new UnauthorizedException("Password is incorrect");
    }
    const check = verifyTotp(user.totpSecret ?? "", code, null);
    if (!check.valid) throw new BadRequestException("That code did not match");

    await this.users.update(
      { id: userId },
      { totpEnabled: false, totpSecret: null, totpLastStep: null },
    );
  }
}
