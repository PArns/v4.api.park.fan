import {
  ConflictException,
  ForbiddenException,
  HttpException,
  Injectable,
  Logger,
} from "@nestjs/common";
import { carriesThrottleBypassKey } from "../../common/throttler/throttler.config";
import {
  isAccountRegistrationOpen,
  isAdminLoginTurnstileEnforced,
} from "../../config/admin-auth.config";
import {
  AuthAttemptsService,
  type RegisterOutcome,
} from "./auth-attempts.service";
import { clientIp } from "./admin-auth.guard";
import { AdminAuthService } from "./admin-auth.service";
import type { SelfServiceRole } from "./entities/admin-user.entity";
import { AdminAuditService } from "./admin-audit.service";
import {
  principalFromSession,
  sessionAnswer,
  type RequestWithAdmin,
} from "./admin-principal";
import { AdminTurnstileService } from "./admin-turnstile.service";
import type { RegisterAccountDto } from "./dto/auth.dto";

/** The audit action each self-service role's registration is recorded under. */
const REGISTER_ACTIONS: Record<SelfServiceRole, string> = {
  developer: "developer.register",
  user: "account.register",
};

/**
 * Somebody opening their own account: the Turnstile check, the account and its
 * first session, the audit entry, and the login's answer. One flow for the
 * developer portal and for visitors, so the two cannot drift apart.
 */
@Injectable()
export class SelfRegistrationService {
  private readonly logger = new Logger(SelfRegistrationService.name);

  constructor(
    private readonly auth: AdminAuthService,
    private readonly turnstile: AdminTurnstileService,
    private readonly audit: AdminAuditService,
    private readonly attempts: AuthAttemptsService,
  ) {}

  /**
   * Registers and signs in; the answer has the login's shape. Every attempt is counted by how it
   * ended, a refused one too.
   */
  async register(
    role: SelfServiceRole,
    body: RegisterAccountDto,
    request: RequestWithAdmin,
  ) {
    const kind = role === "user" ? "register-user" : "register-developer";
    try {
      const answer = await this.registerUncounted(role, body, request);
      await this.attempts.count(kind, "created");
      return answer;
    } catch (error) {
      await this.attempts.count(kind, registerOutcome(error));
      throw error;
    }
  }

  private async registerUncounted(
    role: SelfServiceRole,
    body: RegisterAccountDto,
    request: RequestWithAdmin,
  ) {
    if (role === "user" && !isAccountRegistrationOpen()) {
      throw new ForbiddenException({
        error: "registration-closed",
        message: "Visitor accounts cannot be created yet",
      });
    }
    const ip = clientIp(request);
    // As at the login: our own frontend verified the challenge before
    // forwarding and sends the bypass key; anyone else solves it here.
    if (
      isAdminLoginTurnstileEnforced() &&
      !carriesThrottleBypassKey(request.headers)
    ) {
      const verdict = await this.turnstile.verify(
        body.turnstileToken ?? "",
        ip,
      );
      if (!verdict.success) {
        this.logger.warn(
          `Turnstile refused a ${role} registration: ${verdict.reason ?? "no reason given"}`,
        );
        throw new ForbiddenException({
          error: "turnstile-failed",
          message: "A solved Turnstile token is required to register.",
        });
      }
    }
    const userAgent = request.headers?.["user-agent"];
    const issued = await this.auth.registerSelf({
      role,
      email: body.email,
      displayName: body.displayName,
      password: body.password,
      ip,
      userAgent: typeof userAgent === "string" ? userAgent : null,
    });
    const { session } = issued;
    await this.audit.record({
      actor: principalFromSession(session, ip, null),
      action: REGISTER_ACTIONS[role],
      entityType: "admin_user",
      entityId: session.userId,
      entityLabel: session.email,
    });
    return sessionAnswer(issued.token, session);
  }
}

/** How a refused registration ended, from what it threw. */
function registerOutcome(error: unknown): RegisterOutcome {
  if (error instanceof ConflictException) return "exists";
  if (error instanceof HttpException && error.getStatus() === 403) {
    const body = error.getResponse() as { error?: string };
    return body?.error === "registration-closed"
      ? "closed"
      : "turnstile-failed";
  }
  return "failed";
}
