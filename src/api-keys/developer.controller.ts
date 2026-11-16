import {
  BadRequestException,
  Body,
  Controller,
  Delete,
  ForbiddenException,
  Get,
  HttpCode,
  HttpStatus,
  Logger,
  NotFoundException,
  Param,
  Post,
  Req,
  UseGuards,
  UseInterceptors,
} from "@nestjs/common";
import {
  ApiOperation,
  ApiResponse,
  ApiSecurity,
  ApiTags,
} from "@nestjs/swagger";
import { Throttle } from "@nestjs/throttler";
import { AdminAuthGuard, clientIp } from "../admin/auth/admin-auth.guard";
import {
  AdminLegacyAccess,
  AdminMinRole,
  AdminPublic,
  CurrentAdmin,
} from "../admin/auth/admin-auth.decorators";
import { AdminAuthService } from "../admin/auth/admin-auth.service";
import { AdminAuditService } from "../admin/auth/admin-audit.service";
import type {
  AdminPrincipal,
  RequestWithAdmin,
} from "../admin/auth/admin-principal";
import { requireAccount } from "../admin/auth/admin-principal";
import { AdminTurnstileService } from "../admin/auth/admin-turnstile.service";
import { carriesThrottleBypassKey } from "../common/throttler/throttler.config";
import { NoCdnCacheInterceptor } from "../common/interceptors/no-cdn-cache.interceptor";
import { isCanonicalUuid } from "../common/utils/uuid-param.util";
import { isAdminLoginTurnstileEnforced } from "../config/admin-auth.config";
import { ApiKeyRequestService } from "./api-key-request.service";
import { ApiKeyService } from "./api-key.service";
import { RotateApiKeyDto } from "./dto/create-api-key.dto";
import {
  CreateApiKeyRequestDto,
  RegisterDeveloperDto,
} from "./dto/api-key-request.dto";

/** A key id from a path, checked before it reaches a query. */
function keyId(id: string): string {
  if (!isCanonicalUuid(id)) throw new BadRequestException("id must be a uuid");
  return id;
}

/**
 * The developer dashboard's API: an outside developer registers, files key
 * requests, and sees, picks up, rotates and revokes the keys issued to their
 * account, with each key's statistics. Behind the admin's session guard at
 * `developer`, the role below `viewer`: staff reach it too, but every
 * handler shows only the caller's own keys and requests, so nothing here
 * reveals anyone else's.
 */
@ApiTags("developer")
@ApiSecurity("admin-auth")
@Controller("developer")
@UseGuards(AdminAuthGuard)
@AdminMinRole("developer")
@AdminLegacyAccess(false)
// Every answer is one account's own: never a copy a cache may hand anyone else.
@UseInterceptors(new NoCdnCacheInterceptor())
export class DeveloperController {
  private readonly logger = new Logger(DeveloperController.name);

  constructor(
    private readonly auth: AdminAuthService,
    private readonly turnstile: AdminTurnstileService,
    private readonly audit: AdminAuditService,
    private readonly apiKeys: ApiKeyService,
    private readonly requests: ApiKeyRequestService,
  ) {}

  @Post("register")
  @AdminPublic()
  @HttpCode(HttpStatus.CREATED)
  // Fewer than the login: an account is made once, and each one is a row.
  @Throttle({ default: { limit: 5, ttl: 60_000 } })
  @ApiOperation({
    summary: "Register a developer account",
    description:
      "Creates an account with the role `developer` and signs it in: the answer is the login's (`token`, `expiresAt`, `user`). The account reaches its own session endpoints and `/v1/developer/*`, nothing of the admin. 409 `account-exists` for a taken address.",
  })
  @ApiResponse({ status: 201, description: "Registered and signed in" })
  @ApiResponse({
    status: 403,
    description: "The Turnstile challenge was not solved",
  })
  @ApiResponse({
    status: 409,
    description: "That address already has an account",
  })
  async register(
    @Body() body: RegisterDeveloperDto,
    @Req() request: RequestWithAdmin,
  ) {
    // As at the login: our own frontend verified the challenge before
    // forwarding and sends the bypass key; anyone else solves it here.
    if (
      isAdminLoginTurnstileEnforced() &&
      !carriesThrottleBypassKey(request.headers)
    ) {
      const verdict = await this.turnstile.verify(
        body.turnstileToken ?? "",
        clientIp(request),
      );
      if (!verdict.success) {
        this.logger.warn(
          `Turnstile refused a developer registration: ${verdict.reason ?? "no reason given"}`,
        );
        throw new ForbiddenException({
          error: "turnstile-failed",
          message: "A solved Turnstile token is required to register.",
        });
      }
    }
    const userAgent = request.headers?.["user-agent"];
    const issued = await this.auth.registerDeveloper({
      email: body.email,
      displayName: body.displayName,
      password: body.password,
      ip: clientIp(request),
      userAgent: typeof userAgent === "string" ? userAgent : null,
    });
    const { session } = issued;
    await this.audit.record({
      actor: {
        userId: session.userId,
        email: session.email,
        displayName: session.displayName,
        role: session.role,
        sessionToken: null,
        legacy: false,
        ip: clientIp(request),
        mustChangePassword: false,
        mustEnrolTotp: false,
      },
      action: "developer.register",
      entityType: "admin_user",
      entityId: session.userId,
      entityLabel: session.email,
    });
    return {
      status: "ok" as const,
      token: issued.token,
      expiresAt: new Date(session.expiresAt).toISOString(),
      user: {
        id: session.userId,
        email: session.email,
        displayName: session.displayName,
        role: session.role,
        mustChangePassword: false,
        mustEnrolTotp: false,
      },
    };
  }

  @Get("overview")
  @ApiOperation({
    summary: "The caller's keys and requests",
    description:
      "Every key issued to the caller's account, with this hour's count and the use per day (30) and month (12), and every request it filed, newest first. A key with `revealedAt: null` has not been picked up: `POST keys/:id/rotate` with `graceHours: 0` shows its first secret.",
  })
  async overview(@CurrentAdmin() admin: AdminPrincipal) {
    const userId = requireAccount(admin);
    const [keys, requests] = await Promise.all([
      this.apiKeys.list(Date.now(), { ownerUserId: userId }),
      this.requests.listForUser(userId),
    ]);
    return {
      account: {
        email: admin.email,
        displayName: admin.displayName,
        role: admin.role,
      },
      keys,
      requests,
    };
  }

  @Post("requests")
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: "File a key request",
    description:
      "Undecided until the owner decides; every request is decided by hand. `tier` says which key the terms allow: `free` for open source that earns nothing but donations, `paid` otherwise. At most three can wait at once (409 `too-many-pending`).",
  })
  async createRequest(
    @CurrentAdmin() admin: AdminPrincipal,
    @Body() body: CreateApiKeyRequestDto,
  ) {
    const userId = requireAccount(admin);
    const created = await this.requests.create(userId, body);
    await this.audit.record({
      actor: admin,
      action: "api-key-request.create",
      entityType: "api_key_request",
      entityId: created.id,
      entityLabel: created.projectName,
      after: {
        tier: created.tier,
        permissions: created.permissions,
        revenue: created.revenue,
        source: created.source,
      },
    });
    return { item: created };
  }

  @Delete("requests/:id")
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({ summary: "Withdraw an undecided request of the caller's" })
  async withdrawRequest(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("id") id: string,
  ): Promise<void> {
    const userId = requireAccount(admin);
    const withdrawn = await this.requests.withdraw(keyId(id), userId);
    await this.audit.record({
      actor: admin,
      action: "api-key-request.withdraw",
      entityType: "api_key_request",
      entityId: withdrawn.id,
      entityLabel: withdrawn.projectName,
    });
  }

  @Post("keys/:id/rotate")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Pick up or rotate a key of the caller's",
    description:
      "Gives the key a new secret and answers it this once, as the admin's rotation does. The first pickup of an approved key is a rotation with `graceHours: 0`, since nobody has seen its first secret.",
  })
  async rotateKey(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("id") id: string,
    @Body() body: RotateApiKeyDto,
  ) {
    const owned = await this.ownedKey(admin, id);
    const rotated = await this.apiKeys.rotate(owned.id, body.graceHours);
    if (!rotated) throw new NotFoundException("No such key");
    await this.audit.record({
      actor: admin,
      action: owned.revealedAt ? "api-key.rotate" : "api-key.pickup",
      entityType: "api_key",
      entityId: rotated.item.id,
      entityLabel: rotated.item.name,
      before: { prefix: rotated.previousPrefix },
      after: { prefix: rotated.item.prefix, graceHours: body.graceHours },
    });
    return { key: rotated.key, item: rotated.item };
  }

  @Delete("keys/:id/previous")
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({
    summary: "End the grace period of a key of the caller's now",
  })
  async endGrace(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("id") id: string,
  ): Promise<void> {
    const owned = await this.ownedKey(admin, id);
    const ended = await this.apiKeys.revokePrevious(owned.id);
    if (!ended) {
      throw new NotFoundException("No grace period running on this key");
    }
    await this.audit.record({
      actor: admin,
      action: "api-key.previous.revoke",
      entityType: "api_key",
      entityId: ended.item.id,
      entityLabel: ended.item.name,
    });
  }

  @Delete("keys/:id")
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({
    summary: "Revoke a key of the caller's",
    description:
      "For good: a revoked key never works again. A new key is a new request.",
  })
  async revokeKey(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("id") id: string,
  ): Promise<void> {
    const owned = await this.ownedKey(admin, id);
    const revoked = await this.apiKeys.revoke(owned.id);
    if (!revoked) throw new NotFoundException("No such active key");
    await this.audit.record({
      actor: admin,
      action: "api-key.revoke",
      entityType: "api_key",
      entityId: revoked.id,
      entityLabel: revoked.name,
    });
  }

  /** A key of the caller's account; 404 for anyone else's, so ids reveal nothing. */
  private async ownedKey(admin: AdminPrincipal, id: string) {
    const owned = await this.apiKeys.findOwned(
      keyId(id),
      requireAccount(admin),
    );
    if (!owned) throw new NotFoundException("No such key");
    return owned;
  }
}
