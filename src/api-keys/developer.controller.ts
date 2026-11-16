import {
  Body,
  Controller,
  Delete,
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
import { AdminAuthGuard } from "../admin/auth/admin-auth.guard";
import {
  AdminLegacyAccess,
  AdminMinRole,
  AdminPublic,
  CurrentAdmin,
} from "../admin/auth/admin-auth.decorators";
import { AdminAuditService } from "../admin/auth/admin-audit.service";
import type {
  AdminPrincipal,
  RequestWithAdmin,
} from "../admin/auth/admin-principal";
import { requireAccount } from "../admin/auth/admin-principal";
import { NoCdnCacheInterceptor } from "../common/interceptors/no-cdn-cache.interceptor";
import { CanonicalUuidPipe } from "../common/utils/uuid-param.util";
import { ApiKeyRequestService } from "./api-key-request.service";
import { ApiKeyService } from "./api-key.service";
import { RotateApiKeyDto } from "./dto/create-api-key.dto";
import { CreateApiKeyRequestDto } from "./dto/api-key-request.dto";
import { RegisterAccountDto } from "../admin/auth/dto/auth.dto";
import { AdminAuthService } from "../admin/auth/admin-auth.service";
import { SelfRegistrationService } from "../admin/auth/self-registration.service";

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
    private readonly registration: SelfRegistrationService,
    private readonly audit: AdminAuditService,
    private readonly apiKeys: ApiKeyService,
    private readonly requests: ApiKeyRequestService,
    private readonly auth: AdminAuthService,
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
  register(@Body() body: RegisterAccountDto, @Req() request: RequestWithAdmin) {
    return this.registration.register("developer", body, request);
  }

  @Get("overview")
  @ApiOperation({
    summary: "The caller's keys and requests",
    description:
      "Every key issued to the caller's account, with this hour's count, whether the account has a second factor (`account.totpEnabled`) and the use per day (30) and month (12), and every request it filed, newest first. A key with `revealedAt: null` has not been picked up: `POST keys/:id/rotate` with `graceHours: 0` shows its first secret.",
  })
  async overview(@CurrentAdmin() admin: AdminPrincipal) {
    const userId = requireAccount(admin);
    const [keys, requests, user] = await Promise.all([
      this.apiKeys.list(Date.now(), { ownerUserId: userId }),
      this.requests.listForUser(userId),
      this.auth.findById(userId),
    ]);
    return {
      account: {
        email: admin.email,
        displayName: admin.displayName,
        role: admin.role,
        totpEnabled: user?.totpEnabled ?? false,
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
    @Param("id", CanonicalUuidPipe) id: string,
  ): Promise<void> {
    const userId = requireAccount(admin);
    const withdrawn = await this.requests.withdraw(id, userId);
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
    @Param("id", CanonicalUuidPipe) id: string,
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
    @Param("id", CanonicalUuidPipe) id: string,
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
    @Param("id", CanonicalUuidPipe) id: string,
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
    const owned = await this.apiKeys.findOwned(id, requireAccount(admin));
    if (!owned) throw new NotFoundException("No such key");
    return owned;
  }
}
