import {
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  Patch,
  Post,
  Put,
  Req,
  Res,
  UnauthorizedException,
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
import {
  AdminAuthService,
  toPublicUser,
  type PublicAdminUser,
} from "../admin/auth/admin-auth.service";
import { AdminAuditService } from "../admin/auth/admin-audit.service";
import {
  requireAccount,
  type AdminPrincipal,
  type RequestWithAdmin,
} from "../admin/auth/admin-principal";
import { RegisterAccountDto } from "../admin/auth/dto/auth.dto";
import { SelfRegistrationService } from "../admin/auth/self-registration.service";
import { NoCdnCacheInterceptor } from "../common/interceptors/no-cdn-cache.interceptor";
import { AccountService } from "./account.service";
import type { Response } from "express";
import {
  DeleteOwnAccountDto,
  UpdateAccountProfileDto,
  PutAccountStorageDto,
  UpdateAccountDataDto,
} from "./dto/account.dto";

/** An account as its own page shows it: no hashes, no lockout state. */
function accountView(user: PublicAdminUser) {
  return {
    email: user.email,
    displayName: user.displayName,
    role: user.role,
    totpEnabled: user.totpEnabled,
    createdAt: user.createdAt,
    lastLoginAt: user.lastLoginAt,
  };
}

/**
 * A visitor's account on park.fan: registration, the synced favorites, ridden
 * rides and temperature unit, a member's own storage for members-only
 * features, and deleting the account. Behind the admin's
 * session guard at `user`, the lowest role: every account reaches it, and each
 * handler reads and writes only the caller's own row.
 */
@ApiTags("account")
@ApiSecurity("admin-auth")
@Controller("account")
@UseGuards(AdminAuthGuard)
@AdminMinRole("user")
@AdminLegacyAccess(false)
// Every answer is one account's own: never a copy a cache may hand anyone else.
@UseInterceptors(new NoCdnCacheInterceptor())
export class AccountController {
  constructor(
    private readonly registration: SelfRegistrationService,
    private readonly auth: AdminAuthService,
    private readonly audit: AdminAuditService,
    private readonly account: AccountService,
  ) {}

  @Post("register")
  @AdminPublic()
  @HttpCode(HttpStatus.CREATED)
  // Fewer than the login: an account is made once, and each one is a row.
  @Throttle({ default: { limit: 5, ttl: 60_000 } })
  @ApiOperation({
    summary: "Register a visitor account",
    description:
      "Creates an account with the role `user` and signs it in: the answer is the login's (`token`, `expiresAt`, `user`). It reaches its own session endpoints and `/v1/account/*`, nothing else. 409 `account-exists` for a taken address.",
  })
  @ApiResponse({ status: 201, description: "Registered and signed in" })
  @ApiResponse({ status: 403, description: "Turnstile not solved" })
  @ApiResponse({ status: 409, description: "That address has an account" })
  register(@Body() body: RegisterAccountDto, @Req() request: RequestWithAdmin) {
    return this.registration.register("user", body, request);
  }

  @Get()
  @ApiOperation({
    summary: "The caller's account and synced state",
    description:
      "The account (address, name, role, second factor, when it was made and last signed in) and its synced favorites, ridden rides and temperature unit.",
  })
  async read(@CurrentAdmin() admin: AdminPrincipal) {
    const userId = requireAccount(admin);
    const [account, data] = await Promise.all([
      this.ownAccount(userId),
      this.account.read(userId),
    ]);
    return { account, data };
  }

  @Patch("profile")
  @ApiOperation({ summary: "Rename one's own account" })
  async updateProfile(
    @CurrentAdmin() admin: AdminPrincipal,
    @Body() body: UpdateAccountProfileDto,
  ) {
    const user = await this.auth.updateOwnProfile(requireAccount(admin), {
      displayName: body.displayName,
    });
    return { account: accountView(user) };
  }

  @Get("export")
  @ApiOperation({
    summary: "Everything stored about the caller, as one JSON file",
    description:
      "The account, its synced state and every stored value, as a download: what Art. 20 GDPR calls the data in a portable format.",
  })
  async export(
    @CurrentAdmin() admin: AdminPrincipal,
    @Res({ passthrough: true }) response: Response,
  ) {
    const userId = requireAccount(admin);
    const [account, data, storage] = await Promise.all([
      this.ownAccount(userId),
      this.account.read(userId),
      this.account.exportStorage(userId),
    ]);
    response.setHeader(
      "Content-Disposition",
      'attachment; filename="park-fan-account.json"',
    );
    return {
      exportedAt: new Date().toISOString(),
      account,
      data,
      storage,
    };
  }

  /** The caller's account as the account page shows it. */
  private async ownAccount(userId: string) {
    const user = await this.auth.findById(userId);
    if (!user) throw new UnauthorizedException("Session no longer valid");
    return accountView(toPublicUser(user));
  }

  @Put("data")
  @ApiOperation({
    summary: "Write the caller's synced state",
    description:
      "Replaces each field the body names (favorites by kind, ridden rides, temperature unit) and keeps the others. The browser merges before it writes.",
  })
  async write(
    @CurrentAdmin() admin: AdminPrincipal,
    @Body() body: UpdateAccountDataDto,
  ) {
    return { data: await this.account.write(requireAccount(admin), body) };
  }

  @Get("storage")
  @ApiOperation({
    summary: "The keys in the caller's storage",
    description:
      "Every key a members-only feature stored for this account, newest first, with its size in bytes; not the values.",
  })
  async listStorage(@CurrentAdmin() admin: AdminPrincipal) {
    return { items: await this.account.listStorage(requireAccount(admin)) };
  }

  @Get("storage/:key")
  @ApiOperation({ summary: "One value of the caller's storage" })
  @ApiResponse({ status: 404, description: "Nothing stored under this key" })
  readStorage(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("key") key: string,
  ) {
    return this.account.readStorage(requireAccount(admin), key);
  }

  @Put("storage/:key")
  @ApiOperation({
    summary: "Store a value in the caller's storage",
    description:
      "Replaces the value under the key. A key is 1 to 64 of `a-z0-9._-`; a value is any JSON up to 64 KB; an account keeps at most 100 keys (400 `too-many-keys`, 413 `value-too-large`).",
  })
  writeStorage(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("key") key: string,
    @Body() body: PutAccountStorageDto,
  ) {
    return this.account.writeStorage(requireAccount(admin), key, body.value);
  }

  @Delete("storage/:key")
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({ summary: "Remove a key from the caller's storage" })
  async deleteStorage(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("key") key: string,
  ): Promise<void> {
    await this.account.deleteStorage(requireAccount(admin), key);
  }

  @Delete()
  @ApiOperation({
    summary: "Delete one's own account",
    description:
      "For a visitor or developer account, confirmed with its password: removes the account with its sessions, synced state, and for a developer its keys and requests. 401 `wrong-password`; a staff account is deleted by an owner.",
  })
  async remove(
    @CurrentAdmin() admin: AdminPrincipal,
    @Body() body: DeleteOwnAccountDto,
  ) {
    const { user, erased } = await this.auth.removeOwn(
      requireAccount(admin),
      body.password,
    );
    await this.audit.record({
      actor: admin,
      action: "account.delete",
      entityType: "admin_user",
      entityId: user.id,
      entityLabel: user.email,
      before: { email: user.email, role: user.role },
      after: erased,
    });
    return { erased };
  }
}
