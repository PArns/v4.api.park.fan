import {
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  HttpStatus,
  Post,
  Put,
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
import { AdminAuthService } from "../admin/auth/admin-auth.service";
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
import { DeleteOwnAccountDto, UpdateAccountDataDto } from "./dto/account.dto";

/**
 * A visitor's account on park.fan: registration, the synced favorites, ridden
 * rides and temperature unit, and deleting the account. Behind the admin's
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
  @ApiOperation({ summary: "The caller's account and synced state" })
  async read(@CurrentAdmin() admin: AdminPrincipal) {
    const userId = requireAccount(admin);
    return {
      account: {
        email: admin.email,
        displayName: admin.displayName,
        role: admin.role,
      },
      data: await this.account.read(userId),
    };
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
