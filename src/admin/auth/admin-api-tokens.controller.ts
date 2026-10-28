import {
  BadRequestException,
  Body,
  Controller,
  Delete,
  ForbiddenException,
  Get,
  HttpCode,
  HttpStatus,
  NotFoundException,
  Param,
  Post,
  Query,
  UnauthorizedException,
  UseGuards,
} from "@nestjs/common";
import {
  ApiOperation,
  ApiQuery,
  ApiResponse,
  ApiSecurity,
  ApiTags,
} from "@nestjs/swagger";
import { AdminAuthGuard } from "./admin-auth.guard";
import { AdminLegacyAccess, CurrentAdmin } from "./admin-auth.decorators";
import type { AdminPrincipal } from "./admin-principal";
import { AdminApiTokenStore } from "./admin-api-token.store";
import { AdminAuditService } from "./admin-audit.service";
import { AdminCreateApiTokenDto } from "./dto/auth.dto";
import { roleAtLeast } from "./entities/admin-user.entity";
import { isCanonicalUuid } from "../../common/utils/uuid-param.util";
import { stripInvisibleCharacters } from "../../common/utils/visible-text.util";

/**
 * Personal access tokens and OAuth grants of the signed-in account, for the
 * "API access / MCP" section of the account page.
 *
 * Sessions only: an API token is refused here like on every endpoint but the
 * MCP server, so a token can never mint or extend another; the shared pass is
 * refused like on the rest of account management.
 */
@ApiTags("admin")
@ApiSecurity("admin-auth")
@Controller("admin/auth/tokens")
@UseGuards(AdminAuthGuard)
@AdminLegacyAccess(false)
export class AdminApiTokensController {
  constructor(
    private readonly tokens: AdminApiTokenStore,
    private readonly audit: AdminAuditService,
  ) {}

  @Get()
  @ApiOperation({
    summary: "Your personal access tokens and OAuth grants",
    description:
      "Active ones only. `kind: oauth` rows are grants an MCP client obtained " +
      "through the OAuth flow. An owner may pass `userId` to list another account's.",
  })
  @ApiQuery({ name: "userId", required: false })
  async list(
    @CurrentAdmin() admin: AdminPrincipal,
    @Query("userId") userId?: string,
  ) {
    const self = requireAccount(admin);
    const target = userId ?? self;
    if (!isCanonicalUuid(target)) {
      throw new BadRequestException("userId must be a uuid");
    }
    if (target !== self && !roleAtLeast(admin.role, "owner")) {
      throw new ForbiddenException(
        "Only an owner may list another account's tokens",
      );
    }
    return { items: await this.tokens.list(target) };
  }

  @Post()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: "Create a personal access token",
    description:
      "Read-only (`admin:read`), for the MCP server at /v1/mcp. The token is " +
      "in this response and nowhere else; only its hash is stored.",
  })
  @ApiResponse({ status: 201, description: "`{ token, item }`" })
  async create(
    @CurrentAdmin() admin: AdminPrincipal,
    @Body() body: AdminCreateApiTokenDto,
  ) {
    const userId = requireAccount(admin);
    const name = stripInvisibleCharacters(body.name);
    if (!name) throw new BadRequestException("A token needs a name");

    const created = await this.tokens.createPersonalToken(
      userId,
      name,
      body.expiresInDays ?? null,
    );
    await this.audit.record({
      actor: admin,
      action: "auth.token.create",
      entityType: "admin_api_token",
      entityId: created.item.id,
      entityLabel: name,
      after: {
        kind: "pat",
        scope: created.item.scope,
        prefix: created.item.prefix,
        expiresAt: created.item.expiresAt,
      },
    });
    return created;
  }

  @Delete(":id")
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({
    summary: "Revoke a personal access token or a whole OAuth grant",
    description: "Your own; an owner may revoke anybody's.",
  })
  @ApiResponse({ status: 404, description: "No such active token of yours" })
  async revoke(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("id") id: string,
  ): Promise<void> {
    const userId = requireAccount(admin);
    const revoked =
      (await this.tokens.revoke(id, userId, "user")) ??
      (roleAtLeast(admin.role, "owner")
        ? await this.tokens.revoke(id, null, "owner")
        : null);
    if (!revoked) throw new NotFoundException("No such active token");

    await this.audit.record({
      actor: admin,
      action: "auth.token.revoke",
      entityType: "admin_api_token",
      entityId: revoked.id,
      entityLabel: revoked.name ?? revoked.clientName ?? revoked.prefix,
      before: {
        kind: revoked.kind,
        prefix: revoked.prefix,
        ...(revoked.userId !== userId && { userId: revoked.userId }),
      },
    });
  }
}

/** The account behind a session; the guard has already refused the shared pass. */
function requireAccount(admin: AdminPrincipal): string {
  if (!admin.userId) {
    throw new UnauthorizedException("No account behind this session");
  }
  return admin.userId;
}
