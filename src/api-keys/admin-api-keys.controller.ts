import {
  BadRequestException,
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  HttpStatus,
  NotFoundException,
  Param,
  Patch,
  Post,
  UseGuards,
} from "@nestjs/common";
import {
  ApiOperation,
  ApiResponse,
  ApiSecurity,
  ApiTags,
} from "@nestjs/swagger";
import { AdminAuthGuard } from "../admin/auth/admin-auth.guard";
import {
  AdminLegacyAccess,
  AdminMinRole,
  CurrentAdmin,
} from "../admin/auth/admin-auth.decorators";
import { AdminAuditService } from "../admin/auth/admin-audit.service";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import { isCanonicalUuid } from "../common/utils/uuid-param.util";
import { stripInvisibleCharacters } from "../common/utils/visible-text.util";
import { ApiKeyService } from "./api-key.service";
import { CreateApiKeyDto, UpdateApiKeyDto } from "./dto/create-api-key.dto";

/** An ISO 8601 date from a body, as a Date; null and absent pass through. */
function toDate(value: string | null | undefined): Date | null | undefined {
  return value === null || value === undefined ? value : new Date(value);
}

/**
 * The data keys that unlock the forecast routes, for the admin's "API keys"
 * page. Owner only: a key decides who may use park.fan's forecasts, which is a
 * licensing decision, not an editorial one.
 */
@ApiTags("admin")
@ApiSecurity("admin-auth")
@Controller("admin/api-keys")
@UseGuards(AdminAuthGuard)
@AdminMinRole("owner")
@AdminLegacyAccess(false)
export class AdminApiKeysController {
  constructor(
    private readonly apiKeys: ApiKeyService,
    private readonly audit: AdminAuditService,
  ) {}

  @Get()
  @ApiOperation({
    summary: "Every data API key, active ones first",
    description: "Revoked keys stay in the list with `revokedAt` set.",
  })
  async list() {
    return { items: await this.apiKeys.list() };
  }

  @Post()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: "Issue a data API key",
    description:
      "The key is in this response and nowhere else; only its hash is stored.",
  })
  @ApiResponse({ status: 201, description: "`{ key, item }`" })
  async create(
    @CurrentAdmin() admin: AdminPrincipal,
    @Body() body: CreateApiKeyDto,
  ) {
    const name = stripInvisibleCharacters(body.name);
    if (!name) throw new BadRequestException("A key needs a name");

    const created = await this.apiKeys.create(
      {
        name,
        contact: body.contact ? stripInvisibleCharacters(body.contact) : null,
        usage: body.usage,
        note: body.note ? stripInvisibleCharacters(body.note) : null,
        requestsPerHour: body.requestsPerHour,
        allowedParkIds: body.allowedParkIds,
        expiresAt: toDate(body.expiresAt),
      },
      admin.userId ?? null,
    );
    await this.audit.record({
      actor: admin,
      action: "api-key.create",
      entityType: "api_key",
      entityId: created.item.id,
      entityLabel: name,
      after: {
        usage: created.item.usage,
        prefix: created.item.prefix,
        contact: created.item.contact,
        requestsPerHour: created.item.requestsPerHour,
        allowedParkIds: created.item.allowedParkIds,
        expiresAt: created.item.expiresAt,
      },
    });
    return created;
  }

  @Patch(":id")
  @ApiOperation({
    summary: "Change a data API key's hourly allowance, parks or end date",
    description:
      "`requestsPerHour` per clock hour (null: no per-key limit), `allowedParkIds` (null: every park) and `expiresAt` (null: never; a new date brings an expired key back). A field left out stays as it is. Applies to the next request.",
  })
  @ApiResponse({ status: 404, description: "No such active key" })
  async update(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("id") id: string,
    @Body() body: UpdateApiKeyDto,
  ) {
    if (!isCanonicalUuid(id))
      throw new BadRequestException("id must be a uuid");
    const changed = await this.apiKeys.update(id, {
      requestsPerHour: body.requestsPerHour,
      allowedParkIds: body.allowedParkIds,
      expiresAt: toDate(body.expiresAt),
    });
    if (!changed) throw new NotFoundException("No such active key");
    await this.audit.record({
      actor: admin,
      action: "api-key.update",
      entityType: "api_key",
      entityId: changed.item.id,
      entityLabel: changed.item.name,
      before: { ...changed.before },
      after: { ...changed.after },
    });
    return { item: changed.item };
  }

  @Delete(":id")
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({ summary: "Revoke a data API key" })
  @ApiResponse({ status: 404, description: "No such active key" })
  async revoke(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("id") id: string,
  ): Promise<void> {
    if (!isCanonicalUuid(id))
      throw new BadRequestException("id must be a uuid");
    const revoked = await this.apiKeys.revoke(id);
    if (!revoked) throw new NotFoundException("No such active key");
    await this.audit.record({
      actor: admin,
      action: "api-key.revoke",
      entityType: "api_key",
      entityId: revoked.id,
      entityLabel: revoked.name,
      before: { usage: revoked.usage, prefix: revoked.prefix },
    });
  }
}
