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
import {
  CreateApiKeyDto,
  RotateApiKeyDto,
  UpdateApiKeyDto,
} from "./dto/create-api-key.dto";

/** An ISO 8601 date from a body, as a Date; null and absent pass through. */
function toDate(value: string | null | undefined): Date | null | undefined {
  return value === null || value === undefined ? value : new Date(value);
}

/**
 * The data keys that unlock the forecast routes, for the admin's "API keys"
 * page. Owner only, rotation included: a key decides who may use park.fan's
 * forecasts, which is a licensing decision, not an editorial one.
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
    description:
      "Revoked keys stay in the list with `revokedAt` set until `POST /:id/delete` removes them. Each key carries `requestsThisHour` and `stats`: its requests per UTC day for the last 30 days (with `limited`, those its hourly allowance refused) and per UTC month for the last 12. `rotatedAt` is the last rotation; `previousExpiresAt` is when the secret it replaced stops working (null without one, past once its grace has run out), `previousLastUsedAt` when that secret was last presented. `lastUsedAt` is the current secret's. No hash is ever in the answer.",
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

  @Post(":id/rotate")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Give a data API key a new secret",
    description:
      "The new key is in this response and nowhere else, as on create; name, usage, allowance, parks, end date and statistics stay. With `graceHours` above 0 the old secret keeps working that long beside the new one (`item.previousExpiresAt`); with 0 it is refused from the next request on. A rotation during a grace period replaces the secret that grace kept, which stops at once.",
  })
  @ApiResponse({ status: 200, description: "`{ key, item }`" })
  @ApiResponse({ status: 404, description: "No such key" })
  @ApiResponse({
    status: 409,
    description: "The key is revoked or past its end date",
  })
  async rotate(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("id") id: string,
    @Body() body: RotateApiKeyDto,
  ) {
    if (!isCanonicalUuid(id))
      throw new BadRequestException("id must be a uuid");
    const rotated = await this.apiKeys.rotate(id, body.graceHours);
    if (!rotated) throw new NotFoundException("No such key");
    await this.audit.record({
      actor: admin,
      action: "api-key.rotate",
      entityType: "api_key",
      entityId: rotated.item.id,
      entityLabel: rotated.item.name,
      before: { prefix: rotated.previousPrefix },
      after: {
        prefix: rotated.item.prefix,
        graceHours: body.graceHours,
        previousExpiresAt: rotated.item.previousExpiresAt,
      },
    });
    return { key: rotated.key, item: rotated.item };
  }

  @Delete(":id/previous")
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({
    summary: "End a rotation's grace period now",
    description:
      "The secret the last rotation replaced is refused from the next request on; the current one is untouched.",
  })
  @ApiResponse({
    status: 404,
    description: "No such active key, or no grace period running",
  })
  async revokePrevious(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("id") id: string,
  ): Promise<void> {
    if (!isCanonicalUuid(id))
      throw new BadRequestException("id must be a uuid");
    const ended = await this.apiKeys.revokePrevious(id);
    if (!ended) {
      throw new NotFoundException(
        "No such active key, or no grace period running",
      );
    }
    await this.audit.record({
      actor: admin,
      action: "api-key.previous.revoke",
      entityType: "api_key",
      entityId: ended.item.id,
      entityLabel: ended.item.name,
      before: { previousExpiresAt: ended.previousExpiresAt },
      after: { previousExpiresAt: null },
    });
  }

  @Post(":id/delete")
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({
    summary: "Delete a revoked data API key for good",
    description:
      "The key leaves the list, and its request statistics go with it; the audit log keeps its history. Only a revoked key: an active one is revoked first (`DELETE /:id`), so its holder is refused before anything is erased.",
  })
  @ApiResponse({ status: 404, description: "No such key" })
  @ApiResponse({ status: 409, description: "The key is not revoked" })
  async remove(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("id") id: string,
  ): Promise<void> {
    if (!isCanonicalUuid(id))
      throw new BadRequestException("id must be a uuid");
    const deleted = await this.apiKeys.deleteRevoked(id);
    if (!deleted) throw new NotFoundException("No such key");
    await this.audit.record({
      actor: admin,
      action: "api-key.delete",
      entityType: "api_key",
      entityId: deleted.id,
      entityLabel: deleted.name,
      before: {
        usage: deleted.usage,
        prefix: deleted.prefix,
        createdAt: deleted.createdAt,
        revokedAt: deleted.revokedAt,
      },
    });
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
