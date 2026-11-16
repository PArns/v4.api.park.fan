import {
  BadRequestException,
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  Post,
  UseGuards,
} from "@nestjs/common";
import { ApiOperation, ApiSecurity, ApiTags } from "@nestjs/swagger";
import { AdminAuthGuard } from "../admin/auth/admin-auth.guard";
import {
  AdminLegacyAccess,
  AdminMinRole,
  CurrentAdmin,
} from "../admin/auth/admin-auth.decorators";
import { AdminAuditService } from "../admin/auth/admin-audit.service";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import { isCanonicalUuid } from "../common/utils/uuid-param.util";
import { ApiKeyRequestService } from "./api-key-request.service";
import {
  ApproveApiKeyRequestDto,
  RejectApiKeyRequestDto,
} from "./dto/api-key-request.dto";

function requestId(id: string): string {
  if (!isCanonicalUuid(id)) throw new BadRequestException("id must be a uuid");
  return id;
}

/**
 * The key requests developers file on their dashboard, for the admin's API
 * keys page. Owner only, like the keys: approving one issues a key, which is
 * a licensing decision. Every request waits here; none issues a key by itself.
 */
@ApiTags("admin")
@ApiSecurity("admin-auth")
@Controller("admin/api-key-requests")
@UseGuards(AdminAuthGuard)
@AdminMinRole("owner")
@AdminLegacyAccess(false)
export class AdminApiKeyRequestsController {
  constructor(
    private readonly requests: ApiKeyRequestService,
    private readonly audit: AdminAuditService,
  ) {}

  @Get()
  @ApiOperation({
    summary: "Every key request, undecided ones first",
    description:
      "Each with `applicant` (the account's address and name), `tier` (`free` or `paid`, from the answers) and, once approved, `keyId`.",
  })
  async list() {
    return { items: await this.requests.listAll() };
  }

  @Post(":id/approve")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Approve a request and issue its key",
    description:
      "Issues a key bound to the applicant's account, with the permissions, allowance, parks and end date given here. Nobody sees its first secret: the developer picks the key up on the dashboard, which gives it a new one. `message` is what the developer reads beside the approval.",
  })
  async approve(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("id") id: string,
    @Body() body: ApproveApiKeyRequestDto,
  ) {
    const { request, key } = await this.requests.approve(
      requestId(id),
      {
        name: body.name,
        permissions: body.permissions,
        usage: body.usage,
        requestsPerHour: body.requestsPerHour,
        allowedParkIds: body.allowedParkIds,
        expiresAt:
          body.expiresAt === undefined || body.expiresAt === null
            ? body.expiresAt
            : new Date(body.expiresAt),
        note: body.note,
        message: body.message,
      },
      { userId: admin.userId ?? null, email: admin.email },
    );
    await this.audit.record({
      actor: admin,
      action: "api-key-request.approve",
      entityType: "api_key_request",
      entityId: request.id,
      entityLabel: request.projectName,
      after: {
        keyId: key.id,
        prefix: key.prefix,
        usage: key.usage,
        permissions: key.permissions,
        requestsPerHour: key.requestsPerHour,
      },
    });
    return { item: request, key };
  }

  @Post(":id/reject")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Turn a request down",
    description: "`reason` is what the developer reads on the dashboard.",
  })
  async reject(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("id") id: string,
    @Body() body: RejectApiKeyRequestDto,
  ) {
    const request = await this.requests.reject(
      requestId(id),
      body.reason,
      admin.email,
    );
    await this.audit.record({
      actor: admin,
      action: "api-key-request.reject",
      entityType: "api_key_request",
      entityId: request.id,
      entityLabel: request.projectName,
      after: { reason: request.decisionNote },
    });
    return { item: request };
  }

  @Post(":id/reopen")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: "Put a rejected request back into the queue" })
  async reopen(@CurrentAdmin() admin: AdminPrincipal, @Param("id") id: string) {
    const request = await this.requests.reopen(requestId(id));
    await this.audit.record({
      actor: admin,
      action: "api-key-request.reopen",
      entityType: "api_key_request",
      entityId: request.id,
      entityLabel: request.projectName,
    });
    return { item: request };
  }

  @Delete(":id")
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({
    summary: "Delete a request for good",
    description: "A key issued for it stays; revoke it on its own.",
  })
  async remove(
    @CurrentAdmin() admin: AdminPrincipal,
    @Param("id") id: string,
  ): Promise<void> {
    const request = await this.requests.remove(requestId(id));
    await this.audit.record({
      actor: admin,
      action: "api-key-request.delete",
      entityType: "api_key_request",
      entityId: request.id,
      entityLabel: request.projectName,
    });
  }
}
