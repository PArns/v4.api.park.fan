import {
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  Post,
  UseGuards,
} from "@nestjs/common";
import {
  ApiOperation,
  ApiResponse,
  ApiSecurity,
  ApiTags,
} from "@nestjs/swagger";
import { AdminAuthGuard } from "../auth/admin-auth.guard";
import { AdminLegacyAccess, CurrentAdmin } from "../auth/admin-auth.decorators";
import type { AdminPrincipal } from "../auth/admin-principal";
import { OAuthAuthorizationService } from "./oauth-authorization.service";

/**
 * What park.fan's consent page calls, through the admin proxy with the
 * signed-in session. Any role may approve: a grant is read-only whatever the
 * account could do, and never more than the account itself.
 *
 * The shared pass is refused: a grant is a credential bound to a person, and
 * the pass has none. API tokens are refused like everywhere but `/v1/mcp`,
 * so a token cannot approve its own successor.
 */
@ApiTags("admin")
@ApiSecurity("admin-auth")
@Controller("admin/oauth/requests")
@UseGuards(AdminAuthGuard)
@AdminLegacyAccess(false)
export class AdminOAuthConsentController {
  constructor(private readonly authorization: OAuthAuthorizationService) {}

  @Get(":id")
  @ApiOperation({
    summary: "A pending OAuth authorization request, for the consent page",
  })
  @ApiResponse({ status: 404, description: "Expired or already answered" })
  describe(@Param("id") id: string) {
    return this.authorization.describe(id);
  }

  @Post(":id/approve")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Approve: returns the client redirect carrying a single-use code",
  })
  approve(@Param("id") id: string, @CurrentAdmin() admin: AdminPrincipal) {
    return this.authorization.approve(id, admin);
  }

  @Post(":id/deny")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Deny: returns the client redirect carrying access_denied",
  })
  deny(@Param("id") id: string) {
    return this.authorization.deny(id);
  }
}
