import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  NotFoundException,
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
import { Throttle } from "@nestjs/throttler";
import { RATE_LIMITED_RESPONSE } from "../../common/filters/rate-limited-response";
import { AdminAuthService } from "../auth/admin-auth.service";
import { AdminOAuthApproveDto } from "../auth/dto/auth.dto";
import { AdminAuthGuard } from "../auth/admin-auth.guard";
import { AdminLegacyAccess, CurrentAdmin } from "../auth/admin-auth.decorators";
import type { AdminPrincipal } from "../auth/admin-principal";
import { GeoipService } from "../../geoip/geoip.service";
import { OAuthAuthorizationService } from "./oauth-authorization.service";
import { requestOrigin } from "./request-origin";

/** The identity-check bucket of consent approvals. */
const OAUTH_APPROVE_ACTION = "oauth-approve";

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
  constructor(
    private readonly authorization: OAuthAuthorizationService,
    private readonly accounts: AdminAuthService,
    private readonly geoip: GeoipService,
  ) {}

  @Get(":id")
  @ApiOperation({
    summary: "A pending OAuth authorization request, for the consent page",
  })
  @ApiResponse({ status: 404, description: "Expired or already answered" })
  describe(@Param("id") id: string, @CurrentAdmin() admin: AdminPrincipal) {
    // The page is read through the admin proxy, whose User-Agent is the
    // frontend's own; the browser is the one the session signed in with.
    return this.authorization.describe(
      id,
      requestOrigin(this.geoip, admin.ip, admin.sessionUserAgent),
    );
  }

  @Post(":id/approve")
  @HttpCode(HttpStatus.OK)
  // Per address, on top of the per-account limiter in `confirmIdentity`.
  @Throttle({ default: { limit: 10, ttl: 60_000 } })
  @ApiOperation({
    summary: "Approve: returns the client redirect carrying a single-use code",
    description:
      "Asks for the account password again, and for a current two-factor " +
      "code when the account has two-factor on: a stolen session must not " +
      "mint a 30-day refreshable grant. A wrong proof leaves the request " +
      "pending.",
  })
  @ApiResponse({
    status: 401,
    description:
      "`error`: `invalid-password`, `totp-required` or `invalid-totp`",
  })
  @ApiResponse(RATE_LIMITED_RESPONSE)
  async approve(
    @Param("id") id: string,
    @CurrentAdmin() admin: AdminPrincipal,
    @Body() body: AdminOAuthApproveDto,
  ) {
    if (!admin.userId) throw new NotFoundException("No such pending request");
    await this.accounts.confirmIdentity(
      admin.userId,
      body.password,
      body.totpCode,
      OAUTH_APPROVE_ACTION,
    );
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
