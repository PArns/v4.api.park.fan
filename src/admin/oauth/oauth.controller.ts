import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Post,
  Query,
  Req,
  Res,
  UseFilters,
} from "@nestjs/common";
import { ApiExcludeEndpoint, ApiOperation, ApiTags } from "@nestjs/swagger";
import { Throttle } from "@nestjs/throttler";
import type { Request, Response } from "express";
import { addressBucket, getRateLimitIp } from "../../common/utils/request.util";
import { OAuthAuthorizationService } from "./oauth-authorization.service";
import { OAuthClientService } from "./oauth-client.service";
import { OAuthError, OAuthErrorFilter, setNoStore } from "./oauth-error";
import { sendAuthorizeErrorPage } from "./oauth-error-page";

type Params = Record<string, unknown>;

/**
 * The public half of the OAuth 2.1 authorization server: what an MCP client
 * calls on its own. The admin's half, the consent, is
 * `AdminOAuthConsentController`. Public clients with PKCE only; there are no
 * client secrets anywhere.
 *
 * The `@Throttle()` limits bind here, unlike on most writes: no request on
 * these routes comes through the frontend with the bypass key.
 */
@ApiTags("oauth")
@Controller("oauth")
@UseFilters(OAuthErrorFilter)
export class OAuthController {
  constructor(
    private readonly authorization: OAuthAuthorizationService,
    private readonly clients: OAuthClientService,
  ) {}

  @Post("register")
  @HttpCode(HttpStatus.CREATED)
  @Throttle({ default: { limit: 10, ttl: 60_000 } })
  @ApiOperation({
    summary: "Dynamic client registration (RFC 7591), public clients",
    description:
      "`redirect_uris` must be https on an allowed host (OAUTH_REDIRECT_HOSTS) or http on " +
      "127.0.0.1, localhost or [::1]. `token_endpoint_auth_method` is always `none`.",
  })
  register(
    @Body() body: Params,
    @Req() request: Request,
    @Res({ passthrough: true }) response: Response,
  ) {
    setNoStore(response);
    return this.clients.register(body, addressBucket(getRateLimitIp(request)));
  }

  @Get("authorize")
  @Throttle({ default: { limit: 30, ttl: 60_000 } })
  @ApiOperation({
    summary:
      "Authorization endpoint: validates, then sends the browser to park.fan's consent page",
    description:
      "`response_type=code`, `client_id`, `redirect_uri`, `code_challenge` with " +
      "`code_challenge_method=S256`, optional `state`, `scope` and `resource`. An unknown " +
      "client or redirect URI shows an error page instead of redirecting.",
  })
  async authorize(
    @Query() query: Params,
    @Res() response: Response,
  ): Promise<void> {
    const outcome = await this.authorization.authorize(query);
    if (outcome.kind === "error-page") {
      sendAuthorizeErrorPage(response, outcome.message);
      return;
    }
    response.setHeader("Cache-Control", "no-store");
    response.redirect(302, outcome.location);
  }

  @Post("token")
  @HttpCode(HttpStatus.OK)
  @Throttle({ default: { limit: 60, ttl: 60_000 } })
  @ApiOperation({
    summary:
      "Token endpoint: authorization_code (PKCE S256) and refresh_token (rotating)",
    description:
      "Form-encoded. Access tokens last one hour, refresh tokens 30 days; every refresh " +
      "issues a new refresh token, and presenting a used one revokes the grant.",
  })
  async token(
    @Body() body: Params,
    @Res({ passthrough: true }) response: Response,
  ) {
    setNoStore(response);
    const grantType = body?.grant_type;
    if (grantType === "authorization_code") {
      return this.authorization.exchangeCode(body);
    }
    if (grantType === "refresh_token") {
      return this.authorization.refresh(body);
    }
    throw new OAuthError(
      "unsupported_grant_type",
      "grant_type must be authorization_code or refresh_token",
    );
  }

  @Post("revoke")
  @HttpCode(HttpStatus.OK)
  @Throttle({ default: { limit: 30, ttl: 60_000 } })
  @ApiOperation({
    summary: "Token revocation (RFC 7009): revokes the whole grant",
  })
  async revoke(
    @Body() body: Params,
    @Res({ passthrough: true }) response: Response,
  ): Promise<void> {
    setNoStore(response);
    await this.authorization.revoke(body);
  }

  /** Some clients probe the registration endpoint with GET; say what it takes. */
  @Get("register")
  @ApiExcludeEndpoint()
  registrationHint(): never {
    throw new OAuthError(
      "invalid_request",
      "Register with POST and a JSON body (RFC 7591)",
      405,
    );
  }
}
