import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  ForbiddenException,
  UnauthorizedException,
} from "@nestjs/common";
import type { Response } from "express";
import { bearerToken } from "../admin/auth/admin-auth.guard";
import type { RequestWithAdmin } from "../admin/auth/admin-principal";
import { getMcpResourceMetadataUrl } from "../config/oauth.config";
import { DEFAULT_ADMIN_TOKEN_SCOPE } from "../admin/auth/admin-api-token.constants";
import { setNoStore } from "../admin/oauth/oauth-error";

/** A quoted-string in a header may not hold a quote or a backslash. */
function quoted(value: string): string {
  return `"${value.replace(/["\\]/g, "'")}"`;
}

/**
 * The MCP server's refusals, in the form MCP clients act on (RFC 6750 §3,
 * RFC 9728 §5.1): a 401 names the protected-resource metadata, which is how
 * Claude Code and claude.ai find the authorization server and start the login.
 */
@Catch(UnauthorizedException, ForbiddenException)
export class McpAuthChallengeFilter implements ExceptionFilter {
  catch(
    exception: UnauthorizedException | ForbiddenException,
    host: ArgumentsHost,
  ): void {
    const http = host.switchToHttp();
    const request = http.getRequest<RequestWithAdmin>();
    const response = http.getResponse<Response>();
    if (response.headersSent) return;

    const params = [
      `resource_metadata=${quoted(getMcpResourceMetadataUrl())}`,
      `scope=${quoted(DEFAULT_ADMIN_TOKEN_SCOPE)}`,
    ];
    let status: number;
    let error: string;
    let description: string;

    if (exception instanceof ForbiddenException) {
      status = 403;
      error = "insufficient_scope";
      description = exception.message;
    } else if (bearerToken(request)) {
      status = 401;
      error = "invalid_token";
      description = "The access token is invalid, expired or revoked";
    } else {
      status = 401;
      error = "invalid_request";
      description = "A bearer token is required";
    }
    // RFC 6750 §3.1: no error code when the request carried no credentials.
    if (error !== "invalid_request") {
      params.push(
        `error=${quoted(error)}`,
        `error_description=${quoted(description)}`,
      );
    }

    response.setHeader("WWW-Authenticate", `Bearer ${params.join(", ")}`);
    setNoStore(response);
    response.status(status).json({ error, error_description: description });
  }
}
