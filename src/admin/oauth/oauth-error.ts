import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  Logger,
} from "@nestjs/common";
import type { Response } from "express";

/** The RFC 6749 §5.2 / RFC 7591 §3.2.2 error codes this server answers with. */
export type OAuthErrorCode =
  | "invalid_request"
  | "invalid_client"
  | "invalid_grant"
  | "unauthorized_client"
  | "unsupported_grant_type"
  | "invalid_scope"
  | "invalid_target"
  | "invalid_redirect_uri"
  | "invalid_client_metadata"
  | "access_denied"
  | "unsupported_response_type"
  | "temporarily_unavailable";

/**
 * An error a token, registration or revocation endpoint answers in the OAuth
 * shape, `{ error, error_description }`, rather than in this API's usual
 * envelope: OAuth clients parse that shape and nothing else.
 */
export class OAuthError extends Error {
  constructor(
    readonly code: OAuthErrorCode,
    readonly description: string,
    readonly status = 400,
    readonly retryAfterSeconds?: number,
  ) {
    super(description);
  }
}

/** The headers every OAuth answer carries (RFC 6749 §5.1): never cached. */
export function setNoStore(response: Response): void {
  response.setHeader("Cache-Control", "no-store");
  response.setHeader("Pragma", "no-cache");
}

/**
 * Renders `OAuthError` in the OAuth shape. Anything else that escapes an
 * OAuth handler (a validation 400, the throttler's 429, a crash) is rendered
 * in the same shape too, because the client on the other side cannot read
 * any other.
 */
@Catch()
export class OAuthErrorFilter implements ExceptionFilter {
  private readonly logger = new Logger(OAuthErrorFilter.name);

  catch(exception: unknown, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse<Response>();
    if (response.headersSent) return;

    let status = 500;
    let body: { error: string; error_description: string } = {
      error: "server_error",
      error_description: "The server could not complete the request",
    };

    if (exception instanceof OAuthError) {
      status = exception.status;
      body = {
        error: exception.code,
        error_description: exception.description,
      };
      if (exception.retryAfterSeconds) {
        response.setHeader("Retry-After", String(exception.retryAfterSeconds));
      }
    } else if (exception instanceof HttpException) {
      status = exception.getStatus();
      body = {
        error: status === 429 ? "temporarily_unavailable" : "invalid_request",
        error_description: exception.message,
      };
    } else {
      this.logger.error(
        "OAuth endpoint failed",
        exception instanceof Error ? exception.stack : String(exception),
      );
    }

    setNoStore(response);
    response.status(status).json(body);
  }
}
