import {
  ExceptionFilter,
  Catch,
  ArgumentsHost,
  HttpException,
  HttpStatus,
  Logger,
} from "@nestjs/common";
import { Request, Response } from "express";
import { randomBytes } from "crypto";
import { redactUrl } from "../utils/redact-url.util";
import { logToFile } from "../utils/file-logger.util";

/**
 * Global exception filter for consistent error responses.
 * - Hides stack traces in production
 * - Provides clean, helpful error messages
 * - Maintains detailed logging for debugging
 *
 * Every URL written here is redacted first. This filter is the *only* thing
 * that logs a request which threw — `LoggingInterceptor` logs from a `tap()`,
 * which never runs on the error path — so it sees exactly the traffic that
 * carries a credential and fails: a runbook call to `cache/reset` without
 * `?confirm=true`, a mistyped action, and every request where the guard
 * accepted the shared pass and then refused it for the endpoint. Without the
 * redaction those wrote `?pass=<secret>` into the log in full.
 */
@Catch()
export class HttpExceptionFilter implements ExceptionFilter {
  private readonly logger = new Logger(HttpExceptionFilter.name);
  private readonly isProduction = process.env.NODE_ENV === "production";

  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();
    const request = ctx.getRequest<Request>();
    const url = redactUrl(request.url);

    let status = HttpStatus.INTERNAL_SERVER_ERROR;
    let message: string | string[] = "Internal server error";
    let error: string | undefined;

    if (exception instanceof HttpException) {
      status = exception.getStatus();
      const exceptionResponse = exception.getResponse();

      if (typeof exceptionResponse === "string") {
        message = exceptionResponse;
      } else if (typeof exceptionResponse === "object") {
        const responseObj = exceptionResponse as Record<string, unknown>;
        message = (responseObj.message as string | string[]) || message;
        error = responseObj.error as string;
      }
    } else if (exception instanceof Error) {
      // SECURITY: Sanitize error messages in production to prevent information disclosure
      if (this.isProduction) {
        // Check if error message contains sensitive information
        const sensitivePatterns = [
          /password/i,
          /secret/i,
          /api[_-]?key/i,
          /token/i,
          /credential/i,
          /connection.*string/i,
          /database.*url/i,
          /sql.*error/i,
          /query.*failed/i,
        ];

        const hasSensitiveInfo = sensitivePatterns.some((pattern) =>
          pattern.test(exception.message),
        );

        if (hasSensitiveInfo) {
          // Replace sensitive error with generic message
          message = "An internal error occurred";
          error = "InternalServerError";
          this.logger.error(
            `Sanitized error message containing sensitive information: ${request.method} ${url}`,
            exception.stack,
          );
        } else {
          // Use original message if no sensitive info detected
          message = exception.message;
          error = exception.name;
        }
      } else {
        // Development: show full error details
        message = exception.message;
        error = exception.name;
      }
    }

    // Log full error details (including stack) for debugging
    let reference: string | undefined;
    if (status >= 500) {
      // A short id that travels both ways: it goes into the response body and
      // into the log line, so a screenshot of an error in the admin is enough
      // to find the stack that produced it. Before this, a 500 in the admin
      // was a message with no way back to the request — the park editor's
      // TypeORM failure had to be reproduced against production data to be
      // read at all, because the only copy of the stack was in a container log
      // nobody could reach without a deploy console.
      reference = randomBytes(3).toString("hex");

      // Server errors - log with full stack trace
      this.logger.error(
        `${request.method} ${url} - Status: ${status} - Ref: ${reference}`,
        exception instanceof Error ? exception.stack : String(exception),
      );

      // And to disk, in the same dated files as the slow queries, because the
      // container log is rotated by the platform and lost on every redeploy.
      logToFile("api-errors", {
        reference,
        method: request.method,
        url,
        status,
        name: exception instanceof Error ? exception.name : typeof exception,
        message:
          exception instanceof Error ? exception.message : String(exception),
        stack:
          exception instanceof Error
            ? exception.stack?.split("\n").slice(0, 12).join("\n")
            : undefined,
        // TypeORM's QueryFailedError carries the statement it choked on, which
        // is usually the whole diagnosis. The parameters are deliberately NOT
        // written: they are user input and a login's are a password's
        // neighbours.
        query: readQuery(exception),
        parameterCount: readParameterCount(exception),
      });
    } else {
      // Client errors (4xx) - log as warning
      this.logger.warn(
        `${request.method} ${url} - Status: ${status} - Message: ${
          Array.isArray(message) ? message.join(", ") : message
        }`,
      );
    }

    // Build response
    const errorResponse: any = {
      statusCode: status,
      timestamp: new Date().toISOString(),
      path: url,
      message,
      ...(error && { error }),
      // Only on a server error, and only ever an opaque handle: it says
      // nothing about the failure, it just names the log line that does.
      ...(reference && { reference }),
    };

    // Only include stack trace in development
    if (!this.isProduction && exception instanceof Error) {
      errorResponse.stack = exception.stack;
    }

    // The failure is recorded above either way. But a response that is
    // already on the wire cannot be answered a second time: `status().json()`
    // would throw ERR_HTTP_HEADERS_SENT from inside the filter itself, where
    // nothing catches it. That is the second half of the abandoned-request
    // 500s — an interceptor throws on the flushed response, and the filter
    // meant to report it throws again.
    if (response.headersSent) return;

    response.status(status).json(errorResponse);
  }
}

/** The statement a TypeORM `QueryFailedError` failed on, truncated. */
function readQuery(exception: unknown): string | undefined {
  const query = (exception as { query?: unknown })?.query;
  return typeof query === "string" ? query.slice(0, 2000) : undefined;
}

/** How many parameters it carried — the values stay out of the log. */
function readParameterCount(exception: unknown): number | undefined {
  const parameters = (exception as { parameters?: unknown })?.parameters;
  return Array.isArray(parameters) ? parameters.length : undefined;
}
