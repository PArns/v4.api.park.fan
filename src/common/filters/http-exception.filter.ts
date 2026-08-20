import {
  ExceptionFilter,
  Catch,
  ArgumentsHost,
  HttpException,
  HttpStatus,
  Logger,
} from "@nestjs/common";
import { Request, Response } from "express";
import { redactUrl } from "../utils/redact-url.util";

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
    if (status >= 500) {
      // Server errors - log with full stack trace
      this.logger.error(
        `${request.method} ${url} - Status: ${status}`,
        exception instanceof Error ? exception.stack : String(exception),
      );
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
    };

    // Only include stack trace in development
    if (!this.isProduction && exception instanceof Error) {
      errorResponse.stack = exception.stack;
    }

    response.status(status).json(errorResponse);
  }
}
