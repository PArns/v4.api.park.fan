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
 * It builds the response body rather than forwarding the thrown one, which is
 * what keeps every error on this API the same shape. The envelope is this
 * filter's — but the fields a handler adds beyond it travel (`extraFields`),
 * because a body rebuilt from `message` and `error` alone silently deleted
 * them: `retryAfterSeconds` never once reached a client, on any 429 either
 * limiter raised, and a test in PAR-136 pinned a figure that stopped at the
 * process boundary.
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
    let extras: Record<string, unknown> = {};

    if (exception instanceof HttpException) {
      status = exception.getStatus();
      const exceptionResponse = exception.getResponse();

      if (typeof exceptionResponse === "string") {
        message = exceptionResponse;
      } else if (typeof exceptionResponse === "object") {
        const responseObj = exceptionResponse as Record<string, unknown>;
        message = (responseObj.message as string | string[]) || message;
        error = responseObj.error as string;
        extras = extraFields(responseObj);
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

    // Build response.
    //
    // The extras go FIRST and everything below overwrites them: a thrower may
    // add fields, never redefine the envelope. That matters for `path`, which
    // is redacted here, and for `stack`, which is withheld in production —
    // spread the other way round and a thrown body could put either back.
    const errorResponse: any = {
      ...extras,
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

    // A limiter that knows when the caller may come back says so in the header
    // the standard reserves for it (RFC 9110 §10.2.3), not only in a field our
    // own clients had to learn. It is not a choice so much as a second copy of
    // one already made: `CfThrottlerGuard` extends Nest's `ThrottlerGuard`,
    // which sets `Retry-After` on the 429s it raises itself. Without this line
    // the API answers two kinds of 429 under two contracts, and which one a
    // caller meets depends on which limiter happened to fire first.
    const retryAfter = retryAfterHeader(
      status,
      errorResponse.retryAfterSeconds,
    );
    if (retryAfter !== undefined) response.header("Retry-After", retryAfter);

    response.status(status).json(errorResponse);
  }
}

/**
 * The fields of a thrown object body that are the thrower's own.
 *
 * `statusCode`, `message` and `error` are read into the envelope above;
 * `timestamp`, `path`, `reference` and `stack` are the filter's to write and a
 * thrown body may not supply them. Everything else is something a handler
 * deliberately attached — `retryAfterSeconds` on a 429, `reason` on a refused
 * Turnstile token — and used to be dropped on the floor here.
 *
 * Only what survives `JSON.stringify` is kept. The body reaches this filter
 * from arbitrary call sites, and a circular reference or a BigInt in it would
 * otherwise throw inside `response.json()`, i.e. inside the last thing that
 * could still answer the request.
 */
function extraFields(body: Record<string, unknown>): Record<string, unknown> {
  const kept: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(body)) {
    if (RESERVED_FIELDS.has(key) || value === undefined) continue;
    kept[key] = value;
  }

  if (Object.keys(kept).length === 0) return {};
  try {
    JSON.stringify(kept);
  } catch {
    return {};
  }
  return kept;
}

const RESERVED_FIELDS = new Set([
  "statusCode",
  "timestamp",
  "path",
  "message",
  "error",
  "reference",
  "stack",
]);

/**
 * The `Retry-After` value for a 429 that carries a wait, in delta-seconds.
 *
 * Whole seconds and at least 1: the header has no sub-second form, and a
 * rounded-down 0.4 would read as "come back now" to the client the limiter just
 * turned away. A missing, negative or non-finite figure yields no header at all
 * rather than a guess.
 */
function retryAfterHeader(
  status: number,
  retryAfterSeconds: unknown,
): string | undefined {
  if (status !== HttpStatus.TOO_MANY_REQUESTS) return undefined;
  if (typeof retryAfterSeconds !== "number") return undefined;
  if (!Number.isFinite(retryAfterSeconds) || retryAfterSeconds <= 0) {
    return undefined;
  }
  return String(Math.ceil(retryAfterSeconds));
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
