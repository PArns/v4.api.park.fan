import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
  Logger,
} from "@nestjs/common";
import { Observable } from "rxjs";
import { tap } from "rxjs/operators";
import { Request, Response } from "express";
import { redactUrl } from "../utils/redact-url.util";
import { truncateIp } from "../utils/request.util";

/**
 * The one surface whose log lines name where a request came from: the admin,
 * which only accounts reach. Every other route is a visitor's or a client's,
 * and the privacy policy says the server logs no IP addresses, so its lines
 * carry none. Even the admin's carry a /24 (IPv4) or /48 (IPv6), never the
 * address, which is also all `admin_audit_log.actor_ip` keeps
 * (docs/rules/no-address-in-logs.md).
 */
const OPERATOR_PATH = /^\/v1\/admin(?:[/?#]|$)/i;

/**
 * What a request's log line may say about its caller: a truncated address on
 * an admin route, nothing on any other, and nothing for an address that is
 * not one.
 */
export function addressForLog(
  url: string,
  ip: string | null | undefined,
): string | null {
  return OPERATOR_PATH.test(url) ? truncateIp(ip) : null;
}

/**
 * Global logging interceptor for HTTP requests.
 *
 * Only logs interesting events:
 * - Errors (4xx, 5xx status codes)
 * - Slow requests (>1000ms)
 * - Admin/ML endpoints
 *
 * Filters out routine GET/POST requests to reduce log spam.
 *
 * URLs go through `redactUrl` first, because this logs every request whose URL
 * contains `/admin` and the deprecated shared pass travels in the query string.
 * It only ever sees a request that returned, though: `tap()` below is a
 * next-only observer, so a request that threw is logged by `HttpExceptionFilter`
 * instead — which is why the redaction is a shared util and not a function in
 * this file.
 */
@Injectable()
export class LoggingInterceptor implements NestInterceptor {
  private readonly logger = new Logger("HTTP");

  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    const ctx = context.switchToHttp();
    const request = ctx.getRequest<Request>();
    const response = ctx.getResponse<Response>();

    const { method, ip } = request;
    const url = redactUrl(request.url);

    const startTime = Date.now();

    return next.handle().pipe(
      tap(() => {
        const { statusCode } = response;
        const responseTime = Date.now() - startTime;

        // Only log interesting events:
        const isError = statusCode >= 400;
        const isSlow = responseTime > 1000; // >1s
        const lowerUrl = url.toLowerCase();
        const isAdminOrML =
          lowerUrl.includes("/admin") ||
          lowerUrl.includes("/ml") ||
          lowerUrl.includes("/train");

        if (isError || isSlow || isAdminOrML) {
          const emoji = isError ? "❌" : isSlow ? "🐌" : "🔧";
          const address = addressForLog(url, ip);
          const who = address ? ` - ${address}` : "";
          this.logger.log(
            `${emoji} ${method} ${url} ${statusCode} - ${responseTime}ms${who}`,
          );
        }
      }),
    );
  }
}
