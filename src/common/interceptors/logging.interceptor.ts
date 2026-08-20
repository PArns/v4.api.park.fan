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
 * URLs are redacted before they are written. The deprecated shared admin pass
 * travels as `?pass=<secret>` and this interceptor logs every request whose URL
 * contains `/admin`, so without this it wrote a full-privilege, non-expiring,
 * non-revocable credential into the application log on every scripted call —
 * readable by everyone who can reach the container logs, which is a much wider
 * group than the people who hold the secret.
 */

/** Query parameters whose value must never reach the log. */
const REDACTED_PARAMS = new Set(["pass", "token", "password", "secret"]);

export function redactUrl(url: string): string {
  const cut = url.indexOf("?");
  if (cut === -1) return url;

  const path = url.slice(0, cut);
  const params = new URLSearchParams(url.slice(cut + 1));
  let touched = false;
  for (const key of [...params.keys()]) {
    if (REDACTED_PARAMS.has(key.toLowerCase())) {
      params.set(key, "***");
      touched = true;
    }
  }
  if (!touched) return url;
  const rest = params.toString();
  return rest ? `${path}?${rest}` : path;
}
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
        const isAdminOrML =
          url.includes("/admin") ||
          url.includes("/ml") ||
          url.includes("/train");

        if (isError || isSlow || isAdminOrML) {
          const emoji = isError ? "❌" : isSlow ? "🐌" : "🔧";
          this.logger.log(
            `${emoji} ${method} ${url} ${statusCode} - ${responseTime}ms - ${ip}`,
          );
        }
      }),
    );
  }
}
