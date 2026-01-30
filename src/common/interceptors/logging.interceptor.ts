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
 */
@Injectable()
export class LoggingInterceptor implements NestInterceptor {
  private readonly logger = new Logger("HTTP");

  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    const ctx = context.switchToHttp();
    const request = ctx.getRequest<Request>();
    const response = ctx.getResponse<Response>();

    const { method, url, ip } = request;
    const userAgent = request.get("user-agent") || "";

    const startTime = Date.now();

    return next.handle().pipe(
      tap(() => {
        const { statusCode } = response;
        const responseTime = Date.now() - startTime;

        // Only log interesting events:
        const isError = statusCode >= 400;
        const isSlow = responseTime > 1000; // >1s
        const isAdminOrML =
          url.includes("/admin") || url.includes("/ml") || url.includes("/train");

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
