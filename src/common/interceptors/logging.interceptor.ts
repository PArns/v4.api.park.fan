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
import { recordSlowRequest } from "../slow-request.logger";

/** Threshold in ms above which a request is considered slow and written to the slow-request log. */
const SLOW_THRESHOLD_MS = 1000;

/**
 * Global logging interceptor for HTTP requests.
 *
 * - Errors (4xx, 5xx): always logged to main stream.
 * - Slow requests (>1s): written to dedicated file (see SLOW_REQUEST_LOG_PATH) so they are not
 *   lost in the log stream; one short line is still logged to the main stream.
 * - Admin/ML endpoints: logged to main stream.
 */
@Injectable()
export class LoggingInterceptor implements NestInterceptor {
  private readonly logger = new Logger("HTTP");

  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    const ctx = context.switchToHttp();
    const request = ctx.getRequest<Request>();
    const response = ctx.getResponse<Response>();

    const { method, url, ip } = request;

    const startTime = Date.now();

    return next.handle().pipe(
      tap(() => {
        const { statusCode } = response;
        const responseTime = Date.now() - startTime;

        const isError = statusCode >= 400;
        const isSlow = responseTime > SLOW_THRESHOLD_MS;
        const isAdminOrML =
          url.includes("/admin") ||
          url.includes("/ml") ||
          url.includes("/train");

        if (isSlow) {
          recordSlowRequest({
            ts: new Date().toISOString(),
            method,
            url,
            statusCode,
            responseTimeMs: responseTime,
            ip: ip || undefined,
          });
          this.logger.warn(
            `Slow request (see slow-request log): ${method} ${url} ${statusCode} - ${responseTime}ms`,
          );
        }

        if (isError || isAdminOrML) {
          const emoji = isError ? "❌" : "🔧";
          this.logger.log(
            `${emoji} ${method} ${url} ${statusCode} - ${responseTime}ms - ${ip}`,
          );
        }
      }),
    );
  }
}
