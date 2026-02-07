import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
} from "@nestjs/common";
import { Observable } from "rxjs";
import { tap } from "rxjs/operators";
import { Response } from "express";

/**
 * No CDN Cache Interceptor
 *
 * Sets Cache-Control so that shared caches (e.g. Cloudflare) do NOT cache the response.
 * Use on endpoints where the response depends on client identity (e.g. IP for GeoIP).
 *
 * Sets: Cache-Control: private, no-store
 * - private: only the client (browser) may cache, not CDNs or proxies
 * - no-store: do not store the response at all
 */
@Injectable()
export class NoCdnCacheInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const response = context.switchToHttp().getResponse<Response>();

    return next.handle().pipe(
      tap(() => {
        response.setHeader(
          "Cache-Control",
          "private, no-store, no-cache, must-revalidate",
        );
        response.setHeader("Pragma", "no-cache");
      }),
    );
  }
}
