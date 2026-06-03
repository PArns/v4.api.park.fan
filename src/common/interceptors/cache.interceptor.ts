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
 * HTTP Cache Interceptor
 *
 * Sets the TTL cache headers honoured by Cloudflare and browsers for a
 * specific route.
 *
 * - `Cache-Control` carries the BROWSER TTL (`max-age`) plus an
 *   `s-maxage` mirror so caches that do not understand
 *   `CDN-Cache-Control` (most plain HTTP caches) still pick up the CDN
 *   intent.
 * - `CDN-Cache-Control` carries the CDN TTL when it differs from the
 *   browser TTL. Cloudflare reads this header in preference to
 *   `Cache-Control`, browsers ignore it. This lets us cache long on
 *   the edge (low origin load) while keeping a short browser cache
 *   (fresh data after a reload).
 *
 * ETag/conditional-request handling is intentionally NOT done here. The
 * global `CacheControlInterceptor` runs further out (after
 * `ExcludeNullInterceptor`) and therefore hashes the *final* wire body;
 * computing an ETag here would hash a pre-null-stripped body that never
 * reaches the client and waste a second MD5 pass per response.
 *
 * Usage:
 *   @UseInterceptors(new HttpCacheInterceptor(900))
 *     → browser & CDN both cache 15 min
 *   @UseInterceptors(new HttpCacheInterceptor(60, 900))
 *     → browser caches 1 min, CDN caches 15 min
 */
@Injectable()
export class HttpCacheInterceptor implements NestInterceptor {
  constructor(
    /** Browser cache TTL in seconds. */
    private readonly maxAge: number = 300,
    /**
     * CDN cache TTL in seconds. Defaults to `maxAge` (= no browser/CDN
     * split). Pass a longer value to let Cloudflare cache aggressively
     * while keeping browsers on a shorter TTL.
     */
    private readonly sMaxAge?: number,
  ) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const response = context.switchToHttp().getResponse<Response>();

    return next.handle().pipe(
      tap(() => {
        const cdnMaxAge = this.sMaxAge ?? this.maxAge;
        // stale-while-revalidate doubles the TTL for forecast-like
        // endpoints (>2 min) so Cloudflare can serve stale-but-known
        // content while it asynchronously refreshes from origin.
        const swr = this.maxAge <= 120 ? this.maxAge : this.maxAge * 2;

        // Browser-facing header. We keep `s-maxage` here too so any
        // non-Cloudflare cache (Varnish, nginx, …) still sees a CDN TTL.
        response.setHeader(
          "Cache-Control",
          `public, max-age=${this.maxAge}, s-maxage=${cdnMaxAge}, stale-while-revalidate=${swr}`,
        );

        // Only emit the Cloudflare-specific header when it actually
        // adds information (browser/CDN split). Otherwise it would just
        // duplicate Cache-Control.
        if (this.sMaxAge != null && this.sMaxAge !== this.maxAge) {
          response.setHeader(
            "CDN-Cache-Control",
            `public, max-age=${cdnMaxAge}, stale-while-revalidate=${swr}`,
          );
        }
      }),
    );
  }
}
