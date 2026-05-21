import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
} from "@nestjs/common";
import { Observable } from "rxjs";
import { tap } from "rxjs/operators";
import { Response } from "express";
import * as crypto from "crypto";

/**
 * HTTP Cache Interceptor
 *
 * Sets the cache headers honoured by Cloudflare and browsers.
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
 * - `ETag` is set so Cloudflare and clients can short-circuit identical
 *   bodies with a 304.
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
    const request = context.switchToHttp().getRequest();

    return next.handle().pipe(
      tap((data) => {
        const etag = this.generateETag(data);

        // Short-circuit if the client already has this exact body.
        if (request.headers["if-none-match"] === etag) {
          response.status(304);
          return;
        }

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

        response.setHeader("ETag", etag);
      }),
    );
  }

  /**
   * Strong ETag from the JSON body. MD5 is sufficient because we use
   * the hash for cache identity, not security.
   */
  private generateETag(data: unknown): string {
    const hash = crypto
      .createHash("md5")
      .update(JSON.stringify(data))
      .digest("hex");
    return `"${hash}"`;
  }
}
