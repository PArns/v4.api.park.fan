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
 * Sets appropriate Cache-Control headers for Cloudflare caching
 * based on endpoint patterns and data volatility.
 *
 * This runs as the OUTERMOST global interceptor, so by the time `tap`
 * sees the body it is the final payload (post `ExcludeNullInterceptor`).
 * That makes it the single correct place to compute the ETag — the
 * per-route `HttpCacheInterceptor` runs further in and would hash a
 * pre-null-stripped body that never reaches the wire.
 *
 * Responsibilities:
 * - ETag generation (MD5 of the final body) for every object response.
 *   Cloudflare and direct clients use it for conditional requests; the
 *   CDN answers `If-None-Match` from the edge without hitting origin.
 * - `Last-Modified` only when the payload carries a real timestamp (a
 *   `Date.now()` fallback is meaningless and only churns caches).
 * - `Cache-Control`, but only when a more specific local interceptor has
 *   not already set it.
 */
@Injectable()
export class CacheControlInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const ctx = context.switchToHttp();
    const response = ctx.getResponse<Response>();
    const request = ctx.getRequest();
    const path = request.url;
    const method = request.method;

    return next.handle().pipe(
      tap((data) => {
        // 1. Strong ETag from the final body. We do not short-circuit to
        //    304 here: NestJS overwrites the status code from the route
        //    handler metadata, so an interceptor-set 304 never reaches the
        //    client. Conditional revalidation is handled by Cloudflare at
        //    the edge (and by direct clients) using this ETag.
        if (data && typeof data === "object") {
          response.setHeader("ETag", this.generateETag(data));
        }

        // 2. Last-Modified only when the payload exposes a real timestamp.
        const lastModified = this.extractLastModified(data);
        if (lastModified) {
          response.setHeader("Last-Modified", lastModified.toUTCString());
        }

        // 3. Set Cache-Control ONLY if not already set by a local interceptor
        if (!response.getHeader("Cache-Control")) {
          const cacheHeader = this.getCacheHeaderForPath(path, method);
          if (cacheHeader) {
            response.setHeader("Cache-Control", cacheHeader);
            response.setHeader("Vary", "Accept-Encoding");
          }
        }
      }),
    );
  }

  private generateETag(data: any): string {
    try {
      const hash = crypto
        .createHash("md5")
        .update(JSON.stringify(data))
        .digest("hex");
      return `"${hash}"`;
    } catch {
      return "";
    }
  }

  private extractLastModified(data: any): Date | null {
    // Only emit Last-Modified when the payload carries a real timestamp.
    // A `new Date()` fallback would change on every request and defeat the
    // purpose of the header (and fragment caches needlessly).
    if (data) {
      if (data.lastUpdated) return new Date(data.lastUpdated);
      if (data.updatedAt) return new Date(data.updatedAt);
      if (data.timestamp) return new Date(data.timestamp);
    }
    return null;
  }

  private getCacheHeaderForPath(path: string, method: string): string | null {
    // No caching for write operations
    if (method !== "GET" && method !== "HEAD") {
      return "no-store, no-cache, must-revalidate";
    }

    // Root endpoint - long cache (1 hour) for README
    if (path === "/" || path === "") {
      return null; // Let AppController handle it (already sets max-age=3600)
    }

    // Swagger/OpenAPI - cache the spec and UI assets
    if (path.includes("/api") || path.includes("/api-json")) {
      // Cache Swagger spec for 5 minutes, UI assets for 1 hour
      if (path.includes("-json") || path.includes("swagger")) {
        return "public, max-age=3600, s-maxage=3600"; // 1 hour for static assets
      }
      return "public, max-age=300, s-maxage=300"; // 5 minutes for API spec
    }

    // Admin endpoints - NEVER cache. These are operator-only, may carry
    // real-time operational state, and must not land in a shared CDN cache.
    // (Matches "/admin" as a path segment so we don't snag slugs like
    // ".../adminton-park".)
    if (/\/admin(\/|$|\?)/.test(path)) {
      return "private, no-store, no-cache, must-revalidate";
    }

    // ML internal surface (monitoring, drift, alerts, anomalies, dashboard,
    // accuracy, ml-health) - real-time operator data, never cache. Note the
    // USER-facing ML predictions live under "/parks/.../predictions" and are
    // handled by the "/predictions" branch below, not here.
    if (/\/ml(\/|$|\?)/.test(path)) {
      return "private, no-store, no-cache, must-revalidate";
    }

    // Health endpoints - minimal cache (2s) for monitoring
    if (path.includes("/health")) {
      return "public, max-age=2, s-maxage=2";
    }

    // Wait times - moderate cache (5 minutes)
    if (path.includes("/wait-times")) {
      return "public, max-age=300, s-maxage=300, stale-while-revalidate=600";
    }

    // Parks metadata - moderate cache (5 min)
    if (path.includes("/parks")) {
      return "public, max-age=300, s-maxage=300, stale-while-revalidate=600";
    }

    //Queue data - moderate cache (5 minutes)
    if (path.includes("/queue-data")) {
      return "public, max-age=300, s-maxage=300, stale-while-revalidate=600";
    }

    // Predictions - depends on type (user-facing, under /parks/.../predictions)
    if (path.includes("/predictions")) {
      // Daily predictions - longer cache (1 hour)
      if (path.includes("daily") || path.includes("predictionType=daily")) {
        return "public, max-age=3600, s-maxage=3600, stale-while-revalidate=7200";
      }
      // Hourly predictions - moderate cache (5 min)
      return "public, max-age=300, s-maxage=300, stale-while-revalidate=600";
    }

    // Attractions - moderate cache (5 min)
    if (path.includes("/attractions")) {
      return "public, max-age=300, s-maxage=300, stale-while-revalidate=600";
    }

    // Shows & Restaurants - moderate cache (5 min)
    if (path.includes("/shows") || path.includes("/restaurants")) {
      return "public, max-age=300, s-maxage=300, stale-while-revalidate=600";
    }

    // Destinations - moderate cache (5 min)
    if (path.includes("/destinations")) {
      return "public, max-age=300, s-maxage=300, stale-while-revalidate=600";
    }

    // Discovery/geo endpoints (5 min — underlying live park status only refreshes on the
    // 5-min wait-times sync, so a shorter cache is pure origin churn for no fresher data)
    if (path.includes("/discovery")) {
      return "public, max-age=300, s-maxage=300, stale-while-revalidate=600";
    }

    // Search - 5 min (matches the Redis search result cache)
    if (path.includes("/search")) {
      return "public, max-age=300, s-maxage=300, stale-while-revalidate=600";
    }

    // Holidays - long cache (1 day)
    if (path.includes("/holidays")) {
      return "public, max-age=86400, s-maxage=86400, stale-while-revalidate=172800";
    }

    // Stats/analytics - 5 min (live stats refresh on the 5-min sync cadence)
    if (path.includes("/stats") || path.includes("/analytics")) {
      return "public, max-age=300, s-maxage=300, stale-while-revalidate=600";
    }

    // Default - moderate cache (5 min) for other GET endpoints, matching
    // the 5-min data sync cadence. SWR mirrors the explicit branches above.
    return "public, max-age=300, s-maxage=300, stale-while-revalidate=600";
  }
}
