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
 * Sets appropriate Cache-Control headers for Cloudflare caching
 * Based on endpoint patterns and data volatility
 */
@Injectable()
export class CacheControlInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const ctx = context.switchToHttp();
    const response = ctx.getResponse<Response>();
    const request = ctx.getRequest();
    const path = request.url;

    return next.handle().pipe(
      tap(() => {
        const cacheHeader = this.getCacheHeaderForPath(path, request.method);
        if (cacheHeader) {
          response.setHeader("Cache-Control", cacheHeader);
          response.setHeader("Vary", "Accept-Encoding");
        }
      }),
    );
  }

  private getCacheHeaderForPath(path: string, method: string): string | null {
    // No caching for write operations
    if (method !== "GET") {
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

    // Health endpoints - minimal cache (2s) for monitoring
    if (path.includes("/health")) {
      return "public, max-age=2, s-maxage=2";
    }

    // Wait times - very short cache (2 minutes)
    if (path.includes("/wait-times")) {
      return "public, max-age=120, s-maxage=120, stale-while-revalidate=300";
    }

    // Parks metadata - moderate cache (5 min)
    if (path.includes("/parks")) {
      return "public, max-age=300, s-maxage=300, stale-while-revalidate=600";
    }

    //Queue data - short cache (30s)
    if (path.includes("/queue-data")) {
      return "public, max-age=30, s-maxage=30, stale-while-revalidate=60";
    }

    // Predictions - depends on type
    if (path.includes("/predictions") || path.includes("/ml")) {
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

    // Discovery/geo endpoints - long cache (1 hour)
    if (path.includes("/discovery")) {
      return "public, max-age=3600, s-maxage=3600, stale-while-revalidate=7200";
    }

    // Search - short cache (2 min)
    if (path.includes("/search")) {
      return "public, max-age=120, s-maxage=120, stale-while-revalidate=300";
    }

    // Holidays - long cache (1 day)
    if (path.includes("/holidays")) {
      return "public, max-age=86400, s-maxage=86400, stale-while-revalidate=172800";
    }

    // Stats/analytics - short cache (1 min)
    if (path.includes("/stats") || path.includes("/analytics")) {
      return "public, max-age=60, s-maxage=60, stale-while-revalidate=120";
    }

    // Default - moderate cache for other endpoints
    return "public, max-age=300, s-maxage=300";
  }
}
