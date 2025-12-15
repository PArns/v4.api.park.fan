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
    // No cache for non-GET requests
    if (method !== "GET") {
      return "no-cache, no-store, must-revalidate";
    }

    // Health endpoints - minimal cache (2s) for monitoring
    if (path.includes("/health")) {
      return "public, max-age=2, s-maxage=2";
    }

    // Admin endpoints - no cache
    if (path.includes("/admin")) {
      return "no-cache, no-store, must-revalidate";
    }

    // Parks - moderate cache (5 min)
    if (path.includes("/parks")) {
      return "public, max-age=300, s-maxage=300, stale-while-revalidate=600";
    }

    // Queue data - short cache (30s)
    if (path.includes("/queue-data")) {
      return "public, max-age=30, s-maxage=30, stale-while-revalidate=60";
    }

    // Predictions - depends on type
    if (path.includes("/predictions")) {
      // Daily predictions - longer cache (1 hour)
      if (path.includes("daily") || path.includes("predictionType=daily")) {
        return "public, max-age=3600, s-maxage=3600, stale-while-revalidate=7200";
      }
      // Hourly predictions - moderate cache (1 min)
      return "public, max-age=60, s-maxage=60, stale-while-revalidate=120";
    }

    // Attractions - moderate cache (5 min)
    if (path.includes("/attractions")) {
      return "public, max-age=300, s-maxage=300, stale-while-revalidate=600";
    }

    // Stats/aggregated data - short cache (1 min)
    if (path.includes("/stats")) {
      return "public, max-age=60, s-maxage=60, stale-while-revalidate=120";
    }

    // Default - short cache for other endpoints
    return "public, max-age=60, s-maxage=60";
  }
}
