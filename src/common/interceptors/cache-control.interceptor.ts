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
 * Based on endpoint patterns and data volatility.
 *
 * NOW INCLUDES:
 * - ETag generation (MD5 of body)
 * - Last-Modified header
 * - Respects existing Cache-Control headers (won't overwrite if set)
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
        // 1. Generate ETag if data is present and valid
        if (data && typeof data === "object") {
          const etag = this.generateETag(data);
          const clientETag = request.headers["if-none-match"];

          // Check for conditional request match
          if (clientETag === etag) {
            response.status(304); // Not Modified - NestJS might handle this, but being explicit helps
            return;
          }
          response.setHeader("ETag", etag);
        }

        // 2. Set Last-Modified (default to now if no data timestamp found)
        // Try to find a logical "updatedAt" or "timestamp" in the data
        const lastModified = this.extractLastModified(data);
        response.setHeader("Last-Modified", lastModified.toUTCString());

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

  private extractLastModified(data: any): Date {
    // Try common timestamp fields
    if (data) {
      if (data.lastUpdated) return new Date(data.lastUpdated);
      if (data.updatedAt) return new Date(data.updatedAt);
      if (data.timestamp) return new Date(data.timestamp);
    }
    return new Date();
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

    // Discovery/geo endpoints (5 min)
    if (path.includes("/discovery")) {
      return "public, max-age=300, s-maxage=300, stale-while-revalidate=600";
    }

    // Search - moderate cache (5 min) - matches Redis TTL
    if (path.includes("/search")) {
      return "public, max-age=300, s-maxage=300, stale-while-revalidate=600";
    }

    // Holidays - long cache (1 day)
    if (path.includes("/holidays")) {
      return "public, max-age=86400, s-maxage=86400, stale-while-revalidate=172800";
    }

    // Stats/analytics - moderate cache (5 minutes)
    if (path.includes("/stats") || path.includes("/analytics")) {
      return "public, max-age=300, s-maxage=300, stale-while-revalidate=600";
    }

    // Default - moderate cache for other endpoints
    return "public, max-age=300, s-maxage=300";
  }
}
