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
 * Adds HTTP caching headers to responses for improved client-side caching.
 * Implements:
 * - Cache-Control headers with configurable TTL
 * - ETag generation for conditional requests
 * - Vary header for content negotiation
 *
 * Usage:
 * @UseInterceptors(new HttpCacheInterceptor(300)) // 5 minutes
 */
@Injectable()
export class HttpCacheInterceptor implements NestInterceptor {
  constructor(
    private readonly maxAge: number = 300, // Default 5 minutes
    private readonly sMaxAge?: number, // CDN cache (optional)
  ) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const response = context.switchToHttp().getResponse<Response>();
    const request = context.switchToHttp().getRequest();

    return next.handle().pipe(
      tap((data) => {
        // Generate ETag from response data
        const etag = this.generateETag(data);

        // Check if client has cached version (If-None-Match)
        const clientETag = request.headers["if-none-match"];
        if (clientETag === etag) {
          response.status(304); // Not Modified
          return;
        }

        // Set cache headers
        // Include s-maxage for Cloudflare CDN caching
        // For live data endpoints (2 min), use shorter stale-while-revalidate
        const staleWhileRevalidate =
          this.maxAge <= 120 ? this.maxAge : this.maxAge * 2;
        const cacheControl = this.sMaxAge
          ? `public, max-age=${this.maxAge}, s-maxage=${this.sMaxAge}, stale-while-revalidate=${staleWhileRevalidate}`
          : `public, max-age=${this.maxAge}, s-maxage=${this.maxAge}, stale-while-revalidate=${staleWhileRevalidate}`;

        response.setHeader("Cache-Control", cacheControl);
        response.setHeader("ETag", etag);
        response.setHeader("Vary", "Accept");

        // Add Last-Modified for additional caching strategy
        response.setHeader("Last-Modified", new Date().toUTCString());
      }),
    );
  }

  /**
   * Generate ETag from response data
   * Uses MD5 hash of stringified JSON for fast computation
   */
  private generateETag(data: any): string {
    const hash = crypto
      .createHash("md5")
      .update(JSON.stringify(data))
      .digest("hex");
    return `"${hash}"`;
  }
}
