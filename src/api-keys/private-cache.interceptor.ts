import {
  CallHandler,
  ExecutionContext,
  Injectable,
  NestInterceptor,
} from "@nestjs/common";
import type { Response } from "express";
import { Observable, tap } from "rxjs";

/**
 * The `Cache-Control` a keyed response leaves with: what the handler chose for
 * the browser, but never a shared cache. Cloudflare caches by URL and does not
 * look at `X-API-Key`, so a forecast cached there once would reach every later
 * caller, key or no key.
 */
export function privateCacheControl(current: string | undefined): string {
  const maxAge = /(?:^|,)\s*max-age=(\d+)/i.exec(current ?? "")?.[1];
  return maxAge ? `private, max-age=${maxAge}` : "private, no-store";
}

/**
 * Keeps forecast responses out of shared caches. Set before the handler, so a
 * 401 or a 404 leaves private too, and rewritten after it, because several
 * forecast handlers set their own public `Cache-Control` on the response.
 */
@Injectable()
export class PrivateCacheInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    const response = context.switchToHttp().getResponse<Response>();
    if (!response.headersSent) {
      response.setHeader("Cache-Control", "private, no-store");
    }
    return next.handle().pipe(
      tap(() => {
        if (response.headersSent) return;
        const current = response.getHeader("Cache-Control");
        response.setHeader(
          "Cache-Control",
          privateCacheControl(
            typeof current === "string" ? current : undefined,
          ),
        );
        response.removeHeader("CDN-Cache-Control");
      }),
    );
  }
}
