import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
} from "@nestjs/common";
import { Observable } from "rxjs";
import { Response } from "express";

/**
 * No CDN Cache Interceptor
 *
 * Sets Cache-Control so that shared caches (e.g. Cloudflare) do NOT cache the response.
 * Use on endpoints where the response depends on client identity (e.g. IP for GeoIP)
 * or the caller's own credentials — a 400, 404 or 429 from one of those is exactly as
 * unfit for a shared cache as a 200, since it is just as identity-dependent.
 *
 * Sets: Cache-Control: private, no-store
 * - private: only the client (browser) may cache, not CDNs or proxies
 * - no-store: do not store the response at all
 */
@Injectable()
export class NoCdnCacheInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const response = context.switchToHttp().getResponse<Response>();

    // Set before the handler runs, not in a `tap()` after it: `tap()`'s
    // single-argument form only fires on the success path, so an error
    // response — the throttler's 429, a 404, a validation 400 — shipped
    // with no Cache-Control at all, free for Cloudflare to cache under
    // whatever default it likes and hand a later caller somebody else's
    // rejection. Nothing has been written to the response yet this early in
    // the request, so `headersSent` here is a defensive no-op rather than
    // the load-bearing guard it is in `cache.interceptor.ts` (which sets its
    // header from inside `tap()`, on purpose — it must NOT mark an error
    // response publicly cacheable, and only a success ever reaches there).
    if (!response.headersSent) {
      response.setHeader(
        "Cache-Control",
        "private, no-store, no-cache, must-revalidate",
      );
      response.setHeader("Pragma", "no-cache");
    }

    return next.handle();
  }
}
