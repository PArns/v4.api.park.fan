import { ExecutionContext, Injectable } from "@nestjs/common";
import { ThrottlerGuard } from "@nestjs/throttler";
import {
  getThrottleBypassHeader,
  getThrottleBypassKeys,
} from "../throttler/throttler.config";

/**
 * Rate-limit guard that keys on the REAL client IP and supports a
 * header-based bypass allow-list.
 *
 * Behind Cloudflare every origin request arrives from a Cloudflare edge
 * address, so the default `req.ip` tracker would lump all clients into a
 * handful of buckets and throttle the whole world at once. Cloudflare
 * forwards the originating client in `CF-Connecting-IP`; we prefer that,
 * then fall back to the first `X-Forwarded-For` hop, then `req.ip`.
 */
@Injectable()
export class CfThrottlerGuard extends ThrottlerGuard {
  protected async getTracker(req: Record<string, any>): Promise<string> {
    const cfIp = req.headers?.["cf-connecting-ip"];
    if (typeof cfIp === "string" && cfIp.length > 0) {
      return cfIp;
    }

    const forwardedFor = req.headers?.["x-forwarded-for"];
    if (typeof forwardedFor === "string" && forwardedFor.length > 0) {
      return forwardedFor.split(",")[0].trim();
    }

    return req.ip ?? "unknown";
  }

  /**
   * Skip rate limiting for callers presenting a valid bypass key in the
   * configured header (our frontend), in addition to the default
   * @SkipThrottle() handling. No-op when no bypass keys are configured.
   */
  protected async shouldSkip(context: ExecutionContext): Promise<boolean> {
    const bypassKeys = getThrottleBypassKeys();
    if (bypassKeys.length > 0) {
      const req = context.switchToHttp().getRequest();
      const provided = req.headers?.[getThrottleBypassHeader()];
      const values = Array.isArray(provided) ? provided : [provided];
      if (
        values.some(
          (value) => typeof value === "string" && bypassKeys.includes(value),
        )
      ) {
        return true;
      }
    }

    return super.shouldSkip(context);
  }
}
