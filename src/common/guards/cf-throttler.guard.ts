import { Injectable } from "@nestjs/common";
import { ThrottlerGuard } from "@nestjs/throttler";

/**
 * Rate-limit guard that keys on the REAL client IP.
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
}
