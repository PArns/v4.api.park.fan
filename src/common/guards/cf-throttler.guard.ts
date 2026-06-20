import { ExecutionContext, Injectable } from "@nestjs/common";
import { ThrottlerGuard } from "@nestjs/throttler";
import { isIP } from "net";
import {
  getThrottleBypassHeader,
  getThrottleBypassKeys,
  getCfOriginSecret,
  getCfOriginSecretHeader,
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
 *
 * SECURITY: those headers are forgeable by anyone reaching the origin directly,
 * so we (a) only accept syntactically valid IPs — otherwise an attacker could
 * send a unique garbage value per request to spawn unbounded throttle buckets
 * (a memory-exhaustion vector) and never hit the limit — and (b) optionally
 * require an origin-verification secret header (see getCfOriginSecret) before
 * trusting them at all.
 */
@Injectable()
export class CfThrottlerGuard extends ThrottlerGuard {
  protected async getTracker(req: Record<string, any>): Promise<string> {
    const realIp = (typeof req.ip === "string" && req.ip) || "unknown";

    // If an origin secret is configured, only trust the CF-set client-IP
    // headers when the request carries the matching secret header; otherwise
    // the request didn't provably come through Cloudflare → key on the real
    // connection IP. No-op until CF_ORIGIN_SECRET is set.
    const secret = getCfOriginSecret();
    if (secret) {
      const provided = req.headers?.[getCfOriginSecretHeader()];
      const values = Array.isArray(provided) ? provided : [provided];
      const verified = values.some(
        (v) => typeof v === "string" && v === secret,
      );
      if (!verified) return realIp;
    }

    const cfIp = req.headers?.["cf-connecting-ip"];
    if (typeof cfIp === "string" && isIP(cfIp)) {
      return cfIp;
    }

    const forwardedFor = req.headers?.["x-forwarded-for"];
    if (typeof forwardedFor === "string" && forwardedFor.length > 0) {
      const first = forwardedFor.split(",")[0].trim();
      if (isIP(first)) return first;
    }

    return realIp;
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
