import {
  ExecutionContext,
  HttpException,
  HttpStatus,
  Injectable,
} from "@nestjs/common";
import { ModuleRef, Reflector } from "@nestjs/core";
import {
  InjectThrottlerOptions,
  InjectThrottlerStorage,
  ThrottlerException,
  ThrottlerGuard,
  type ThrottlerModuleOptions,
  type ThrottlerRequest,
  type ThrottlerStorage,
} from "@nestjs/throttler";
import { isIP } from "net";
import { DATA_TERMS_URL } from "../../api-keys/api-key.constants";
import { ApiKeyService } from "../../api-keys/api-key.service";
import { readPresentedApiKey } from "../../api-keys/forecast-access.guard";
import {
  ANONYMOUS_THROTTLERS,
  carriesCfOriginSecret,
  carriesThrottleBypassKey,
  isAnonymousDataRequest,
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
  constructor(
    @InjectThrottlerOptions() options: ThrottlerModuleOptions,
    @InjectThrottlerStorage() storageService: ThrottlerStorage,
    reflector: Reflector,
    private readonly moduleRef: ModuleRef,
  ) {
    super(options, storageService, reflector);
  }

  /**
   * The anonymous limits count only public data reads made without an active
   * data key; every other request passes them. Past one, the 429 says that a
   * key lifts it, and carries the wait as `retryAfterSeconds`, from which the
   * exception filter writes the standard `Retry-After`.
   */
  protected async handleRequest(props: ThrottlerRequest): Promise<boolean> {
    const name = props.throttler.name;
    const anonymous =
      name === ANONYMOUS_THROTTLERS.minute ||
      name === ANONYMOUS_THROTTLERS.hour;
    if (!anonymous) return super.handleRequest(props);

    const http = props.context.switchToHttp();
    const req = http.getRequest<{
      method: string;
      originalUrl?: string;
      url: string;
      headers: Record<string, unknown>;
    }>();
    if (!isAnonymousDataRequest(req.method, req.originalUrl ?? req.url)) {
      return true;
    }
    if (await this.carriesActiveDataKey(req.headers)) return true;

    try {
      return await super.handleRequest(props);
    } catch (error: unknown) {
      if (!(error instanceof ThrottlerException)) throw error;
      const res = http.getResponse<{ getHeader?: (n: string) => unknown }>();
      const wait = Number(res.getHeader?.(`Retry-After-${name}`));
      throw new HttpException(
        {
          error: "rate-limited",
          message:
            name === ANONYMOUS_THROTTLERS.hour
              ? "Too many requests this hour without an API key. A free key for non-commercial use lifts this limit."
              : "Too many requests this minute without an API key. A free key for non-commercial use lifts this limit.",
          ...(Number.isFinite(wait) && wait > 0 && { retryAfterSeconds: wait }),
          terms: DATA_TERMS_URL,
        },
        HttpStatus.TOO_MANY_REQUESTS,
      );
    }
  }

  /** Whether the request carries an active data key; a made-up one does not count. */
  private async carriesActiveDataKey(
    headers: Record<string, unknown>,
  ): Promise<boolean> {
    const presented = readPresentedApiKey(headers);
    if (!presented) return false;
    const resolved = await this.moduleRef
      .get(ApiKeyService, { strict: false })
      .resolve(presented)
      .catch(() => null);
    return resolved !== null;
  }

  protected async getTracker(req: Record<string, any>): Promise<string> {
    // The socket, not `req.ip`: with `trust proxy` on, `req.ip` is the
    // caller-written leftmost X-Forwarded-For hop.
    const socketIp = req.socket?.remoteAddress;
    const realIp = (typeof socketIp === "string" && socketIp) || "unknown";

    // If an origin secret is configured, only trust the CF-set client-IP
    // headers when the request carries the matching secret header; otherwise
    // the request didn't provably come through Cloudflare → key on the real
    // connection IP. No-op until CF_ORIGIN_SECRET is set.
    if (!carriesCfOriginSecret(req.headers)) return realIp;

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
    const req = context.switchToHttp().getRequest();
    if (carriesThrottleBypassKey(req.headers)) return true;

    return super.shouldSkip(context);
  }
}
