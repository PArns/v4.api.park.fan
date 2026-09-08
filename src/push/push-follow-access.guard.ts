import {
  BadRequestException,
  HttpException,
  HttpStatus,
  Injectable,
} from "@nestjs/common";
import { Request } from "express";
import { getClientIp } from "../common/utils/request.util";
import { PushService } from "./push.service";
import { PushFollowWriteRateLimitService } from "./push-follow-write-rate-limit.service";
import { PushSubscription } from "./entities/push-subscription.entity";

/**
 * The two checks every `ride-alerts`/`show-follows` handler opens with.
 *
 * Lives in `PushModule` for the same reason `PushFollowWriteRateLimitService`
 * does (see that module's own docstring): both resource modules already
 * import this one for `PushService`, and this is one shared pattern rather
 * than a concern of either resource. Extracted after both controllers
 * carried a byte-for-byte identical `subscriptionOrThrow` and a `guard`
 * differing only in which rate-limit bucket and message it names.
 */
@Injectable()
export class PushFollowAccessGuard {
  constructor(
    private readonly pushService: PushService,
    private readonly rateLimit: PushFollowWriteRateLimitService,
  ) {}

  /**
   * Resolve `endpoint` to its subscription, or throw.
   *
   * `@Query("endpoint")` carries no DTO, so the global `ValidationPipe` never
   * runs on it (it only validates class-shaped bodies) — a request with no
   * `endpoint` at all reaches here as `undefined`. TypeORM's default
   * `invalidWhereValuesBehavior.undefined` is "ignore", so
   * `findOne({ where: { endpoint: undefined } })` drops the only condition
   * and returns an arbitrary subscription — a stranger's alerts or follows.
   * `endpoint` on the write DTOs is already guarded by `@IsNotEmpty()`, so
   * this only ever fires for the unvalidated query param, but it is the one
   * check both resources need identically.
   */
  async subscriptionOrThrow(endpoint: string): Promise<PushSubscription> {
    if (typeof endpoint !== "string" || endpoint.trim().length === 0) {
      throw new BadRequestException("Missing endpoint");
    }
    const subscription = await this.pushService.findByEndpoint(endpoint);
    if (!subscription) {
      throw new HttpException(
        "No push subscription for this endpoint",
        HttpStatus.NOT_FOUND,
      );
    }
    return subscription;
  }

  /** Rate-limit a write, naming which bucket in both the log and the 429 body. */
  async writeGuard(
    request: Request,
    kind: "ride-alert" | "show-follow",
  ): Promise<void> {
    const verdict = await this.rateLimit.check(getClientIp(request), kind);
    if (verdict.allowed) return;
    throw new HttpException(
      {
        statusCode: HttpStatus.TOO_MANY_REQUESTS,
        message: `Too many ${kind} writes from this address`,
        retryAfterSeconds: verdict.retryAfterSeconds,
      },
      HttpStatus.TOO_MANY_REQUESTS,
    );
  }
}
