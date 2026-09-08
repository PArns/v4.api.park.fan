import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { PushController } from "./push.controller";
import { PushService } from "./push.service";
import { PushFollowWriteRateLimitService } from "./push-follow-write-rate-limit.service";
import { PushFollowAccessGuard } from "./push-follow-access.guard";
import { PushSubscription } from "./entities/push-subscription.entity";
import { TripsModule } from "../trips/trips.module";
import { RedisModule } from "../common/redis/redis.module";

/**
 * Web-push subscriptions. Sending lives here; deciding WHAT to send is the
 * notification job's job, in `queues/`.
 *
 * `PushFollowWriteRateLimitService` and `PushFollowAccessGuard` live here
 * rather than in `ride-alerts` or `show-follows` because both of those
 * modules already import this one for `PushService` (resolving an endpoint
 * to a subscription), and both are one pattern shared by both resources —
 * not a `push` endpoint concern of its own.
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([PushSubscription]),
    TripsModule,
    RedisModule,
  ],
  controllers: [PushController],
  providers: [
    PushService,
    PushFollowWriteRateLimitService,
    PushFollowAccessGuard,
  ],
  exports: [
    PushService,
    PushFollowWriteRateLimitService,
    PushFollowAccessGuard,
  ],
})
export class PushModule {}
