import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { PushController } from "./push.controller";
import { PushService } from "./push.service";
import { PushSubscription } from "./entities/push-subscription.entity";
import { TripsModule } from "../trips/trips.module";

/**
 * Web-push subscriptions. Sending lives here; deciding WHAT to send is the
 * notification job's job, in `queues/`.
 */
@Module({
  imports: [TypeOrmModule.forFeature([PushSubscription]), TripsModule],
  controllers: [PushController],
  providers: [PushService],
  exports: [PushService],
})
export class PushModule {}
