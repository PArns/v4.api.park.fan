import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { ShowFollowsController } from "./show-follows.controller";
import { ShowFollowsService } from "./show-follows.service";
import { ShowFollow } from "./entities/show-follow.entity";
import { Show } from "../shows/entities/show.entity";
import { PushModule } from "../push/push.module";

/**
 * Followed shows. CRUD lives here; the sweep that decides when to notify is
 * `PushNotificationProcessor`'s show-follow branch in `queues/`, on the same
 * five-minute cron as the trip planner's own notifications.
 */
@Module({
  imports: [TypeOrmModule.forFeature([ShowFollow, Show]), PushModule],
  controllers: [ShowFollowsController],
  providers: [ShowFollowsService],
  exports: [ShowFollowsService],
})
export class ShowFollowsModule {}
