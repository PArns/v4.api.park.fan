import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { RideAlertsController } from "./ride-alerts.controller";
import { RideAlertsService } from "./ride-alerts.service";
import { RideAlert } from "./entities/ride-alert.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { PushModule } from "../push/push.module";
import { QueueDataModule } from "../queue-data/queue-data.module";

/**
 * Wait-time alerts. CRUD lives here; the sweep that decides when one fires
 * is `RideAlertsService.checkAndNotify`, invoked from the wait-times
 * processor in `queues/` right after each poll cycle's batch save — not on
 * a cron of its own. `QueueDataModule` is what supplies the current reading
 * to check against.
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([RideAlert, Attraction]),
    PushModule,
    QueueDataModule,
  ],
  controllers: [RideAlertsController],
  providers: [RideAlertsService],
  exports: [RideAlertsService],
})
export class RideAlertsModule {}
