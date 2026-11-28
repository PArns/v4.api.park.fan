import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { BullModule } from "@nestjs/bull";
import { bullQueue } from "../queues/queue-registrations";
import { RedisModule } from "../common/redis/redis.module";
import { ParksModule } from "../parks/parks.module";
import { AdminWeatherClosureController } from "./admin-weather-closure.controller";
import { AttractionsModule } from "../attractions/attractions.module";
import { AdminController } from "./admin.controller";
import { AdminJobsController } from "./admin-jobs.controller";
import { ParkDuplicateMergeService } from "./park-duplicate-merge.service";
import { AdminAuthModule } from "./auth/admin-auth.module";
import { AdminContentModule } from "./content/admin-content.module";
import { SystemHealthService } from "./system-health.service";
import { DowntimeMeasurementService } from "./downtime-measurement.service";
import { MlOverviewService } from "./ml-overview.service";
import { AdminQueueStatusService } from "./admin-queue-status.service";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { MonitoringModule } from "../monitoring/monitoring.module";

@Module({
  imports: [
    TypeOrmModule.forFeature([QueueData]),
    AdminAuthModule,
    AdminContentModule,
    MonitoringModule,
    RedisModule,
    ParksModule,
    AttractionsModule,
    BullModule.registerQueue(bullQueue("holidays")),
    BullModule.registerQueue(bullQueue("park-metadata")),
    BullModule.registerQueue(bullQueue("park-enrichment")),
    BullModule.registerQueue(bullQueue("ml-training")),
    BullModule.registerQueue(bullQueue("nf-training")),
    BullModule.registerQueue(bullQueue("wait-times")),
    BullModule.registerQueue(bullQueue("downtime")),
    BullModule.registerQueue(bullQueue("weather-closure")),
    BullModule.registerQueue(bullQueue("children-metadata")),
    BullModule.registerQueue(bullQueue("six-flags-heights")),
    BullModule.registerQueue(bullQueue("ride-stats")),
    BullModule.registerQueue(bullQueue("manual-metadata")),
    BullModule.registerQueue(bullQueue("prediction-accuracy")),
    BullModule.registerQueue(bullQueue("analytics")),
    BullModule.registerQueue(bullQueue("pcn-shadow")),
    BullModule.registerQueue(bullQueue("park-day-operation")),
    BullModule.registerQueue(bullQueue("search-console")),
  ],
  controllers: [
    AdminController,
    AdminJobsController,
    AdminWeatherClosureController,
  ],
  providers: [
    SystemHealthService,
    DowntimeMeasurementService,
    MlOverviewService,
    AdminQueueStatusService,
    ParkDuplicateMergeService,
  ],
})
export class AdminModule {}
