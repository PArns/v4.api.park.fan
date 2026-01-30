import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { StatsService } from "./stats.service";
import { ParkDailyStats } from "./entities/park-daily-stats.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { Park } from "../parks/entities/park.entity";

import { StatsSchedulerService } from "./stats-scheduler.service";

@Module({
  imports: [TypeOrmModule.forFeature([ParkDailyStats, QueueData, Park])],
  providers: [StatsService, StatsSchedulerService],
  exports: [StatsService],
})
export class StatsModule {}
