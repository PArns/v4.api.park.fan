import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { StatsService } from "./stats.service";
import { ParkDailyStats } from "./entities/park-daily-stats.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { Park } from "../parks/entities/park.entity";

@Module({
  imports: [TypeOrmModule.forFeature([ParkDailyStats, QueueData, Park])],
  providers: [StatsService],
  exports: [StatsService],
})
export class StatsModule {}
