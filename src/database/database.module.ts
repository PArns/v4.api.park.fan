import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { BullModule } from "@nestjs/bull";
import { TimescaleInitService } from "./timescale-init.service";
import { DbSeedService } from "./db-seed.service";
import { Park } from "../parks/entities/park.entity";
import { Holiday } from "../holidays/entities/holiday.entity";
import { WeatherData } from "../parks/entities/weather-data.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { MLModel } from "../ml/entities/ml-model.entity";

/**
 * Database Module
 *
 * Handles database-specific initialization and utilities:
 * - TimescaleDB hypertable setup
 * - Database auto-seeding when empty (intelligent - checks all data types)
 * - Database health checks
 * - Migration utilities (future)
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([Park, Holiday, WeatherData, QueueData, MLModel]),
    BullModule.registerQueue(
      { name: "park-metadata" },
      { name: "children-metadata" },
      { name: "weather" },
      { name: "holidays" },
      { name: "wait-times" },
      { name: "ml-training" },
    ),
  ],
  providers: [TimescaleInitService, DbSeedService],
  exports: [TimescaleInitService, DbSeedService],
})
export class DatabaseModule { }
