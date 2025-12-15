import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { HealthController } from "./health.controller";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { MLModel } from "../ml/entities/ml-model.entity";

/**
 * Health Module
 *
 * Provides health check endpoints with API statistics.
 */
@Module({
  imports: [TypeOrmModule.forFeature([Park, Attraction, QueueData, MLModel])],
  controllers: [HealthController],
})
export class HealthModule {}
