import { Module } from "@nestjs/common";
import { HealthController } from "./health.controller";
import { QueuesModule } from "../queues/queues.module";

/**
 * Health Module
 *
 * Provides health check endpoints for monitoring.
 * Imports QueuesModule to access queue instances (no duplicate registration).
 */
@Module({
  imports: [QueuesModule],
  controllers: [HealthController],
})
export class HealthModule {}
