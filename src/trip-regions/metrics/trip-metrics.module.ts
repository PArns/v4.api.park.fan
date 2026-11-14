import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { TripRequestMetric } from "./trip-request-metric.entity";
import { TripRequestMetricsService } from "./trip-request-metrics.service";
import { TripRequestMetricsInterceptor } from "./trip-request-metrics.interceptor";

/**
 * The trip-region routes' operational metrics: the table, the writer, and the
 * interceptor the routes wear. Its own module so the nightly prune in
 * `QueuesModule` and the MCP tool reach the writer without the planner's
 * services. docs/frontend/trip-regions.md#what-the-server-records.
 */
@Module({
  imports: [TypeOrmModule.forFeature([TripRequestMetric])],
  providers: [TripRequestMetricsService, TripRequestMetricsInterceptor],
  exports: [TripRequestMetricsService, TripRequestMetricsInterceptor],
})
export class TripMetricsModule {}
