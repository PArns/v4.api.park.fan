import {
  MiddlewareConsumer,
  Module,
  NestModule,
  RequestMethod,
} from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { RedisModule } from "../common/redis/redis.module";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Show } from "../shows/entities/show.entity";
import { SiteTrafficDay } from "./entities/site-traffic-day.entity";
import { SiteEventDay } from "./entities/site-event-day.entity";
import { SiteEventEntityDay } from "./entities/site-event-entity-day.entity";
import { SiteVisitorDay } from "./entities/site-visitor-day.entity";
import { SiteAnalyticsCollectService } from "./site-analytics-collect.service";
import { SiteAnalyticsService } from "./site-analytics.service";
import {
  AdminSiteAnalyticsController,
  SiteAnalyticsCollectController,
} from "./site-analytics.controller";
import { textBodyMiddleware } from "./text-body.middleware";

/** The route that reads a `text/plain` body, and the only one. */
export const COLLECT_ROUTE = {
  path: "analytics/collect",
  method: RequestMethod.POST,
};

/**
 * Our own cookieless, aggregate-only site statistic: page views, visits,
 * visitors and a closed list of events per UTC day, and the admin's reports
 * on them. `SiteAnalyticsService` is exported for the admin's MCP tools. See
 * docs/frontend/site-analytics.md.
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([
      SiteTrafficDay,
      SiteEventDay,
      SiteEventEntityDay,
      SiteVisitorDay,
      Park,
      Attraction,
      Show,
    ]),
    RedisModule,
  ],
  controllers: [SiteAnalyticsCollectController, AdminSiteAnalyticsController],
  providers: [SiteAnalyticsCollectService, SiteAnalyticsService],
  exports: [SiteAnalyticsCollectService, SiteAnalyticsService],
})
export class SiteAnalyticsModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(textBodyMiddleware()).forRoutes(COLLECT_ROUTE);
  }
}
