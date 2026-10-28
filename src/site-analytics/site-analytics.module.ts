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
import { Restaurant } from "../restaurants/entities/restaurant.entity";
import { SitePageviewRow } from "./entities/site-pageview-row.entity";
import { SiteEventRow } from "./entities/site-event-row.entity";
import { SiteVisitorDay } from "./entities/site-visitor-day.entity";
import { SiteBlockedEvent } from "./entities/site-blocked-event.entity";
import { SiteEntityMerge } from "./entities/site-entity-merge.entity";
import { SiteAnalyticsFlush } from "./entities/site-analytics-flush.entity";
import { SiteAnalyticsImport } from "./entities/site-analytics-import.entity";
import { SiteAnalyticsCollectService } from "./site-analytics-collect.service";
import { SiteAnalyticsStorageService } from "./site-analytics-storage.service";
import { SiteAnalyticsService } from "./site-analytics.service";
import { SiteAnalyticsSeoService } from "./site-analytics-seo.service";
import { SiteAnalyticsBlocklist } from "./site-analytics-blocklist.service";
import { SiteEntityNames } from "./site-entity-names.service";
import {
  AdminSiteAnalyticsController,
  SiteAnalyticsCollectController,
} from "./site-analytics.controller";
import { collectBodyMiddleware } from "./collect-body.middleware";

/** The route that reads a `text/plain` body, and the only one. */
export const COLLECT_ROUTE = {
  path: "analytics/collect",
  method: RequestMethod.POST,
};

/**
 * Our own cookieless, aggregate-only site statistic: page views, visits,
 * visitors and any event the site sends, counted per UTC hour, and
 * the admin's reports on them. `SiteAnalyticsService` and
 * `SiteAnalyticsSeoService` are exported for the admin's MCP tools,
 * `SiteAnalyticsCollectService` for the most-read counter's old endpoint.
 * See docs/frontend/site-analytics.md.
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([
      SitePageviewRow,
      SiteEventRow,
      SiteVisitorDay,
      SiteBlockedEvent,
      SiteEntityMerge,
      SiteAnalyticsFlush,
      SiteAnalyticsImport,
      Park,
      Attraction,
      Show,
      Restaurant,
    ]),
    RedisModule,
  ],
  controllers: [SiteAnalyticsCollectController, AdminSiteAnalyticsController],
  providers: [
    SiteAnalyticsCollectService,
    SiteAnalyticsStorageService,
    SiteAnalyticsService,
    SiteAnalyticsSeoService,
    SiteAnalyticsBlocklist,
    SiteEntityNames,
  ],
  exports: [
    SiteAnalyticsCollectService,
    SiteAnalyticsStorageService,
    SiteAnalyticsService,
    SiteAnalyticsSeoService,
  ],
})
export class SiteAnalyticsModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(collectBodyMiddleware()).forRoutes(COLLECT_ROUTE);
  }
}
