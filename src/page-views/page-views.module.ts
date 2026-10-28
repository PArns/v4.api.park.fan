import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { SiteAnalyticsModule } from "../site-analytics/site-analytics.module";
import { PageViewDay } from "./entities/page-view-day.entity";
import { PageViewsService } from "./page-views.service";
import {
  AdminPageViewsController,
  PageViewsController,
} from "./page-views.controller";

/**
 * The "most read" lists and the admin's popularity overview, read from the
 * site statistic's `page_read` events. `PageViewDay` stays registered: its
 * table is kept (and still moved by merges) until a later release drops it.
 * See docs/frontend/page-views.md.
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([PageViewDay, Park, Attraction]),
    SiteAnalyticsModule,
  ],
  controllers: [PageViewsController, AdminPageViewsController],
  providers: [PageViewsService],
  exports: [PageViewsService],
})
export class PageViewsModule {}
