import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { RedisModule } from "../common/redis/redis.module";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { PageViewDay } from "./entities/page-view-day.entity";
import { PageViewsService } from "./page-views.service";
import {
  AdminPageViewsController,
  PageViewsController,
} from "./page-views.controller";

/**
 * Page views per kind, page, language and day — the "most read" lists and the
 * admin's popularity overview. See docs/frontend/page-views.md.
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([PageViewDay, Park, Attraction]),
    RedisModule,
  ],
  controllers: [PageViewsController, AdminPageViewsController],
  providers: [PageViewsService],
  exports: [PageViewsService],
})
export class PageViewsModule {}
