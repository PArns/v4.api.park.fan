import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { PageViewsService } from "./page-views.service";
import {
  AdminPageViewsController,
  PageViewsController,
} from "./page-views.controller";

/**
 * The "most read" lists and the admin's popularity overview, read from the
 * site statistic's `page_read` events. See docs/frontend/page-views.md.
 */
@Module({
  imports: [TypeOrmModule.forFeature([Park, Attraction])],
  controllers: [PageViewsController, AdminPageViewsController],
  providers: [PageViewsService],
  exports: [PageViewsService],
})
export class PageViewsModule {}
