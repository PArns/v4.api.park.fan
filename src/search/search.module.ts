import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { SearchController } from "./search.controller";
import { SearchService } from "./search.service";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Show } from "../shows/entities/show.entity";
import { Restaurant } from "../restaurants/entities/restaurant.entity";
import { ParksModule } from "../parks/parks.module";
import { AnalyticsModule } from "../analytics/analytics.module";
import { QueueDataModule } from "../queue-data/queue-data.module";

@Module({
  imports: [
    TypeOrmModule.forFeature([Park, Attraction, Show, Restaurant]),
    ParksModule,
    AnalyticsModule,
    QueueDataModule,
  ],
  controllers: [SearchController],
  providers: [SearchService],
  exports: [SearchService],
})
export class SearchModule {}
