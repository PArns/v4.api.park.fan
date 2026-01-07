import { Module, forwardRef } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { FavoritesController } from "./favorites.controller";
import { FavoritesService } from "./favorites.service";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Show } from "../shows/entities/show.entity";
import { Restaurant } from "../restaurants/entities/restaurant.entity";
import { ParksModule } from "../parks/parks.module";
import { AttractionsModule } from "../attractions/attractions.module";
import { ShowsModule } from "../shows/shows.module";
import { RestaurantsModule } from "../restaurants/restaurants.module";
import { QueueDataModule } from "../queue-data/queue-data.module";
import { AnalyticsModule } from "../analytics/analytics.module";

/**
 * Favorites Module
 *
 * Provides endpoints to retrieve favorite entities with full information.
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([Park, Attraction, Show, Restaurant]),
    forwardRef(() => ParksModule),
    forwardRef(() => AttractionsModule),
    forwardRef(() => ShowsModule),
    forwardRef(() => RestaurantsModule),
    forwardRef(() => QueueDataModule),
    forwardRef(() => AnalyticsModule),
  ],
  controllers: [FavoritesController],
  providers: [FavoritesService],
  exports: [FavoritesService],
})
export class FavoritesModule {}
