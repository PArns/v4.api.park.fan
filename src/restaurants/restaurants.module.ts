import { Module, forwardRef } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { Restaurant } from "./entities/restaurant.entity";
import { RestaurantLiveData } from "./entities/restaurant-live-data.entity";
import { RestaurantsController } from "./restaurants.controller";
import { RestaurantsService } from "./restaurants.service";
import { ThemeParksModule } from "../external-apis/themeparks/themeparks.module";
import { ParksModule } from "../parks/parks.module";

/**
 * Restaurants Module
 *
 * Handles restaurant entities and dining locations.
 * Example: "Be Our Guest Restaurant", "Cinderella's Royal Table"
 *
 * Phase 6: Shows & Restaurants
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([Restaurant, RestaurantLiveData]),
    ThemeParksModule,
    forwardRef(() => ParksModule),
  ],
  controllers: [RestaurantsController],
  providers: [RestaurantsService],
  exports: [RestaurantsService],
})
export class RestaurantsModule {}
