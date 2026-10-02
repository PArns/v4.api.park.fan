import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { Restaurant } from "./entities/restaurant.entity";
import { RestaurantLiveData } from "./entities/restaurant-live-data.entity";
import { RestaurantsService } from "./restaurants.service";

/**
 * Restaurants Module
 *
 * Handles restaurant entities and dining locations.
 * Example: "Be Our Guest Restaurant", "Cinderella's Royal Table"
 *
 * Phase 6: Shows & Restaurants
 */
@Module({
  imports: [TypeOrmModule.forFeature([Restaurant, RestaurantLiveData])],
  controllers: [],
  providers: [RestaurantsService],
  exports: [RestaurantsService],
})
export class RestaurantsModule {}
