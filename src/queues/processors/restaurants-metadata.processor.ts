import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { RestaurantsService } from "../../restaurants/restaurants.service";

/**
 * Restaurants Metadata Processor
 *
 * @deprecated Phase 6.2: Use ChildrenMetadataProcessor instead (combines attractions, shows, restaurants)
 * This processor is kept for backward compatibility but will be removed in future versions.
 * The new combined processor reduces API requests by 67% (105 vs 315).
 *
 * Processes jobs in the 'restaurants-metadata' queue.
 * Fetches restaurants from ThemeParks.wiki and saves to DB.
 */
@Processor("restaurants-metadata")
export class RestaurantsMetadataProcessor {
  private readonly logger = new Logger(RestaurantsMetadataProcessor.name);

  constructor(private restaurantsService: RestaurantsService) {}

  @Process("fetch-all-restaurants")
  async handleFetchAllRestaurants(_job: Job): Promise<void> {
    this.logger.log("🍽️ Starting restaurants metadata sync...");

    try {
      const restaurantCount = await this.restaurantsService.syncRestaurants();
      this.logger.log(`✅ Synced ${restaurantCount} restaurants`);

      this.logger.log("🎉 Restaurants metadata sync complete!");
    } catch (error) {
      this.logger.error("❌ Restaurants metadata sync failed", error);
      throw error; // Bull will retry
    }
  }
}
