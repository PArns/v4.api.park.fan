import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { AttractionsService } from "../../attractions/attractions.service";

/**
 * Attractions Metadata Processor
 *
 * @deprecated Phase 6.2: Use ChildrenMetadataProcessor instead (combines attractions, shows, restaurants)
 * This processor is kept for backward compatibility but will be removed in future versions.
 * The new combined processor reduces API requests by 67% (105 vs 315).
 *
 * Processes jobs in the 'attractions-metadata' queue.
 * Fetches attractions from ThemeParks.wiki and saves to DB.
 */
@Processor("attractions-metadata")
export class AttractionsMetadataProcessor {
  private readonly logger = new Logger(AttractionsMetadataProcessor.name);

  constructor(private attractionsService: AttractionsService) {}

  @Process("fetch-all-attractions")
  async handleFetchAllAttractions(_job: Job): Promise<void> {
    this.logger.log("🎢 Starting attractions metadata sync...");

    try {
      const attractionCount = await this.attractionsService.syncAttractions();
      this.logger.log(`✅ Synced ${attractionCount} attractions`);

      this.logger.log("🎉 Attractions metadata sync complete!");
    } catch (error) {
      this.logger.error("❌ Attractions metadata sync failed", error);
      throw error; // Bull will retry
    }
  }
}
