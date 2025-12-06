import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { ShowsService } from "../../shows/shows.service";

/**
 * Shows Metadata Processor
 *
 * @deprecated Phase 6.2: Use ChildrenMetadataProcessor instead (combines attractions, shows, restaurants)
 * This processor is kept for backward compatibility but will be removed in future versions.
 * The new combined processor reduces API requests by 67% (105 vs 315).
 *
 * Processes jobs in the 'shows-metadata' queue.
 * Fetches shows from ThemeParks.wiki and saves to DB.
 */
@Processor("shows-metadata")
export class ShowsMetadataProcessor {
  private readonly logger = new Logger(ShowsMetadataProcessor.name);

  constructor(private showsService: ShowsService) {}

  @Process("fetch-all-shows")
  async handleFetchAllShows(_job: Job): Promise<void> {
    this.logger.log("🎭 Starting shows metadata sync...");

    try {
      const showCount = await this.showsService.syncShows();
      this.logger.log(`✅ Synced ${showCount} shows`);

      this.logger.log("🎉 Shows metadata sync complete!");
    } catch (error) {
      this.logger.error("❌ Shows metadata sync failed", error);
      throw error; // Bull will retry
    }
  }
}
