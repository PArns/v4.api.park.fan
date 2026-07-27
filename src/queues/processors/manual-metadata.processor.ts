import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { ManualMetadataService } from "../../attractions/services/manual-metadata.service";
import { RevalidationService } from "../../common/revalidation/revalidation.service";

/**
 * Applies the curated attraction seed on its own queue.
 *
 * Deliberately separate from children-metadata: that queue is occupied for
 * hours by the detail sweep's ~7000 rate-limited wiki requests, and a job
 * queued behind it would inherit exactly the delay this exists to avoid.
 */
@Processor("manual-metadata")
export class ManualMetadataProcessor {
  private readonly logger = new Logger(ManualMetadataProcessor.name);

  constructor(
    private readonly manualMetadata: ManualMetadataService,
    private readonly revalidationService: RevalidationService,
  ) {}

  @Process("apply-seed")
  async handleApplySeed(_job: Job): Promise<void> {
    this.logger.log("🔗 Applying curated attraction metadata...");
    const result = await this.manualMetadata.apply();

    if (result.rcdb + result.heights + result.wet > 0) {
      await this.revalidationService.revalidateTags(["parks", "attractions"]);
    }
  }
}
