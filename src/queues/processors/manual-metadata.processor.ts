import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { ManualMetadataService } from "../../attractions/services/manual-metadata.service";
import { RideProfileService } from "../../attractions/services/ride-profile.service";
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
    private readonly rideProfiles: RideProfileService,
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

  /**
   * Writes the curated ride profiles — the ride ↔ glossary link.
   *
   * Shares this queue with the metadata seed because it has the same shape:
   * checked-in data, pure database work, and worth being able to re-run the
   * moment the seed file changes.
   */
  @Process("apply-ride-profiles")
  async handleApplyRideProfiles(_job: Job): Promise<void> {
    this.logger.log("🎢 Applying curated ride profiles...");
    const result = await this.rideProfiles.apply();

    if (result.written > 0) {
      await this.revalidationService.revalidateTags(["parks", "attractions"]);
    }
  }
}
