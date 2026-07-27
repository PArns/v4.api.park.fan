import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { IsNull } from "typeorm";
import { AttractionsService } from "../../attractions/attractions.service";
import { SixFlagsClient } from "../../external-apis/six-flags/six-flags.client";
import { sixFlagsSlugFor } from "../../external-apis/six-flags/six-flags.parks";
import { inchesToCentimetres } from "../../external-apis/six-flags/six-flags.parser";

/** Pause between ride pages. Heights change a few times a year; there is no hurry. */
const REQUEST_GAP_MS = 400;

/**
 * Fills minimumHeight for the Six Flags and former Cedar Fair parks.
 *
 * ThemeParks.wiki carries no height for any of them — the field is absent
 * from their entity documents, so sync-attraction-details has nothing to
 * read and ~1400 rides across 26 parks sit at NULL permanently. The parks
 * publish the number on each ride page, which is where this reads it.
 *
 * Gap-filling only: a height we already hold, from the wiki or from the
 * curated seed, always wins. Each run therefore shrinks, and a ride whose
 * page we cannot match is simply retried next time.
 */
@Processor("six-flags-heights")
export class SixFlagsHeightsProcessor {
  private readonly logger = new Logger(SixFlagsHeightsProcessor.name);

  constructor(
    private readonly attractionsService: AttractionsService,
    private readonly sixFlags: SixFlagsClient,
  ) {}

  @Process("sync-heights")
  async handleSyncHeights(_job: Job): Promise<void> {
    const repo = this.attractionsService.getRepository();
    const pending = await repo.find({
      where: { minimumHeight: IsNull() },
      relations: ["park"],
      select: { id: true, slug: true, minimumHeight: true },
    });

    const targets = pending.filter((a) => a.park && sixFlagsSlugFor(a.park.slug));
    this.logger.log(
      `📏 Six Flags height sync: ${targets.length} rides without a height across ${
        new Set(targets.map((a) => a.park.slug)).size
      } parks`,
    );

    let filled = 0;
    for (const attraction of targets) {
      const siteSlug = sixFlagsSlugFor(attraction.park.slug);
      if (!siteSlug) continue;

      try {
        const inches = await this.sixFlags.fetchMinHeightInches(
          siteSlug,
          attraction.slug,
        );
        if (inches !== null) {
          await repo.update(attraction.id, {
            minimumHeight: inchesToCentimetres(inches),
            // Centimetres are canonical, but these parks publish inches and
            // their guests read the number off park signage.
            minimumHeightUnit: "in",
          });
          filled++;
        }
      } catch (error) {
        // One unreachable page must not end the run — the rest are still worth
        // having, and this ride is picked up again next time.
        this.logger.warn(
          `Height lookup failed for ${attraction.park.slug}/${attraction.slug}: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }

      if (REQUEST_GAP_MS > 0) {
        await new Promise((resolve) => setTimeout(resolve, REQUEST_GAP_MS));
      }
    }

    this.logger.log(
      `✅ Six Flags height sync done: ${filled} of ${targets.length} rides filled`,
    );
  }
}
