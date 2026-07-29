import { Inject, Injectable, Logger, Optional } from "@nestjs/common";
import { AttractionsService } from "../attractions.service";
import {
  MANUAL_ATTRACTION_METADATA,
  ManualAttractionMetadata,
} from "../data/manual-attraction-metadata";
import { TouchedPark } from "./ride-profile.service";

export interface ManualMetadataResult {
  rcdb: number;
  heights: number;
  wet: number;
  /**
   * The parks this run actually wrote into, with their changed attractions.
   * The park response is served from `park:integrated:{parkId}` (up to 6h old
   * for a closed park), so a corrected height stays invisible until that entry
   * is evicted — and the frontend, told to revalidate, refetches the stale
   * copy and pins it for a day.
   */
  touchedParks: TouchedPark[];
}

/** Injection token so tests can supply their own seed. */
export const MANUAL_METADATA_SEED = "MANUAL_METADATA_SEED";

/**
 * Applies the curated attraction seed: RCDB ids, fallback minimum heights, and
 * corrections for wrong upstream flags.
 *
 * Pure database work — no upstream calls. It used to be the tail end of the
 * attraction detail sweep, which first pulls ~7000 rate-limited wiki
 * documents, so a one-line seed change stayed invisible for hours and any
 * deploy during the sweep lost it entirely.
 */
@Injectable()
export class ManualMetadataService {
  private readonly logger = new Logger(ManualMetadataService.name);

  constructor(
    private readonly attractionsService: AttractionsService,
    @Optional()
    @Inject(MANUAL_METADATA_SEED)
    private readonly seed: ManualAttractionMetadata[] = MANUAL_ATTRACTION_METADATA,
  ) {}

  async apply(): Promise<ManualMetadataResult> {
    const repo = this.attractionsService.getRepository();
    const result: ManualMetadataResult = {
      rcdb: 0,
      heights: 0,
      wet: 0,
      touchedParks: [],
    };
    // Only rows that really changed — the writes below are conditional, so
    // unlike the ride-profile seed this can name exactly the affected parks.
    const byPark = new Map<string, string[]>();

    for (const entry of this.seed ?? MANUAL_ATTRACTION_METADATA) {
      const attraction = await repo
        .createQueryBuilder("attraction")
        .innerJoin("attraction.park", "park")
        .where("park.citySlug = :citySlug", { citySlug: entry.citySlug })
        .andWhere("park.slug = :parkSlug", { parkSlug: entry.parkSlug })
        .andWhere("attraction.slug = :attractionSlug", {
          attractionSlug: entry.attractionSlug,
        })
        .select([
          "attraction.id",
          "attraction.parkId",
          "attraction.minimumHeight",
          "attraction.rcdbId",
          "attraction.mayGetWet",
        ])
        .getOne();

      if (!attraction) continue; // slugs drift as parks rename rides — skip silently

      const update: Partial<{
        rcdbId: number;
        minimumHeight: number;
        minimumHeightUnit: "cm" | "in";
        mayGetWet: boolean;
      }> = {};

      if (entry.rcdbId && attraction.rcdbId !== entry.rcdbId) {
        update.rcdbId = entry.rcdbId;
        result.rcdb++;
      }
      // Curated height is a FALLBACK — never overwrite an upstream value
      if (entry.minimumHeightCm && attraction.minimumHeight === null) {
        update.minimumHeight = entry.minimumHeightCm;
        update.minimumHeightUnit = entry.minimumHeightUnit ?? "cm";
        result.heights++;
      }
      // Curated wet flag is a CORRECTION — it exists to overrule upstream
      if (
        entry.mayGetWet !== undefined &&
        attraction.mayGetWet !== entry.mayGetWet
      ) {
        update.mayGetWet = entry.mayGetWet;
        result.wet++;
      }

      if (Object.keys(update).length > 0) {
        await repo.update(attraction.id, update);
        if (attraction.parkId) {
          const ids = byPark.get(attraction.parkId) ?? [];
          ids.push(attraction.id);
          byPark.set(attraction.parkId, ids);
        }
      }
    }

    result.touchedParks = [...byPark].map(([parkId, attractionIds]) => ({
      parkId,
      attractionIds,
    }));

    this.logger.log(
      `🔗 Manual metadata applied: ${result.rcdb} RCDB ids, ${result.heights} fallback heights, ${result.wet} wet-flag corrections`,
    );
    return result;
  }
}
