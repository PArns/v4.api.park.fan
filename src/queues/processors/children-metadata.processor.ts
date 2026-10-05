import { Processor, Process, InjectQueue } from "@nestjs/bull";
import { CacheKeys } from "../../common/cache/cache-keys";
import { Logger, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { In, IsNull, Not, Repository } from "typeorm";
import { Job, Queue } from "bull";
import { AttractionsService } from "../../attractions/attractions.service";
import {
  AttractionRetirementService,
  ABSENT_UPSTREAM_REASON,
  RECLASSIFIED_UPSTREAM_REASON,
  isReclassifiedUpstreamReason,
} from "../../attractions/services/attraction-retirement.service";
import { SYNTHETIC_SOURCES } from "../../common/utils/outage-rows.sql";
import { ShowsService } from "../../shows/shows.service";
import { RestaurantsService } from "../../restaurants/restaurants.service";
import { ParksService } from "../../parks/parks.service";
import { ThemeParksClient } from "../../external-apis/themeparks/themeparks.client";
import { ThemeParksMapper } from "../../external-apis/themeparks/themeparks.mapper";
import { EntityResponse } from "../../external-apis/themeparks/themeparks.types";
import { generateSlug, generateUniqueSlug } from "../../common/utils/slug.util";
import { extractQueueTimesNumericId } from "../../common/utils/external-id.util";
import {
  findExistingAttraction,
  normalizeName,
} from "../../attractions/utils/attraction-match.util";
import { publishedHeightUnit } from "../../common/utils/height-unit.util";
import { ExternalEntityMapping } from "../../database/entities/external-entity-mapping.entity";
import { QueueTimesDataSource } from "../../external-apis/queue-times/queue-times-data-source";
import { THEMEPARKS_EXCLUSIONS } from "../../external-apis/themeparks/themeparks.exclusions";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { RevalidationService } from "../../common/revalidation/revalidation.service";
import { invalidateParkCaches } from "../../common/cache/park-cache-invalidation";

/**
 * Rows already matched during the current park's sync pass. A park can hold
 * several rides with the same name (Wet'n'Wild has five "Restroom" rows), so
 * the name fallback must never hand the same row to two incoming entities.
 */
interface SyncClaimContext {
  claimed: Set<string>;
  /**
   * Normalized names of this park's live shows, loaded once per park sync.
   *
   * Only `syncQtAttraction` fills it, and only on the first incoming ride that
   * has no existing row — a park whose rides all match costs no query at all.
   */
  showNames?: Set<string>;
  /**
   * Every id the park's `/children` carried in this run, whatever its type.
   * Lets `findExistingAttraction` tell a re-issued entity (old id gone) from a
   * rename (old id still listed). Only the wiki path fills it.
   */
  listedExternalIds?: Set<string>;
}

/**
 * The table each `internal_entity_type` of `external_entity_mapping` points
 * into.
 *
 * A closed constant rather than a parameter, because the name is interpolated
 * into SQL — the same rule `PARK_CHILD_ENTITIES` follows in
 * `merge-dependencies.ts`. A caller that iterates this cannot be handed
 * anything else.
 */
const CHILD_ENTITY_TABLES: Record<
  ExternalEntityMapping["internalEntityType"],
  string
> = {
  park: "parks",
  attraction: "attractions",
  show: "shows",
  restaurant: "restaurants",
};

/**
 * The exact `retired_reason` the children sync writes on a `shows` or
 * `restaurants` row whose entity the wiki now calls an `ATTRACTION`.
 *
 * The mirror of {@link RECLASSIFIED_UPSTREAM_REASON}, and it carries the same
 * two obligations. It has to be an **exact** string, because the sync only
 * un-retires rows carrying one of these wordings — a retirement entered by
 * hand has to survive every nightly run, and a fuzzy match would eventually
 * swallow one. And it is **user-facing**, so it reads as a sentence: a retired
 * show keeps answering on its own detail endpoint, reason included.
 *
 * ⚠️ **An edit here moves the previous value into
 * {@link RECLASSIFIED_AS_ATTRACTION_REASONS} in the same commit.** Without
 * that, every row already retired under the old wording is stranded: the
 * un-retire check stops recognising it and the retire filter skips it because
 * `retiredAt` is set. A spec pins the literal.
 */
export const RECLASSIFIED_AS_ATTRACTION_REASON =
  "ThemeParks.wiki lists this entity as an attraction rather than a show or a " +
  "restaurant, so it is no longer tracked here. The date is when this was " +
  "noticed, not when the reclassification happened. " +
  "Source: https://api.themeparks.wiki/";

/**
 * Every wording this direction has ever written, newest first. The un-retire
 * check accepts all of them, so a row retired under an older text still comes
 * back when the wiki calls the entity a show or a restaurant again.
 */
export const RECLASSIFIED_AS_ATTRACTION_REASONS: readonly string[] = [
  RECLASSIFIED_AS_ATTRACTION_REASON,
];

/** True for a retirement this sync wrote, under any wording it has used. */
export function isReclassifiedAsAttractionReason(
  reason: string | null | undefined,
): boolean {
  return reason != null && RECLASSIFIED_AS_ATTRACTION_REASONS.includes(reason);
}

/**
 * How long an attraction's id has to be missing from its park's `/children`,
 * while the park keeps syncing, before the sync retires the row.
 *
 * PO decision, 2026-10-02 (PAR-621). Well past any feed hiccup. Measured that
 * day: 417 of 515 absent rows had been missing for more than 60 days, 311 for
 * more than 90. Seasonal rows are not retired by it at all ({@link
 * isSeasonalRow}, PAR-682): a maze between two seasons is not gone, and when
 * the wiki lists it again under a new id the sync moves the old row onto it.
 */
export const ABSENT_UPSTREAM_RETIRE_DAYS = 60;

/**
 * How far back a reading keeps an absent row from being retired. One week is
 * the window the 77 still-measured rows of 2026-10-02 were counted in.
 */
export const ABSENT_UPSTREAM_READING_DAYS = 7;

/**
 * How far back the seed of `absent_since` looks for a row's last real reading.
 *
 * It has to reach past {@link ABSENT_UPSTREAM_RETIRE_DAYS}, or the seed is
 * useless for exactly the rows the column was added for: all six rows measured
 * on 2026-10-04 had been silent far longer than 60 days, so a 60-day lookback
 * would have found nothing and fallen back to the `updatedAt` the curation
 * wrote. 400 days covers the whole retention of `queue_data` (oldest row
 * 2025-12-24) and is still a time bound, which is what keeps TimescaleDB's
 * chunk exclusion working (📚 G-128) — an unbounded `max()` over the hypertable
 * is the shape that took the API down on 2026-09-28 (📚 G-123).
 *
 * Measured against production in the grouped form the job uses: 0.49 s for the
 * six rows, 6.70 s for a constructed 200-row set. The real candidate set is a
 * park's rows that are absent upstream AND have no `absent_since` yet — around
 * six per park on the first run after the deploy, and none after it.
 */
export const ABSENT_UPSTREAM_SEED_LOOKBACK_DAYS = 400;

/**
 * How recent a row's own write has to be for the seed to treat the absence as
 * new rather than back-dating it.
 *
 * Three runs of the `0 4 * * *` children cron. A row the sync wrote inside
 * that window was listed inside it, so its absence is at most that old
 * whatever its readings say, and back-dating it to an old reading would take
 * away the whole 60-day grace against a feed that drops an id for one run.
 *
 * **The converse does not hold, and this is a heuristic rather than a proof.**
 * `updatedAt` only moves on a real `UPDATE`, so the sync listing a row it does
 * not change leaves the column where it was. Measured on 2026-10-05: of the
 * 2.501 active rows in scope, 106 carry a write older than this window and
 * **23 of those 106 are still listed upstream** — 7 of them with every other
 * gate open (Knott's Soak City, a water park with no readings in October).
 * Should the feed drop one of those ids, the seed back-dates it and the row is
 * due the same day instead of 60 days later. The retirement is reversible and
 * lands on the "season or gone?" list, so this is a known cost rather than an
 * open bug; what the window cannot be is evidence of absence. PAR-714 carries
 * the measurement and the options.
 *
 * Three rather than two because a park's own run can fail — the error is
 * caught per park and the cron carries no `attempts`, so one bad run leaves a
 * listed row's `updatedAt` at almost exactly two days and the branch would be
 * decided by batch jitter. The third run is the slack for that.
 *
 * **It is not a corner case.** Measured on 2026-10-04: of 2.393 active rows in
 * scope, 520 had received no real reading in 60 days — and 421 of those 520
 * had been written inside two days, so the feed was listing them. Without this
 * window every one of those 421 would have been retired on the first run the
 * wiki failed to list it, instead of 60 days later.
 *
 * The cost is in the other direction and is the one this path has always
 * accepted: a curation write inside the window makes a long-absent row look
 * newly absent and delays its retirement by a full
 * {@link ABSENT_UPSTREAM_RETIRE_DAYS}. It can only delay, never retire early.
 */
export const ABSENT_UPSTREAM_SEED_FRESH_WRITE_HOURS = 72;

/**
 * Whether the absence step leaves a row alone because it is seasonal.
 *
 * A maze that is missing from `/children` between November and September is
 * out of season, not gone, and the season window says exactly that — the
 * retirement said something else (PAR-682). The curated column wins where an
 * editor set one, `false` included: that is "the detector is wrong, this is
 * not seasonal", and such a row is retired like any other.
 *
 * A seasonal row the wiki re-issues under a new id is not left behind as a
 * twin either: `findExistingAttraction` hands it the new id, because its old
 * one is no longer listed.
 */
export function isSeasonalRow(row: {
  isSeasonal: boolean;
  curatedIsSeasonal: boolean | null;
}): boolean {
  return (row.curatedIsSeasonal ?? row.isSeasonal) === true;
}

/**
 * Children Metadata Processor (Combined)
 *
 * OPTIMIZATION: Instead of 3 separate processors calling getEntityChildren(),
 * this processor calls it ONCE per park and syncs ALL entity types:
 * - Attractions
 * - Shows
 * - Restaurants
 *
 * Request Reduction: 315 requests → 105 requests (67% reduction!)
 *
 * Phase 6.2: Performance Optimization
 */
@Processor("children-metadata")
export class ChildrenMetadataProcessor {
  private readonly logger = new Logger(ChildrenMetadataProcessor.name);

  constructor(
    private attractionsService: AttractionsService,
    private attractionRetirementService: AttractionRetirementService,
    private showsService: ShowsService,
    private restaurantsService: RestaurantsService,
    private parksService: ParksService,
    private themeParksClient: ThemeParksClient,
    private themeParksMapper: ThemeParksMapper,
    private qtSource: QueueTimesDataSource,
    @InjectRepository(ExternalEntityMapping)
    private mappingRepository: Repository<ExternalEntityMapping>,
    @InjectQueue("entity-mappings")
    private entityMappingsQueue: Queue,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private revalidationService: RevalidationService,
  ) {}

  @Process("fetch-all-children")
  async handleFetchChildren(_job: Job): Promise<void> {
    this.logger.log(
      "🎢 Starting COMBINED children metadata sync (Attractions + Shows + Restaurants)...",
    );

    try {
      // Ensure parks are synced first
      let parks = await this.parksService.findAll();

      if (parks.length === 0) {
        this.logger.warn("No parks found. Syncing parks first...");
        await this.parksService.syncParks();
        parks = await this.parksService.findAll();
      }

      const totalParks = parks.length;
      let syncedAttractions = 0;
      let syncedShows = 0;
      let syncedRestaurants = 0;

      this.logger.log(`📊 Total parks to process: ${totalParks}`);

      // Process parks in batches to avoid rate limiting
      const BATCH_SIZE = 10;
      const BATCH_DELAY_MS = 2000; // 2 seconds between batches

      for (let i = 0; i < parks.length; i += BATCH_SIZE) {
        const batch = parks.slice(i, Math.min(i + BATCH_SIZE, parks.length));
        const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
        const totalBatches = Math.ceil(parks.length / BATCH_SIZE);

        this.logger.log(
          `📦 Processing batch ${batchNumber}/${totalBatches} (${batch.length} parks)...`,
        );

        // Process batch in parallel
        const batchResults = await Promise.all(
          batch.map(async (park, index) => {
            const parkIndex = i + index + 1;

            // Fallback: If no Wiki ID, try Queue-Times
            if (!park.wikiEntityId) {
              if (park.queueTimesEntityId) {
                // Fetch from Queue-Times
                try {
                  const qtEntities = await this.qtSource.fetchParkEntities(
                    park.queueTimesEntityId,
                  );
                  let qtAttractions = 0;
                  const qtCtx: SyncClaimContext = { claimed: new Set() };

                  for (const entity of qtEntities) {
                    if (entity.entityType === "ATTRACTION") {
                      // Map QT entity to internal entity structure manually or via helper
                      // Since QT data is thinner, we use a simplified sync
                      await this.syncQtAttraction(entity, park.id, qtCtx);
                      qtAttractions++;
                    }
                  }
                  return {
                    attractions: qtAttractions,
                    shows: 0,
                    restaurants: 0,
                  };
                } catch (e) {
                  this.logger.error(
                    `Failed to fetch from QT for ${park.name}: ${e}`,
                  );
                  return { attractions: 0, shows: 0, restaurants: 0 };
                }
              }
              return { attractions: 0, shows: 0, restaurants: 0 };
            }

            try {
              // SINGLE API CALL for all children using explicit Wiki ID
              const childrenResponse =
                await this.themeParksClient.getEntityChildren(
                  park.wikiEntityId,
                );

              // Filter by entity type and EXCLUSIONS
              const attractions = childrenResponse.children.filter(
                (child) =>
                  child.entityType === "ATTRACTION" &&
                  !THEMEPARKS_EXCLUSIONS.includes(child.id),
              );
              const shows = childrenResponse.children.filter(
                (child) =>
                  child.entityType === "SHOW" &&
                  !THEMEPARKS_EXCLUSIONS.includes(child.id),
              );
              const restaurants = childrenResponse.children.filter(
                (child) =>
                  child.entityType === "RESTAURANT" &&
                  !THEMEPARKS_EXCLUSIONS.includes(child.id),
              );

              let parkAttractions = 0;
              let parkShows = 0;
              let parkRestaurants = 0;

              // Every id the response carried counts as listed, whatever its
              // type or exclusion — the same set the absence step reads below.
              const listedExternalIds = new Set(
                childrenResponse.children.map((child) => child.id),
              );

              // Sync Attractions
              const attractionCtx: SyncClaimContext = {
                claimed: new Set(),
                listedExternalIds,
              };
              for (const attractionEntity of attractions) {
                await this.syncAttraction(
                  attractionEntity,
                  park.id,
                  attractionCtx,
                );
                parkAttractions++;
              }

              // Sync Shows
              for (const showEntity of shows) {
                await this.syncShow(showEntity, park.id);
                parkShows++;
              }

              // Sync Restaurants
              for (const restaurantEntity of restaurants) {
                await this.syncRestaurant(restaurantEntity, park.id);
                parkRestaurants++;
              }

              // An entity can change its entityType upstream while keeping its
              // id, and then it is synced into a second table while the first
              // one keeps its row. Do this after the show/restaurant syncs so
              // the replacement row exists before the old one is retired.
              // Guarded like the two steps below it: a failure here must not
              // cost this park its mapping job and its cache eviction.
              //
              // Ids that arrived as an ATTRACTION in the same response are
              // excluded. `/children` listing one entity under two types is
              // the same upstream fault `dedupePollEntities` handles for live
              // data, and without this the row would be un-retired by
              // syncAttraction and retired again here on every single run —
              // each pass evicting caches and revalidating the frontend.
              const syncedAsAttraction = new Set(
                attractions.map((child) => child.id),
              );
              try {
                await this.retireReclassifiedAttractions(
                  park.id,
                  park.name,
                  [...shows, ...restaurants]
                    .map((child) => child.id)
                    .filter((id) => !syncedAsAttraction.has(id)),
                );
              } catch (e) {
                this.logger.error(
                  `Failed to retire reclassified attractions for ${park.name}: ${e}`,
                );
              }

              // And the other direction, `SHOW`/`RESTAURANT` -> `ATTRACTION`.
              // It used to be unbuildable: until this change `shows` and
              // `restaurants` had no `retired_at` column to set, so an entity
              // that became a ride left its old row standing forever while
              // `syncAttraction` grew the replacement beside it.
              //
              // Same two guards as above, for the same two reasons. Ids that
              // arrived as a SHOW or a RESTAURANT in this very response are
              // excluded, so one entity listed under two types cannot make
              // the pair retire and un-retire each other on every run. And it
              // runs AFTER `syncAttraction`, so the replacement row exists
              // before the old one goes.
              const syncedAsChildEntity = new Set(
                [...shows, ...restaurants].map((child) => child.id),
              );
              try {
                await this.retireReclassifiedChildEntities(
                  park.name,
                  attractions
                    .map((child) => child.id)
                    .filter((id) => !syncedAsChildEntity.has(id)),
                );
              } catch (e) {
                this.logger.error(
                  `Failed to retire reclassified shows/restaurants for ${park.name}: ${e}`,
                );
              }

              // And the third case, which neither path above can see: an
              // entity that left `/children` altogether. Its id is in none of
              // the three lists, so no `In(...)` ever reaches it. Every id
              // the response carried counts as listed, whatever its type or
              // exclusion, so this path only acts on true absence.
              try {
                await this.retireAbsentAttractions(
                  park.id,
                  park.name,
                  attractions.length,
                  listedExternalIds,
                  attractionCtx.claimed,
                );
              } catch (e) {
                this.logger.error(
                  `Failed to retire absent attractions for ${park.name}: ${e}`,
                );
              }

              // Phase 6.6.3: Queue Entity Mapping Job (to match with Queue-Times)
              try {
                await this.entityMappingsQueue.add(
                  "sync-park-mappings",
                  {
                    parkId: park.id,
                  },
                  {
                    removeOnComplete: true,
                    attempts: 3,
                  },
                );
              } catch (e) {
                this.logger.error(
                  `Failed to queue mapping job for ${park.name}: ${e}`,
                );
              }

              // Invalidate integrated park cache to ensure new data (shows/restaurants) is visible immediately
              try {
                await this.redis.del(CacheKeys.parkIntegrated(park.id));
                // this.logger.debug(
                //   `🧹 Invalidated integrated cache for ${park.name} after sync`,
                // );
              } catch (e) {
                this.logger.warn(
                  `Failed to invalidate cache for ${park.name}: ${e}`,
                );
              }

              return {
                attractions: parkAttractions,
                shows: parkShows,
                restaurants: parkRestaurants,
              };
            } catch (error) {
              this.logger.error(
                `❌ [${parkIndex}/${totalParks}] Failed to sync ${park.name}:`,
                error,
              );
              return { attractions: 0, shows: 0, restaurants: 0 };
            }
          }),
        );

        // Aggregate batch results and show progress
        batchResults.forEach((result) => {
          syncedAttractions += result.attractions;
          syncedShows += result.shows;
          syncedRestaurants += result.restaurants;
        });

        // Log progress after each batch
        const processed = Math.min(i + BATCH_SIZE, parks.length);
        const percent = Math.round((processed / totalParks) * 100);
        this.logger.log(
          `Progress: ${processed}/${totalParks} (${percent}%) - ` +
            `${syncedAttractions} attractions, ${syncedShows} shows, ${syncedRestaurants} restaurants`,
        );

        // Delay between batches (except for last batch)
        if (i + BATCH_SIZE < parks.length) {
          // this.logger.verbose(
          //   `⏸️  Pausing ${BATCH_DELAY_MS}ms before next batch...`,
          // );
          await this.sleep(BATCH_DELAY_MS);
        }
      }

      this.logger.log("🎉 Combined children metadata sync complete!");
      this.logger.log(`📊 Final Stats:`);
      this.logger.log(`   - Attractions: ${syncedAttractions}`);
      this.logger.log(`   - Shows: ${syncedShows}`);
      this.logger.log(`   - Restaurants: ${syncedRestaurants}`);
      this.logger.log(
        `   - Total Children: ${syncedAttractions + syncedShows + syncedRestaurants}`,
      );
      this.logger.log(
        `   - API Requests: ${totalParks} (vs. ${totalParks * 3} with old approach)`,
      );
      this.logger.log(
        `   - Request Reduction: ${Math.round(((totalParks * 3 - totalParks) / (totalParks * 3)) * 100)}%`,
      );
    } catch (error) {
      this.logger.error("❌ Combined children metadata sync failed", error);
      throw error; // Bull will retry
    }
  }

  /**
   * Attraction detail sync: minimum rider height + manual metadata overrides.
   *
   * The /children bulk endpoint used by fetch-all-children does NOT carry
   * `minimumHeight` — only the per-entity document (GET /v1/entity/{id}) does.
   * Height restrictions change rarely, so this runs as its own low-frequency
   * job instead of bloating the daily children sync with ~5k extra requests.
   *
   * There is no curated seed to apply afterwards any more — `rcdb_id` and the
   * curated heights live in the database and nothing here writes them. Note
   * what this loop still DOES overwrite: `minimum_height` whenever the wiki
   * publishes a different number (deliberate — the park's own sign wins) and
   * `may_get_wet` likewise. A hand-made correction to the wet flag therefore
   * belongs in `curated_may_get_wet`, which this sync never touches.
   */
  @Process("sync-attraction-details")
  async handleSyncAttractionDetails(_job: Job): Promise<void> {
    this.logger.log("📏 Starting attraction detail sync (minimumHeight)...");

    const repo = this.attractionsService.getRepository();
    const attractions = await repo.find({
      // Retired attractions are skipped: this loop costs one rate-limited wiki
      // request each, and a demolished ride's height will not change.
      where: { retiredAt: IsNull() },
      // The park comes along for countryCode: the wiki reports centimetres
      // for everyone, but US parks publish the inch figure on their signage.
      relations: ["park"],
      select: {
        id: true,
        externalId: true,
        minimumHeight: true,
        maximumHeight: true,
        mayGetWet: true,
        park: { id: true, countryCode: true },
      },
    });

    // Only ThemeParks.wiki entities have per-entity documents (UUID ids);
    // Queue-Times-only attractions (e.g. "qt-ride-8") are skipped.
    const UUID_RE =
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    const wikiAttractions = attractions.filter((a) =>
      UUID_RE.test(a.externalId),
    );

    this.logger.log(
      `📊 ${wikiAttractions.length}/${attractions.length} attractions have wiki entity documents`,
    );

    // Deliberately slow: ~4 requests/s stays under the wiki's per-IP rate
    // limit (the first run at 10-parallel/250ms tripped a 429 after ~1k
    // requests and the distributed block failed everything after it).
    const BATCH_SIZE = 4;
    const BATCH_DELAY_MS = 1000;
    const RATE_LIMIT_RE = /blocked for (\d+)s/;
    const MAX_ATTEMPTS = 5;
    let updated = 0;
    let failed = 0;

    for (let i = 0; i < wikiAttractions.length; i += BATCH_SIZE) {
      const batch = wikiAttractions.slice(i, i + BATCH_SIZE);
      await Promise.all(
        batch.map(async (attraction) => {
          for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
              const entity = await this.themeParksClient.getEntity(
                attraction.externalId,
              );
              const toCm = (v: unknown): number | null =>
                typeof v === "number" && v > 0 ? Math.round(v) : null;
              const minHeight = toCm(entity.minimumHeight);
              const maxHeight = toCm(entity.maximumHeight);
              const mayGetWet =
                typeof entity.mayGetWet === "boolean" ? entity.mayGetWet : null;

              const update: Partial<{
                minimumHeight: number;
                minimumHeightUnit: "cm" | "in";
                maximumHeight: number;
                mayGetWet: boolean;
              }> = {};
              if (
                minHeight !== null &&
                minHeight !== attraction.minimumHeight
              ) {
                update.minimumHeight = minHeight;
                // The wiki always hands us centimetres, but US parks publish
                // inches — 122 here is the park's 48" sign. Record which
                // number a ride page should show.
                update.minimumHeightUnit = publishedHeightUnit(
                  attraction.park?.countryCode,
                );
              }
              if (maxHeight !== null && maxHeight !== attraction.maximumHeight)
                update.maximumHeight = maxHeight;
              if (mayGetWet !== null && mayGetWet !== attraction.mayGetWet)
                update.mayGetWet = mayGetWet;

              if (Object.keys(update).length > 0) {
                await repo.update(attraction.id, update);
                updated++;
              }
              return;
            } catch (e) {
              // The client surfaces 429s as "... (blocked for Xs)" — wait out
              // the distributed block and retry instead of counting a failure.
              const blockMatch =
                e instanceof Error ? e.message.match(RATE_LIMIT_RE) : null;
              if (blockMatch && attempt < MAX_ATTEMPTS) {
                const waitS = Math.min(parseInt(blockMatch[1], 10) || 15, 120);
                await this.sleep((waitS + 1) * 1000);
                continue;
              }
              failed++; // individual entity failures shouldn't kill the run
              return;
            }
          }
        }),
      );

      if (i % 500 === 0 && i > 0) {
        this.logger.log(
          `Progress: ${i}/${wikiAttractions.length} (${updated} updated, ${failed} failed)`,
        );
      }
      if (i + BATCH_SIZE < wikiAttractions.length) {
        await this.sleep(BATCH_DELAY_MS);
      }
    }

    this.logger.log(
      `📏 Wiki detail sync done: ${updated} heights updated, ${failed} failed`,
    );

    // The frontend caches the park/attraction structure payloads for a day
    // (Vercel Data Cache, tags 'parks'/'attractions') — bust them so the new
    // metadata (heights, RCDB ids) shows up without waiting out the TTL.
    await this.revalidationService.revalidateTags(["parks", "attractions"]);

    this.logger.log("🎉 Attraction detail sync complete!");
  }

  /**
   * Sync a single attraction (extracted from AttractionsService)
   */
  private async syncAttraction(
    attractionEntity: EntityResponse,
    parkId: string,
    ctx?: SyncClaimContext,
  ): Promise<void> {
    const mappedData = this.themeParksMapper.mapAttraction(
      attractionEntity,
      parkId,
    );

    // Match within this park across all sources. `externalId` alone is
    // source-scoped — the wiki's UUID never equals Queue-Times' "qt-ride-N"
    // for the same physical ride — so keying on it created a second row for
    // every ride both sources report. The old lookup was also global rather
    // than park-scoped.
    const existingAttractions = await this.attractionsService
      .getRepository()
      .find({
        where: { parkId },
        select: [
          "id",
          "externalId",
          "slug",
          "name",
          "queueTimesEntityId",
          "retiredReason",
        ],
      });

    const existing = findExistingAttraction(
      {
        externalId: mappedData.externalId!,
        name: mappedData.name!,
        queueTimesEntityId: mappedData.queueTimesEntityId,
      },
      existingAttractions.filter((a) => !ctx?.claimed.has(a.id)),
      ctx?.listedExternalIds,
    );

    if (existing) {
      ctx?.claimed.add(existing.id);

      // A re-issued entity: the row answered to a wiki id the park no longer
      // lists, and the wiki now reports the same attraction under a new one.
      // The row moves onto the new id, so it keeps its history and its slug
      // instead of growing a `-2` twin beside it (PAR-682). A Queue-Times id
      // on the row is left alone — that is the cross-source adoption the name
      // match has always done, and it never rewrote the id. Nor is an id the
      // park still lists: a row reached through a shared Queue-Times id can
      // belong to another live wiki entity, and taking its id would orphan it.
      const reissued =
        !!ctx?.listedExternalIds &&
        !!existing.externalId &&
        !existing.externalId.startsWith("qt-ride-") &&
        existing.externalId !== mappedData.externalId &&
        !ctx.listedExternalIds.has(existing.externalId);

      // Update existing attraction (keep existing slug)
      await this.attractionsService.getRepository().update(existing.id, {
        name: mappedData.name,
        latitude: mappedData.latitude,
        longitude: mappedData.longitude,
        attractionType: mappedData.attractionType,
        ...(reissued ? { externalId: mappedData.externalId } : {}),
      });

      if (reissued) {
        await this.createMapping(
          existing.id,
          "attraction",
          "themeparks-wiki",
          mappedData.externalId!,
        );
        this.logger.log(
          `♻️ ${mappedData.name}: wiki re-issued ${existing.externalId} as ` +
            `${mappedData.externalId}; row ${existing.id} keeps its history`,
        );
      }

      // The entity is an attraction again, so the retirement
      // `retireReclassifiedAttractions` wrote is wrong now. Only that exact
      // reason is undone — a retirement a human entered through the admin
      // endpoint has to survive this run and every one after it. It goes
      // through the service rather than a column write, because lifting a
      // retirement has the same cache and sitemap consequences as setting one.
      if (isReclassifiedUpstreamReason(existing.retiredReason)) {
        await this.attractionRetirementService.unretire(existing.id);
      }
    } else {
      // Generate unique slug for this park
      const baseSlug = mappedData.slug || generateSlug(mappedData.name!);
      const existingSlugs = existingAttractions.map((a) => a.slug);

      // Generate unique slug
      const uniqueSlug = generateUniqueSlug(baseSlug, existingSlugs);
      mappedData.slug = uniqueSlug;

      // Insert new attraction
      const saved = await this.attractionsService
        .getRepository()
        .save(mappedData);

      // Create mapping for themeparks-wiki
      await this.createMapping(
        saved.id,
        "attraction",
        "themeparks-wiki",
        saved.externalId,
      );
    }
  }

  /**
   * Sync a single show (extracted from ShowsService)
   */
  private async syncShow(
    showEntity: EntityResponse,
    parkId: string,
  ): Promise<void> {
    const mappedData = this.themeParksMapper.mapShow(showEntity, parkId);

    // Check if show exists (by externalId)
    const existing = await this.showsService.getRepository().findOne({
      where: { externalId: mappedData.externalId },
    });

    if (existing) {
      // Update existing show (keep existing slug)
      await this.showsService.getRepository().update(existing.id, {
        name: mappedData.name,
        latitude: mappedData.latitude,
        longitude: mappedData.longitude,
      });

      // The entity is a show again, so the retirement
      // `retireReclassifiedChildEntities` wrote is wrong now. Only that exact
      // reason is undone — a retirement entered by hand has to survive this
      // run and every one after it.
      //
      // Unlike `syncAttraction`, the way back is NOT park-scoped: the lookup
      // above is by `externalId`, which is unique across the whole table, so
      // the row is found whichever park's `/children` carried the id. It
      // comes back under its OLD park, though — the update above does not
      // move `parkId` — so a row whose park changed upstream un-retires into
      // the wrong park's payload and still needs moving by hand.
      if (isReclassifiedAsAttractionReason(existing.retiredReason)) {
        await this.unretireChildEntity("show", existing.id, existing.parkId);
      }
    } else {
      // Generate unique slug for this park
      const baseSlug = mappedData.slug || generateSlug(mappedData.name!);

      // Get all existing slugs for this park
      const existingShows = await this.showsService.getRepository().find({
        where: { parkId },
        select: ["slug"],
      });
      const existingSlugs = existingShows.map((s) => s.slug);

      // Generate unique slug
      const uniqueSlug = generateUniqueSlug(baseSlug, existingSlugs);
      mappedData.slug = uniqueSlug;

      // Insert new show
      await this.showsService.getRepository().save(mappedData);
    }
  }

  /**
   * Sync a single restaurant (extracted from RestaurantsService)
   */
  private async syncRestaurant(
    restaurantEntity: EntityResponse,
    parkId: string,
  ): Promise<void> {
    const mappedData = this.themeParksMapper.mapRestaurant(
      restaurantEntity,
      parkId,
    );

    // Check if restaurant exists (by externalId)
    const existing = await this.restaurantsService.getRepository().findOne({
      where: { externalId: mappedData.externalId },
    });

    if (existing) {
      // Update existing restaurant (keep existing slug)
      await this.restaurantsService.getRepository().update(existing.id, {
        name: mappedData.name,
        latitude: mappedData.latitude,
        longitude: mappedData.longitude,
        cuisineType: mappedData.cuisineType,
        cuisines: mappedData.cuisines,
        requiresReservation: mappedData.requiresReservation,
      });

      // See `syncShow`: only a retirement this sync wrote is lifted.
      if (isReclassifiedAsAttractionReason(existing.retiredReason)) {
        await this.unretireChildEntity(
          "restaurant",
          existing.id,
          existing.parkId,
        );
      }
    } else {
      // Generate unique slug for this park
      const baseSlug = mappedData.slug || generateSlug(mappedData.name!);

      // Get all existing slugs for this park
      const existingRestaurants = await this.restaurantsService
        .getRepository()
        .find({
          where: { parkId },
          select: ["slug"],
        });
      const existingSlugs = existingRestaurants.map((r) => r.slug);

      // Generate unique slug
      const uniqueSlug = generateUniqueSlug(baseSlug, existingSlugs);
      mappedData.slug = uniqueSlug;

      // Insert new restaurant
      await this.restaurantsService.getRepository().save(mappedData);
    }
  }

  /**
   * Retires attraction rows whose entity is now a show or a restaurant upstream.
   *
   * ThemeParks.wiki reclassifies entities without changing their id — on
   * 2026-04-25 it moved 17 Universal Studios Singapore meet-and-greets from
   * `ATTRACTION` to `SHOW`, and on 2026-04-23 fifteen more at the two Tokyo
   * parks. The sync followed into `shows` and left `attractions` untouched,
   * because `externalId` is unique *per table* and nothing compares across the
   * two. The abandoned row then reads CLOSED forever: no source reports it, so
   * reverse-reconciliation writes a CLOSED row every poll cycle — 11,832 of
   * them for USS alone in the 30 days before this was fixed — and the park page
   * shows a permanently closed ride that does not exist as a ride any more.
   *
   * **A row with a second source is left alone, and the test is structural.**
   * An earlier round of this change also held back rows that had received a
   * genuine reading in the last 30 days. That guard was withdrawn: after the
   * wiki flips an entity's type, `WaitTimesProcessor` resolves
   * `themeparks-wiki:<externalId>` to the shows row, so the attraction's last
   * genuine reading is the day of the reclassification itself — the guard
   * would have postponed every future repair by 30 days, leaving exactly the
   * wrongly-CLOSED ride this method exists to remove on the park page for a
   * month. A row's own columns say what it is; its readings say when we last
   * heard, and that is a different question.
   *
   * A row is left alone when any source other than the wiki claims it.
   * Disneyland Paris' `Mickey's PhilharMagic` is a show to the wiki and a
   * queueing ride to Queue-Times, and was still receiving real OPERATING
   * readings; retiring it would delete a live ride over a disagreement between
   * two sources, which is a curation decision and not a sync one.
   *
   * There are two ways a row can carry a second source, and both are checked.
   * `queue_times_entity_id` is set when the mapping job matches a Queue-Times
   * ride — but **only** for Queue-Times: a `wartezeiten-app` match writes just
   * the `external_entity_mapping` row, and `WaitTimesProcessor` resolves live
   * data through that mapping all the same. 39 attractions carried exactly
   * that combination on 2026-09-15 (a wartezeiten mapping and no Queue-Times
   * id), so checking the column alone would retire a ride that is still being
   * measured. Only a row that exists purely because the wiki once called it an
   * attraction is retired here.
   *
   * **This is reversible, and that is what keeps it safe to run unattended.**
   * A row retired here carries `RECLASSIFIED_UPSTREAM_REASON` verbatim, and
   * `syncAttraction` lifts the retirement the moment the wiki lists the entity
   * as an `ATTRACTION` again — so a single malformed `/children` response
   * cannot strand a park's rides. Only rows carrying a reason this sync wrote
   * are un-retired, so a retirement a human entered through the admin endpoint
   * survives every nightly run.
   *
   * **That protection is one-directional, deliberately.** A human retirement
   * survives; a human *un*-retirement does not. `POST /admin/unretire-
   * attraction/:id` clears `retired_reason`, and the row then matches this
   * filter again, so the next run retires it while the wiki still calls the
   * entity a show. That is the sync winning an argument with a person, which
   * is what a sync is for — the wiki is the source for what an entity *is*.
   * Overriding it means correcting the entity upstream, or adding the id to
   * `THEMEPARKS_EXCLUSIONS` so this sync stops having an opinion about it.
   *
   * **The row's data supply comes back with it, in the normal case.** The
   * `shows` row keeps existing, and `WaitTimesProcessor` builds its lookup
   * with the shows after the attractions — so an un-retired attraction used
   * to lose `themeparks-wiki:<externalId>` to the stale show and go straight
   * back to collecting `system-reconciliation` CLOSED rows. Two things now
   * prevent that: `retireReclassifiedChildEntities` retires the show in the
   * same pass, and that lookup skips retired rows. What is left is the case
   * where the show is held back by `withoutForeignSourceMappings` — a second
   * source claims it — and there the shadowing is the lesser evil, because
   * the alternative is retiring a row another feed is still filling.
   *
   * The reverse direction now exists as
   * `retireReclassifiedChildEntities`, which runs immediately after this one
   * in the same pass. Note that the two are not each other's undo: this
   * method retires an attraction whose entity became a SHOW, and that one
   * retires a show whose entity became an ATTRACTION. Each entity is excluded
   * from the other's candidate list in the same response, so a `/children`
   * payload listing one id under both types cannot make the pair fight.
   * That exclusion is per response: were the wiki ever to list the same id
   * under two different PARKS, one as an ATTRACTION and one as a SHOW, the
   * two directions would retire and un-retire it once per run, evicting
   * caches each time. Not observed, and `externalId` being unique per table
   * means the row itself cannot be duplicated — but it is the shape of the
   * failure if it ever is.
   *
   * One neighbouring job had to learn that a retirement can be temporary:
   * `detect-seasonal` cleared `is_seasonal` and `season_months` for every
   * retired row, and a row retired here would have lost its season for good —
   * while retired it receives no readings at all (`wait-times.processor.ts`
   * loads its attractions with `retiredAt: IsNull()`), so nothing could derive
   * the months again. It skips these retirements now.
   *
   * The lookup here is deliberately not scoped to the park:
   * `attractions.externalId` is globally unique, so there is at most one row
   * either way, and scoping it would miss a row whose park changed upstream.
   * The diagnostic query in §5.6 of the doc answers a narrower question: it
   * joins only `shows`, so it misses an entity that became a `RESTAURANT`, and
   * it applies none of the second-source filters above.
   * **The way back is park-scoped**, because `syncAttraction` only ever looks
   * at its own park's rows — so a row that moved parks upstream is retired
   * automatically but has to be brought back by hand.
   */
  private async retireReclassifiedAttractions(
    parkId: string,
    parkName: string,
    reclassifiedExternalIds: string[],
  ): Promise<void> {
    if (reclassifiedExternalIds.length === 0) return;

    const candidates = await this.attractionsService.getRepository().find({
      where: {
        externalId: In(reclassifiedExternalIds),
        retiredAt: IsNull(),
        queueTimesEntityId: IsNull(),
      },
      select: ["id", "name", "parkId"],
    });
    if (candidates.length === 0) return;

    const stale = await this.withoutForeignSourceMappings(candidates);
    if (stale.length === 0) return;

    // The wiki does not say when it reclassified an entity, so this is the day
    // it was noticed. `RECLASSIFIED_UPSTREAM_REASON` says so, because the
    // column otherwise reads as the day the ride stopped existing.
    const retiredAt = new Date().toISOString();
    await this.attractionRetirementService.retire(
      stale.map((attraction) => ({
        attractionId: attraction.id,
        retiredAt,
        reason: RECLASSIFIED_UPSTREAM_REASON,
      })),
    );

    // The park in the message is the one whose `/children` carried the id. The
    // lookup is not park-scoped, so a row that moved parks upstream is named
    // with its own park id instead of being filed under the wrong name.
    const elsewhere = stale.filter((a) => a.parkId !== parkId);
    this.logger.log(
      `🪦 ${parkName}: retired ${stale.length} attraction row(s) reclassified upstream — ` +
        stale.map((a) => a.name).join(", "),
    );
    for (const row of elsewhere) {
      this.logger.warn(
        `"${row.name}" belongs to park ${row.parkId}, not to ${parkName} — ` +
          "retired anyway, but syncAttraction is park-scoped: it will not bring " +
          "this row back, and if the entity turns up as an ATTRACTION under the " +
          "new park it will try to insert a second row on the same unique " +
          "externalId. Move or un-retire it by hand.",
      );
    }
  }

  /**
   * Retires `shows` and `restaurants` rows whose entity is now an `ATTRACTION`
   * upstream — the counterpart of `retireReclassifiedAttractions`, and the
   * half that could not be built until these two tables grew a `retired_at`.
   *
   * ThemeParks.wiki reclassifies entities without changing their id, and it
   * does so in both directions. The attraction side of that was fixed first
   * because it was the one doing visible damage: an abandoned `attractions`
   * row reads CLOSED forever. This direction is quieter — the stale `shows`
   * row keeps whatever `show_live_data` it had and simply never updates
   * again — but it is the same duplicate, and the park payload served both
   * rows at once.
   *
   * **No park scoping, for the same reason as the other direction.** Both
   * `shows.externalId` and `restaurants.externalId` are unique across their
   * whole table, so there is at most one row either way, and scoping the
   * lookup would miss a row whose park changed upstream. The way back is not
   * park-scoped either here: `syncShow` and `syncRestaurant` both look a row
   * up by `externalId` alone, so unlike an attraction, a child entity that
   * moved parks upstream is still found and un-retired. It comes back under
   * its OLD `parkId` — neither sync method moves that column — so the row
   * reappears in the wrong park's payload and has to be moved by hand.
   *
   * **The second-source guard is kept, and it holds nobody back today.** On
   * the attraction side it is load-bearing: `external_entity_mapping` carries
   * 5,486 `queue-times` and 1,295 `wartezeiten-app` rows against
   * `internal_entity_type = 'attraction'`, so a row a second source feeds must
   * not be retired over a disagreement between two sources. Measured against
   * production on 2026-09-16, the same table holds **zero** rows for
   * `'show'` or `'restaurant'` — nothing writes them, and `show_live_data`
   * has no `data_source` column at all, because the wiki is its only feeder.
   * The check is structural rather than behavioural (📚 G-83) and costs one
   * query per park, so it stays: it is the clause that starts working by
   * itself the day a second source begins claiming shows. What it must not be
   * read as is protection that is doing something now.
   */
  private async retireReclassifiedChildEntities(
    parkName: string,
    reclassifiedExternalIds: string[],
  ): Promise<void> {
    if (reclassifiedExternalIds.length === 0) return;

    const [shows, restaurants] = await Promise.all([
      this.showsService.getRepository().find({
        where: { externalId: In(reclassifiedExternalIds), retiredAt: IsNull() },
        select: ["id", "name", "parkId"],
      }),
      this.restaurantsService.getRepository().find({
        where: { externalId: In(reclassifiedExternalIds), retiredAt: IsNull() },
        select: ["id", "name", "parkId"],
      }),
    ]);
    if (shows.length === 0 && restaurants.length === 0) return;

    const [staleShows, staleRestaurants] = await Promise.all([
      this.withoutForeignSourceMappings(shows, "show"),
      this.withoutForeignSourceMappings(restaurants, "restaurant"),
    ]);
    if (staleShows.length === 0 && staleRestaurants.length === 0) return;

    // The wiki does not say when it reclassified an entity, so this is the day
    // it was noticed. `RECLASSIFIED_AS_ATTRACTION_REASON` says so, because the
    // column otherwise reads as the day the show stopped being performed.
    const retiredAt = new Date();
    const touchedParks = new Set<string>();

    if (staleShows.length > 0) {
      await this.showsService
        .getRepository()
        .update(
          { id: In(staleShows.map((row) => row.id)) },
          { retiredAt, retiredReason: RECLASSIFIED_AS_ATTRACTION_REASON },
        );
      staleShows.forEach((row) => touchedParks.add(row.parkId));
    }
    if (staleRestaurants.length > 0) {
      await this.restaurantsService
        .getRepository()
        .update(
          { id: In(staleRestaurants.map((row) => row.id)) },
          { retiredAt, retiredReason: RECLASSIFIED_AS_ATTRACTION_REASON },
        );
      staleRestaurants.forEach((row) => touchedParks.add(row.parkId));
    }

    // Eviction and revalidation belong to the write, not to the caller: a
    // retirement is a plain column write, so without these the park payload
    // keeps serving the row for up to 24h plus the CDN window, and the
    // frontend keeps advertising its slug in the sitemap. This is the same
    // reason `AttractionRetirementService` exists; there is no service here
    // because the sync is the only writer — nothing else retires a show.
    await this.evictAndRevalidate(touchedParks);

    const names = [...staleShows, ...staleRestaurants].map((row) => row.name);
    this.logger.log(
      `🪦 ${parkName}: retired ${staleShows.length} show(s) and ` +
        `${staleRestaurants.length} restaurant(s) reclassified as attractions — ` +
        names.join(", "),
    );
  }

  /** Undo — the wiki calls the entity a show or a restaurant again. */
  private async unretireChildEntity(
    kind: "show" | "restaurant",
    id: string,
    parkId: string,
  ): Promise<void> {
    const repository =
      kind === "show"
        ? this.showsService.getRepository()
        : this.restaurantsService.getRepository();
    await repository.update(id, { retiredAt: null, retiredReason: null });
    await this.evictAndRevalidate(new Set([parkId]));
    this.logger.log(`↩️  Un-retired ${kind} ${id}`);
  }

  /**
   * The cache half of a retirement, shared by both directions so they cannot
   * drift apart. Failures are logged and swallowed: a stale cache entry is
   * worth less than the sync run it would otherwise abort.
   */
  private async evictAndRevalidate(parkIds: Set<string>): Promise<void> {
    if (parkIds.size === 0) return;
    for (const parkId of parkIds) {
      await invalidateParkCaches(this.redis, parkId).catch((e) =>
        this.logger.warn(
          `Cache eviction failed for park ${parkId}: ${(e as Error)?.message ?? e}`,
        ),
      );
    }
    await this.revalidationService
      .revalidateTags(["geo", "parks", "attractions"])
      .catch((e) =>
        this.logger.warn(`Revalidation failed: ${(e as Error)?.message ?? e}`),
      );
  }

  /**
   * Retires the attractions whose id has been missing from their park's
   * `/children` for {@link ABSENT_UPSTREAM_RETIRE_DAYS}, while the park itself
   * kept syncing.
   *
   * Neither reclassification path can see these rows: both draw their
   * candidates from the ids the response carries, and an entity that left the
   * list is in none of them. On 2026-10-02 that left 515 active rows in 83
   * parks, and 43 of them were served as CLOSED in place of their live twin.
   * The wiki re-issues seasonal mazes (and once all of Walibi Belgium's rides)
   * under a new id, the dead row keeps the counter-free slug, and
   * `outranksNameDuplicate` hands it the name group (PAR-621).
   *
   * **Absence is read from `absent_since`, a column this path owns.** It used
   * to be read off `updatedAt`, on the grounds that `syncAttraction` writes
   * every row it matches on every run — true, and checked against `/children`
   * for 243 rows in four parks with no crossing in either direction. What the
   * docstring then called a price worth paying ("any other repository write
   * also moves the column, which can only delay a retirement, never cause
   * one") turned out to land on the rows that get the most care: a curation
   * write pushed six absent rows in three parks out by a full
   * {@link ABSENT_UPSTREAM_RETIRE_DAYS} days, and in PAR-587 that was the
   * entire difference between two duplicates resolving themselves and a dead
   * row keeping the slug. So this run also maintains the column — it sets it
   * for rows it finds absent and clears it for rows the feed listed again —
   * and the gate reads it instead (PAR-656, PO decision 2026-10-03).
   *
   * The clock can still be short of the true absence, and the case is now a
   * park whose sync failed for longer than
   * {@link ABSENT_UPSTREAM_SEED_FRESH_WRITE_HOURS}: a row that disappeared
   * during the outage falls outside the fresh-write window, gets back-dated to
   * its last real reading, and can be retired on the park's first good run
   * rather than 60 days later. The reading gate still applies, the retirement
   * carries {@link ABSENT_UPSTREAM_REASON} rather than a closure, and the row
   * comes back by itself the moment the id is listed again.
   *
   * **The park has to have synced, and this run proves it** (our feed going
   * quiet is not the entity going away). This runs only after a successful
   * `/children` call that listed at least one attraction, so a park whose sync
   * fails or comes back empty retires nothing. Rows claimed in this run are
   * excluded even when their own id was not listed: a row matched by name or
   * by Queue-Times id is in the feed under another id.
   *
   * The gates are the ones `retireReclassifiedAttractions` applies, plus one
   * its candidates never needed: no `queue_times_entity_id`, no mapping from
   * another source, and no reading in {@link ABSENT_UPSTREAM_READING_DAYS}
   * that is more than our own bookkeeping (`system-reconciliation`,
   * `system-heartbeat`, carried heartbeats). On 2026-10-02, 77 of the 515 were
   * still receiving such readings.
   *
   * **It undoes itself.** The reason is {@link ABSENT_UPSTREAM_REASON}, which
   * sits in `RECLASSIFIED_UPSTREAM_REASONS`: `syncAttraction` lifts it the
   * moment the id is listed again, `retiredKindOf` reads it as `reclassified`
   * rather than `closed`, and `detect-seasonal` keeps the row's season.
   */
  private async retireAbsentAttractions(
    parkId: string,
    parkName: string,
    listedAttractionCount: number,
    listedExternalIds: Set<string>,
    claimedRowIds: Set<string>,
    now: Date = new Date(),
  ): Promise<void> {
    if (listedAttractionCount === 0) return;

    const cutoff = new Date(
      now.getTime() - ABSENT_UPSTREAM_RETIRE_DAYS * 86_400_000,
    );
    // Every active row of the park, not just the old ones: the `absent_since`
    // bookkeeping below has to see a row the run before this one marked as
    // absent in order to clear it again, and that row can be a day old.
    const rows = await this.attractionsService.getRepository().find({
      where: {
        parkId,
        retiredAt: IsNull(),
        queueTimesEntityId: IsNull(),
      },
      select: [
        "id",
        "name",
        "externalId",
        "updatedAt",
        "absentSince",
        "isSeasonal",
        "curatedIsSeasonal",
      ],
    });

    // A row the wiki could list at all: it carries a wiki id, and that id is
    // not a Queue-Times one the wiki never issued. Rows outside this set are
    // left alone entirely — the feed says nothing about them in either
    // direction, so neither the clock nor the retirement applies.
    const inScope = rows.filter(
      (row) => !!row.externalId && !row.externalId.startsWith("qt-ride-"),
    );
    const absent = inScope.filter(
      (row) =>
        !claimedRowIds.has(row.id) && !listedExternalIds.has(row.externalId),
    );
    const absentIds = new Set(absent.map((row) => row.id));
    await this.clearAbsenceWindow(
      inScope.filter((row) => !absentIds.has(row.id)),
    );
    if (absent.length === 0) return;

    // A row absent for the first time gets its clock here, and from then on
    // nothing but this path and a hand-entered un-retirement moves it.
    //
    // The clock records what the feed did, so it runs for a seasonal row too
    // and the season gate below decides what follows from it. That order is
    // the point: an editor who answers the "season or gone?" list with
    // `curated_is_seasonal = false` gets the row retired on the absence it has
    // already accrued, not 60 days after the answer.
    await this.seedAbsenceWindow(absent, now);

    const due = absent.filter(
      (row) =>
        row.absentSince != null && row.absentSince.getTime() < cutoff.getTime(),
    );
    if (due.length === 0) return;

    // A seasonal row is out of season, not gone — unless a row this run
    // claimed already carries its name. That twin was grown before the sync
    // learned to hand a re-issued id to the old row, and the dead row beside
    // it would take the name group back from it (§5.9 of the status doc), so
    // it is retired as before.
    const claimedNames = await this.claimedAttractionNames(claimedRowIds);
    const retirable = due.filter(
      (row) => !isSeasonalRow(row) || claimedNames.has(normalizeName(row.name)),
    );
    if (retirable.length === 0) return;

    const unclaimed = await this.withoutForeignSourceMappings(retirable);
    const silent = await this.withoutRecentReadings(unclaimed, now);
    if (silent.length === 0) return;

    // The wiki does not say when an entity left the list; `absent_since` only
    // bounds it from below. So this is the day it was noticed, and the reason
    // says so.
    await this.attractionRetirementService.retire(
      silent.map((attraction) => ({
        attractionId: attraction.id,
        retiredAt: now.toISOString(),
        reason: ABSENT_UPSTREAM_REASON,
      })),
    );

    this.logger.log(
      `🪦 ${parkName}: retired ${silent.length} attraction row(s) absent upstream ` +
        `for ${ABSENT_UPSTREAM_RETIRE_DAYS}+ days — ` +
        silent.map((a) => a.name).join(", "),
    );
  }

  /** Normalized names of the rows this run matched, for the twin check. */
  private async claimedAttractionNames(
    claimedRowIds: Set<string>,
  ): Promise<Set<string>> {
    if (claimedRowIds.size === 0) return new Set();
    const rows = await this.attractionsService.getRepository().find({
      where: { id: In([...claimedRowIds]) },
      select: ["id", "name"],
    });
    return new Set(rows.map((row) => normalizeName(row.name)));
  }

  /**
   * Starts the absence clock on the rows that do not have one yet, and writes
   * the value back onto the objects so this run can already act on it.
   *
   * A row written inside {@link ABSENT_UPSTREAM_SEED_FRESH_WRITE_HOURS} is
   * seeded with `now`: the sync writes what it lists, so the feed had this row
   * that recently and the absence starts today no matter how old its readings
   * are. Only a row whose own write is older than that gets back-dated, to the
   * **earlier** of its `updatedAt` and its last real reading — both are
   * evidence that the entity still existed, and the older of the two is the
   * one the newer writer may have overwritten. A row curated last week and
   * silent since April is absent since April, not since last week; getting
   * that backwards is the bug this column was added for.
   *
   * Where no real reading is found inside
   * {@link ABSENT_UPSTREAM_SEED_LOOKBACK_DAYS}, `updatedAt` is all that is
   * left. The seed is then at worst too late, which only ever delays a
   * retirement: measured on 2026-10-04, that was one row of six (`NEXUS AI`,
   * created 2026-08-20, never once read by a real source).
   *
   * The write is raw SQL on purpose. Going through the repository would move
   * `updatedAt` as well, and a bookkeeping column that disturbs the column it
   * was built to replace is worth nothing.
   */
  private async seedAbsenceWindow(
    absent: { id: string; updatedAt: Date; absentSince: Date | null }[],
    now: Date,
  ): Promise<void> {
    const unseeded = absent.filter((row) => row.absentSince == null);
    if (unseeded.length === 0) return;

    // A row the sync wrote inside the window was listed inside the window, so
    // its absence is new and nothing older may be read into it.
    const freshWrite = new Date(
      now.getTime() - ABSENT_UPSTREAM_SEED_FRESH_WRITE_HOURS * 3_600_000,
    );
    const backdatable: typeof unseeded = [];
    for (const row of unseeded) {
      if (row.updatedAt.getTime() < freshWrite.getTime()) backdatable.push(row);
      else row.absentSince = now;
    }

    // Only the back-datable rows are worth the lookback, so a park whose feed
    // just dropped an id pays nothing for it. This is also the steady state:
    // after the first run that fills the column, there is nothing to seed.
    const lastRead = new Map<string, Date>();
    if (backdatable.length > 0) {
      const since = new Date(
        now.getTime() - ABSENT_UPSTREAM_SEED_LOOKBACK_DAYS * 86_400_000,
      );
      const readings: { attractionId: string; last_read: Date }[] =
        await this.mappingRepository.manager.query(
          `SELECT "attractionId", max("timestamp") AS last_read
             FROM queue_data
            WHERE "attractionId" = ANY($1::uuid[])
              AND "timestamp" >= $2
              AND data_source <> ALL($3::text[])
              AND is_heartbeat IS NOT TRUE
            GROUP BY "attractionId"`,
          [backdatable.map((row) => row.id), since, [...SYNTHETIC_SOURCES]],
        );
      for (const row of readings) {
        lastRead.set(row.attractionId, new Date(row.last_read));
      }
    }

    for (const row of backdatable) {
      const read = lastRead.get(row.id);
      row.absentSince =
        read != null && read.getTime() < row.updatedAt.getTime()
          ? read
          : row.updatedAt;
    }

    await this.mappingRepository.manager.query(
      `UPDATE attractions AS a
          SET absent_since = v.absent_since
         FROM (SELECT * FROM unnest($1::uuid[], $2::timestamptz[])
                 AS t(id, absent_since)) AS v
        WHERE a.id = v.id AND a.absent_since IS NULL`,
      [
        unseeded.map((row) => row.id),
        unseeded.map((row) => row.absentSince!.toISOString()),
      ],
    );
  }

  /**
   * Stops the clock on rows the feed listed again, or that this run claimed
   * under another id. Only the rows that carry a value are written, so a park
   * whose feed is complete issues no statement at all.
   *
   * `syncAttraction` already lifts its own retirement when an id comes back;
   * this is the other half, and it runs for rows that were never retired too —
   * a row that was absent for a week and is listed again must not keep a
   * six-day head start for the next time it disappears.
   */
  private async clearAbsenceWindow(
    present: { id: string; absentSince: Date | null }[],
  ): Promise<void> {
    const stale = present.filter((row) => row.absentSince != null);
    if (stale.length === 0) return;

    await this.mappingRepository.manager.query(
      `UPDATE attractions SET absent_since = NULL WHERE id = ANY($1::uuid[])`,
      [stale.map((row) => row.id)],
    );
    for (const row of stale) row.absentSince = null;
  }

  /**
   * Drops every candidate that received a reading in the last
   * {@link ABSENT_UPSTREAM_READING_DAYS}. Our own bookkeeping does not count:
   * reverse reconciliation writes CLOSED for exactly the rows no source
   * mentions, so counting it would keep every absent row alive forever.
   */
  private async withoutRecentReadings<T extends { id: string }>(
    candidates: T[],
    now: Date,
  ): Promise<T[]> {
    if (candidates.length === 0) return candidates;
    const since = new Date(
      now.getTime() - ABSENT_UPSTREAM_READING_DAYS * 86_400_000,
    );
    const rows: { attractionId: string }[] =
      await this.mappingRepository.manager.query(
        `SELECT DISTINCT "attractionId"
           FROM queue_data
          WHERE "attractionId" = ANY($1::uuid[])
            AND "timestamp" >= $2
            AND data_source <> ALL($3::text[])
            AND is_heartbeat IS NOT TRUE`,
        [candidates.map((c) => c.id), since, [...SYNTHETIC_SOURCES]],
      );
    const read = new Set(rows.map((r) => r.attractionId));
    return candidates.filter((c) => !read.has(c.id));
  }

  /**
   * Drops the rows another source has claimed, whatever the wiki now says.
   *
   * The companion to `queue_times_entity_id`, and the reason that column is
   * not enough on its own: the entity mapping job writes it for Queue-Times
   * matches only, while a `wartezeiten-app` match leaves nothing but the
   * `external_entity_mapping` row — which `WaitTimesProcessor` resolves live
   * data through regardless.
   */
  private async withoutForeignSourceMappings<T extends { id: string }>(
    candidates: T[],
    internalEntityType: "attraction" | "show" | "restaurant" = "attraction",
  ): Promise<T[]> {
    if (candidates.length === 0) return candidates;
    const mappings = await this.mappingRepository.find({
      where: {
        internalEntityId: In(candidates.map((c) => c.id)),
        internalEntityType,
        externalSource: Not("themeparks-wiki"),
      },
      select: ["internalEntityId"],
    });
    const claimed = new Set(mappings.map((m) => m.internalEntityId));
    return candidates.filter((c) => !claimed.has(c.id));
  }

  /**
   * Sync a single attraction from Queue-Times (Simplified)
   */
  private async syncQtAttraction(
    entity: any,
    parkId: string,
    ctx?: SyncClaimContext,
  ): Promise<void> {
    // Extract numeric Queue-Times ID (e.g., "8" from "qt-ride-8")
    const qtNumericId = extractQueueTimesNumericId(entity.externalId);

    // Check if attraction exists (by externalId)
    // QT externalId differs from the wiki's for the same ride, so match
    // within the park across sources rather than on externalId alone.
    const existingAttractions = await this.attractionsService
      .getRepository()
      .find({
        where: { parkId },
        select: ["id", "externalId", "slug", "name", "queueTimesEntityId"],
      });

    const existing = findExistingAttraction(
      {
        externalId: entity.externalId,
        name: entity.name,
        queueTimesEntityId: qtNumericId,
      },
      existingAttractions.filter((a) => !ctx?.claimed.has(a.id)),
    );

    if (existing) {
      ctx?.claimed.add(existing.id);
      // Update name and queueTimesEntityId if needed
      const updateData: any = {};
      if (existing.name !== entity.name) {
        updateData.name = entity.name;
      }
      if (qtNumericId && !existing.queueTimesEntityId) {
        updateData.queueTimesEntityId = qtNumericId;
      }

      if (Object.keys(updateData).length > 0) {
        await this.attractionsService
          .getRepository()
          .update(existing.id, updateData);
      }

      // The mapping used to be written on the new-row branch only, so a ride
      // that lost its mapping after it was created never got one back — and
      // losing it is not hypothetical: a merge deletes the row a mapping names
      // and the table has no FK to stop it. Without the mapping
      // `WaitTimesProcessor` cannot resolve this ride's Queue-Times readings
      // at all, because its `externalId` fallback covers `themeparks-wiki`
      // only. `createMapping` is a no-op when the mapping is already correct,
      // so this costs one indexed lookup per ride per sync.
      await this.createMapping(
        existing.id,
        "attraction",
        "queue-times",
        entity.externalId,
      );
    } else {
      // Queue-Times sells a ticket for anything you queue for, so it reports
      // meet & greets and theatre shows as rides. ThemeParks.wiki reports the
      // same thing as a SHOW, and we sync that into `shows`. Creating the ride
      // row here puts one entity in the catalogue twice — and the ride half is
      // the half that starves: `ConflictResolverService.mergeEntities` folds
      // the Queue-Times ATTRACTION into the wiki SHOW of the same name (its
      // key carries no `entityType`), so this row's `qt-ride-…` id never
      // reaches `mappingLookup` and `reconcileMissingAttractions` writes it
      // `system-reconciliation` CLOSED forever, while the show beside it is
      // fed normally.
      //
      // 21 such rows across 6 parks were retired for PAR-161; all 21 had a
      // `shows` row that predated them, the newest by eight months. This park
      // only reaches this branch while it has no `wikiEntityId` — a window,
      // not a state — which is why the rows kept appearing long after the
      // shows were in place.
      //
      // The guard covers creation only. An existing row keeps being updated
      // above, mapping included: dropping a ride that is already fed would
      // strand its Queue-Times readings, which is the failure PAR-311 fixed.
      let showNames = ctx?.showNames;
      if (!showNames) {
        const parkShows = await this.showsService.getRepository().find({
          where: { parkId, retiredAt: IsNull() },
          select: ["name"],
        });
        showNames = new Set(
          parkShows
            .map((s) => normalizeName(s.name ?? ""))
            .filter((n) => n.length > 0),
        );
        if (ctx) ctx.showNames = showNames;
      }

      const normalized = normalizeName(entity.name ?? "");
      if (normalized.length > 0 && showNames.has(normalized)) {
        this.logger.log(
          `⏭️  Not creating Queue-Times attraction "${entity.name}" — this park already carries a show of that name`,
        );
        return;
      }

      // Generate slug
      const baseSlug = generateSlug(entity.name);
      const existingSlugs = existingAttractions.map((a) => a.slug);
      const uniqueSlug = generateUniqueSlug(baseSlug, existingSlugs);

      const newAttraction = await this.attractionsService.getRepository().save({
        externalId: entity.externalId,
        name: entity.name,
        slug: uniqueSlug,
        parkId: parkId,
        latitude: entity.latitude || undefined,
        longitude: entity.longitude || undefined,
        queueTimesEntityId: qtNumericId || null,
      } as any);

      // Create mapping
      await this.createMapping(
        newAttraction.id,
        "attraction",
        "queue-times",
        newAttraction.externalId,
      );
    }
  }

  /**
   * Create external entity mapping (with duplicate check)
   */
  private async createMapping(
    internalEntityId: string,
    internalEntityType: "attraction" | "show" | "restaurant",
    externalSource: string,
    externalEntityId: string,
  ): Promise<void> {
    // Check if mapping already exists
    const existing = await this.mappingRepository.findOne({
      where: {
        externalSource,
        externalEntityId,
      },
    });

    if (!existing) {
      await this.mappingRepository.save({
        internalEntityId,
        internalEntityType,
        externalSource,
        externalEntityId,
        matchConfidence: 1.0,
        matchStrategy: "exact",
      });
      return;
    }

    if (existing.internalEntityId === internalEntityId) return;

    // The row holds our id but names somebody else. Two cases, and only one of
    // them is ours to touch: if that somebody still exists, two live entities
    // are arguing over one upstream id, and that is settled by the writer that
    // sees both sides of the match — `EntityMappingsProcessor`, which upserts
    // on `(externalSource, externalEntityId)`. Not `ParkMetadataProcessor`: its
    // own conflict branch reads the same index, but every one of its call
    // sites passes `internalEntityType: "park"`, so it never re-homes an
    // attraction's id. If the named entity does NOT exist, the row is stranded
    // — the table has no FK, so a merge or a park consolidation that deleted
    // its target left it behind in silence.
    //
    // Returning here, as this method used to, is what makes a stranded row
    // expensive rather than merely untidy: the unique index is on
    // `(external_source, external_entity_id)` alone, so the dead row keeps
    // holding the upstream's id, the live attraction never gets a mapping, and
    // `WaitTimesProcessor` cannot resolve it (its `externalId` fallback covers
    // `themeparks-wiki` only). The ride then gets a permanent
    // `system-reconciliation` CLOSED series while the feed reports it open —
    // measured on 2026-09-18 at 30 rides of Energylandia, every one of them
    // open in the feed at that moment.
    const targetExists = await this.entityExists(
      existing.internalEntityType,
      existing.internalEntityId,
    );
    if (targetExists) return;

    this.logger.warn(
      `🔗 Reclaiming stranded mapping ${externalSource}:${externalEntityId} — it named ${existing.internalEntityId}, which no longer exists`,
    );
    await this.mappingRepository.update(existing.id, {
      internalEntityId,
      internalEntityType,
      matchConfidence: 1.0,
      matchMethod: "exact",
    });
  }

  /**
   * Whether the entity a mapping row names is still in the catalogue.
   *
   * `internal_entity_id` is a `character varying` with no foreign key and
   * `internal_entity_type` decides which of four tables it points into, so
   * this is the only way to ask.
   */
  private async entityExists(
    entityType: ExternalEntityMapping["internalEntityType"],
    internalEntityId: string,
  ): Promise<boolean> {
    // An own-property check, not a truthiness check on the lookup: the column
    // is a `character varying`, so a value outside the four types reaches this
    // — and a bare `CHILD_ENTITY_TABLES[entityType]` would answer
    // `constructor` with a function, which interpolates into the SQL below and
    // takes the sync down with a syntax error instead of leaving the row
    // alone. `Object.hasOwn` needs a lib this build does not target.
    if (!Object.prototype.hasOwnProperty.call(CHILD_ENTITY_TABLES, entityType))
      return true;
    const table = CHILD_ENTITY_TABLES[entityType];

    // `internal_entity_id` is text and the id columns are uuid, so a row whose
    // value is not a uuid at all would raise 22P02 on the cast. Compare as
    // text, exactly as the orphan count in PAR-311 does.
    const rows = await this.mappingRepository.manager.query(
      `SELECT 1 FROM ${table} WHERE id::text = $1 LIMIT 1`,
      [internalEntityId],
    );
    return rows.length > 0;
  }

  /**
   * Sleep utility for batch delays
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
