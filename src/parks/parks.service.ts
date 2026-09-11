import { Inject, Injectable, Logger, forwardRef } from "@nestjs/common";
import { CacheKeys } from "../common/cache/cache-keys";
import { safeJsonParse } from "../common/utils/json.util";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, In, IsNull } from "typeorm";
import { Park } from "./entities/park.entity";
import { ScheduleEntry, ScheduleType } from "./entities/schedule-entry.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Show } from "../shows/entities/show.entity";
import { Restaurant } from "../restaurants/entities/restaurant.entity";
import { ThemeParksClient } from "../external-apis/themeparks/themeparks.client";
import { ThemeParksMapper } from "../external-apis/themeparks/themeparks.mapper";
import { DestinationsService } from "../destinations/destinations.service";
import { HolidaysService } from "../holidays/holidays.service";
import { generateSlug, generateUniqueSlug } from "../common/utils/slug.util";
import { normalizeSortDirection } from "../common/utils/query.util";
import {
  formatInParkTimezone,
  getCurrentDateInTimezone,
  getStartOfDayInTimezone,
  getTomorrowDateInTimezone,
} from "../common/utils/date.util";
import { addDays, subDays } from "date-fns";
import { normalizeRegionCode } from "../common/utils/region.util";
import {
  correctTwelveHourClockClose,
  normalizeClosingTime,
} from "../common/utils/operating-window.util";
import {
  calculateHolidayInfo,
  HolidayEntry,
} from "../common/utils/holiday.utils";
import {
  calculateParkPriority,
  findDuplicatePark,
  hasScheduleData,
  hasRecentQueueData,
} from "./utils/park-merge.util";
import {
  applyMergeDependencies,
  ATTRACTION_DEPENDENCIES,
  MergeDependency,
  PARK_CHILD_ENTITIES,
  PARK_DEPENDENCIES,
  PARK_INLINE_DEPENDENCIES,
} from "./utils/merge-dependencies";
import { captureParkPath, samePath } from "./services/park-rename.service";
import {
  isParkOpen,
  RideStatusData,
} from "../common/utils/status-calculator.util";

import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { NegativeCache } from "../common/utils/negative-cache.util";

/**
 * Input shape for saveScheduleData. Covers both the ThemeParks.wiki schedule
 * API entries and the ad-hoc updates built by the wartezeiten/wait-times
 * processors.
 */
export interface ScheduleSyncEntry {
  date: string | Date;
  type?: string;
  openingTime?: string | Date | null;
  closingTime?: string | Date | null;
  description?: string | null;
  purchases?: ScheduleEntry["purchases"];
}

/**
 * Aborts the priority merge's transaction when its losing park still holds a
 * child row, so the reparenting rolls back with the DELETE that could not
 * happen. Caught by `mergePriorityDuplicateLoserSafely` and nowhere else — it
 * is a rollback signal, not a failure of the sync run around it.
 */
class PriorityMergeIncompleteError extends Error {}

@Injectable()
export class ParksService {
  private readonly logger = new Logger(ParksService.name);
  private readonly TTL_SCHEDULE = 60 * 60; // 1 hour - park schedules

  /** Negative cache for geographic path lookups that returned null (404).
   *  Key: "continent:country:city:slug". */
  private readonly notFoundCache = new NegativeCache();

  constructor(
    @InjectRepository(Park)
    private parkRepository: Repository<Park>,
    @InjectRepository(ScheduleEntry)
    private scheduleRepository: Repository<ScheduleEntry>,
    private themeParksClient: ThemeParksClient,
    private themeParksMapper: ThemeParksMapper,
    private destinationsService: DestinationsService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    @Inject(forwardRef(() => HolidaysService))
    private holidaysService: HolidaysService,
  ) {}

  /**
   * Syncs all parks from ThemeParks.wiki
   *
   * Strategy:
   * 1. Ensure destinations are synced first
   * 2. Fetch full entity data for each park
   * 3. Map and save to DB
   */
  async syncParks(): Promise<number> {
    this.logger.log("Syncing parks from ThemeParks.wiki...");

    // Ensure destinations are synced first
    const { data: destinations } = await this.destinationsService.findAll(
      1,
      1000,
    );

    if (destinations.length === 0) {
      this.logger.warn("No destinations found. Syncing destinations first...");
      await this.destinationsService.syncDestinations();
    }

    // Fetch full park data
    const apiResponse = await this.themeParksClient.getDestinations();
    let syncedCount = 0;

    for (const apiDestination of apiResponse.destinations) {
      // Find our DB destination
      const destination = await this.destinationsService.findByExternalId(
        apiDestination.id,
      );

      if (!destination) {
        this.logger.warn(
          `Destination ${apiDestination.id} not found in DB, skipping parks`,
        );
        continue;
      }

      const destinationParks = await this.parkRepository.find({
        where: { destinationId: destination.id },
      });
      const parksByExternalId = new Map(
        destinationParks.map((p) => [p.externalId, p]),
      );

      for (const parkSummary of apiDestination.parks) {
        // Fetch full park entity data
        const parkEntity = await this.themeParksClient.getEntity(
          parkSummary.id,
        );

        const mappedData = this.themeParksMapper.mapPark(
          parkEntity,
          destination.id,
        );

        // Check if park exists (by externalId)
        let existing = mappedData.externalId
          ? (parksByExternalId.get(mappedData.externalId) ??
            (await this.parkRepository.findOne({
              where: { externalId: mappedData.externalId },
            })))
          : null;

        // If not found by externalId, check for potential duplicates by name
        if (!existing) {
          const allParks = destinationParks;

          const duplicate = findDuplicatePark(
            mappedData.name!,
            allParks,
            0.9, // 90% similarity threshold
          );

          if (duplicate) {
            // Silent duplicate detection - log only if merging

            // Check which park has schedule data AND queue data
            const duplicateHasSchedule = await hasScheduleData(
              duplicate.id,
              this.scheduleRepository,
            );

            // Check for recent queue data (strongest signal of active park)
            const duplicateHasQueueData = await hasRecentQueueData(
              duplicate.id,
              this.parkRepository.manager,
            );

            const duplicatePriority = calculateParkPriority(
              duplicate,
              duplicateHasSchedule,
              duplicateHasQueueData,
            );

            // Calculate priority for new park (assume no schedule/queue data yet)
            const newParkPriority = calculateParkPriority(
              { ...duplicate, ...mappedData } as Park,
              false,
              false,
            );

            // TRUE MERGE: Consolidate external IDs from both parks
            // Winner keeps its data + inherits missing IDs from loser
            // This ensures we get both schedule data AND queue-times capabilities
            if (duplicatePriority >= newParkPriority) {
              // Keep existing park, merge new data into it (silent)

              // Consolidate ALL external IDs from both sources
              const updateData: Partial<Park> = {};

              // Add Wiki ID if missing
              if (mappedData.wikiEntityId && !duplicate.wikiEntityId) {
                updateData.wikiEntityId = mappedData.wikiEntityId;
              }

              // Add Queue-Times ID if missing (enables queue data)
              if (
                mappedData.queueTimesEntityId &&
                !duplicate.queueTimesEntityId
              ) {
                updateData.queueTimesEntityId = mappedData.queueTimesEntityId;
              }

              // Update other fields to latest data
              updateData.name = mappedData.name;
              updateData.latitude = mappedData.latitude;
              updateData.longitude = mappedData.longitude;
              updateData.timezone = mappedData.timezone;

              if (Object.keys(updateData).length > 0) {
                await this.parkRepository.update(duplicate.id, updateData);
              }

              // TRUE MERGE: Migrate child entities (shows, restaurants)
              //
              // `losingPark` is provably `null` on every reachable run, and the
              // comment that used to sit here read that the wrong way round
              // ("would have been found at the externalId check above; null here
              // is safe"). It is not an accident that it is null, it is the
              // condition of standing here at all: `existing` (see the lookup
              // above this `if (!existing)` block) is set from this very map,
              // falling back to a global `findOne` on the same externalId. A hit
              // in either skips this branch, so reaching it means both missed,
              // and the map cannot then answer. The branch is entered *because*
              // no park carries this externalId — so there is no losing park to
              // merge away, and `mergePriorityDuplicateLoser` never runs.
              //
              // It is still written as if it did. The block below used to delete
              // a park outside any transaction and without moving what points at
              // it; three sibling issues are rebuilding the same file, and one
              // change to the lookup above turns that into a live destructive
              // path. Whether the block should exist at all is a separate
              // question (PAR-142) — while it exists, it behaves like the other
              // two raw merge paths.
              const losingPark =
                (mappedData.externalId &&
                  parksByExternalId.get(mappedData.externalId)) ||
                null;

              if (losingPark && losingPark.id !== duplicate.id) {
                await this.mergePriorityDuplicateLoserSafely(
                  duplicate,
                  losingPark,
                );
              }

              existing = duplicate;
            } else {
              // New park has higher priority -  silent merge

              // Inherit IDs from duplicate if new park doesn't have them
              if (duplicate.wikiEntityId && !mappedData.wikiEntityId) {
                mappedData.wikiEntityId = duplicate.wikiEntityId;
              }
              if (
                duplicate.queueTimesEntityId &&
                !mappedData.queueTimesEntityId
              ) {
                mappedData.queueTimesEntityId = duplicate.queueTimesEntityId;
              }

              // In this case, we'd need to migrate FROM duplicate TO the new park
              // But we're setting existing = duplicate, so the new park won't be created
              // This is the correct behavior: Keep duplicate, skip creating new park
              existing = duplicate;
            }
          }
        }

        if (existing) {
          // Update existing park (keep existing slug)
          await this.parkRepository.update(existing.id, {
            name: mappedData.name,
            latitude: mappedData.latitude,
            longitude: mappedData.longitude,
            timezone: mappedData.timezone,
          });

          // Check for "Ghost Parks" (duplicates by Queue-Times ID)
          // This fixes the "Split Brain" issue where we have one park from Wiki and another from Queue-Times
          const qtId =
            mappedData.queueTimesEntityId || existing.queueTimesEntityId;

          if (qtId) {
            // Look for any OTHER park that has this Queue-Times ID
            // or has an externalId matching 'qt-{id}'
            const ghostPark = await this.parkRepository
              .createQueryBuilder("park")
              .where("park.id != :currentId", { currentId: existing.id })
              .andWhere(
                "(park.queue_times_entity_id = :qtId OR park.externalId = :qtExternalId)",
                {
                  qtId: qtId,
                  qtExternalId: `qt-${qtId}`,
                },
              )
              .getOne();

            if (ghostPark) {
              this.logger.log(
                `👻 Found Ghost Park "${ghostPark.name}" (ID: ${ghostPark.id}) matching Queue-Times ID ${qtId}`,
              );
              this.logger.log(
                `🔀 Merging Ghost Park "${ghostPark.name}" into "${existing.name}"`,
              );

              // Migrate child entities with collision handling
              await this.parkRepository.manager.transaction(
                async (transactionalEntityManager) => {
                  await this.liftTimescaleDecompressionLimit(
                    transactionalEntityManager,
                  );

                  // 1. Handle Attraction Collisions
                  // Fetch attractions from both parks in one query, then split
                  type GhostAttractionRow = {
                    id: string;
                    parkId: string;
                    slug: string;
                    queue_times_entity_id: string | null;
                    land_name: string | null;
                    land_external_id: string | null;
                  };
                  const bothParkAttractions: GhostAttractionRow[] =
                    await transactionalEntityManager.query(
                      `SELECT id, "parkId", slug, "queue_times_entity_id", "land_name", "land_external_id" FROM attractions WHERE "parkId" = ANY($1::uuid[])`,
                      [[existing.id, ghostPark.id]],
                    );
                  const existingAttractions = bothParkAttractions.filter(
                    (a) => a.parkId === existing.id,
                  );
                  const ghostAttractions = bothParkAttractions.filter(
                    (a) => a.parkId === ghostPark.id,
                  );

                  // Partition once, then run batched statements instead of
                  // 1-2 queries per ghost attraction.
                  const existingBySlug = new Map<string, GhostAttractionRow>(
                    existingAttractions.map((a) => [a.slug, a]),
                  );
                  const collisions: Array<{
                    ghost: GhostAttractionRow;
                    matchId: string;
                  }> = [];
                  const movedIds: string[] = [];

                  for (const ghostAttr of ghostAttractions) {
                    const match = existingBySlug.get(ghostAttr.slug);
                    if (match) {
                      collisions.push({ ghost: ghostAttr, matchId: match.id });
                    } else {
                      movedIds.push(ghostAttr.id);
                    }
                  }

                  if (collisions.length > 0) {
                    // COLLISION: Merge data into existing attractions, then delete
                    // the ghosts. We specifically want the Land Info and
                    // Queue-Times ID from each ghost.
                    const values: string[] = [];
                    const params: Array<string | null> = [];
                    collisions.forEach(({ ghost, matchId }, i) => {
                      const o = i * 4;
                      values.push(
                        `($${o + 1}::uuid, $${o + 2}, $${o + 3}, $${o + 4})`,
                      );
                      params.push(
                        matchId,
                        ghost.land_name,
                        ghost.land_external_id,
                        ghost.queue_times_entity_id,
                      );
                    });
                    // `last_merged_at` is stamped on every survivor in the same
                    // statement, and unconditionally, exactly as
                    // `ParkMergeService.consolidateEntities` and
                    // `AttractionMergeService.merge` do: the merge happened
                    // whether or not this particular row inherited a column.
                    // There are two seams, and both need the stamp. The ghost's
                    // `queue_data` is reparented onto the survivor below, so its
                    // history becomes two interleaved series flapping between
                    // OPERATING and DOWN at the same instant; and the survivor
                    // inherits the Queue-Times id, so from this moment that feed
                    // writes into a row the wiki feed has been writing into all
                    // along. The nightly downtime reconstruction reads either as
                    // genuine outages unless the ride is held out until the
                    // stamp ages out.
                    await transactionalEntityManager.query(
                      `UPDATE attractions a
                       SET "land_name" = COALESCE(v.land_name, a."land_name"),
                           "land_external_id" = COALESCE(v.land_external_id, a."land_external_id"),
                           "queue_times_entity_id" = COALESCE(v.qt_id, a."queue_times_entity_id"),
                           "last_merged_at" = NOW()
                       FROM (VALUES ${values.join(", ")}) AS v(id, land_name, land_external_id, qt_id)
                       WHERE a.id = v.id`,
                      params,
                    );
                    // Every dependent row of each ghost moves onto its
                    // survivor before the ghost is deleted. Without this the
                    // DELETE below raises 23503 and takes the whole sync run
                    // with it — `repairDuplicates()` is awaited unguarded at
                    // the end of `syncParks`.
                    await this.consolidateMergedAttractions(
                      transactionalEntityManager,
                      collisions.map((c) => ({
                        winnerId: c.matchId,
                        loserId: c.ghost.id,
                      })),
                    );
                    await transactionalEntityManager.query(
                      `DELETE FROM attractions WHERE id = ANY($1::uuid[])`,
                      [collisions.map((c) => c.ghost.id)],
                    );
                    this.logger.log(
                      `    Merged attraction data: ${collisions
                        .map((c) => c.ghost.slug)
                        .join(", ")}`,
                    );
                  }

                  if (movedIds.length > 0) {
                    // NO COLLISION: Move attractions to the surviving park
                    await transactionalEntityManager.query(
                      `UPDATE attractions SET "parkId" = $1 WHERE id = ANY($2::uuid[])`,
                      [existing.id, movedIds],
                    );
                  }

                  // 2./3. Shows and restaurants, partitioned by slug against
                  // their own unique `(parkId, slug)` — see
                  // `migrateParkChildEntities`. A blind move here used to raise
                  // 23505 and take the two steps below with it.
                  await this.migrateParkChildEntities(
                    transactionalEntityManager,
                    existing.id,
                    ghostPark.id,
                  );

                  // 4. Move everything else the ghost park owns, and keep the
                  // path it was served under alive. Without the first the
                  // DELETE below raises 23503 on park_occupancy and the
                  // inherited rides arrive filed under a park id that is about
                  // to stop existing; without the second the ghost's own URLs
                  // 404 instead of redirecting.
                  await this.consolidateMergedPark(
                    transactionalEntityManager,
                    existing,
                    ghostPark,
                  );

                  // 5. Delete the ghost park
                  await transactionalEntityManager.delete(Park, ghostPark.id);
                },
              );
              this.logger.log(`✅ Ghost Park merged and deleted successfully.`);
            }
          }
        } else {
          // Generate unique slug for this destination
          const baseSlug = mappedData.slug || generateSlug(mappedData.name!);

          // Get all existing slugs for this destination
          const existingParks = await this.parkRepository.find({
            where: { destinationId: destination.id },
            select: ["slug"],
          });
          const existingSlugs = existingParks.map((p) => p.slug);

          // Generate unique slug
          const uniqueSlug = generateUniqueSlug(baseSlug, existingSlugs);
          mappedData.slug = uniqueSlug;

          // Insert new park
          try {
            await this.parkRepository.save(mappedData);
          } catch (error: unknown) {
            // Handle race condition where another sync created the park
            if (
              error instanceof Error &&
              error.message.includes("duplicate key")
            ) {
              this.logger.warn(
                `Park ${mappedData.name} already exists (race condition), updating instead`,
              );
              // Refetch and update
              const refetched = await this.parkRepository.findOne({
                where: { externalId: mappedData.externalId },
              });
              if (refetched) {
                await this.parkRepository.update(refetched.id, {
                  name: mappedData.name,
                  latitude: mappedData.latitude,
                  longitude: mappedData.longitude,
                  timezone: mappedData.timezone,
                });
              }
            } else {
              throw error;
            }
          }
        }

        syncedCount++;
      }
    }

    this.logger.log(`✅ Synced ${syncedCount} parks`);

    // Run self-repair to clean up any duplicates (e.g. Wiki vs Queue-Times split)
    await this.repairDuplicates();

    return syncedCount;
  }

  /**
   * Reparents every dependent row of a losing attraction onto the survivor, for
   * the two raw merge paths in this file.
   *
   * Both used to hand-roll this, and both were wrong in the same three ways:
   * `repairDuplicates` moved 3 of the tables in `ATTRACTION_DEPENDENCIES` and
   * wrote `prediction_accuracy."attractionId"`, a column that does not exist
   * (42703); the collision block in `syncParks` moved nothing at all and
   * trusted the losing row's history to disappear with it. It does not:
   * `queue_data`, `wait_time_predictions`, `prediction_accuracy` and
   * `ml_prediction_anomalies` declare `@ManyToOne(() => Attraction)` with no
   * `onDelete`, so the FK is NO ACTION and the `DELETE FROM attractions` raises
   * 23503 instead of leaving orphans behind. Either way the transaction rolled
   * back, which is why the `last_merged_at` stamp both paths write could never
   * commit.
   *
   * `ParkMergeService.consolidateEntityData` is the shape this follows, down to
   * moving `external_entity_mapping` first. That table carries no FK, so an
   * orphaned row survives the DELETE in silence, and the reader that matters
   * never sees it: `wait-times.processor.ts` loads mappings by
   * `internalEntityId IN (<the park's live attractions>)`, so a
   * `queue-times:<id>` row still naming the deleted ghost drops that ride's
   * Queue-Times readings until the `entity-mappings` job re-upserts it. The
   * reference deletes colliding rows before the move; that is dead code here,
   * because the unique index is on `(external_source, external_entity_id)`
   * alone, so a pair the winner holds cannot also sit on the loser.
   *
   * The TimescaleDB decompression limit belongs to the transaction, not to
   * this method — see `liftTimescaleDecompressionLimit`.
   */
  private async consolidateMergedAttractions(
    manager: { query: (sql: string, params?: unknown[]) => Promise<unknown> },
    pairs: Array<{ winnerId: string; loserId: string }>,
  ): Promise<void> {
    await this.consolidateMergedEntities(
      manager,
      ATTRACTION_DEPENDENCIES,
      pairs,
    );
  }

  /**
   * The body of `consolidateMergedAttractions`, for any entity type that has a
   * dependency list: mapping first, then the declared tables.
   *
   * Shows and restaurants reach it through `migrateParkChildEntities`. Nothing
   * about the sequence is attraction-specific — the mapping table is keyed on
   * `internal_entity_id` for every entity type at once, and `external_source`
   * plus `external_entity_id` are unique across all of them, so a pair the
   * winner already holds cannot also sit on the loser and no conflict delete is
   * needed for any of the three.
   */
  private async consolidateMergedEntities(
    manager: { query: (sql: string, params?: unknown[]) => Promise<unknown> },
    dependencies: MergeDependency[],
    pairs: Array<{ winnerId: string; loserId: string }>,
  ): Promise<void> {
    if (pairs.length === 0) return;

    for (const { winnerId, loserId } of pairs) {
      await manager.query(
        `UPDATE external_entity_mapping SET "internal_entity_id" = $1 WHERE "internal_entity_id" = $2`,
        [winnerId, loserId],
      );
      await applyMergeDependencies(manager, dependencies, winnerId, loserId);
    }
  }

  /**
   * Moves a losing park's shows and restaurants onto the survivor, resolving
   * the ones both parks know by the same slug instead of walking into the
   * unique index.
   *
   * Both raw merge paths used to issue a blind
   * `UPDATE shows SET "parkId" = $1 WHERE "parkId" = $2` and the same for
   * restaurants, under a comment that admitted it: "Blind update OK if slugs
   * distinctive, else duplicate logic needed? mostly safe for now". It is not.
   * Both tables carry `@Index(["parkId", "slug"], { unique: true })`, and a
   * ghost park is by definition the same park from a second source — so
   * `aquanura` on both rows is the ordinary case, the UPDATE raises **23505**,
   * and the transaction rolls back **before** the attraction step and the park
   * step have run at all. Everything PAR-90 and PAR-99 fixed one and two levels
   * down was therefore unreachable on exactly the merges that had something to
   * merge.
   *
   * Matching is by slug alone, like the attraction partition above it and
   * unlike `ParkMergeService.migrateEntities`, which also matches on name. The
   * slug is what the constraint is about; two rows the database is willing to
   * keep apart are a curation question, and that path has a person behind it.
   *
   * What happens to a losing show's dependent rows is `SHOW_DEPENDENCIES`, and
   * it is the answer to the part of this that is not a constraint violation:
   * `show_live_data` and `show_follows` cascade, so deleting the losing row
   * without them destroys its showtime history and somebody's push reminder
   * inside a transaction that reports success; `show_schedule_patterns` has no
   * FK and would be left pointing at a row that is gone. Restaurants have
   * `restaurant_live_data` and nothing else.
   *
   * Callers must hold a transaction and must have lifted the TimescaleDB
   * decompression cap — both live-data tables are hypertables. Runs before
   * `consolidateMergedPark`, because that is where the park row's own
   * dependents move and this must not still be adding rows to the loser.
   */
  private async migrateParkChildEntities(
    manager: { query: (sql: string, params?: unknown[]) => Promise<unknown> },
    winnerParkId: string,
    loserParkId: string,
  ): Promise<void> {
    for (const { table, dependencies } of PARK_CHILD_ENTITIES) {
      const rows = (await manager.query(
        `SELECT id, "parkId", slug FROM ${table} WHERE "parkId" = ANY($1::uuid[])`,
        [[winnerParkId, loserParkId]],
      )) as Array<{ id: string; parkId: string; slug: string }>;

      const winnerBySlug = new Map<string, string>(
        rows
          .filter((row) => row.parkId === winnerParkId)
          .map((row) => [row.slug, row.id]),
      );

      const collisions: Array<{ winnerId: string; loserId: string }> = [];
      const movedIds: string[] = [];
      for (const row of rows.filter((r) => r.parkId === loserParkId)) {
        const match = winnerBySlug.get(row.slug);
        if (match) {
          collisions.push({ winnerId: match, loserId: row.id });
        } else {
          movedIds.push(row.id);
        }
      }

      if (collisions.length > 0) {
        await this.consolidateMergedEntities(manager, dependencies, collisions);
        await manager.query(`DELETE FROM ${table} WHERE id = ANY($1::uuid[])`, [
          collisions.map((c) => c.loserId),
        ]);
        this.logger.log(
          `    Merged ${collisions.length} colliding ${table} into the survivor`,
        );
      }

      // By id, never by `parkId`: the rows that collided are gone, but a second
      // statement scoped to the losing park would be exactly the blind update
      // this method exists to replace if one were ever inserted between the
      // SELECT and here.
      if (movedIds.length > 0) {
        await manager.query(
          `UPDATE ${table} SET "parkId" = $1 WHERE id = ANY($2::uuid[])`,
          [winnerParkId, movedIds],
        );
      }
    }
  }

  /**
   * Lifts the TimescaleDB decompression cap for the rest of the transaction.
   *
   * The docstring of `applyMergeDependencies` puts this on the caller, and a
   * merge needs it twice over: `queue_data` on the attraction side and
   * `park_occupancy` on the park side are both hypertables, and a merge moves
   * far more than the 100000 compressed tuples one statement may decompress by
   * default.
   *
   * Issued once, at the top of the merge transaction, rather than inside either
   * consolidation step: the park step runs whether or not a single attraction
   * collided, so hanging the GUC off the collision path would leave the park's
   * own hypertable capped on exactly the merges that have least to move.
   *
   * `SET LOCAL`, unlike the plain `SET` the two merge services use: it ends
   * with the transaction either way, so there is no reset statement to leak a
   * hard-coded 100000 onto a pooled connection, and none to answer an
   * already-aborted transaction with 25P02 and bury the error that caused it.
   */
  private async liftTimescaleDecompressionLimit(manager: {
    query: (sql: string, params?: unknown[]) => Promise<unknown>;
  }): Promise<void> {
    await manager.query(
      "SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0",
    );
  }

  /**
   * Reparents every park-scoped row of a ghost park onto the survivor, for the
   * two raw merge paths in this file. Runs after the attractions, shows and
   * restaurants have moved and immediately before `DELETE FROM parks`.
   *
   * Both paths used to go straight from the last `UPDATE restaurants` to
   * `manager.delete(Park, ghostPark.id)`, which is the same failure class as
   * the attraction side one level up, with the same two halves:
   *
   * `park_occupancy` declares `@ManyToOne(() => Park)` with no `onDelete`, so
   * its FK is NO ACTION and the park DELETE raises **23503** — one statement
   * later than the attraction DELETE used to, and just as fatal, because
   * `syncParks` awaits `repairDuplicates()` unguarded. `attraction_p50_baselines`
   * and `attraction_p90_baselines` carry a denormalised `parkId` under the same
   * NO ACTION rule and do it for every ride that moved across without colliding.
   *
   * Where it does not abort, it loses things quietly. The merge moves
   * `attractionId` and never the `parkId` beside it, so the inherited history in
   * `attraction_hourly_history` and `queue_data_aggregates` ends up filed under
   * a park id that no longer exists — and both are read by park id
   * (`analytics.service.ts` `WHERE ahh."parkId" = $1::uuid`,
   * `park-historical-stats.service.ts` on `qda."parkId"`), so the survivor's
   * statistics never show what the merge existed to preserve. `park_seasons`
   * and `park_slug_aliases` cascade away with the row, and so do the schedule,
   * the daily stats, the weather and the headliner set.
   *
   * That answers the one question the ticket left open about
   * `attraction_rope_drop` and `attraction_typical_waits`: today they cascade
   * out with the ghost park, and `PARK_DEPENDENCIES` has said `move` about both
   * for as long as it has existed. Applying it here is what makes that true on
   * these two paths as well — the survivor keeps a rope-drop tip and a P50/P90
   * pair for every ride it inherited, instead of the ride arriving with its
   * history and none of its published numbers.
   *
   * `ParkMergeService.mergeParks` is the shape this follows: its steps 3-4
   * (here `PARK_INLINE_DEPENDENCIES` plus the two below), its step 5b
   * (`PARK_DEPENDENCIES`) and its step 5c (the ghost's own path), in that
   * order. The order is load-bearing twice over — the attraction consolidation
   * above discards the losing ride's own `attraction_p50_baselines` row before
   * this moves the surviving one's `parkId`, so the two never race for the same
   * row; and 5b moves the ghost's existing aliases before 5c adds the path it
   * was itself served under, so a ghost that had already been renamed once
   * arrives with its whole history rather than only its last URL.
   *
   * Takes the two parks rather than their ids because step 5c needs the four
   * slugs that make up the ghost's public path, and they exist nowhere but on
   * the row that is about to be deleted.
   */
  private async consolidateMergedPark(
    manager: { query: (sql: string, params?: unknown[]) => Promise<unknown> },
    winner: Park,
    loser: Park,
  ): Promise<void> {
    const winnerParkId = winner.id;
    const loserParkId = loser.id;

    // Park-level mappings. No FK, so an orphan here survives the DELETE in
    // silence and the sync stops recognising the feed it came from. The
    // `internal_entity_type` filter is what `mergeParks` writes and costs
    // nothing; ids are unique across entity types, so it changes no row either
    // way, but it says which rows are meant.
    await manager.query(
      `UPDATE external_entity_mapping SET "internal_entity_id" = $1
       WHERE "internal_entity_id" = $2 AND "internal_entity_type" = 'park'`,
      [winnerParkId, loserParkId],
    );

    // `park_p50_baselines` is winner-authoritative rather than move-or-discard,
    // which is why it is not a `MergeDependency`: the row is one per park and
    // load-bearing (live crowd levels and an ML feature both read it), so the
    // survivor's own always wins — but where the survivor has none, inheriting
    // the ghost's beats rating nothing until the next baseline run. Same rule
    // `mergeParks` applies with `migrateTableData(..., null)`.
    const winnerBaseline = await manager.query(
      `SELECT 1 FROM park_p50_baselines WHERE "parkId" = $1 LIMIT 1`,
      [winnerParkId],
    );
    if (Array.isArray(winnerBaseline) && winnerBaseline.length > 0) {
      await manager.query(
        `DELETE FROM park_p50_baselines WHERE "parkId" = $1`,
        [loserParkId],
      );
    } else {
      await manager.query(
        `UPDATE park_p50_baselines SET "parkId" = $1 WHERE "parkId" = $2`,
        [winnerParkId, loserParkId],
      );
    }

    // `schedule_entries` holds park-level rows (`attractionId IS NULL`, the
    // park's opening hours) and per-ride rows in the same table. Two rows are
    // the same statement only if all three of date, type and ride agree, and
    // the ride is nullable — which is why this is not a `MergeDependency`:
    // `applyMergeDependencies` compares its conflict key with a row-wise `IN`,
    // and `(date, type, NULL) IN (SELECT date, type, NULL …)` is NULL, not
    // true. A key of (date, scheduleType) alone reads across the difference and
    // deletes the ghost's whole per-ride schedule whenever the survivor has any
    // row for that day; `IS NOT DISTINCT FROM` compares the three as written.
    // Nothing here is a constraint — the PK is a surrogate id — so the delete
    // exists only to stop one park holding a day twice.
    await manager.query(
      `DELETE FROM schedule_entries loser
       WHERE loser."parkId" = $2
         AND EXISTS (
           SELECT 1 FROM schedule_entries winner
           WHERE winner."parkId" = $1
             AND winner."date" = loser."date"
             AND winner."scheduleType" = loser."scheduleType"
             AND winner."attractionId" IS NOT DISTINCT FROM loser."attractionId"
         )`,
      [winnerParkId, loserParkId],
    );
    await manager.query(
      `UPDATE schedule_entries SET "parkId" = $1 WHERE "parkId" = $2`,
      [winnerParkId, loserParkId],
    );

    await applyMergeDependencies(
      manager,
      PARK_INLINE_DEPENDENCIES,
      winnerParkId,
      loserParkId,
    );
    await applyMergeDependencies(
      manager,
      PARK_DEPENDENCIES,
      winnerParkId,
      loserParkId,
    );

    // The ghost's own path was live and indexed. It is not a hypothetical URL:
    // the park stood in the database with its own four slugs, so it stood in
    // the sitemap, so it stood in the index — `mergeParks` names the case it
    // was written for, the Tampa row for Universal Islands of Adventure serving
    // an empty page in six locales. The rows in `park_slug_aliases` move above;
    // this path exists nowhere but as columns on the row the caller deletes one
    // statement later, so without this every one of those URLs answers 404
    // instead of redirecting to the survivor.
    //
    // `mergeParks` step 5c, and deliberately the same two guards rather than a
    // second reading of them: no alias where either park is missing a slug
    // (`captureParkPath` returns null), and none where both were served under
    // the same path (`samePath`) — pointing a path at the park that already
    // answers it would only add a row the lookup has to step over.
    const loserPath = captureParkPath(loser);
    const winnerPath = captureParkPath(winner);
    if (loserPath && winnerPath && !samePath(loserPath, winnerPath)) {
      // What `orIgnore()` compiles to in `mergeParks`, written out because this
      // method speaks raw SQL throughout. It is not decoration: the path is
      // unique across the table, and a survivor that already carries this exact
      // path — from an earlier rename, or from a repair run that merged the
      // same pair before — would otherwise raise 23505 and take the whole sync
      // transaction with it, `repairDuplicates()` being awaited unguarded at
      // the end of `syncParks`.
      await manager.query(
        `INSERT INTO park_slug_aliases ("parkId", "continentSlug", "countrySlug", "citySlug", "slug")
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT DO NOTHING`,
        [
          winnerParkId,
          loserPath.continentSlug,
          loserPath.countrySlug,
          loserPath.citySlug,
          loserPath.slug,
        ],
      );
    }
  }

  /**
   * The third raw merge path: `syncParks`' priority merge, where a name
   * duplicate outranks the incoming park and absorbs it.
   *
   * **This never runs today.** Its caller reaches the branch only when no park
   * carries the incoming `externalId`, and the losing park is looked up by that
   * same `externalId` — see the comment at the call site for the three cases.
   * Whether the block should exist at all is PAR-142; what it may not be while
   * it does exist is a `DELETE FROM parks` that behaves worse than the two paths
   * beside it, on a file three open issues are rebuilding.
   *
   * It used to be four statements on `parkRepository.manager` and a
   * `parkRepository.delete()`, with no transaction between them, and it was
   * wrong in the two ways the other paths were wrong before PAR-99:
   *
   * The DELETE raises **23503**. `park_occupancy`, `attraction_p50_baselines`
   * and `attraction_p90_baselines` declare `@ManyToOne(() => Park)` with no
   * `onDelete`, so their FK is NO ACTION. Where it does get through,
   * `park_seasons`, `park_slug_aliases`, `schedule_entries`, `weather_data`,
   * `park_daily_stats`, `attraction_rope_drop`, `attraction_typical_waits` and
   * `attraction_ride_profiles` cascade away — three of them hand-curated and
   * written by no feed in this codebase. `consolidateMergedPark` is what answers
   * both, and it has to run before the park row goes.
   *
   * And **without a transaction the failure is not a no-op.** The child moves
   * were committed one statement at a time, so a 23503 on the DELETE left a
   * losing park stripped of its shows, restaurants and rides but still present
   * and still served. The next sync could not even see it: `isEmpty` is true
   * again, so it retried the same DELETE and got the same 23503, for ever. One
   * transaction is the whole fix for that — either the moves and the DELETE
   * commit together, or neither does.
   *
   * **Attractions need collision handling, and that is a constraint rather than
   * a preference.** `attractions` carries `@Index(["parkId", "slug"], { unique:
   * true })`, so a blind `UPDATE attractions SET "parkId"` raises **23505** the
   * moment both parks know a ride by the same slug — which is the normal case
   * for two rows describing one park. The ghost path one level up already
   * partitions by slug; this does the same, and hands the colliding losers to
   * `consolidateMergedAttractions` so their `queue_data` and predictions land on
   * the survivor instead of blocking its DELETE.
   *
   * `shows` and `restaurants` stay blind moves here, exactly as they are on the
   * other two paths: they have the same unresolved collision question, and it is
   * PAR-104's, not this one's.
   */
  private async mergePriorityDuplicateLoser(
    winner: Park,
    loser: Park,
  ): Promise<void> {
    // Both sibling paths establish this before they call anything: the ghost
    // lookup filters `park.id != :currentId`, and `repairDuplicates` builds its
    // ghost list with `filter((p) => p.id !== primary.id)`. Here it would be
    // the call site's inequality test, which is one edit away from a park that
    // merges into itself — and that case is not a no-op. Every ride would
    // collide with itself, so the whole park's attractions would be handed to
    // `consolidateMergedAttractions` as their own losers and then deleted, one
    // statement before the park.
    if (winner.id === loser.id) return;

    await this.parkRepository.manager.transaction(
      async (transactionalEntityManager) => {
        await this.liftTimescaleDecompressionLimit(transactionalEntityManager);

        // 1. Attractions, partitioned by slug against the unique index.
        type PriorityMergeAttractionRow = {
          id: string;
          parkId: string;
          slug: string;
          queue_times_entity_id: string | null;
          land_name: string | null;
          land_external_id: string | null;
        };
        const bothParkAttractions: PriorityMergeAttractionRow[] =
          await transactionalEntityManager.query(
            `SELECT id, "parkId", slug, "queue_times_entity_id", "land_name", "land_external_id" FROM attractions WHERE "parkId" = ANY($1::uuid[])`,
            [[winner.id, loser.id]],
          );
        const winnerBySlug = new Map<string, PriorityMergeAttractionRow>(
          bothParkAttractions
            .filter((a) => a.parkId === winner.id)
            .map((a) => [a.slug, a]),
        );

        const collisions: Array<{
          loserRow: PriorityMergeAttractionRow;
          matchId: string;
        }> = [];
        const movedIds: string[] = [];
        for (const loserAttr of bothParkAttractions.filter(
          (a) => a.parkId === loser.id,
        )) {
          const match = winnerBySlug.get(loserAttr.slug);
          if (match) {
            collisions.push({ loserRow: loserAttr, matchId: match.id });
          } else {
            movedIds.push(loserAttr.id);
          }
        }

        if (collisions.length > 0) {
          // `COALESCE(v.x, a.x)` — the LOSER's land info and Queue-Times id win
          // where it has them, and the survivor's value is the fallback. That is
          // the ghost path's expression verbatim, and the two existing paths do
          // not agree with each other about it: `repairDuplicates` writes
          // `COALESCE(a.x, $1)` and keeps the survivor's. This follows the ghost
          // path because it is the same branch of `syncParks` and the loser is
          // the fresher read of the two. Reconciling the two precedences is a
          // decision about both of them, not a side effect of adding a third.
          //
          // `last_merged_at` is unconditional, as on both other paths: the
          // losing row's `queue_data` is reparented onto the survivor below, so
          // its history becomes two interleaved series flapping between
          // OPERATING and DOWN at the same instant, and the stamp is what holds
          // the ride out of the nightly downtime reconstruction until it ages
          // out.
          const values: string[] = [];
          const params: Array<string | null> = [];
          collisions.forEach(({ loserRow, matchId }, i) => {
            const o = i * 4;
            values.push(`($${o + 1}::uuid, $${o + 2}, $${o + 3}, $${o + 4})`);
            params.push(
              matchId,
              loserRow.land_name,
              loserRow.land_external_id,
              loserRow.queue_times_entity_id,
            );
          });
          await transactionalEntityManager.query(
            `UPDATE attractions a
             SET "land_name" = COALESCE(v.land_name, a."land_name"),
                 "land_external_id" = COALESCE(v.land_external_id, a."land_external_id"),
                 "queue_times_entity_id" = COALESCE(v.qt_id, a."queue_times_entity_id"),
                 "last_merged_at" = NOW()
             FROM (VALUES ${values.join(", ")}) AS v(id, land_name, land_external_id, qt_id)
             WHERE a.id = v.id`,
            params,
          );
          await this.consolidateMergedAttractions(
            transactionalEntityManager,
            collisions.map((c) => ({
              winnerId: c.matchId,
              loserId: c.loserRow.id,
            })),
          );
          await transactionalEntityManager.query(
            `DELETE FROM attractions WHERE id = ANY($1::uuid[])`,
            [collisions.map((c) => c.loserRow.id)],
          );
        }

        if (movedIds.length > 0) {
          await transactionalEntityManager.query(
            `UPDATE attractions SET "parkId" = $1 WHERE id = ANY($2::uuid[])`,
            [winner.id, movedIds],
          );
        }

        // 2. Shows
        const showResult = await transactionalEntityManager.query(
          `UPDATE shows SET "parkId" = $1 WHERE "parkId" = $2::uuid`,
          [winner.id, loser.id],
        );

        // 3. Restaurants
        const restaurantResult = await transactionalEntityManager.query(
          `UPDATE restaurants SET "parkId" = $1 WHERE "parkId" = $2::uuid`,
          [winner.id, loser.id],
        );

        const totalMigrated =
          collisions.length +
          movedIds.length +
          (showResult?.[1] || 0) +
          (restaurantResult?.[1] || 0);

        // 4. The emptiness check the original ended on. Every show, restaurant
        // and ride of the loser has just been moved or merged away, so a
        // leftover means somebody inserted one while this ran.
        //
        // It aborts the merge rather than keeping the park, and the difference
        // matters: the rides that DID move carry a denormalised `parkId` in
        // `attraction_hourly_history`, `queue_data_aggregates`, the two
        // baselines, `attraction_rope_drop`, `attraction_typical_waits` and
        // `attraction_ride_profiles`, and only `consolidateMergedPark` below
        // moves it. Committing the moves without that step files the inherited
        // history under a park id the survivor does not have, and both readers
        // go by park id (`analytics.service.ts`,
        // `park-historical-stats.service.ts`), so the survivor's statistics
        // would quietly show nothing for the rides it just gained. Rolling back
        // leaves both parks exactly as they were and the next sync retries —
        // which is what "the moves and the DELETE commit together, or neither
        // does" has to mean.
        const remaining = await transactionalEntityManager.query(
          `SELECT
            (SELECT COUNT(*) FROM shows WHERE "parkId" = $1::uuid) as shows,
            (SELECT COUNT(*) FROM restaurants WHERE "parkId" = $1::uuid) as restaurants,
            (SELECT COUNT(*) FROM attractions WHERE "parkId" = $1::uuid) as attractions
          `,
          [loser.id],
        );
        const isEmpty =
          remaining[0].shows === "0" &&
          remaining[0].restaurants === "0" &&
          remaining[0].attractions === "0";

        if (!isEmpty) {
          throw new PriorityMergeIncompleteError(
            `Priority merge left "${loser.name}" non-empty (shows=${remaining[0].shows}, restaurants=${remaining[0].restaurants}, attractions=${remaining[0].attractions})`,
          );
        }

        // 5. Everything else the loser owns, then the row itself. Same order as
        // the other two paths: nothing may still point at it when it goes.
        await this.consolidateMergedPark(
          transactionalEntityManager,
          winner.id,
          loser.id,
        );
        await transactionalEntityManager.delete(Park, loser.id);

        if (totalMigrated > 0) {
          this.logger.log(
            `🔀 Migrated ${totalMigrated} entities from "${loser.name}" to "${winner.name}"`,
          );
        }
      },
    );
  }

  /**
   * Rolls the priority merge back without taking the sync run with it.
   *
   * The abort is deliberate and the swallow is too: the merge has not happened,
   * both parks stand as they did, and the next sync tries again. Anything else
   * thrown inside that transaction is a real failure and keeps propagating.
   */
  private async mergePriorityDuplicateLoserSafely(
    winner: Park,
    loser: Park,
  ): Promise<void> {
    try {
      await this.mergePriorityDuplicateLoser(winner, loser);
    } catch (error: unknown) {
      if (error instanceof PriorityMergeIncompleteError) {
        this.logger.warn(`⚠️ ${error.message}; rolled back, park kept.`);
        return;
      }
      throw error;
    }
  }

  /**
   * Scans for and merges duplicate parks based on shared Queue-Times IDs.
   * This fixes "Split Brain" issues where a park exists separately from Wiki and Queue-Times sources.
   */
  async repairDuplicates(): Promise<void> {
    this.logger.debug("🔧 Running Duplicate Park Repair...");

    // Find all Queue-Times IDs that are used by more than one park
    const duplicates = await this.parkRepository.query(`
      SELECT "queue_times_entity_id"
      FROM parks
      WHERE "queue_times_entity_id" IS NOT NULL
      GROUP BY "queue_times_entity_id"
      HAVING COUNT(*) > 1
    `);

    if (duplicates.length > 0) {
      this.logger.warn(
        `Found ${duplicates.length} duplicate sets into repair.`,
      );
    } else {
      this.logger.debug("Found 0 duplicate sets to repair.");
    }

    for (const dup of duplicates) {
      const qtId = dup.queue_times_entity_id;

      // Get all parks with this ID
      const parks = await this.parkRepository.find({
        where: { queueTimesEntityId: qtId },
        order: { createdAt: "ASC" }, // Oldest first usually, but we prefer Wiki-sourced
      });

      if (parks.length < 2) continue;

      // Identify Primary (Winner) and Ghost (Loser)
      // Preference: Park with Wiki ID > Oldest Park
      let primary = parks.find((p) => p.wikiEntityId !== null);
      if (!primary) primary = parks[0]; // Fallback to first one

      const ghosts = parks.filter((p) => p.id !== primary!.id);

      for (const ghostPark of ghosts) {
        this.logger.log(
          `🔀 Merging Ghost Park "${ghostPark.name}" into "${primary!.name}" (Shared QT ID: ${qtId})`,
        );

        await this.parkRepository.manager.transaction(
          async (transactionalEntityManager) => {
            await this.liftTimescaleDecompressionLimit(
              transactionalEntityManager,
            );

            // 1. Handle Attraction Collisions
            const existingAttractions = await transactionalEntityManager.query(
              `SELECT id, slug, "land_name", "land_external_id" FROM attractions WHERE "parkId" = $1::uuid`,
              [primary!.id],
            );
            const ghostAttractions = await transactionalEntityManager.query(
              `SELECT id, slug, "land_name", "land_external_id" FROM attractions WHERE "parkId" = $1::uuid`,
              [ghostPark.id],
            );

            // O(1) slug lookups instead of a linear .find() per ghost
            // attraction (quadratic on parks with many attractions).
            const existingBySlug = new Map<string, any>(
              existingAttractions.map((a: any) => [a.slug, a]),
            );

            for (const ghostAttr of ghostAttractions) {
              const match = existingBySlug.get(ghostAttr.slug);

              if (match) {
                // COLLISION: Merge data, move mappings, delete ghost attraction

                // 1. Copy Land Data if missing in primary, and stamp the
                //    survivor. The land columns are filled in only where they
                //    are empty; `last_merged_at` is unconditional, because the
                //    merge happened whether or not anything was inherited —
                //    step 2 below reparents this ghost's `queue_data` onto the
                //    survivor, whose history is then two interleaved series
                //    flapping between OPERATING and DOWN at the same instant.
                //    The stamp is what holds the ride out of the nightly
                //    downtime reconstruction until it ages out.
                await transactionalEntityManager.query(
                  `UPDATE attractions
                   SET "land_name" = COALESCE("land_name", $1),
                       "land_external_id" = COALESCE("land_external_id", $2),
                       "last_merged_at" = NOW()
                   WHERE id = $3`,
                  [ghostAttr.land_name, ghostAttr.land_external_id, match.id],
                );

                // 2. Move the Queue-Times mapping and every dependent row, not
                //    the three tables this block used to name by hand.
                await this.consolidateMergedAttractions(
                  transactionalEntityManager,
                  [{ winnerId: match.id, loserId: ghostAttr.id }],
                );

                // 3. Delete the ghost attraction
                await transactionalEntityManager.query(
                  `DELETE FROM attractions WHERE id = $1`,
                  [ghostAttr.id],
                );
                this.logger.log(
                  `    Merged attraction data & mappings: ${ghostAttr.slug}`,
                );
              } else {
                // NO COLLISION: Move
                await transactionalEntityManager.query(
                  `UPDATE attractions SET "parkId" = $1 WHERE id = $2`,
                  [primary!.id, ghostAttr.id],
                );
              }
            }

            // 2./3. Shows and restaurants, partitioned by slug against their
            // own unique `(parkId, slug)` — see `migrateParkChildEntities`.
            await this.migrateParkChildEntities(
              transactionalEntityManager,
              primary!.id,
              ghostPark.id,
            );

            // 4. Move everything else the ghost park owns, and keep the path it
            // was served under alive — see `consolidateMergedPark`. The DELETE
            // below is where 23503 lands once the attraction level stops
            // raising it first.
            await this.consolidateMergedPark(
              transactionalEntityManager,
              primary!,
              ghostPark,
            );

            // 5. Delete the ghost park
            await transactionalEntityManager.delete(Park, ghostPark.id);
          },
        );
        this.logger.log(
          `✅ Ghost Park "${ghostPark.name}" merged and deleted.`,
        );
      }
    }
  }

  /**
   * Finds park by externalId
   */
  async findByExternalId(externalId: string): Promise<Park | null> {
    return this.parkRepository.findOne({
      where: { externalId },
      relations: ["destination", "attractions"],
    });
  }

  /**
   * Finds park by internal ID
   */
  async findById(id: string): Promise<Park | null> {
    return this.parkRepository.findOne({
      where: { id },
      relations: ["destination"],
    });
  }

  /**
   * Finds multiple parks by internal ID (order not guaranteed).
   */
  async findByIds(ids: string[]): Promise<Park[]> {
    if (ids.length === 0) return [];
    return this.parkRepository.find({
      where: { id: In(ids) },
      relations: ["destination"],
    });
  }

  /**
   * Finds all parks
   */
  async findAll(): Promise<Park[]> {
    return this.parkRepository.find({
      relations: ["destination"],
      order: { name: "ASC" },
    });
  }

  /**
   * Returns all parks, triggering a sync if none exist yet.
   * Shared bootstrap helper for entity sync methods (attractions, shows, restaurants).
   */
  async ensureParksLoaded(): Promise<Park[]> {
    let parks = await this.findAll();
    if (parks.length === 0) {
      this.logger.warn("No parks found. Syncing parks first...");
      await this.syncParks();
      parks = await this.findAll();
    }
    return parks;
  }

  /**
   * Finds all parks with filtering and sorting
   */
  async findAllWithFilters(filters: {
    continent?: string;
    country?: string;
    city?: string;
    continentSlug?: string;
    countrySlug?: string;
    citySlug?: string;
    sort?: string;
    page?: number;
    limit?: number;
  }): Promise<{ data: Park[]; total: number }> {
    const queryBuilder = this.parkRepository
      .createQueryBuilder("park")
      .leftJoinAndSelect("park.destination", "destination");

    // Apply filters - prefer slugs over names (from geo routes)
    if (filters.continentSlug) {
      queryBuilder.andWhere("park.continentSlug = :continentSlug", {
        continentSlug: filters.continentSlug,
      });
    } else if (filters.continent) {
      queryBuilder.andWhere("LOWER(park.continent) = LOWER(:continent)", {
        continent: filters.continent,
      });
    }

    if (filters.countrySlug) {
      queryBuilder.andWhere("park.countrySlug = :countrySlug", {
        countrySlug: filters.countrySlug,
      });
    } else if (filters.country) {
      queryBuilder.andWhere("LOWER(park.country) = LOWER(:country)", {
        country: filters.country,
      });
    }

    if (filters.citySlug) {
      queryBuilder.andWhere("park.citySlug = :citySlug", {
        citySlug: filters.citySlug,
      });
    } else if (filters.city) {
      queryBuilder.andWhere("LOWER(park.city) = LOWER(:city)", {
        city: filters.city,
      });
    }

    // Apply sorting
    if (filters.sort) {
      const [field, direction = "asc"] = filters.sort.split(":");
      const sortDirection = normalizeSortDirection(direction);

      if (field === "name") {
        queryBuilder.orderBy("park.name", sortDirection);
      } else if (field === "openStatus") {
        // Sort by open status (OPERATING first, then CLOSED)
        // This is a placeholder - actual status comes from schedule/queue data
        queryBuilder.orderBy("park.name", sortDirection);
      }
    } else {
      // Default sort by name
      queryBuilder.orderBy("park.name", "ASC");
    }

    // Apply pagination
    const page = filters.page || 1;
    const limit = filters.limit || 10;
    queryBuilder.skip((page - 1) * limit).take(limit);

    const [data, total] = await queryBuilder.getManyAndCount();
    return { data, total };
  }

  /**
   * Finds park by slug
   */
  async findBySlug(slug: string): Promise<Park | null> {
    const park = await this.parkRepository.findOne({
      where: { slug },
      relations: ["destination"],
    });
    if (!park) return null;
    return this.loadParkRelations(park);
  }

  /**
   * Saves schedule data for a park from ThemeParks.wiki API.
   *
   * Strategy:
   * - Upsert based on (parkId, date, scheduleType)
   * - Update if changed, otherwise skip
   * - Keep historical schedule entries for analysis
   *
   * @param parkId - Our internal park ID (UUID)
   * @param scheduleData - Schedule data from ThemeParks.wiki API
   */
  async saveScheduleData(
    parkId: string,
    scheduleData: ScheduleSyncEntry[],
  ): Promise<number> {
    if (!scheduleData || scheduleData.length === 0) {
      return 0;
    }

    // 1. Fetch Park geo data for holiday checking
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: [
        "id",
        "countryCode",
        "regionCode",
        "timezone",
        "curatedUsesTwelveHourClock",
      ],
    });

    // 2. Normalize date + type once per entry; reused by the holiday prefetch,
    // upsert, and delete phases.
    // The API returns date-only strings ("YYYY-MM-DD") that represent the park's local
    // calendar date. new Date("YYYY-MM-DD") produces midnight UTC, so applying
    // formatInParkTimezone to it would shift the date back by one day for parks west
    // of UTC (e.g. "2026-03-02" → "2026-03-01" in America/New_York). Instead we detect
    // date-only strings and use them directly; full datetime strings are still converted.
    const normalizedEntries = scheduleData.map((entry) => {
      const raw: string =
        typeof entry.date === "string"
          ? entry.date
          : entry.date instanceof Date
            ? entry.date.toISOString().split("T")[0]
            : String(entry.date);
      const dateStr = /^\d{4}-\d{2}-\d{2}$/.test(raw)
        ? raw
        : formatInParkTimezone(new Date(raw), park!.timezone);

      // Normalize API type: "Closed"/"CLOSED" → CLOSED so off-season (e.g. Phantasialand Feb)
      // is persisted. Also normalize "PARK_OPEN" to OPERATING.
      const rawType = entry.type?.toString().toUpperCase();
      let scheduleType: ScheduleType;
      if (rawType === "CLOSED") {
        scheduleType = ScheduleType.CLOSED;
      } else if (rawType === "PARK_OPEN") {
        scheduleType = ScheduleType.OPERATING;
      } else {
        scheduleType = entry.type as ScheduleType;
      }

      return { entry, dateStr, scheduleType };
    });

    // 3. Pre-fetch holidays for the date range (Extended by +/ 1 day for bridge day checks)
    const holidayMap = new Map<string, string | HolidayEntry>(); // Date -> Name or HolidayEntry
    if (park?.countryCode) {
      try {
        // Use noon-UTC timestamps so formatInParkTimezone() stays on the same
        // calendar day for parks in every timezone (west-of-UTC parks shift
        // midnight-UTC to the previous day, which would narrow the holiday range).
        const dates = normalizedEntries.map((e) =>
          new Date(`${e.dateStr}T12:00:00Z`).getTime(),
        );
        const minDate = new Date(Math.min(...dates));
        const maxDate = new Date(Math.max(...dates));

        // Extend range by 1 day for bridge day detection
        minDate.setDate(minDate.getDate() - 1);
        maxDate.setDate(maxDate.getDate() + 1);

        const holidays = await this.holidaysService.getHolidays(
          park.countryCode,
          formatInParkTimezone(minDate, park.timezone),
          formatInParkTimezone(maxDate, park.timezone),
        );

        // Normalize region codes for consistent comparison
        const normalizedParkRegion = normalizeRegionCode(park.regionCode);

        for (const h of holidays) {
          // Match: Nationwide holidays OR region matches (using normalized codes)
          const normalizedHolidayRegion = normalizeRegionCode(h.region);
          if (
            h.isNationwide ||
            (normalizedParkRegion &&
              normalizedHolidayRegion === normalizedParkRegion)
          ) {
            // Use park's timezone to determine the holiday date string
            // Normalize to noon UTC to prevent timezone shifts (YYYY-MM-DD from DB)
            const normalizedDate = new Date(h.date);
            normalizedDate.setUTCHours(12, 0, 0, 0);
            const dateStr = formatInParkTimezone(normalizedDate, park.timezone);

            // If multiple holidays on same day, prefer public holidays over school holidays
            const existing = holidayMap.get(dateStr);
            const hType = h.holidayType;

            if (!existing) {
              // Store as HolidayEntry to preserve type information for bridge day logic
              holidayMap.set(dateStr, {
                name: h.localName || h.name || "",
                type: hType,
                allTypes: [hType],
              });
            } else {
              if (typeof existing !== "string") {
                // Aggregate types
                if (!existing.allTypes) existing.allTypes = [existing.type];
                if (!existing.allTypes.includes(hType)) {
                  existing.allTypes.push(hType);
                }

                // Prioritize public holidays for the main entry
                if (hType === "public" || hType === "bank") {
                  existing.name = h.localName || h.name || "";
                  existing.type = hType;
                }
              }
            }
          }
        }
      } catch (error) {
        this.logger.warn(
          `Failed to fetch holidays for schedule sync (Park ${parkId}): ${error}`,
        );
      }
    }

    // Pre-load all existing entries for the affected dates in a single query
    // instead of one SELECT per entry (a full sync covers ~365 days per park).
    // Select the date as text so the park-local YYYY-MM-DD key is driver/TZ-safe.
    type ExistingScheduleRow = {
      id: string;
      date: string;
      scheduleType: ScheduleType;
      openingTime: Date | null;
      closingTime: Date | null;
      description: string | null;
      purchases: ScheduleEntry["purchases"];
      isHoliday: boolean;
      holidayName: string | null;
      isBridgeDay: boolean;
    };
    const existingByKey = new Map<string, ExistingScheduleRow>();
    const affectedDates = [...new Set(normalizedEntries.map((e) => e.dateStr))];
    if (affectedDates.length > 0) {
      const existingRows: ExistingScheduleRow[] = await this.scheduleRepository
        .createQueryBuilder("schedule")
        .select("schedule.id", "id")
        .addSelect("to_char(schedule.date, 'YYYY-MM-DD')", "date")
        .addSelect('schedule."scheduleType"', "scheduleType")
        .addSelect('schedule."openingTime"', "openingTime")
        .addSelect('schedule."closingTime"', "closingTime")
        .addSelect("schedule.description", "description")
        .addSelect("schedule.purchases", "purchases")
        .addSelect('schedule."isHoliday"', "isHoliday")
        .addSelect('schedule."holidayName"', "holidayName")
        .addSelect('schedule."isBridgeDay"', "isBridgeDay")
        .where("schedule.parkId = :parkId", { parkId })
        .andWhere("schedule.date IN (:...dates)", { dates: affectedDates })
        .getRawMany();
      for (const row of existingRows) {
        const key = `${row.date}|${row.scheduleType}`;
        if (!existingByKey.has(key)) {
          existingByKey.set(key, row);
        }
      }
    }

    // Keyed by date|type so duplicate pairs within one payload collapse (last wins),
    // mirroring the previous insert-then-update behaviour.
    const toInsert = new Map<string, Partial<ScheduleEntry>>();
    const toUpdate: Array<{ id: string; data: Partial<ScheduleEntry> }> = [];

    for (const { entry, dateStr, scheduleType } of normalizedEntries) {
      // Use noon UTC so timezone conversions inside calculateHolidayInfo won't shift the date
      const dateObj = new Date(`${dateStr}T12:00:00Z`);

      const holidayInfo = calculateHolidayInfo(
        dateObj,
        holidayMap,
        park!.timezone,
      );

      const openingTime = entry.openingTime
        ? new Date(entry.openingTime)
        : null;
      const rawClosingTime = entry.closingTime
        ? new Date(entry.closingTime)
        : null;
      // Two repairs, and the order matters. A park flagged as publishing a
      // 12-hour clock gets read that way first, because the signal it needs —
      // the closing falling before the opening — is what the generic repair
      // consumes. Everything else: sources misdate the closing time in both
      // directions (a past-midnight close stamped with the day's own date reads
      // as CLOSED all day; an overshot day or typo'd year never closes at all),
      // and there the time-of-day is the trustworthy part.
      const closingTime =
        (park!.curatedUsesTwelveHourClock
          ? correctTwelveHourClockClose(
              openingTime,
              rawClosingTime,
              park!.timezone,
            )
          : null) ??
        normalizeClosingTime(openingTime, rawClosingTime, park!.timezone);

      const scheduleEntry: Partial<ScheduleEntry> = {
        parkId,
        date: dateObj,
        scheduleType,
        openingTime,
        closingTime,
        description: entry.description || null,
        purchases: entry.purchases || null,
        isHoliday: holidayInfo.isHoliday,
        holidayName: holidayInfo.holidayName,
        isBridgeDay: holidayInfo.isBridgeDay,
      };

      const key = `${dateStr}|${scheduleType}`;
      const existing = existingByKey.get(key);

      if (!existing) {
        toInsert.set(key, scheduleEntry);
        continue;
      }

      // Update if times, description, purchases, or holiday/bridge status changed.
      // purchases is jsonb; compare by content (jsonb normalizes key order, so a
      // false mismatch only causes a redundant update, never a missed one).
      const hasChanges =
        existing.openingTime?.getTime() !==
          scheduleEntry.openingTime?.getTime() ||
        existing.closingTime?.getTime() !==
          scheduleEntry.closingTime?.getTime() ||
        existing.description !== scheduleEntry.description ||
        JSON.stringify(existing.purchases) !==
          JSON.stringify(scheduleEntry.purchases) ||
        existing.isHoliday !== scheduleEntry.isHoliday ||
        existing.holidayName !== scheduleEntry.holidayName ||
        existing.isBridgeDay !== scheduleEntry.isBridgeDay;

      if (hasChanges) {
        toUpdate.push({ id: existing.id, data: scheduleEntry });
      }
    }

    let savedCount = 0;

    if (toInsert.size > 0) {
      await this.scheduleRepository.save([...toInsert.values()]);
      savedCount += toInsert.size;
    }

    if (toUpdate.length > 0) {
      // One statement for every changed entry (was: one UPDATE per entry).
      // The payload is a single jsonb object id → changed fields; `updatedAt`
      // is set explicitly because raw SQL bypasses @UpdateDateColumn.
      const payload: Record<string, unknown> = {};
      for (const u of toUpdate) {
        payload[u.id] = {
          openingTime: u.data.openingTime ?? null,
          closingTime: u.data.closingTime ?? null,
          description: u.data.description ?? null,
          purchases: u.data.purchases ?? null,
          isHoliday: u.data.isHoliday ?? false,
          holidayName: u.data.holidayName ?? null,
          isBridgeDay: u.data.isBridgeDay ?? false,
        };
      }

      await this.scheduleRepository.query(
        `UPDATE schedule_entries s
         SET "openingTime" = (v.value->>'openingTime')::timestamptz,
             "closingTime" = (v.value->>'closingTime')::timestamptz,
             "description" = v.value->>'description',
             "purchases"   = CASE WHEN v.value->'purchases' = 'null'::jsonb
                                  THEN NULL ELSE v.value->'purchases' END,
             "isHoliday"   = (v.value->>'isHoliday')::boolean,
             "holidayName" = v.value->>'holidayName',
             "isBridgeDay" = (v.value->>'isBridgeDay')::boolean,
             "updatedAt"   = NOW()
         FROM jsonb_each($1::jsonb) v
         WHERE s.id = v.key::uuid`,
        [JSON.stringify(payload)],
      );
    }
    savedCount += toUpdate.length;

    // Invalidate once after all writes instead of once per written entry.
    if (savedCount > 0) {
      await this.invalidateScheduleCache(parkId);
    }

    // Batch DELETE operations: Cleanup placeholders when we have real data from the API.
    // Use date strings for reliable deletion (avoids TZ-dependent off-by-one with Date objects).

    // Filter normalized entries for deletion
    const deleteUnknownDates = normalizedEntries
      .filter((e) => e.scheduleType !== ScheduleType.UNKNOWN)
      .map((e) => e.dateStr);

    const deleteClosedDates = normalizedEntries
      .filter((e) => e.scheduleType === ScheduleType.OPERATING)
      .map((e) => e.dateStr);

    const deleteOperatingDates = normalizedEntries
      .filter((e) => e.scheduleType === ScheduleType.CLOSED)
      .map((e) => e.dateStr);

    if (deleteUnknownDates.length > 0) {
      await this.scheduleRepository
        .createQueryBuilder()
        .delete()
        .from(ScheduleEntry)
        .where('"parkId" = :parkId', { parkId })
        .andWhere("date IN (:...dates)", { dates: deleteUnknownDates })
        .andWhere('"scheduleType" = :type', {
          type: ScheduleType.UNKNOWN,
        })
        .execute();
    }

    if (deleteClosedDates.length > 0) {
      await this.scheduleRepository
        .createQueryBuilder()
        .delete()
        .from(ScheduleEntry)
        .where('"parkId" = :parkId', { parkId })
        .andWhere("date IN (:...dates)", { dates: deleteClosedDates })
        .andWhere('"scheduleType" = :type', {
          type: ScheduleType.CLOSED,
        })
        .execute();
    }

    if (deleteOperatingDates.length > 0) {
      await this.scheduleRepository
        .createQueryBuilder()
        .delete()
        .from(ScheduleEntry)
        .where('"parkId" = :parkId', { parkId })
        .andWhere("date IN (:...dates)", { dates: deleteOperatingDates })
        .andWhere('"scheduleType" = :type', {
          type: ScheduleType.OPERATING,
        })
        .execute();
    }

    return savedCount;
  }

  /**
   * True if park has at least one OPERATING schedule entry (any date).
   * Used to decide whether we trust schedule as source of truth for UNKNOWN→OPERATING inference.
   */
  async hasOperatingSchedule(parkId: string): Promise<boolean> {
    const count = await this.scheduleRepository.count({
      where: {
        parkId,
        scheduleType: ScheduleType.OPERATING,
        attractionId: IsNull(),
      },
    });
    return count > 0;
  }

  /**
   * Returns the min/max OPERATING dates for a park (YYYY-MM-DD strings in park timezone).
   * Used by the calendar to infer CLOSED for dates between operating ranges that have no schedule entry.
   */
  async getOperatingDateRange(
    parkId: string,
    timezone: string,
  ): Promise<{ minDate: string | null; maxDate: string | null }> {
    // Read-through cache: MIN/MAX over schedule_entries, called on every calendar build.
    // Schedule changes only on the daily sync / on-demand refresh, so 1h is safely fresh.
    const cacheKey = CacheKeys.parkOpDateRange(parkId);
    const cached = await this.redis.get(cacheKey);
    if (cached !== null) {
      try {
        return JSON.parse(cached) as {
          minDate: string | null;
          maxDate: string | null;
        };
      } catch {
        // fall through
      }
    }
    const result = await this.scheduleRepository
      .createQueryBuilder("schedule")
      .select("MIN(schedule.date)", "minDate")
      .addSelect("MAX(schedule.date)", "maxDate")
      .where("schedule.parkId = :parkId", { parkId })
      .andWhere("schedule.scheduleType = :type", {
        type: ScheduleType.OPERATING,
      })
      .andWhere("schedule.attractionId IS NULL") // Only park-level schedules
      .getRawOne<{
        minDate: string | Date | null;
        maxDate: string | Date | null;
      }>();

    const fmt = (v: string | Date | null | undefined): string | null => {
      if (!v) return null;
      if (typeof v === "string") return v; // already YYYY-MM-DD from PG date column
      return formatInParkTimezone(v, timezone);
    };

    const range = result
      ? { minDate: fmt(result.minDate), maxDate: fmt(result.maxDate) }
      : { minDate: null, maxDate: null };
    await this.redis
      .set(cacheKey, JSON.stringify(range), "EX", 60 * 60)
      .catch(() => undefined);
    return range;
  }

  /**
   * Batch version of hasOperatingSchedule check.
   */
  async getBatchHasOperatingSchedule(
    parkIds: string[],
  ): Promise<Map<string, boolean>> {
    if (parkIds.length === 0) return new Map();

    const results = await this.scheduleRepository.manager.query(
      `
      SELECT "parkId", COUNT(*) > 0 as "hasSchedule"
      FROM schedule_entries
      WHERE "parkId" = ANY($1)
        AND "scheduleType" = 'OPERATING'
        AND "attractionId" IS NULL
      GROUP BY "parkId"
    `,
      [parkIds],
    );

    const map = new Map<string, boolean>();
    parkIds.forEach((id) => map.set(id, false)); // Default to false
    results.forEach((r: any) => map.set(r.parkId, r.hasSchedule));
    return map;
  }

  /**
   * Checks if a park has a history of seasonal closures (gaps > 21 days between OPERATING entries).
   * Optimized using window functions for significantly better performance (from ~70ms to ~0.5ms).
   */
  async isParkSeasonal(parkId: string): Promise<boolean> {
    const cacheKey = `park:isSeasonal:${parkId}`;
    const cached = await this.redis.get(cacheKey);
    if (cached !== null) return cached === "1";

    const result = await this.scheduleRepository.manager.query(
      `
      SELECT EXISTS (
        SELECT 1 
        FROM (
          SELECT date, LEAD(date) OVER (ORDER BY date) as next_date
          FROM schedule_entries
          WHERE "parkId" = $1 
            AND "scheduleType" = 'OPERATING'
            AND "attractionId" IS NULL
        ) t
        WHERE next_date - date > 21
      ) as "is_seasonal"
    `,
      [parkId],
    );

    const isSeasonal = result[0]?.is_seasonal === true;
    await this.redis.set(cacheKey, isSeasonal ? "1" : "0", "EX", 86400); // 24h
    return isSeasonal;
  }

  /**
   * Derives historical opening hours from ride activity.
   * Logic:
   * - Opening: First 15min window where >= 10% of attractions (min 2, max 10) show activity, rounded down.
   * - Closing: Last 15min window with activity, rounded up.
   */
  async getDerivedHistoricalHours(
    parkId: string,
    fromDate: string,
    toDate: string,
    timezone: string,
  ): Promise<Map<string, { openingTime: string; closingTime: string }>> {
    const cacheKey = `park:derivedHours:${parkId}:${fromDate}:${toDate}`;
    const parsed = safeJsonParse<
      Record<string, { openingTime: string; closingTime: string }>
    >(await this.redis.get(cacheKey)); // corrupt entry = miss
    if (parsed !== null) {
      return new Map(Object.entries(parsed));
    }

    // Past days come from the pre-aggregated attraction_hourly_history (one
    // immutable row per attraction/day, slots already bucketed to 15-min in
    // park-local time) — that avoids a wide scan of the compressed queue_data
    // hypertable (the old single-source version took up to ~25s on a busy
    // park/season). Only *today* (not yet rolled up into the history table) is
    // derived from raw queue_data, which is a single recent — uncompressed —
    // day and so stays cheap. Validated as bit-identical to the old query on
    // the two highest-volume parks (Europa-Park, Phantasialand).
    const results = await this.scheduleRepository.manager.query(
      `
      WITH park_rides AS (
        SELECT id
        FROM attractions
        WHERE "parkId" = $1
          AND name NOT ILIKE '%bar%'
          AND name NOT ILIKE '%snack%'
          AND name NOT ILIKE '%corner%'
          AND name NOT ILIKE '%restaurant%'
          AND name NOT ILIKE '%shop%'
          AND name NOT ILIKE '%cafe%'
          AND name NOT ILIKE '%hire%'
      ),
      park_info AS (
        SELECT COUNT(*) as total_attr FROM park_rides
      ),
      -- Past days: pre-aggregated 15-min slots (park-local "HH:MM").
      hist_activity AS (
        SELECT
          h.date as "date",
          (s->>'time_slot') as "slot",
          COUNT(DISTINCT h."attractionId") as "active_count"
        FROM attraction_hourly_history h
        CROSS JOIN LATERAL jsonb_array_elements(h.slots) s
        WHERE h."attractionId" IN (SELECT id FROM park_rides)
          AND h.date >= $2::date
          AND h.date <= $3::date
          AND h.date < (now() AT TIME ZONE $4)::date
        GROUP BY 1, 2
      ),
      -- Today only: raw queue_data, restricted to today's (uncompressed) chunk.
      today_activity AS (
        SELECT
          (q.timestamp AT TIME ZONE $4)::date as "date",
          to_char(
            date_trunc('hour', q.timestamp AT TIME ZONE $4)
              + (date_part('minute', q.timestamp AT TIME ZONE $4)::int / 15 * interval '15 min'),
            'HH24:MI'
          ) as "slot",
          COUNT(DISTINCT q."attractionId") FILTER (WHERE q."waitTime" >= 5) as "active_count"
        FROM queue_data q
        WHERE q."attractionId" IN (SELECT id FROM park_rides)
          AND (now() AT TIME ZONE $4)::date >= $2::date
          AND (now() AT TIME ZONE $4)::date <= $3::date
          AND q.timestamp >= ((now() AT TIME ZONE $4)::date AT TIME ZONE $4)
        GROUP BY 1, 2
      ),
      activity AS (
        SELECT * FROM hist_activity
        UNION ALL
        SELECT * FROM today_activity
      ),
      daily_bounds AS (
        SELECT
          "date",
          MIN("slot") FILTER (WHERE "active_count" >= LEAST(10, GREATEST(2, (SELECT total_attr FROM park_info) * 0.10))) as "first_slot",
          MAX("slot") FILTER (WHERE "active_count" >= LEAST(10, GREATEST(2, (SELECT total_attr FROM park_info) * 0.10))) as "last_slot"
        FROM activity
        GROUP BY 1
      )
      SELECT
        "date",
        date_trunc('hour', "date"::timestamp + "first_slot"::time) as "derived_open",
        date_trunc('hour', "date"::timestamp + "last_slot"::time + INTERVAL '59 minutes') as "derived_close"
      FROM daily_bounds
      WHERE "first_slot" IS NOT NULL AND "last_slot" IS NOT NULL
    `,
      [parkId, fromDate, toDate, timezone],
    );

    const map = new Map<string, { openingTime: string; closingTime: string }>();
    results.forEach((r: any) => {
      const dStr = r.date.toISOString().split("T")[0];
      map.set(dStr, {
        openingTime: r.derived_open.toISOString(),
        closingTime: r.derived_close.toISOString(),
      });
    });

    const todayInPark = getCurrentDateInTimezone(timezone);
    const ttl = toDate < todayInPark ? 86400 : 300;
    await this.redis.set(
      cacheKey,
      JSON.stringify(Object.fromEntries(map)),
      "EX",
      ttl,
    );

    return map;
  }
  /**
   * Fills missing schedule entries with CLOSED or UNKNOWN and holiday/bridge metadata.
   *
   * Gap classification (CLOSED vs UNKNOWN):
   * - CLOSED: Day has no schedule but there is at least one OPERATING day before AND after
   *   (in our stored schedule). So we know the park was closed that day (e.g. mid-week closure).
   * - UNKNOWN: Day has no schedule and we have no OPERATING at or after it, or no OPERATING
   *   before it. So either "before season / before we have data" or "after last known schedule".
   *   Also: if the park has no OPERATING entries at all, all gaps stay UNKNOWN.
   *
   * Demotion: Gap-fill CLOSED (no opening/closing times) that is now after the last OPERATING
   * date gets demoted to UNKNOWN — e.g. when the API removes future OPERATING entries.
   *
   * This allows the calendar to show "Closed" vs "Opening hours not yet available" correctly.
   */
  async fillScheduleGaps(
    parkId: string,
    lookAheadDays = 182,
    lookBackDays = 182,
  ): Promise<number> {
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "countryCode", "regionCode", "timezone"],
    });

    if (!park?.countryCode) return 0;

    // Clean up duplicates before gap-filling to prevent conflicts
    // (e.g. when parallel schedule syncs create duplicate entries)
    await this.cleanupDuplicateScheduleEntriesForPark(parkId);

    // Range: (today - lookBackDays) through (today + lookAheadDays) in PARK timezone.
    // Use noon-UTC dates for arithmetic to avoid DST boundary issues.
    const todayStr = getCurrentDateInTimezone(park.timezone);
    const todayNoon = new Date(`${todayStr}T12:00:00Z`);
    const startStr = subDays(todayNoon, lookBackDays)
      .toISOString()
      .slice(0, 10);
    const endStr = addDays(todayNoon, lookAheadDays).toISOString().slice(0, 10);

    // 0. Min/max OPERATING dates for this park (any time) to classify gaps as CLOSED vs UNKNOWN
    const operatingRange = await this.getOperatingDateRange(
      parkId,
      park.timezone,
    );
    const minOpStr = operatingRange.minDate;
    const maxOpStr = operatingRange.maxDate;

    const isGapClosed = (dateStr: string): boolean => {
      if (!minOpStr || !maxOpStr) return false; // no OPERATING at all → UNKNOWN
      return dateStr > minOpStr && dateStr < maxOpStr; // strictly between
    };

    // 1. Fetch existing entries (use date strings for range to avoid TZ issues in query)
    const existingEntries = await this.scheduleRepository
      .createQueryBuilder("schedule")
      .where("schedule.parkId = :parkId", { parkId })
      .andWhere("schedule.date >= :startDate", { startDate: startStr })
      .andWhere("schedule.date <= :endDate", { endDate: endStr })
      .getMany();

    // Map existing entries by their local date string for O(1) lookup
    const existingEntryMap = new Map<string, ScheduleEntry>();
    existingEntries.forEach((e) => {
      const eDateStr = formatInParkTimezone(
        e.date instanceof Date ? e.date : new Date(e.date),
        park.timezone,
      );
      existingEntryMap.set(eDateStr, e);
    });

    const existingDates = new Set(existingEntryMap.keys());

    // 2. Fetch Holidays
    // Extend range by 1 day for bridge day detection
    const holidayStartStr = subDays(new Date(`${startStr}T12:00:00Z`), 1)
      .toISOString()
      .slice(0, 10);
    const holidayEndStr = addDays(new Date(`${endStr}T12:00:00Z`), 1)
      .toISOString()
      .slice(0, 10);

    const holidays = await this.holidaysService.getHolidays(
      park.countryCode,
      holidayStartStr,
      holidayEndStr,
    );

    // Map Holidays by Date (with type information for bridge day logic)
    const holidayMap = new Map<string, string | HolidayEntry>();
    // Normalize region codes for consistent comparison
    const normalizedParkRegion = normalizeRegionCode(park.regionCode);

    for (const h of holidays) {
      // Match: Nationwide holidays OR region matches (using normalized codes)
      const normalizedHolidayRegion = normalizeRegionCode(h.region);
      if (
        h.isNationwide ||
        (normalizedParkRegion &&
          normalizedHolidayRegion === normalizedParkRegion)
      ) {
        // Use park's timezone to determine the holiday date string
        // Normalize to noon UTC to prevent timezone shifts (YYYY-MM-DD from DB)
        const normalizedDate = new Date(h.date);
        normalizedDate.setUTCHours(12, 0, 0, 0);
        const d = formatInParkTimezone(normalizedDate, park.timezone);

        // If multiple holidays on same day, prefer public holidays over school holidays
        const existing = holidayMap.get(d);
        const hType = h.holidayType;

        if (!existing) {
          // Store as HolidayEntry to preserve type information for bridge day logic
          holidayMap.set(d, {
            name: h.localName || h.name || "",
            type: hType,
            allTypes: [hType],
          });
        } else {
          if (typeof existing !== "string") {
            // Aggregate types
            if (!existing.allTypes) existing.allTypes = [existing.type];
            if (!existing.allTypes.includes(hType)) {
              existing.allTypes.push(hType);
            }

            // Prioritize public holidays for the main entry
            if (hType === "public" || hType === "bank") {
              existing.name = h.localName || h.name || "";
              existing.type = hType;
            }
          }
        }
      }
    }

    let filledCount = 0;

    // 3. Batch collectors for INSERT/UPDATE operations (performance optimization)
    const entriesToInsert: Partial<ScheduleEntry>[] = [];
    const holidayUpdates: Array<{
      id: string;
      fields: {
        isHoliday: boolean;
        holidayName: string | null;
        isBridgeDay: boolean;
      };
    }> = [];
    const statusPromotions: string[] = []; // IDs to promote UNKNOWN → CLOSED
    const statusDemotions: string[] = []; // IDs to demote gap-filled CLOSED → UNKNOWN

    // 4. Iterate all days in range using date strings (avoids DST/timezone issues entirely)
    let dateStr = startStr;
    while (dateStr <= endStr) {
      // Noon-UTC date for this dateStr (DST-safe: noon never hits DST boundary)
      const noonUtc = new Date(`${dateStr}T12:00:00Z`);

      const holidayInfo = calculateHolidayInfo(
        noonUtc,
        holidayMap,
        park.timezone,
      );

      // If no entry exists for this date, collect it for batch insert
      if (!existingDates.has(dateStr)) {
        const scheduleType = isGapClosed(dateStr)
          ? ScheduleType.CLOSED
          : ScheduleType.UNKNOWN;

        entriesToInsert.push({
          parkId,
          date: noonUtc, // Noon UTC → correct date regardless of system/PG timezone
          scheduleType,
          description: "Gap-filled", // Distinguishes from API-provided entries (prevents wrong demotion)
          isHoliday: holidayInfo.isHoliday,
          holidayName: holidayInfo.holidayName,
          isBridgeDay: holidayInfo.isBridgeDay,
          openingTime: null,
          closingTime: null,
        });

        existingDates.add(dateStr); // Prevent duplicate within same run
        filledCount++;
      } else {
        // Entry exists: collect updates for batch processing (O(1) lookup via Map)
        const existing = existingEntryMap.get(dateStr)!;

        const holidayChanged =
          existing.isHoliday !== holidayInfo.isHoliday ||
          existing.holidayName !== holidayInfo.holidayName ||
          existing.isBridgeDay !== holidayInfo.isBridgeDay;
        const shouldBeClosed =
          existing.scheduleType === ScheduleType.UNKNOWN &&
          isGapClosed(dateStr);
        const shouldBeUnknown =
          existing.scheduleType === ScheduleType.CLOSED &&
          existing.description === "Gap-filled" && // Only demote gap-fill, never API-provided CLOSED
          maxOpStr !== null &&
          dateStr > maxOpStr;

        if (holidayChanged || shouldBeClosed || shouldBeUnknown) {
          // Collect updates instead of executing immediately
          if (shouldBeClosed) {
            statusPromotions.push(existing.id);
          } else if (shouldBeUnknown) {
            statusDemotions.push(existing.id);
          } else if (holidayChanged) {
            holidayUpdates.push({
              id: existing.id,
              fields: {
                isHoliday: holidayInfo.isHoliday,
                holidayName: holidayInfo.holidayName,
                isBridgeDay: holidayInfo.isBridgeDay,
              },
            });
          }
          filledCount++;
        }
      }

      // Advance to next day (noon UTC + 1 day → always correct, no DST issues)
      dateStr = addDays(noonUtc, 1).toISOString().slice(0, 10);
    }

    // 5. Execute batch operations (reduces ~364 queries to ~5 queries, 98.6% reduction)

    // Batch INSERT for new gap-filled entries
    if (entriesToInsert.length > 0) {
      await this.scheduleRepository
        .createQueryBuilder()
        .insert()
        .into(ScheduleEntry)
        .values(entriesToInsert)
        .execute();
    }

    // Batch UPDATE for UNKNOWN → CLOSED promotions
    if (statusPromotions.length > 0) {
      await this.scheduleRepository
        .createQueryBuilder()
        .update(ScheduleEntry)
        .set({
          scheduleType: ScheduleType.CLOSED,
          description: "Gap-filled", // Mark so we can safely demote later if maxOp shrinks
        })
        .whereInIds(statusPromotions)
        .execute();
    }

    // Batch UPDATE for gap-filled CLOSED → UNKNOWN demotions
    if (statusDemotions.length > 0) {
      await this.scheduleRepository
        .createQueryBuilder()
        .update(ScheduleEntry)
        .set({ scheduleType: ScheduleType.UNKNOWN })
        .whereInIds(statusDemotions)
        .execute();
    }

    // Batch UPDATE for holiday changes (fields differ per entry, so use VALUES)
    if (holidayUpdates.length > 0) {
      const values: string[] = [];
      const params: Array<string | boolean | null> = [];
      holidayUpdates.forEach((update, i) => {
        const o = i * 4;
        values.push(
          `($${o + 1}::uuid, $${o + 2}::boolean, $${o + 3}, $${o + 4}::boolean)`,
        );
        params.push(
          update.id,
          update.fields.isHoliday,
          update.fields.holidayName,
          update.fields.isBridgeDay,
        );
      });
      await this.scheduleRepository.query(
        `UPDATE schedule_entries s
         SET "isHoliday" = v.is_holiday,
             "holidayName" = v.holiday_name,
             "isBridgeDay" = v.is_bridge_day
         FROM (VALUES ${values.join(", ")}) AS v(id, is_holiday, holiday_name, is_bridge_day)
         WHERE s.id = v.id`,
        params,
      );
    }

    if (filledCount > 0) {
      await this.invalidateScheduleCache(parkId);
      this.logger.debug(
        `Filled or updated ${filledCount} schedule entries for Park ${parkId}`,
      );
    }
    return filledCount;
  }

  /**
   * Refreshes holiday and bridge day metadata for ALL parks
   */
  async fillAllParksGaps(): Promise<number> {
    this.logger.log("🔄 Starting gap filling for ALL parks...");

    // Clean up duplicate schedule entries before filling gaps
    await this.cleanupDuplicateScheduleEntries();

    const parks = await this.parkRepository.find({ select: ["id", "name"] });
    let totalUpdated = 0;

    for (const park of parks) {
      try {
        const count = await this.fillScheduleGaps(park.id);
        totalUpdated += count;
      } catch (error) {
        this.logger.error(`Failed gap filling for ${park.name}: ${error}`);
      }
    }

    this.logger.log(
      `✅ Completed gap filling. Total entries updated: ${totalUpdated}`,
    );
    return totalUpdated;
  }

  /**
   * Full schedule deduplication: handles ALL schedule entries, not just gap-filled ones.
   *
   * Phase 1 — Same-type duplicates:
   *   Multiple entries with identical (parkId, date, scheduleType).
   *   Keeps the most recent (by updatedAt), deletes the rest.
   *
   * Phase 2 — Cross-type conflicts:
   *   Multiple entries for the same (parkId, date) with different scheduleTypes.
   *   Priority: OPERATING > API-provided CLOSED > Gap-filled CLOSED > UNKNOWN.
   *   When a higher-priority entry exists, lower-priority entries are removed.
   */
  async cleanupDuplicateScheduleEntries(): Promise<number> {
    let deletedCount = 0;

    // ── Phase 1: same-type duplicates ──────────────────────────────────
    // Optimized: Single SQL query with window function (instead of N+1 queries)
    const deletedSameType = await this.scheduleRepository.query(`
      DELETE FROM schedule_entries
      WHERE id IN (
        SELECT id FROM (
          SELECT id,
                 ROW_NUMBER() OVER (
                   PARTITION BY "parkId", date, "scheduleType"
                   ORDER BY "updatedAt" DESC
                 ) as rn
          FROM schedule_entries
        ) sub
        WHERE rn > 1
      )
    `);
    deletedCount += deletedSameType[1] || 0;

    // ── Phase 2: cross-type conflicts ──────────────────────────────────
    // Optimized: Single SQL query with CTE + priority logic (instead of N+1 queries)
    const deletedCrossType = await this.scheduleRepository.query(`
      WITH ranked AS (
        SELECT id,
               ROW_NUMBER() OVER (
                 PARTITION BY "parkId", date
                 ORDER BY
                   CASE
                     WHEN "scheduleType" = 'OPERATING' THEN 0
                     WHEN "scheduleType" = 'CLOSED' AND description != 'Gap-filled' THEN 1
                     WHEN "scheduleType" = 'CLOSED' THEN 2
                     WHEN "scheduleType" = 'UNKNOWN' THEN 3
                     ELSE 4
                   END,
                   "updatedAt" DESC
               ) as rn
        FROM schedule_entries
        WHERE ("parkId", date) IN (
          SELECT "parkId", date
          FROM schedule_entries
          GROUP BY "parkId", date
          HAVING COUNT(DISTINCT "scheduleType") > 1
        )
      )
      DELETE FROM schedule_entries
      WHERE id IN (SELECT id FROM ranked WHERE rn > 1)
    `);
    deletedCount += deletedCrossType[1] || 0;

    if (deletedCount > 0) {
      this.logger.log(
        `🧹 Cleaned up ${deletedCount} duplicate schedule entries (optimized SQL).`,
      );
    } else {
      this.logger.debug("No duplicate schedule entries found.");
    }
    return deletedCount;
  }

  /**
   * Cleanup duplicate schedule entries for a single park (optimized for per-park operations).
   *
   * This is called by fillScheduleGaps to prevent duplicates from accumulating between
   * daily global cleanups. Uses the same priority logic as cleanupDuplicateScheduleEntries
   * but scoped to a single park for better performance.
   *
   * Phase 1: Remove same-type duplicates (keeps most recent by updatedAt)
   * Phase 2: Remove cross-type conflicts (priority: OPERATING > API-CLOSED > Gap-CLOSED > UNKNOWN)
   */
  private async cleanupDuplicateScheduleEntriesForPark(
    parkId: string,
  ): Promise<number> {
    let deletedCount = 0;

    // Phase 1: Same-type duplicates for this park
    const deletedSameType = await this.scheduleRepository.query(
      `
      DELETE FROM schedule_entries
      WHERE id IN (
        SELECT id FROM (
          SELECT id,
                 ROW_NUMBER() OVER (
                   PARTITION BY "parkId", date, "scheduleType"
                   ORDER BY "updatedAt" DESC
                 ) as rn
          FROM schedule_entries
          WHERE "parkId" = $1::uuid
        ) sub
        WHERE rn > 1
      )
    `,
      [parkId],
    );
    deletedCount += deletedSameType[1] || 0;

    // Phase 2: Cross-type conflicts for this park
    const deletedCrossType = await this.scheduleRepository.query(
      `
      WITH ranked AS (
        SELECT id,
               ROW_NUMBER() OVER (
                 PARTITION BY "parkId", date
                 ORDER BY
                   CASE
                     WHEN "scheduleType" = 'OPERATING' THEN 0
                     WHEN "scheduleType" = 'CLOSED' AND description != 'Gap-filled' THEN 1
                     WHEN "scheduleType" = 'CLOSED' THEN 2
                     WHEN "scheduleType" = 'UNKNOWN' THEN 3
                     ELSE 4
                   END,
                   "updatedAt" DESC
               ) as rn
        FROM schedule_entries
        WHERE "parkId" = $1::uuid
          AND ("parkId", date) IN (
            SELECT "parkId", date
            FROM schedule_entries
            WHERE "parkId" = $1::uuid
            GROUP BY "parkId", date
            HAVING COUNT(DISTINCT "scheduleType") > 1
          )
      )
      DELETE FROM schedule_entries
      WHERE id IN (SELECT id FROM ranked WHERE rn > 1)
    `,
      [parkId],
    );
    deletedCount += deletedCrossType[1] || 0;

    if (deletedCount > 0) {
      this.logger.debug(
        `🧹 Cleaned up ${deletedCount} duplicate schedule entries for park ${parkId}`,
      );
    }

    return deletedCount;
  }

  /**
   * Finds all parks that have latitude/longitude but missing geographic data
   *
   * Used for geocoding enrichment. Only returns parks that need geocoding.
   * Excludes parks that have already been attempted (to avoid wasting API quota).
   * Also filters out parks with imprecise coordinates (e.g., rounded to whole numbers).
   *
   * @returns Parks that need geocoding (have lat/lng but missing continent/country/city)
   */
  async findParksWithoutGeodata(): Promise<Park[]> {
    const parks = await this.parkRepository
      .createQueryBuilder("park")
      .where("park.latitude IS NOT NULL")
      .andWhere("park.longitude IS NOT NULL")
      // Remove strict missing data check to allow re-geocoding/verification of parks that have data (e.g. from Wiki)
      // but haven't been processed by our geocoder yet (geocodingAttemptedAt IS NULL).
      // This is crucial for applying Metro Mappings (e.g. fixing Bay Lake -> Orlando).
      .andWhere(
        "(park.geocodingAttemptedAt IS NULL OR (park.metadataRetryCount < 3 AND (park.countryCode IS NULL OR park.regionCode IS NULL OR park.city IS NULL OR park.country IS NULL)))",
      )
      .getMany();

    // Filter out parks with imprecise coordinates (rounded to whole numbers)
    // e.g., lat=28.0, lng=-81.0 is too generic
    return parks.filter((park) => {
      const lat = park.latitude;
      const lng = park.longitude;

      // Check if coordinates are too rounded (exactly .0 or only 1 decimal place)
      const latDecimals = (lat.toString().split(".")[1] || "").length;
      const lngDecimals = (lng.toString().split(".")[1] || "").length;

      // Skip if both coordinates have 0 or only 1 decimal place
      if (latDecimals <= 1 && lngDecimals <= 1) {
        this.logger.warn(
          `Skipping ${park.name} - coordinates too imprecise (${lat}, ${lng})`,
        );
        return false;
      }

      return true;
    });
  }

  /**
   * Updates geographic data for a park
   *
   * Used after geocoding to populate continent, country, and city fields.
   * Also generates corresponding slugs for continent, country, and city.
   * Also marks the park as attempted.
   *
   * IMPORTANT: Only updates fields that are currently NULL.
   * This allows manual data entry without risk of being overwritten.
   *
   * @param parkId - Park ID (UUID)
   * @param geodata - Geographic data (continent, country, city)
   */
  async updateGeodata(parkId: string, geodata: Partial<Park>): Promise<void> {
    const updates: Partial<Park> = {
      ...geodata,
      geocodingAttemptedAt: new Date(),
    };

    // Generate geographic slugs from their respective fields
    if (geodata.continent) {
      updates.continentSlug = generateSlug(geodata.continent);
    }
    if (geodata.country) {
      updates.countrySlug = generateSlug(geodata.country);
    }
    if (geodata.city) {
      updates.citySlug = generateSlug(geodata.city);
    }

    await this.parkRepository.update(parkId, updates);
  }

  /**
   * Marks a park as having had a geocoding attempt (even if failed)
   *
   * Used to prevent repeated API calls for parks that cannot be geocoded.
   *
   * @param parkId - Park ID (UUID)
   */
  async markGeocodingAttempted(parkId: string): Promise<void> {
    await this.parkRepository.update(parkId, {
      geocodingAttemptedAt: new Date(),
    });
  }

  /**
   * Gets unique country codes from all parks
   *
   * Used for holiday data sync - we only fetch holidays for countries
   * where we have parks.
   *
   * @returns Array of unique ISO 3166-1 alpha-2 country codes
   */
  async getUniqueCountries(): Promise<string[]> {
    const result = await this.parkRepository
      .createQueryBuilder("park")
      .select("DISTINCT park.country", "country")
      .where("park.country IS NOT NULL")
      .getRawMany();

    return result.map((r) => r.country).filter((c) => c);
  }

  /**
   * Gets schedule data for a park within a date range
   *
   * @param parkId - Park ID (UUID)
   * @param startDate - Start date (inclusive)
   * @param endDate - End date (inclusive)
   * @returns Schedule entries ordered by date
   */
  async getSchedule(
    parkId: string,
    startDate: Date,
    endDate: Date,
  ): Promise<ScheduleEntry[]> {
    return this.scheduleRepository
      .createQueryBuilder("schedule")
      .where("schedule.parkId = :parkId", { parkId })
      .andWhere("schedule.date >= :startDate", { startDate })
      .andWhere("schedule.date <= :endDate", { endDate })
      .orderBy("schedule.date", "ASC")
      .addOrderBy("schedule.scheduleType", "ASC")
      .getMany();
  }

  /**
   * Get schedule for a single calendar date (YYYY-MM-DD).
   * Use this for "today" / "tomorrow" to avoid timezone ambiguity:
   * schedule.date is a DATE column; comparing with a string is timezone-safe.
   */
  async getScheduleForDate(
    parkId: string,
    dateStr: string,
  ): Promise<ScheduleEntry[]> {
    return this.scheduleRepository
      .createQueryBuilder("schedule")
      .where("schedule.parkId = :parkId", { parkId })
      .andWhere("schedule.date = :dateStr", { dateStr })
      .orderBy("schedule.scheduleType", "ASC")
      .getMany();
  }

  /**
   * Get today's schedule for a park.
   * "Today" is the current calendar day in the park's timezone (00:00–23:59 park time).
   * Uses date-string equality so results are independent of DB session timezone.
   *
   * @param parkId - Park ID (UUID)
   * @returns Today's schedule entries
   */
  async getTodaySchedule(
    parkId: string,
    timezone?: string,
  ): Promise<ScheduleEntry[]> {
    let tz = timezone;
    if (!tz) {
      const park = await this.parkRepository.findOne({
        where: { id: parkId },
        select: ["id", "timezone"],
      });
      if (!park) return [];
      tz = park.timezone ?? "UTC";
    }

    const todayStr = getCurrentDateInTimezone(tz);
    const cacheKey = CacheKeys.scheduleToday(parkId, todayStr);
    const cached = await this.redis.get(cacheKey);

    const parsed = safeJsonParse<any[]>(cached); // corrupt entry = miss
    if (parsed) {
      return parsed.map((entry) => ({
        ...entry,
        date: new Date(entry.date),
        openingTime: entry.openingTime ? new Date(entry.openingTime) : null,
        closingTime: entry.closingTime ? new Date(entry.closingTime) : null,
      })) as ScheduleEntry[];
    }

    const schedule = await this.getScheduleForDate(parkId, todayStr);

    // Cache result
    await this.redis.set(
      cacheKey,
      JSON.stringify(schedule),
      "EX",
      this.TTL_SCHEDULE,
    );

    return schedule;
  }

  /**
   * Get next scheduled opening for a park
   *
   * Finds the next day when the park will be operating.
   * Useful for parks in off-season to show when they will next open.
   *
   * @param parkId - Park ID (UUID)
   * @returns Next operating schedule entry or null if none found
   */
  /**
   * Get next scheduled opening for a park.
   * "Tomorrow" is the next calendar day in the park's timezone; query uses date string for consistency.
   */
  async getNextSchedule(parkId: string): Promise<ScheduleEntry | null> {
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "timezone"],
    });

    if (!park) return null;

    const tomorrowStr = getTomorrowDateInTimezone(park.timezone || "UTC");
    const cacheKey = CacheKeys.scheduleNext(parkId, tomorrowStr);
    const cached = await this.redis.get(cacheKey);

    if (cached) {
      // "null" is the cached negative result (no upcoming operating day);
      // a corrupt entry instead falls through and rebuilds from the DB.
      if (cached === "null") return null;
      const parsed = safeJsonParse<any>(cached);
      if (parsed) {
        return {
          ...parsed,
          date: new Date(parsed.date),
          openingTime: parsed.openingTime ? new Date(parsed.openingTime) : null,
          closingTime: parsed.closingTime ? new Date(parsed.closingTime) : null,
        } as ScheduleEntry;
      }
    }

    // Look ahead: from park's "tomorrow" up to 365 days (use date string for lower bound)
    const lookAheadDate = new Date(tomorrowStr + "T12:00:00.000Z");
    lookAheadDate.setDate(lookAheadDate.getDate() + 365);

    const nextSchedule = await this.scheduleRepository
      .createQueryBuilder("schedule")
      .where("schedule.parkId = :parkId", { parkId })
      .andWhere("schedule.date >= :tomorrowStr", { tomorrowStr })
      .andWhere("schedule.date <= :lookAheadDate", { lookAheadDate })
      .andWhere("schedule.scheduleType = :type", {
        type: ScheduleType.OPERATING,
      })
      .andWhere("schedule.openingTime IS NOT NULL")
      .andWhere("schedule.closingTime IS NOT NULL")
      .orderBy("schedule.date", "ASC")
      .limit(1)
      .getOne();

    // Cache result (1 hour TTL)
    await this.redis.set(
      cacheKey,
      JSON.stringify(nextSchedule),
      "EX",
      this.TTL_SCHEDULE,
    );

    return nextSchedule;
  }

  /**
   * Batch fetch schedules for multiple parks
   * Returns both today's schedule and next schedule for all parks
   * Optimized to avoid N+1 queries by batching database queries
   *
   * @param parkIds - Array of park IDs
   * @returns Object with today and next schedule maps
   */
  async getBatchSchedules(parkIds: string[]): Promise<{
    today: Map<string, ScheduleEntry[]>;
    next: Map<string, ScheduleEntry | null>;
  }> {
    const todayMap = new Map<string, ScheduleEntry[]>();
    const nextMap = new Map<string, ScheduleEntry | null>();

    if (parkIds.length === 0) {
      return { today: todayMap, next: nextMap };
    }

    // Fetch parks with timezones
    const parks = await this.parkRepository.find({
      where: { id: In(parkIds) },
      select: ["id", "timezone"],
    });

    const timezoneMap = new Map<string, string>();
    for (const park of parks) {
      timezoneMap.set(park.id, park.timezone || "UTC");
    }

    // Try to get from cache first (today/tomorrow in each park's timezone)
    const cacheKeysToday = parks.map((p) =>
      CacheKeys.scheduleToday(
        p.id,
        getCurrentDateInTimezone(p.timezone || "UTC"),
      ),
    );
    const cacheKeysNext = parks.map((p) =>
      CacheKeys.scheduleNext(
        p.id,
        getTomorrowDateInTimezone(p.timezone || "UTC"),
      ),
    );

    const cachedToday = await this.redis.mget(...cacheKeysToday);
    const cachedNext = await this.redis.mget(...cacheKeysNext);

    // Process cached results and identify which parks need DB queries
    const parksNeedingTodayQuery: string[] = [];
    const parksNeedingNextQuery: string[] = [];

    for (let i = 0; i < parks.length; i++) {
      const park = parks[i];
      const parkId = park.id;

      // Process today's schedule
      const cachedTodayValue = cachedToday[i];
      if (cachedTodayValue) {
        try {
          const parsed = JSON.parse(cachedTodayValue) as any[];
          todayMap.set(
            parkId,
            parsed.map((entry) => ({
              ...entry,
              date: new Date(entry.date),
              openingTime: entry.openingTime
                ? new Date(entry.openingTime)
                : null,
              closingTime: entry.closingTime
                ? new Date(entry.closingTime)
                : null,
            })) as ScheduleEntry[],
          );
        } catch {
          parksNeedingTodayQuery.push(parkId);
        }
      } else {
        parksNeedingTodayQuery.push(parkId);
      }

      // Process next schedule
      const cachedNextValue = cachedNext[i];
      if (cachedNextValue) {
        try {
          const parsed = JSON.parse(cachedNextValue);
          if (parsed) {
            nextMap.set(parkId, {
              ...parsed,
              date: new Date(parsed.date),
              openingTime: parsed.openingTime
                ? new Date(parsed.openingTime)
                : null,
              closingTime: parsed.closingTime
                ? new Date(parsed.closingTime)
                : null,
            } as ScheduleEntry);
          } else {
            nextMap.set(parkId, null);
          }
        } catch {
          parksNeedingNextQuery.push(parkId);
        }
      } else {
        parksNeedingNextQuery.push(parkId);
      }
    }

    // Batch fetch today's schedules for parks not in cache
    if (parksNeedingTodayQuery.length > 0) {
      const parksForToday = parks.filter((p) =>
        parksNeedingTodayQuery.includes(p.id),
      );

      // Group by timezone for efficient querying
      const timezoneGroups = new Map<string, string[]>();
      for (const park of parksForToday) {
        const tz = park.timezone || "UTC";
        if (!timezoneGroups.has(tz)) {
          timezoneGroups.set(tz, []);
        }
        timezoneGroups.get(tz)!.push(park.id);
      }

      const todayPromises = Array.from(timezoneGroups.entries()).map(
        async ([timezone, ids]) => {
          const todayStr = getCurrentDateInTimezone(timezone);

          // Query by date string (schedule.date is DATE); avoids session-timezone ambiguity
          const schedules = await this.scheduleRepository
            .createQueryBuilder("schedule")
            .where("schedule.parkId IN (:...parkIds)", { parkIds: ids })
            .andWhere("schedule.date = :todayStr", { todayStr })
            .orderBy("schedule.parkId", "ASC")
            .addOrderBy("schedule.date", "ASC")
            .addOrderBy("schedule.scheduleType", "ASC")
            .getMany();

          // Group by parkId
          const scheduleMap = new Map<string, ScheduleEntry[]>();
          for (const schedule of schedules) {
            if (!scheduleMap.has(schedule.parkId)) {
              scheduleMap.set(schedule.parkId, []);
            }
            scheduleMap.get(schedule.parkId)!.push(schedule);
          }

          // Cache results
          for (const park of parksForToday.filter((p) => ids.includes(p.id))) {
            const parkSchedules = scheduleMap.get(park.id) || [];
            const cacheKey = CacheKeys.scheduleToday(park.id, todayStr);
            await this.redis.set(
              cacheKey,
              JSON.stringify(parkSchedules),
              "EX",
              this.TTL_SCHEDULE,
            );
            todayMap.set(park.id, parkSchedules);
          }

          return scheduleMap;
        },
      );

      await Promise.all(todayPromises);
    }

    // Batch fetch next schedules for parks not in cache
    if (parksNeedingNextQuery.length > 0) {
      const parksForNext = parks.filter((p) =>
        parksNeedingNextQuery.includes(p.id),
      );

      // Earliest "tomorrow" among parks (for query start); lookAhead = +365 days
      const tomorrowStrs = parksForNext.map((p) =>
        getTomorrowDateInTimezone(p.timezone || "UTC"),
      );
      const minTomorrowStr = tomorrowStrs.sort()[0];
      const minTomorrowDate = new Date(minTomorrowStr + "T12:00:00Z");
      const lookAheadDate = new Date(minTomorrowDate);
      lookAheadDate.setDate(lookAheadDate.getDate() + 365);

      // Fetch all next schedules in one query
      const nextSchedules = await this.scheduleRepository
        .createQueryBuilder("schedule")
        .where("schedule.parkId IN (:...parkIds)", {
          parkIds: parksNeedingNextQuery,
        })
        .andWhere("schedule.date >= :tomorrow", {
          tomorrow: minTomorrowStr,
        })
        .andWhere("schedule.date <= :lookAheadDate", { lookAheadDate })
        .andWhere("schedule.scheduleType = :type", {
          type: ScheduleType.OPERATING,
        })
        .andWhere("schedule.openingTime IS NOT NULL")
        .andWhere("schedule.closingTime IS NOT NULL")
        .orderBy("schedule.parkId", "ASC")
        .addOrderBy("schedule.date", "ASC")
        .getMany();

      // Group by parkId and take first (earliest) for each park
      const nextScheduleMap = new Map<string, ScheduleEntry>();
      for (const schedule of nextSchedules) {
        if (!nextScheduleMap.has(schedule.parkId)) {
          nextScheduleMap.set(schedule.parkId, schedule);
        }
      }

      // Cache and set results
      for (const park of parksForNext) {
        const schedule = nextScheduleMap.get(park.id) || null;
        const timezone = park.timezone || "UTC";
        const cacheKey = CacheKeys.scheduleNext(
          park.id,
          getTomorrowDateInTimezone(timezone),
        );
        await this.redis.set(
          cacheKey,
          JSON.stringify(schedule),
          "EX",
          this.TTL_SCHEDULE,
        );
        nextMap.set(park.id, schedule);
      }
    }

    return { today: todayMap, next: nextMap };
  }

  async getUpcomingSchedule(
    parkId: string,
    days: number = 7,
  ): Promise<ScheduleEntry[]> {
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "timezone"],
    });

    if (!park) return [];

    const tz = park.timezone || "UTC";
    // Range in PARK timezone: start 2 days ago (for late-night hours, cross-timezone), end today + days
    const startDate = getStartOfDayInTimezone(tz);
    const twoDaysAgo = addDays(startDate, -2);
    const endDate = addDays(startDate, days + 1);

    const cacheKey = CacheKeys.scheduleUpcoming(
      parkId,
      getCurrentDateInTimezone(tz),
      days,
    );
    const cached = safeJsonParse<any[]>(await this.redis.get(cacheKey)); // corrupt entry = miss

    if (cached) {
      return cached.map((entry) => ({
        ...entry,
        date: new Date(entry.date),
        openingTime: entry.openingTime ? new Date(entry.openingTime) : null,
        closingTime: entry.closingTime ? new Date(entry.closingTime) : null,
      })) as ScheduleEntry[];
    }

    const schedule = await this.getSchedule(parkId, twoDaysAgo, endDate);

    // Cache result (1 hour TTL)
    await this.redis.set(
      cacheKey,
      JSON.stringify(schedule),
      "EX",
      this.TTL_SCHEDULE,
    );

    return schedule;
  }

  /**
   * Finds parks by continent slug
   *
   * @param continentSlug - Continent slug (e.g., "north-america", "europe")
   * @returns Parks in the continent
   */
  async findByContinent(continentSlug: string): Promise<Park[]> {
    return this.parkRepository.find({
      where: { continentSlug },
      relations: ["destination"],
      order: { name: "ASC" },
    });
  }

  /**
   * Finds parks by continent and country slugs
   *
   * @param continentSlug - Continent slug
   * @param countrySlug - Country slug (e.g., "united-states", "germany")
   * @returns Parks in the country
   */
  async findByCountry(
    continentSlug: string,
    countrySlug: string,
  ): Promise<Park[]> {
    return this.parkRepository.find({
      where: { continentSlug, countrySlug },
      relations: ["destination"],
      order: { name: "ASC" },
    });
  }

  /**
   * Finds parks by continent, country, and city slugs
   *
   * @param continentSlug - Continent slug
   * @param countrySlug - Country slug
   * @param citySlug - City slug (e.g., "orlando", "rust")
   * @returns Parks in the city
   */
  async findByCity(
    continentSlug: string,
    countrySlug: string,
    citySlug: string,
  ): Promise<Park[]> {
    return this.parkRepository.find({
      where: { continentSlug, countrySlug, citySlug },
      relations: ["destination"],
      order: { name: "ASC" },
    });
  }

  /**
   * Finds a park by geographic path (continent/country/city/park)
   *
   * @param continentSlug - Continent slug
   * @param countrySlug - Country slug
   * @param citySlug - City slug
   * @param parkSlug - Park slug
   * @returns Park if found, null otherwise
   */
  async findByGeographicPath(
    continentSlug: string,
    countrySlug: string,
    citySlug: string,
    parkSlug: string,
  ): Promise<Park | null> {
    const cacheKey = `${continentSlug}:${countrySlug}:${citySlug}:${parkSlug}`;
    if (this.notFoundCache.has(cacheKey)) {
      return null; // known 404 — skip DB query
    }

    const park = await this.parkRepository.findOne({
      where: { continentSlug, countrySlug, citySlug, slug: parkSlug },
      relations: ["destination"],
    });

    if (!park) {
      this.notFoundCache.add(cacheKey);
    }

    return park;
  }

  /**
   * Loads attractions, shows, and restaurants onto an already-fetched park entity
   * using 3 parallel queries instead of a single JOIN.
   *
   * TypeORM's `relations` option produces a Cartesian product JOIN — for a park
   * with 200 attractions × 20 shows × 10 restaurants that means 40 000 intermediate
   * rows. This helper avoids that by issuing separate queries in parallel.
   */
  async loadParkRelations(park: Park): Promise<Park> {
    const em = this.parkRepository.manager;
    const [attractions, shows, restaurants] = await Promise.all([
      // Retired attractions leave every list that describes the park as it is
      // today. Their rows and history stay, and their own detail endpoint keeps
      // answering — a page saying "operated until February 2026" beats a 404.
      em.find(Attraction, { where: { parkId: park.id, retiredAt: IsNull() } }),
      em.find(Show, { where: { parkId: park.id } }),
      em.find(Restaurant, { where: { parkId: park.id } }),
    ]);
    park.attractions = attractions;
    park.shows = shows;
    park.restaurants = restaurants;
    return park;
  }

  /**
   * Finds a park by geographic path with all relations (attractions, shows, restaurants).
   * Use only for the main park endpoint that needs full relation data.
   */
  async findByGeographicPathWithRelations(
    continentSlug: string,
    countrySlug: string,
    citySlug: string,
    parkSlug: string,
  ): Promise<Park | null> {
    const park = await this.findByGeographicPath(
      continentSlug,
      countrySlug,
      citySlug,
      parkSlug,
    );
    if (!park) return null;
    return this.loadParkRelations(park);
  }

  /**
   * Checks if a park is currently open based on schedule and timezone
   *
   * Strategy:
   * 1. Get park's timezone
   * 2. Get current date/time in park's timezone
   * 3. Check today's schedule for OPERATING entries
   * 4. Verify current time is within operating hours
   *
   * @param parkId - Park ID (UUID)
   * @returns true if park is currently open, false otherwise
   */
  async isParkCurrentlyOpen(parkId: string): Promise<boolean> {
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "timezone"],
    });

    if (!park || !park.timezone) {
      // No timezone info = assume closed (safe default)
      return false;
    }

    const now = new Date();
    const parkDateStr = getCurrentDateInTimezone(park.timezone);

    // Query schedule for today in park's timezone
    const todaySchedule = await this.scheduleRepository.findOne({
      where: {
        parkId,
        date: parkDateStr as any,
        scheduleType: "OPERATING" as ScheduleType,
      },
    });

    if (!todaySchedule) {
      // No schedule = assume closed
      return false;
    }

    // Check if current time is within operating hours
    // openingTime and closingTime are stored as UTC timestamps
    if (!todaySchedule.openingTime || !todaySchedule.closingTime) {
      return false;
    }

    const openingTime = new Date(todaySchedule.openingTime);
    const closingTime = new Date(todaySchedule.closingTime);

    // Compare UTC timestamps
    return now >= openingTime && now < closingTime;
  }

  /**
   * Checks if a park is scheduled to operate today (even if currently closed)
   *
   * Used by WaitTimesProcessor to decide whether to fetch live data.
   * fetch data if park operates TODAY, even if closed right now (e.g. before opening/after closing).
   */
  async isParkOperatingToday(parkId: string): Promise<boolean> {
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "timezone"],
    });

    if (!park || !park.timezone) {
      return false;
    }

    const parkDateStr = getCurrentDateInTimezone(park.timezone);

    // Query schedule for today in park's timezone
    const todaySchedule = await this.scheduleRepository.findOne({
      where: {
        parkId,
        date: parkDateStr as any,
      },
    });

    // If we have a schedule entry, trust OPERATING/CLOSED explicitly.
    // UNKNOWN means the source has no data for today — treat like "no schedule":
    // default to true so we still generate predictions for potentially-open parks.
    if (todaySchedule) {
      if (todaySchedule.scheduleType === "OPERATING") return true;
      if (todaySchedule.scheduleType === "CLOSED") return false;
      // UNKNOWN → fall through to default below
    }

    // No schedule or UNKNOWN: check today's ride data to decide.
    // Uses park-local midnight as the start (not a rolling 24h) to avoid
    // yesterday's operating data leaking into today's closed determination.
    // ≥3 rides with data AND ≥25% with waitTime ≥ 10 → likely open.
    // ≥3 rides with data AND 0% with waitTime ≥ 10 → likely closed.
    // Too little data → default true (conservative, we don't know).
    const todayStart = getStartOfDayInTimezone(park.timezone);
    const rideStats: {
      withData: string;
      operating5min: string;
      operating10min: string;
    }[] = await this.parkRepository.manager.query(
      `
        SELECT
          COUNT(*) as "withData",
          SUM(CASE WHEN q.status = 'OPERATING' AND q."waitTime" >= 5 THEN 1 ELSE 0 END) as "operating5min",
          SUM(CASE WHEN q.status = 'OPERATING' AND q."waitTime" >= 10 THEN 1 ELSE 0 END) as "operating10min"
        FROM attractions a
        JOIN LATERAL (
          SELECT status, "waitTime"
          FROM queue_data qd
          WHERE qd."attractionId" = a.id
            AND qd.timestamp >= $2
          ORDER BY timestamp DESC
          LIMIT 1
        ) q ON true
        WHERE a."parkId" = $1::uuid
      `,
      [parkId, todayStart],
    );

    if (rideStats.length > 0) {
      const withData = parseInt(rideStats[0].withData, 10);
      const operating5min = parseInt(rideStats[0].operating5min, 10);
      const operating10min = parseInt(rideStats[0].operating10min, 10);
      if (withData >= 3) {
        return (
          operating10min / withData >= 0.25 || operating5min / withData >= 0.5
        );
      }
    }
    return true;
  }

  /**
   * Checks if any rides in the park have been active in the last 2 hours.
   * Used as a safety net to ensure active parks get predictions even if
   * the schedule says CLOSED.
   */
  async hasRecentRideActivity(
    parkId: string,
    hoursBack: number = 2,
  ): Promise<boolean> {
    const since = new Date(Date.now() - hoursBack * 60 * 60 * 1000);

    const result = await this.parkRepository.manager.query(
      `
      SELECT COUNT(*) as "activeRides"
      FROM queue_data qd
      INNER JOIN attractions a ON a.id = qd."attractionId"
      WHERE a."parkId" = $1::uuid
        AND qd.timestamp >= $2
        AND qd.status = 'OPERATING'
        AND qd."waitTime" >= 10
    `,
      [parkId, since],
    );

    const activeRides = parseInt(result[0]?.activeRides || "0", 10);
    // If at least 2 rides are active, we consider the park active
    return activeRides >= 2;
  }

  /**
   * Checks operating status for multiple parks efficiently
   *
   * @param parkIds - Array of Park IDs
   * @returns Map of Park ID -> Status ("OPERATING" or "CLOSED")
   */
  async getBatchParkStatus(
    parkIds: string[],
  ): Promise<Map<string, "OPERATING" | "CLOSED">> {
    if (parkIds.length === 0) {
      return new Map();
    }

    // Load park-level schedule entries for today (+ yesterday to cover post-midnight closings)
    const scheduleEntries = await this.scheduleRepository
      .createQueryBuilder("schedule")
      .where("schedule.parkId IN (:...parkIds)", { parkIds })
      .andWhere("schedule.attractionId IS NULL")
      .andWhere("schedule.date >= CURRENT_DATE - INTERVAL '1 day'")
      .andWhere("schedule.date <= CURRENT_DATE")
      .getMany();

    const schedulesByPark = new Map<string, ScheduleEntry[]>();
    for (const entry of scheduleEntries) {
      if (!schedulesByPark.has(entry.parkId)) {
        schedulesByPark.set(entry.parkId, []);
      }
      schedulesByPark.get(entry.parkId)!.push(entry);
    }

    // Load latest ride data (last 30 min) per attraction — used by isParkOpen() fallback
    // for parks that have no schedule integration at all.
    const rideRows: {
      parkId: string;
      status: string;
      waitTime: number;
      timestamp: string;
    }[] = await this.parkRepository.manager.query(
      `
      SELECT a."parkId", q.status, q."waitTime", q.timestamp
      FROM attractions a
      JOIN LATERAL (
        SELECT status, "waitTime", timestamp
        FROM queue_data qd
        WHERE qd."attractionId" = a.id
          AND qd.timestamp > NOW() - INTERVAL '30 minutes'
        ORDER BY timestamp DESC
        LIMIT 1
      ) q ON true
      WHERE a."parkId" = ANY($1)
      `,
      [parkIds],
    );

    const ridesByPark = new Map<string, RideStatusData[]>();
    for (const row of rideRows) {
      if (!ridesByPark.has(row.parkId)) {
        ridesByPark.set(row.parkId, []);
      }
      ridesByPark.get(row.parkId)!.push({
        status: row.status,
        waitTime: row.waitTime,
        lastUpdated: new Date(row.timestamp),
      });
    }

    const statusMap = new Map<string, "OPERATING" | "CLOSED">();
    for (const id of parkIds) {
      const isOpen = isParkOpen(
        schedulesByPark.get(id) ?? [],
        ridesByPark.get(id) ?? [],
      );
      statusMap.set(id, isOpen ? "OPERATING" : "CLOSED");
    }

    return statusMap;
  }

  /**
   * Gets all unique country codes relevant for holiday sync
   * Includes countries with parks AND all unique influencing countries
   */
  async getSyncCountryCodes(): Promise<string[]> {
    const parks = await this.parkRepository.find({
      select: ["countryCode", "influencingRegions"],
    });

    const codes = new Set<string>();

    for (const park of parks) {
      if (park.countryCode) {
        codes.add(park.countryCode);
      }

      if (park.influencingRegions) {
        for (const reg of park.influencingRegions) {
          if (reg.countryCode) {
            codes.add(reg.countryCode);
          }
        }
      }
    }

    return [...codes];
  }

  /**
   * Invalidates schedule cache for a park
   */
  async invalidateScheduleCache(parkId: string): Promise<void> {
    const keys = await this.redis.keys(CacheKeys.scheduleParkPattern(parkId));
    if (keys.length > 0) {
      await this.redis.del(...keys);
    }
  }

  /**
   * Invalidates calendar month cache for a park.
   * Called after schedule sync so stale UNKNOWN months are not served from cache
   * after ThemeParks Wiki publishes new opening hours.
   */
  async invalidateCalendarMonthCache(parkId: string): Promise<void> {
    const keys = await this.redis.keys(CacheKeys.calendarMonthPattern(parkId));
    if (keys.length > 0) {
      await this.redis.del(...keys);
      this.logger.debug(
        `Cleared ${keys.length} calendar month cache keys for park ${parkId}`,
      );
    }
  }
}
