import { Injectable, Logger } from "@nestjs/common";
import { invalidateParkCaches } from "../../common/cache/park-cache-invalidation";
import { InjectRepository } from "@nestjs/typeorm";
import { DataSource, Repository } from "typeorm";
import { Park } from "../entities/park.entity";
import { canInheritSourceIds } from "../utils/source-id-inheritance.util";
import { ScheduleEntry } from "../entities/schedule-entry.entity";
import { ExternalEntityMapping } from "../../database/entities/external-entity-mapping.entity";
import { ParkSlugAlias } from "../entities/park-slug-alias.entity";
import { captureParkPath, samePath } from "./park-rename.service";
import {
  ATTRACTION_DEPENDENCIES,
  PARK_DEPENDENCIES,
  applyMergeDependencies,
} from "../utils/merge-dependencies";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { RevalidationService } from "../../common/revalidation/revalidation.service";
import { Inject } from "@nestjs/common";
import {
  calculateParkPriority as _calculateParkPriority,
  hasScheduleData as _hasScheduleData,
} from "../utils/park-merge.util";

export interface MergeResult {
  success: boolean;
  winnerId: string;
  loserId: string;
  winnerName: string;
  loserName: string;
  migratedAttractions: number;
  migratedShows: number;
  migratedRestaurants: number;
  migratedScheduleEntries: number;
  migratedMappings: number;
  migratedStats: number;
  /**
   * Upstream feeds the winner did NOT take over, because the loser turned out
   * to describe a different place. Empty in the ordinary case. Non-empty means
   * the surviving park has one fewer source than the pair had, which someone
   * should look at.
   */
  skippedSourceIds: string[];
  errors: string[];
}

@Injectable()
export class ParkMergeService {
  private readonly logger = new Logger(ParkMergeService.name);

  constructor(
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
    @InjectRepository(ScheduleEntry)
    private readonly scheduleRepository: Repository<ScheduleEntry>,
    @InjectRepository(ExternalEntityMapping)
    private readonly mappingRepository: Repository<ExternalEntityMapping>,
    private readonly dataSource: DataSource,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly revalidation: RevalidationService,
  ) {}

  /**
   * Merges two parks into one.
   * Comprehensive merge including all related entities and historical data.
   */
  async mergeParks(winnerId: string, loserId: string): Promise<MergeResult> {
    this.logger.log(
      `🔀 Starting COMPREHENSIVE park merge: ${loserId} → ${winnerId}`,
    );

    const result: MergeResult = {
      success: false,
      winnerId,
      loserId,
      winnerName: "",
      loserName: "",
      migratedAttractions: 0,
      migratedShows: 0,
      migratedRestaurants: 0,
      migratedScheduleEntries: 0,
      migratedMappings: 0,
      migratedStats: 0,
      skippedSourceIds: [],
      errors: [],
    };

    try {
      await this.dataSource.transaction(async (manager) => {
        // Load both parks
        const winner = await manager.findOne(Park, { where: { id: winnerId } });
        const loser = await manager.findOne(Park, { where: { id: loserId } });

        if (!winner || !loser) {
          throw new Error(
            `Park not found (Winner: ${!!winner}, Loser: ${!!loser})`,
          );
        }

        result.winnerName = winner.name;
        result.loserName = loser.name;

        // 1. Consolidate Park-Level Metadata & IDs
        result.skippedSourceIds = await this.consolidateEntityIds(
          manager,
          winner,
          loser,
        );

        // 2. Migrate Core Entities with Collision Handling (Attractions, Shows, Restaurants)
        result.migratedAttractions = await this.migrateEntities(
          manager,
          "attractions",
          winner.id,
          loser.id,
        );
        result.migratedShows = await this.migrateEntities(
          manager,
          "shows",
          winner.id,
          loser.id,
        );
        result.migratedRestaurants = await this.migrateEntities(
          manager,
          "restaurants",
          winner.id,
          loser.id,
        );

        // 3. Migrate Historical Stats & Timeseries
        result.migratedStats = await this.migrateTableData(
          manager,
          "park_daily_stats",
          "parkId",
          winner.id,
          loser.id,
          ["date"],
        );
        result.migratedScheduleEntries = await this.migrateTableData(
          manager,
          "schedule_entries",
          "parkId",
          winner.id,
          loser.id,
          ["date", "scheduleType"],
        );

        // 4. Migrate Park-Specific Analysis Tables
        // park_p50_baselines: winner's baseline is authoritative; only migrate if winner has none
        await this.migrateTableData(
          manager,
          "park_p50_baselines",
          "parkId",
          winner.id,
          loser.id,
          null,
        );
        await this.migrateTableData(
          manager,
          "park_occupancy",
          "parkId",
          winner.id,
          loser.id,
          ["timestamp"],
        );
        await this.migrateTableData(
          manager,
          "headliner_attractions",
          "parkId",
          winner.id,
          loser.id,
          ["attractionId"],
        );
        await this.migrateTableData(
          manager,
          "weather_data",
          "parkId",
          winner.id,
          loser.id,
          ["date"],
        );

        // 5. Migrate Park-Level Mappings
        result.migratedMappings = await manager
          .createQueryBuilder()
          .update(ExternalEntityMapping)
          // Entity property, not the column name — TypeORM resolves the
          // mapping itself and throws "Property ... was not found" otherwise.
          .set({ internalEntityId: winner.id })
          .where(
            "internal_entity_id = :loserId AND internal_entity_type = 'park'",
            { loserId: loser.id },
          )
          .execute()
          .then((r) => r.affected || 0);

        // 5b. Park-scoped tables outside the nine migrated above. Without this
        // the DELETE either fails (attraction_p50/p90 baselines carry a
        // parkId with FK NO ACTION) or silently destroys park_slug_aliases,
        // which is what keeps already-indexed URLs alive.
        await applyMergeDependencies(
          manager,
          PARK_DEPENDENCIES,
          winner.id,
          loser.id,
        );

        // 5c. The loser's own path was live and indexed — the Tampa row for
        // Universal Islands of Adventure served an empty page in six locales.
        // Point it at the winner before the row disappears, or every one of
        // those URLs becomes a 404 instead of a redirect.
        const loserPath = captureParkPath(loser);
        const winnerPath = captureParkPath(winner);
        if (loserPath && winnerPath && !samePath(loserPath, winnerPath)) {
          await manager
            .createQueryBuilder()
            .insert()
            .into(ParkSlugAlias)
            .values({ parkId: winner.id, ...loserPath })
            .orIgnore()
            .execute();
        }

        // 6. Delete the loser park (now empty of related data)
        await manager.delete(Park, loser.id);

        this.logger.log(
          `✅ Successfully merged "${loser.name}" into "${winner.name}"`,
        );
      });

      result.success = true;
      // The winner now owns the loser's migrated attractions; evict their
      // integrated/baseline caches (which embed park context) alongside the
      // park-scoped caches.
      const winnerAttractionIds: string[] = await this.dataSource
        .query(`SELECT id FROM attractions WHERE "parkId" = $1`, [winnerId])
        .then((rows: Array<{ id: string }>) => rows.map((r) => r.id))
        .catch(() => [] as string[]);
      await this.invalidateParkCaches(winnerId, winnerAttractionIds);
      await this.invalidateParkCaches(loserId);

      // A merge removes a park and reparents its rides, so the geo tree, the
      // park list and the attraction pages are all stale on the frontend —
      // whether or not the surviving park's own URL moved. Rename already
      // does this via handlePathChange; a same-path merge went unannounced
      // and left the deleted park on the site for up to 24h.
      try {
        await this.revalidation.revalidateTags(["geo", "parks", "attractions"]);
      } catch (error) {
        // The merge itself is committed; a webhook the frontend did not answer
        // must not turn a successful merge into a failure.
        this.logger.warn(
          `Could not revalidate frontend after merging into ${winnerId}: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      result.errors.push(errorMessage);
      this.logger.error(`❌ Merge failed: ${errorMessage}`);
      throw error;
    }

    return result;
  }

  private static readonly ALLOWED_TABLE_NAMES = new Set([
    "attractions",
    "shows",
    "restaurants",
    "park_daily_stats",
    "schedule_entries",
    "park_p50_baselines",
    "park_occupancy",
    "headliner_attractions",
    "weather_data",
    // Declared in merge-dependencies.ts; the identifiers there are covered by
    // a test that pins them to the safe-identifier pattern.
    ...ATTRACTION_DEPENDENCIES.map((d) => d.table),
    ...PARK_DEPENDENCIES.map((d) => d.table),
  ]);

  private static readonly ALLOWED_COLUMN_NAMES = new Set([
    "parkId",
    "attractionId",
    "date",
    "scheduleType",
    "timestamp",
    ...ATTRACTION_DEPENDENCIES.flatMap((d) => [
      d.column,
      ...(d.conflictColumns ?? []),
    ]),
    ...PARK_DEPENDENCIES.flatMap((d) => [
      d.column,
      ...(d.conflictColumns ?? []),
    ]),
  ]);

  private assertAllowedIdentifier(
    value: string,
    allowedSet: Set<string>,
    context: string,
  ): void {
    if (!allowedSet.has(value)) {
      const msg = `Disallowed SQL identifier "${value}" in ${context} — add it to the allowlist if intentional`;
      this.logger.error(msg);
      throw new Error(msg);
    }
  }

  /**
   * Universal entity migration (Attractions, Shows, Restaurants)
   * Handles collisions by merging time-series data and deleting the duplicate entity.
   */
  private async migrateEntities(
    manager: any,
    tableName: string,
    winnerId: string,
    loserId: string,
  ): Promise<number> {
    this.assertAllowedIdentifier(
      tableName,
      ParkMergeService.ALLOWED_TABLE_NAMES,
      "migrateEntities",
    );

    const loserEntities = await manager.query(
      `SELECT id, slug, name FROM ${tableName} WHERE "parkId" = $1`,
      [loserId],
    );

    if (loserEntities.length === 0) return 0;

    const winnerEntities = await manager.query(
      `SELECT id, slug, name FROM ${tableName} WHERE "parkId" = $1`,
      [winnerId],
    );

    const winnerBySlug = new Map<string, string>(
      winnerEntities.filter((e: any) => e.slug).map((e: any) => [e.slug, e.id]),
    );
    const winnerByName = new Map<string, string>(
      winnerEntities.filter((e: any) => e.name).map((e: any) => [e.name, e.id]),
    );

    const collisions: { loserEntityId: string; winnerEntityId: string }[] = [];
    const toReparent: string[] = [];

    for (const entity of loserEntities) {
      const matchId =
        (entity.slug && winnerBySlug.get(entity.slug)) ||
        (entity.name && winnerByName.get(entity.name));

      if (matchId) {
        collisions.push({ loserEntityId: entity.id, winnerEntityId: matchId });
      } else {
        toReparent.push(entity.id);
      }
    }

    for (const { loserEntityId, winnerEntityId } of collisions) {
      await this.consolidateEntityData(
        manager,
        tableName,
        winnerEntityId,
        loserEntityId,
      );
    }

    if (collisions.length > 0) {
      await manager.query(
        `DELETE FROM ${tableName} WHERE id = ANY($1::uuid[])`,
        [collisions.map((c) => c.loserEntityId)],
      );
    }

    if (toReparent.length > 0) {
      await manager.query(
        `UPDATE ${tableName} SET "parkId" = $1 WHERE id = ANY($2::uuid[])`,
        [winnerId, toReparent],
      );
    }

    return loserEntities.length;
  }

  /**
   * Moves all dependent data (queue_data, mappings, etc.) from one entity to another.
   */
  private async consolidateEntityData(
    manager: any,
    type: string,
    winnerId: string,
    loserId: string,
  ): Promise<void> {
    // 1. Mappings
    await manager.query(
      `DELETE FROM external_entity_mapping WHERE internal_entity_id = $1 AND (external_source, external_entity_id) IN 
       (SELECT external_source, external_entity_id FROM external_entity_mapping WHERE internal_entity_id = $2)`,
      [loserId, winnerId],
    );
    await manager.query(
      `UPDATE external_entity_mapping SET internal_entity_id = $1 WHERE internal_entity_id = $2`,
      [winnerId, loserId],
    );

    if (type === "attractions") {
      // Temporarily lift decompression limit for TimescaleDB
      await manager.query(
        "SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0",
      );

      await applyMergeDependencies(
        manager,
        ATTRACTION_DEPENDENCIES,
        winnerId,
        loserId,
      );

      await manager.query(
        "SET timescaledb.max_tuples_decompressed_per_dml_transaction = 100000",
      );
    }
    // Note: Add show/restaurant specific consolidation if needed
  }

  /**
   * Generic table data migration with duplicate prevention.
   * conflictColumns: columns that form a unique constraint (duplicates removed from loser before migration).
   * Pass null to use winner-authoritative mode: loser rows are only migrated when winner has none.
   */
  private async migrateTableData(
    manager: any,
    tableName: string,
    idColumn: string,
    winnerId: string,
    loserId: string,
    conflictColumns: string[] | null,
  ): Promise<number> {
    this.assertAllowedIdentifier(
      tableName,
      ParkMergeService.ALLOWED_TABLE_NAMES,
      "migrateTableData tableName",
    );
    this.assertAllowedIdentifier(
      idColumn,
      ParkMergeService.ALLOWED_COLUMN_NAMES,
      "migrateTableData idColumn",
    );
    conflictColumns?.forEach((col) =>
      this.assertAllowedIdentifier(
        col,
        ParkMergeService.ALLOWED_COLUMN_NAMES,
        "migrateTableData conflictColumns",
      ),
    );

    if (conflictColumns === null) {
      // Winner-authoritative: only migrate loser's data if winner has no rows at all
      const winnerRows = await manager.query(
        `SELECT 1 FROM ${tableName} WHERE "${idColumn}" = $1 LIMIT 1`,
        [winnerId],
      );
      if (winnerRows.length > 0) {
        // Winner already has data — discard loser's to avoid overwriting authoritative data
        await manager.query(
          `DELETE FROM ${tableName} WHERE "${idColumn}" = $1`,
          [loserId],
        );
        return 0;
      }
    } else if (conflictColumns.length > 0) {
      // Remove loser rows that would collide with winner rows on the given columns
      const conflictList = conflictColumns.map((c) => `"${c}"`).join(", ");
      await manager.query(
        `DELETE FROM ${tableName} WHERE "${idColumn}" = $1 AND (${conflictList}) IN
         (SELECT ${conflictList} FROM ${tableName} WHERE "${idColumn}" = $2)`,
        [loserId, winnerId],
      );
    }

    const result = await manager.query(
      `UPDATE ${tableName} SET "${idColumn}" = $1 WHERE "${idColumn}" = $2`,
      [winnerId, loserId],
    );
    return result[1] || 0;
  }

  /**
   * Fill the winner's empty upstream ids from the loser — unless the two rows
   * describe different places.
   *
   * An upstream id is not a fact about the loser, it is a claim about which
   * park the source is describing. Copied onto the wrong row it wires a park to
   * another park's feed and, worse, hides itself: the sync stops finding the
   * real park under its own id and creates a second row for it days later. See
   * `canInheritSourceIds` for the case that taught us this.
   */
  private async consolidateEntityIds(
    manager: any,
    winner: Park,
    loser: Park,
  ): Promise<string[]> {
    const { allowed, distanceKm } = canInheritSourceIds(winner, loser);
    if (!allowed) {
      const skipped = [
        loser.wikiEntityId && !winner.wikiEntityId ? "wiki" : null,
        loser.queueTimesEntityId && !winner.queueTimesEntityId
          ? "queue-times"
          : null,
        loser.wartezeitenEntityId && !winner.wartezeitenEntityId
          ? "wartezeiten"
          : null,
      ].filter((source): source is string => source !== null);

      if (skipped.length > 0) {
        // Loud on purpose: the park now has one fewer feed than it could, which
        // is a thing someone should look at — and far easier to notice than a
        // park quietly serving another park's wait times.
        this.logger.warn(
          `🌍 Not inheriting ${skipped.join(", ")} id(s) from "${loser.name}" — ` +
            `it sits ${distanceKm} km from "${winner.name}", so the ids describe another place`,
        );
      }
      return skipped;
    }

    const updates: Partial<Park> = {};
    if (!winner.wikiEntityId && loser.wikiEntityId)
      updates.wikiEntityId = loser.wikiEntityId;
    if (!winner.queueTimesEntityId && loser.queueTimesEntityId)
      updates.queueTimesEntityId = loser.queueTimesEntityId;
    if (!winner.wartezeitenEntityId && loser.wartezeitenEntityId)
      updates.wartezeitenEntityId = loser.wartezeitenEntityId;

    if (Object.keys(updates).length > 0) {
      await manager.update(Park, winner.id, updates);
    }
    return [];
  }

  private async invalidateParkCaches(
    parkId: string,
    attractionIds: string[] = [],
  ): Promise<void> {
    try {
      await invalidateParkCaches(this.redis, parkId, attractionIds);
    } catch (e) {
      this.logger.warn(
        `Failed to invalidate caches for park ${parkId}: ${(e as Error)?.message ?? e}`,
      );
    }
  }
}
