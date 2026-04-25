import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { DataSource, Repository } from "typeorm";
import { Park } from "../entities/park.entity";
import { ScheduleEntry } from "../entities/schedule-entry.entity";
import { ExternalEntityMapping } from "../../database/entities/external-entity-mapping.entity";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
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
        await this.consolidateEntityIds(manager, winner, loser);

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
          .set({ internal_entity_id: winner.id })
          .where(
            "internal_entity_id = :loserId AND internal_entity_type = 'park'",
            { loserId: loser.id },
          )
          .execute()
          .then((r) => r.affected || 0);

        // 6. Delete the loser park (now empty of related data)
        await manager.delete(Park, loser.id);

        this.logger.log(
          `✅ Successfully merged "${loser.name}" into "${winner.name}"`,
        );
      });

      result.success = true;
      await this.invalidateParkCaches(winnerId);
      await this.invalidateParkCaches(loserId);
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      result.errors.push(errorMessage);
      this.logger.error(`❌ Merge failed: ${errorMessage}`);
      throw error;
    }

    return result;
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
    const loserEntities = await manager.query(
      `SELECT id, slug, name FROM ${tableName} WHERE "parkId" = $1`,
      [loserId],
    );
    let count = 0;

    for (const entity of loserEntities) {
      // Find potential match in winner park
      const match = await manager.query(
        `SELECT id FROM ${tableName} WHERE "parkId" = $1 AND (slug = $2 OR name = $3)`,
        [winnerId, entity.slug, entity.name],
      );

      if (match.length > 0) {
        const winnerEntityId = match[0].id;
        // Collision: Move all dependent data to winner's entity
        await this.consolidateEntityData(
          manager,
          tableName,
          winnerEntityId,
          entity.id,
        );
        // Delete redundant entity
        await manager.query(`DELETE FROM ${tableName} WHERE id = $1`, [
          entity.id,
        ]);
      } else {
        // No collision: Just re-parent
        await manager.query(
          `UPDATE ${tableName} SET "parkId" = $1 WHERE id = $2`,
          [winnerId, entity.id],
        );
      }
      count++;
    }
    return count;
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

      await manager.query(
        `UPDATE queue_data SET "attractionId" = $1 WHERE "attractionId" = $2`,
        [winnerId, loserId],
      );
      await manager.query(
        `UPDATE forecast_data SET "attractionId" = $1 WHERE "attractionId" = $2`,
        [winnerId, loserId],
      );
      await manager.query(
        `UPDATE wait_time_predictions SET "attractionId" = $1 WHERE "attractionId" = $2`,
        [winnerId, loserId],
      );

      // Accuracy tables
      await manager.query(
        `UPDATE prediction_accuracy SET attraction_id = $1 WHERE attraction_id = $2`,
        [winnerId, loserId],
      );
      await manager.query(
        `DELETE FROM attraction_accuracy_stats WHERE attraction_id = $1`,
        [loserId],
      );
      await manager.query(
        `DELETE FROM attraction_p50_baselines WHERE "attractionId" = $1`,
        [loserId],
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

  private async consolidateEntityIds(
    manager: any,
    winner: Park,
    loser: Park,
  ): Promise<void> {
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
  }

  private async invalidateParkCaches(parkId: string): Promise<void> {
    try {
      const patterns = [
        `park:integrated:${parkId}`,
        `park:${parkId}:*`,
        `schedule:${parkId}:*`,
        `wait-times:${parkId}:*`,
      ];
      for (const pattern of patterns) {
        const keys = await this.redis.keys(pattern);
        if (keys.length > 0) await this.redis.del(...keys);
      }
    } catch (_e) {}
  }
}
