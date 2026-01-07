import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { DataSource, Repository } from "typeorm";
import { Park } from "../entities/park.entity";
import { ScheduleEntry } from "../entities/schedule-entry.entity";
import { ExternalEntityMapping } from "../../database/entities/external-entity-mapping.entity";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { Inject } from "@nestjs/common";
import { calculateParkPriority } from "../utils/park-merge.util";
import { hasScheduleData } from "../utils/park-merge.util";

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
   * Merges two parks into one
   *
   * @param winnerId - ID of the park to keep (winner)
   * @param loserId - ID of the park to merge into winner (loser)
   * @returns Merge result with statistics
   */
  async mergeParks(winnerId: string, loserId: string): Promise<MergeResult> {
    this.logger.log(`🔀 Starting park merge: ${loserId} → ${winnerId}`);

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
      errors: [],
    };

    try {
      await this.dataSource.transaction(async (manager) => {
        // Load both parks
        const winner = await manager.findOne(Park, {
          where: { id: winnerId },
        });
        const loser = await manager.findOne(Park, {
          where: { id: loserId },
        });

        if (!winner) {
          throw new Error(`Winner park ${winnerId} not found`);
        }
        if (!loser) {
          throw new Error(`Loser park ${loserId} not found`);
        }

        result.winnerName = winner.name;
        result.loserName = loser.name;

        // Determine winner if not explicitly provided (shouldn't happen, but safety check)
        // Note: We use the provided winnerId, but log if there's a better candidate
        const hasScheduleWinner = await hasScheduleData(
          winner.id,
          manager.getRepository(ScheduleEntry),
        );
        const hasScheduleLoser = await hasScheduleData(
          loser.id,
          manager.getRepository(ScheduleEntry),
        );
        const priorityWinner = calculateParkPriority(
          winner,
          hasScheduleWinner,
          false,
        );
        const priorityLoser = calculateParkPriority(
          loser,
          hasScheduleLoser,
          false,
        );

        if (priorityLoser > priorityWinner) {
          this.logger.warn(
            `⚠️ Loser park has higher priority (${priorityLoser} vs ${priorityWinner}), but using provided winner`,
          );
        }

        // 1. Consolidate Entity IDs
        await this.consolidateEntityIds(manager, winner, loser);

        // 2. Migrate Attractions (with collision handling)
        const attractionCount = await this.migrateAttractions(
          manager,
          winner.id,
          loser.id,
        );
        result.migratedAttractions = attractionCount;

        // 3. Migrate Shows
        const showCount = await this.migrateShows(manager, winner.id, loser.id);
        result.migratedShows = showCount;

        // 4. Migrate Restaurants
        const restaurantCount = await this.migrateRestaurants(
          manager,
          winner.id,
          loser.id,
        );
        result.migratedRestaurants = restaurantCount;

        // 5. Migrate ScheduleEntries
        const scheduleCount = await this.migrateScheduleEntries(
          manager,
          winner.id,
          loser.id,
        );
        result.migratedScheduleEntries = scheduleCount;

        // 6. Migrate ExternalEntityMappings (for park-level mappings)
        const mappingCount = await this.migrateParkMappings(
          manager,
          winner.id,
          loser.id,
        );
        result.migratedMappings = mappingCount;

        // 7. Delete the loser park
        await manager.delete(Park, loser.id);

        this.logger.log(
          `✅ Successfully merged "${loser.name}" into "${winner.name}"`,
        );
      });

      result.success = true;

      // 8. Invalidate caches
      await this.invalidateParkCaches(winnerId);

      this.logger.log(
        `✅ Merge complete: ${result.migratedAttractions} attractions, ${result.migratedShows} shows, ${result.migratedRestaurants} restaurants, ${result.migratedScheduleEntries} schedule entries, ${result.migratedMappings} mappings`,
      );
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
   * Consolidates Entity IDs from loser into winner
   */
  private async consolidateEntityIds(
    manager: any,
    winner: Park,
    loser: Park,
  ): Promise<void> {
    const updates: Partial<Park> = {};

    // Wiki ID: Winner keeps, loser's is ignored if winner has one
    if (!winner.wikiEntityId && loser.wikiEntityId) {
      updates.wikiEntityId = loser.wikiEntityId;
    }

    // Queue-Times ID: Winner keeps, loser's is ignored if winner has one
    if (!winner.queueTimesEntityId && loser.queueTimesEntityId) {
      updates.queueTimesEntityId = loser.queueTimesEntityId;
    }

    // Wartezeiten ID: Winner keeps, loser's is ignored if winner has one
    if (!winner.wartezeitenEntityId && loser.wartezeitenEntityId) {
      updates.wartezeitenEntityId = loser.wartezeitenEntityId;
    }

    // Update name to Wiki name if available (Wiki has priority)
    if (loser.wikiEntityId && !winner.wikiEntityId) {
      // If loser has Wiki ID but winner doesn't, we might want to use loser's name
      // But typically winner should have Wiki ID if it's the winner
      // So we keep winner's name
    }

    if (Object.keys(updates).length > 0) {
      await manager.update(Park, winner.id, updates);
      this.logger.log(
        `✅ Consolidated Entity IDs: ${Object.keys(updates).join(", ")}`,
      );
    }
  }

  /**
   * Migrates attractions from loser to winner, handling collisions
   */
  private async migrateAttractions(
    manager: any,
    winnerId: string,
    loserId: string,
  ): Promise<number> {
    // Get existing attractions from both parks
    const existingAttractions = await manager.query(
      `SELECT id, slug, "land_name", "land_external_id", "queue_times_entity_id" 
       FROM attractions 
       WHERE "parkId" = $1`,
      [winnerId],
    );

    const loserAttractions = await manager.query(
      `SELECT id, slug, "land_name", "land_external_id", "queue_times_entity_id" 
       FROM attractions 
       WHERE "parkId" = $1`,
      [loserId],
    );

    let migratedCount = 0;

    for (const loserAttr of loserAttractions) {
      const match = existingAttractions.find(
        (a: any) => a.slug === loserAttr.slug,
      );

      if (match) {
        // COLLISION: Merge data, move mappings, delete ghost attraction
        const updates: string[] = [];

        // 1. Copy Land Data if missing in primary
        if (loserAttr.land_name || loserAttr.land_external_id) {
          await manager.query(
            `UPDATE attractions 
             SET "land_name" = COALESCE("land_name", $1),
                 "land_external_id" = COALESCE("land_external_id", $2)
             WHERE id = $3`,
            [loserAttr.land_name, loserAttr.land_external_id, match.id],
          );
          updates.push("land data");
        }

        // 2. Copy Queue-Times ID if missing
        if (loserAttr.queue_times_entity_id && !match.queue_times_entity_id) {
          await manager.query(
            `UPDATE attractions 
             SET "queue_times_entity_id" = $1
             WHERE id = $2`,
            [loserAttr.queue_times_entity_id, match.id],
          );
          updates.push("QT-ID");
        }

        // 3. Move External Mappings from Ghost to Primary
        const mappingUpdate = await manager.query(
          `UPDATE external_entity_mapping 
           SET "internal_entity_id" = $1 
           WHERE "internal_entity_id" = $2`,
          [match.id, loserAttr.id],
        );
        if (mappingUpdate[1] > 0) {
          updates.push(`${mappingUpdate[1]} mappings`);
        }

        // 4. Move Queue Data (Wait Times History)
        const queueDataUpdate = await manager.query(
          `UPDATE queue_data 
           SET "attractionId" = $1 
           WHERE "attractionId" = $2`,
          [match.id, loserAttr.id],
        );
        if (queueDataUpdate[1] > 0) {
          updates.push(`${queueDataUpdate[1]} queue data entries`);
        }

        // 5. Move Wait Time Predictions
        const predictionsUpdate = await manager.query(
          `UPDATE wait_time_predictions 
           SET "attractionId" = $1 
           WHERE "attractionId" = $2`,
          [match.id, loserAttr.id],
        );
        if (predictionsUpdate[1] > 0) {
          updates.push(`${predictionsUpdate[1]} predictions`);
        }

        // 6. Move Prediction Accuracy Records
        const accuracyUpdate = await manager.query(
          `UPDATE prediction_accuracy 
           SET "attraction_id" = $1 
           WHERE "attraction_id" = $2`,
          [match.id, loserAttr.id],
        );
        if (accuracyUpdate[1] > 0) {
          updates.push(`${accuracyUpdate[1]} accuracy records`);
        }

        // 7. Delete the ghost attraction
        await manager.query(`DELETE FROM attractions WHERE id = $1`, [
          loserAttr.id,
        ]);

        this.logger.log(
          `    Merged attraction "${loserAttr.slug}": ${updates.join(", ")}`,
        );
        migratedCount++;
      } else {
        // NO COLLISION: Move
        await manager.query(
          `UPDATE attractions SET "parkId" = $1 WHERE id = $2`,
          [winnerId, loserAttr.id],
        );
        migratedCount++;
      }
    }

    return migratedCount;
  }

  /**
   * Migrates shows from loser to winner
   */
  private async migrateShows(
    manager: any,
    winnerId: string,
    loserId: string,
  ): Promise<number> {
    const result = await manager.query(
      `UPDATE shows SET "parkId" = $1 WHERE "parkId" = $2`,
      [winnerId, loserId],
    );
    return result[1] || 0;
  }

  /**
   * Migrates restaurants from loser to winner
   */
  private async migrateRestaurants(
    manager: any,
    winnerId: string,
    loserId: string,
  ): Promise<number> {
    const result = await manager.query(
      `UPDATE restaurants SET "parkId" = $1 WHERE "parkId" = $2`,
      [winnerId, loserId],
    );
    return result[1] || 0;
  }

  /**
   * Migrates schedule entries from loser to winner
   */
  private async migrateScheduleEntries(
    manager: any,
    winnerId: string,
    loserId: string,
  ): Promise<number> {
    const result = await manager.query(
      `UPDATE schedule_entries SET "parkId" = $1 WHERE "parkId" = $2`,
      [winnerId, loserId],
    );
    return result[1] || 0;
  }

  /**
   * Migrates park-level ExternalEntityMappings from loser to winner
   */
  private async migrateParkMappings(
    manager: any,
    winnerId: string,
    loserId: string,
  ): Promise<number> {
    const result = await manager.query(
      `UPDATE external_entity_mapping 
       SET "internal_entity_id" = $1 
       WHERE "internal_entity_id" = $2 
       AND "internal_entity_type" = 'park'`,
      [winnerId, loserId],
    );
    return result[1] || 0;
  }

  /**
   * Invalidates all caches related to the merged park
   */
  private async invalidateParkCaches(parkId: string): Promise<void> {
    try {
      const patterns = [
        `park:integrated:${parkId}`,
        `park:${parkId}:*`,
        `schedule:${parkId}:*`,
        `schedule:*:${parkId}:*`,
        `wait-times:${parkId}:*`,
      ];

      for (const pattern of patterns) {
        const keys = await this.redis.keys(pattern);
        if (keys.length > 0) {
          await this.redis.del(...keys);
        }
      }
    } catch (error) {
      this.logger.warn(
        `Failed to invalidate cache for park ${parkId}: ${error}`,
      );
    }
  }
}
