import { Injectable, Logger, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { In, Repository } from "typeorm";
import { Park } from "../entities/park.entity";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { ParkMergeService } from "./park-merge.service";

export interface RepairResult {
  fixedQtMismatches: number;
  fixedWzMismatches: number;
  addedQtIds: number;
  addedWzIds: number;
  mergedDuplicates: number;
  errors: Array<{ parkId: string; error: string }>;
}

@Injectable()
export class ParkRepairService {
  private readonly logger = new Logger(ParkRepairService.name);

  constructor(
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly parkMergeService: ParkMergeService,
  ) {}

  /**
   * Fixes mismatched Queue-Times IDs
   */
  async fixMismatchedQueueTimesIds(
    fixes: Array<{ parkId: string; correctQtId: string }>,
  ): Promise<RepairResult> {
    this.logger.log(`🔧 Fixing ${fixes.length} mismatched Queue-Times IDs...`);

    const result: RepairResult = {
      fixedQtMismatches: 0,
      fixedWzMismatches: 0,
      addedQtIds: 0,
      addedWzIds: 0,
      mergedDuplicates: 0,
      errors: [],
    };

    const correctQtIds = fixes.map((f) => f.correctQtId);
    const conflicting = await this.parkRepository.find({
      where: { queueTimesEntityId: In(correctQtIds) },
      select: ["id", "name", "queueTimesEntityId"],
    });
    const qtIdToOwner = new Map(
      conflicting.map((p) => [p.queueTimesEntityId!, p]),
    );

    for (const fix of fixes) {
      try {
        const existingPark = qtIdToOwner.get(fix.correctQtId);
        if (existingPark && existingPark.id !== fix.parkId) {
          throw new Error(
            `QT-ID ${fix.correctQtId} is already used by park "${existingPark.name}" (${existingPark.id})`,
          );
        }

        await this.parkRepository.update(fix.parkId, {
          queueTimesEntityId: fix.correctQtId,
        });

        result.fixedQtMismatches++;
        this.logger.log(
          `✅ Fixed QT-ID for park ${fix.parkId}: ${fix.correctQtId}`,
        );
        await this.invalidateParkCache(fix.parkId);
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        result.errors.push({ parkId: fix.parkId, error: errorMessage });
        this.logger.error(
          `❌ Failed to fix QT-ID for park ${fix.parkId}: ${errorMessage}`,
        );
      }
    }

    this.logger.log(
      `✅ Fixed ${result.fixedQtMismatches} QT-ID mismatches, ${result.errors.length} errors`,
    );
    return result;
  }

  /**
   * Fixes mismatched Wartezeiten.app IDs
   */
  async fixMismatchedWartezeitenIds(
    fixes: Array<{ parkId: string; correctWzId: string }>,
  ): Promise<RepairResult> {
    this.logger.log(
      `🔧 Fixing ${fixes.length} mismatched Wartezeiten.app IDs...`,
    );

    const result: RepairResult = {
      fixedQtMismatches: 0,
      fixedWzMismatches: 0,
      addedQtIds: 0,
      addedWzIds: 0,
      mergedDuplicates: 0,
      errors: [],
    };

    const correctWzIds = fixes.map((f) => f.correctWzId);
    const conflicting = await this.parkRepository.find({
      where: { wartezeitenEntityId: In(correctWzIds) },
      select: ["id", "name", "wartezeitenEntityId"],
    });
    const wzIdToOwner = new Map(
      conflicting.map((p) => [p.wartezeitenEntityId!, p]),
    );

    for (const fix of fixes) {
      try {
        const existingPark = wzIdToOwner.get(fix.correctWzId);
        if (existingPark && existingPark.id !== fix.parkId) {
          throw new Error(
            `WZ-ID ${fix.correctWzId} is already used by park "${existingPark.name}" (${existingPark.id})`,
          );
        }

        await this.parkRepository.update(fix.parkId, {
          wartezeitenEntityId: fix.correctWzId,
        });

        result.fixedWzMismatches++;
        this.logger.log(
          `✅ Fixed WZ-ID for park ${fix.parkId}: ${fix.correctWzId}`,
        );
        await this.invalidateParkCache(fix.parkId);
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        result.errors.push({ parkId: fix.parkId, error: errorMessage });
        this.logger.error(
          `❌ Failed to fix WZ-ID for park ${fix.parkId}: ${errorMessage}`,
        );
      }
    }

    this.logger.log(
      `✅ Fixed ${result.fixedWzMismatches} WZ-ID mismatches, ${result.errors.length} errors`,
    );
    return result;
  }

  /**
   * Adds missing Queue-Times IDs
   */
  async addMissingQueueTimesIds(
    additions: Array<{ parkId: string; qtId: string }>,
  ): Promise<RepairResult> {
    this.logger.log(`🔧 Adding ${additions.length} missing Queue-Times IDs...`);

    const result: RepairResult = {
      fixedQtMismatches: 0,
      fixedWzMismatches: 0,
      addedQtIds: 0,
      addedWzIds: 0,
      mergedDuplicates: 0,
      errors: [],
    };

    const qtIds = additions.map((a) => a.qtId);
    const parkIds = additions.map((a) => a.parkId);
    const [conflicting, targetParks] = await Promise.all([
      this.parkRepository.find({
        where: { queueTimesEntityId: In(qtIds) },
        select: ["id", "name", "queueTimesEntityId"],
      }),
      this.parkRepository.find({
        where: { id: In(parkIds) },
        select: ["id", "queueTimesEntityId"],
      }),
    ]);
    const qtIdToOwner = new Map(
      conflicting.map((p) => [p.queueTimesEntityId!, p]),
    );
    const parkById = new Map(targetParks.map((p) => [p.id, p]));

    for (const addition of additions) {
      try {
        const existingPark = qtIdToOwner.get(addition.qtId);
        if (existingPark) {
          throw new Error(
            `QT-ID ${addition.qtId} is already used by park "${existingPark.name}" (${existingPark.id})`,
          );
        }

        const park = parkById.get(addition.parkId);
        if (!park) {
          throw new Error(`Park ${addition.parkId} not found`);
        }

        if (park.queueTimesEntityId) {
          throw new Error(
            `Park ${addition.parkId} already has QT-ID: ${park.queueTimesEntityId}`,
          );
        }

        await this.parkRepository.update(addition.parkId, {
          queueTimesEntityId: addition.qtId,
        });

        result.addedQtIds++;
        this.logger.log(
          `✅ Added QT-ID ${addition.qtId} to park ${addition.parkId}`,
        );
        await this.invalidateParkCache(addition.parkId);
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        result.errors.push({ parkId: addition.parkId, error: errorMessage });
        this.logger.error(
          `❌ Failed to add QT-ID for park ${addition.parkId}: ${errorMessage}`,
        );
      }
    }

    this.logger.log(
      `✅ Added ${result.addedQtIds} QT-IDs, ${result.errors.length} errors`,
    );
    return result;
  }

  /**
   * Adds missing Wartezeiten.app IDs
   */
  async addMissingWartezeitenIds(
    additions: Array<{ parkId: string; wzId: string }>,
  ): Promise<RepairResult> {
    this.logger.log(
      `🔧 Adding ${additions.length} missing Wartezeiten.app IDs...`,
    );

    const result: RepairResult = {
      fixedQtMismatches: 0,
      fixedWzMismatches: 0,
      addedQtIds: 0,
      addedWzIds: 0,
      mergedDuplicates: 0,
      errors: [],
    };

    const wzIds = additions.map((a) => a.wzId);
    const parkIds = additions.map((a) => a.parkId);
    const [conflicting, targetParks] = await Promise.all([
      this.parkRepository.find({
        where: { wartezeitenEntityId: In(wzIds) },
        select: ["id", "name", "wartezeitenEntityId"],
      }),
      this.parkRepository.find({
        where: { id: In(parkIds) },
        select: ["id", "wartezeitenEntityId"],
      }),
    ]);
    const wzIdToOwner = new Map(
      conflicting.map((p) => [p.wartezeitenEntityId!, p]),
    );
    const parkById = new Map(targetParks.map((p) => [p.id, p]));

    for (const addition of additions) {
      try {
        const existingPark = wzIdToOwner.get(addition.wzId);
        if (existingPark) {
          throw new Error(
            `WZ-ID ${addition.wzId} is already used by park "${existingPark.name}" (${existingPark.id})`,
          );
        }

        const park = parkById.get(addition.parkId);
        if (!park) {
          throw new Error(`Park ${addition.parkId} not found`);
        }

        if (park.wartezeitenEntityId) {
          throw new Error(
            `Park ${addition.parkId} already has WZ-ID: ${park.wartezeitenEntityId}`,
          );
        }

        await this.parkRepository.update(addition.parkId, {
          wartezeitenEntityId: addition.wzId,
        });

        result.addedWzIds++;
        this.logger.log(
          `✅ Added WZ-ID ${addition.wzId} to park ${addition.parkId}`,
        );
        await this.invalidateParkCache(addition.parkId);
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        result.errors.push({ parkId: addition.parkId, error: errorMessage });
        this.logger.error(
          `❌ Failed to add WZ-ID for park ${addition.parkId}: ${errorMessage}`,
        );
      }
    }

    this.logger.log(
      `✅ Added ${result.addedWzIds} WZ-IDs, ${result.errors.length} errors`,
    );
    return result;
  }

  /**
   * Repairs duplicate parks by merging them
   */
  async repairDuplicates(
    duplicates: Array<{ winnerId: string; loserId: string }>,
  ): Promise<RepairResult> {
    this.logger.log(`🔧 Merging ${duplicates.length} duplicate park pairs...`);

    const result: RepairResult = {
      fixedQtMismatches: 0,
      fixedWzMismatches: 0,
      addedQtIds: 0,
      addedWzIds: 0,
      mergedDuplicates: 0,
      errors: [],
    };

    for (const duplicate of duplicates) {
      try {
        const mergeResult = await this.parkMergeService.mergeParks(
          duplicate.winnerId,
          duplicate.loserId,
        );

        if (mergeResult.success) {
          result.mergedDuplicates++;
          this.logger.log(
            `✅ Merged "${mergeResult.loserName}" into "${mergeResult.winnerName}"`,
          );
        } else {
          throw new Error(`Merge failed: ${mergeResult.errors.join(", ")}`);
        }
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        result.errors.push({
          parkId: duplicate.loserId,
          error: errorMessage,
        });
        this.logger.error(
          `❌ Failed to merge duplicate parks ${duplicate.winnerId} / ${duplicate.loserId}: ${errorMessage}`,
        );
      }
    }

    this.logger.log(
      `✅ Merged ${result.mergedDuplicates} duplicate pairs, ${result.errors.length} errors`,
    );
    return result;
  }

  /**
   * Invalidates park-related caches
   */
  private async invalidateParkCache(parkId: string): Promise<void> {
    try {
      const patterns = [
        `park:integrated:${parkId}`,
        `park:${parkId}:*`,
        `schedule:${parkId}:*`,
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
