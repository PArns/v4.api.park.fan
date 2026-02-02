import {
  Controller,
  Post,
  HttpCode,
  HttpStatus,
  Inject,
  Body,
  Query,
  HttpException,
  Logger,
} from "@nestjs/common";
import {
  ApiTags,
  ApiOperation,
  ApiResponse,
  ApiSecurity,
  ApiBody,
} from "@nestjs/swagger";
import { InjectQueue } from "@nestjs/bull";
import { Queue } from "bull";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { ParkValidatorService } from "../parks/services/park-validator.service";
import { ParkRepairService } from "../parks/services/park-repair.service";
import { ParkMergeService } from "../parks/services/park-merge.service";

/**
 * Admin Controller
 *
 * ⚠️ SECURITY NOTICE:
 * These administrative endpoints are protected in production via Cloudflare.
 * Access requires `pass=XXX` query parameter with valid API key.
 *
 * On development/local environments, endpoints are accessible without authentication.
 */
@ApiTags("admin")
@ApiSecurity("admin-auth")
@Controller("admin")
export class AdminController {
  private readonly logger = new Logger(AdminController.name);

  constructor(
    @InjectQueue("holidays") private holidaysQueue: Queue,
    @InjectQueue("park-metadata") private parkMetadataQueue: Queue,
    @InjectQueue("park-enrichment") private parkEnrichmentQueue: Queue,
    @InjectQueue("ml-training") private mlTrainingQueue: Queue,
    @InjectQueue("wait-times") private waitTimesQueue: Queue,
    @InjectQueue("children-metadata") private childrenQueue: Queue,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly parkValidatorService: ParkValidatorService,
    private readonly parkRepairService: ParkRepairService,
    private readonly parkMergeService: ParkMergeService,
  ) {}

  /**
   * Manually trigger holiday sync
   *
   * Forces a complete resync of all holidays from Nager.Date API.
   * Useful after code changes to holiday storage logic.
   */
  @Post("sync-holidays")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Trigger holiday sync",
    description:
      "Manually triggers a complete resync of all holidays from Nager.Date API",
  })
  @ApiResponse({
    status: 202,
    description: "Holiday sync job queued successfully",
  })
  async triggerHolidaySync(): Promise<{ message: string; jobId: string }> {
    const job = await this.holidaysQueue.add(
      "fetch-holidays",
      {},
      { priority: 10 },
    );
    return {
      message: "Holiday sync job queued",
      jobId: job.id.toString(),
    };
  }

  /**
   * Manually trigger park metadata sync
   *
   * Forces a complete resync of all parks from all sources (Wiki, Queue-Times, Wartezeiten).
   * Useful for testing duplicate detection and matching improvements.
   */
  @Post("sync-parks")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Trigger park metadata sync",
    description:
      "Manually triggers a complete resync of all parks from all sources (Wiki, Queue-Times, Wartezeiten)",
  })
  @ApiResponse({
    status: 202,
    description: "Park metadata sync job queued successfully",
  })
  async triggerParkSync(): Promise<{ message: string; jobId: string }> {
    const job = await this.parkMetadataQueue.add(
      "sync-all-parks",
      {},
      { priority: 10 },
    );
    return {
      message: "Park metadata sync job queued",
      jobId: job.id.toString(),
    };
  }

  /**
   * Manually trigger schedule gap filling for all parks
   *
   * Updates holiday/bridge day metadata in schedule entries.
   */
  @Post("fill-schedule-gaps")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Fill schedule gaps",
    description:
      "Triggers schedule gap filling to update holiday/bridge day metadata",
  })
  @ApiResponse({
    status: 202,
    description: "Schedule gap filling job queued successfully",
  })
  async triggerScheduleGapFilling(): Promise<{
    message: string;
    jobId: string;
  }> {
    const job = await this.parkMetadataQueue.add(
      "fill-all-gaps",
      {},
      { priority: 5 },
    );
    return {
      message: "Schedule gap filling job queued",
      jobId: job.id.toString(),
    };
  }

  /**
   * Manually trigger park enrichment
   *
   * Enriches all parks with ISO country codes and influencing regions.
   * Useful for fixing missing countryCode fields.
   */
  @Post("enrich-parks")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Trigger park enrichment",
    description:
      "Manually triggers park enrichment to set countryCode from country names and update influencing regions",
  })
  @ApiResponse({
    status: 202,
    description: "Park enrichment job queued successfully",
  })
  async triggerParkEnrichment(): Promise<{ message: string; jobId: string }> {
    const job = await this.parkEnrichmentQueue.add(
      "enrich-all",
      {},
      { priority: 10 },
    );
    return {
      message: "Park enrichment job queued",
      jobId: job.id.toString(),
    };
  }

  /**
   * Manually trigger ML model training
   *
   * Forces a complete model retraining with latest data.
   */
  @Post("train-ml-model")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Trigger ML training",
    description: "Manually triggers ML model training - takes 1-2 minutes",
  })
  @ApiResponse({
    status: 202,
    description: "ML training job queued successfully",
  })
  async triggerMLTraining(): Promise<{ message: string; jobId: string }> {
    const job = await this.mlTrainingQueue.add(
      "train-model",
      {},
      { priority: 10 },
    );
    return {
      message: "ML training job queued",
      jobId: job.id.toString(),
    };
  }

  /**
   * Flush park-related Redis cache
   *
   * Clears only park-related cached data (schedules, wait times, analytics, etc.)
   * while preserving Bull queue jobs and system caches.
   */
  @Post("flush-cache")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Flush park cache",
    description:
      "Clears park-related cached data (schedules, wait times, analytics) without affecting queue jobs",
  })
  @ApiResponse({
    status: 200,
    description: "Park cache flushed successfully",
  })
  async flushCache(): Promise<{ message: string; keysDeleted: number }> {
    // Define park-related cache key patterns
    const patterns = [
      "schedule:*",
      "park:*",
      "parks:*",
      "wait-times:*",
      "analytics:*",
      "occupancy:*",
      "predictions:*",
      "holiday:*",
      "attraction:*",
      "show:*",
      "restaurant:*",
      "weather:*",
      "search:*",
      "discovery:*",
    ];

    let totalDeleted = 0;

    // Delete keys matching each pattern
    for (const pattern of patterns) {
      const keys = await this.redis.keys(pattern);
      if (keys.length > 0) {
        await this.redis.del(...keys);
        totalDeleted += keys.length;
      }
    }

    return {
      message: "Park cache flushed successfully",
      keysDeleted: totalDeleted,
    };
  }

  /**
   * Complete Cache Reset and Rebuild
   *
   * ⚠️ WARNING: Performs FLUSHALL on Redis, clearing ALL cache data.
   * Queue jobs are NOT affected (separate storage mechanism).
   *
   * SECURITY: This operation is protected by Cloudflare in production and requires
   * explicit confirmation via `confirm=true` query parameter to prevent accidental execution.
   *
   * Use when:
   * - Discovery structure is corrupted or out of sync
   * - Major database schema changes occurred
   * - Cache contains stale/invalid data
   *
   * Pipeline order (by priority):
   * 1. Holidays (100) - Base geographic/temporal metadata
   * 2. Parks (90) - Park metadata, geocoding, matching
   * 3. Children (80) - Attractions, Shows, Restaurants
   * 4. Live Data (70) - Current wait times and schedules
   */
  @Post("cache/reset")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Complete cache reset and rebuild",
    description:
      "⚠️ SECURITY: Performs FLUSHALL on Redis and triggers complete data rebuild pipeline. " +
      "Requires explicit confirmation via ?confirm=true query parameter. Use with extreme caution.",
  })
  @ApiResponse({
    status: 200,
    description: "Cache completely flushed and rebuild jobs triggered",
  })
  @ApiResponse({
    status: 400,
    description:
      "Confirmation required. Add ?confirm=true to confirm FLUSHALL operation.",
  })
  async resetCache(@Query("confirm") confirm?: string): Promise<{
    message: string;
    flushed: string;
    jobsTriggered: string[];
  }> {
    // SECURITY: Require explicit confirmation to prevent accidental FLUSHALL
    if (confirm !== "true") {
      throw new HttpException(
        {
          statusCode: HttpStatus.BAD_REQUEST,
          message:
            "FLUSHALL operation requires explicit confirmation. Add ?confirm=true to confirm.",
          warning:
            "This operation will delete ALL Redis cache data. This cannot be undone.",
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    // Perform complete Redis flush
    // SECURITY: This is a dangerous operation, but protected by Cloudflare in production
    this.logger.warn(
      "⚠️  Executing FLUSHALL on Redis - all cache data will be deleted",
    );
    await this.redis.flushall();

    // Trigger complete rebuild pipeline
    const jobsTriggered: string[] = [];

    await this.holidaysQueue.add("fetch-holidays", {}, { priority: 100 });
    jobsTriggered.push("fetch-holidays");

    await this.parkMetadataQueue.add("sync-all-parks", {}, { priority: 90 });
    jobsTriggered.push("sync-all-parks");

    await this.childrenQueue.add("fetch-all-children", {}, { priority: 80 });
    jobsTriggered.push("fetch-all-children");

    await this.waitTimesQueue.add("fetch-wait-times", {}, { priority: 70 });
    jobsTriggered.push("fetch-wait-times");

    return {
      message: "Complete cache reset and rebuild started",
      flushed: "ALL (FLUSHALL executed)",
      jobsTriggered,
    };
  }

  /**
   * Validate and repair park data
   *
   * Validates all parks against external APIs (Queue-Times, Wartezeiten.app)
   * and optionally repairs found issues automatically.
   */
  @Post("validate-and-repair-parks")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Validate and repair parks",
    description:
      "Validates all parks against Queue-Times and Wartezeiten.app APIs. " +
      "Detects mismatched IDs, missing IDs, and duplicates. " +
      "Optionally repairs issues automatically if autoFix=true.",
  })
  @ApiBody({
    schema: {
      type: "object",
      properties: {
        autoFix: {
          type: "boolean",
          description: "Automatically repair found issues",
          default: false,
        },
      },
    },
  })
  @ApiResponse({
    status: 200,
    description:
      "Validation complete. Returns report with found issues and repair results.",
  })
  async validateAndRepairParks(
    @Body() body: { autoFix?: boolean } = {},
  ): Promise<{
    validation: {
      mismatchedQtIds: number;
      mismatchedWzIds: number;
      missingQtIds: number;
      missingWzIds: number;
      duplicates: number;
      summary: {
        totalParks: number;
        parksWithQtId: number;
        parksWithWzId: number;
        issuesFound: number;
      };
    };
    repair?: {
      fixedQtMismatches: number;
      fixedWzMismatches: number;
      addedQtIds: number;
      addedWzIds: number;
      mergedDuplicates: number;
      errors: number;
    };
    report: {
      mismatchedQtIds: Array<{
        parkId: string;
        parkName: string;
        currentQtId: string;
        reason: string;
      }>;
      mismatchedWzIds: Array<{
        parkId: string;
        parkName: string;
        currentWzId: string;
        reason: string;
      }>;
      missingQtIds: Array<{
        parkId: string;
        parkName: string;
        suggestedQtId: string;
      }>;
      missingWzIds: Array<{
        parkId: string;
        parkName: string;
        suggestedWzId: string;
      }>;
      duplicates: Array<{
        park1: { id: string; name: string };
        park2: { id: string; name: string };
        score: number;
        reason: string;
      }>;
    };
  }> {
    const autoFix = body.autoFix === true;

    // Run validation
    const validationReport = await this.parkValidatorService.validateAll();

    let repairResult = null;

    if (autoFix) {
      // Auto-fix mismatched QT IDs (but we need to determine correct IDs first)
      // For now, we'll only fix missing IDs and note mismatches for manual review
      const qtFixes: Array<{ parkId: string; correctQtId: string }> = [];
      const wzFixes: Array<{ parkId: string; correctWzId: string }> = [];
      const qtAdditions: Array<{ parkId: string; qtId: string }> = [];
      const wzAdditions: Array<{ parkId: string; wzId: string }> = [];

      // Add missing IDs
      for (const missing of validationReport.missingQtIds) {
        qtAdditions.push({
          parkId: missing.parkId,
          qtId: missing.suggestedQtId,
        });
      }

      for (const missing of validationReport.missingWzIds) {
        wzAdditions.push({
          parkId: missing.parkId,
          wzId: missing.suggestedWzId,
        });
      }

      // Note: Mismatched IDs require manual review to determine correct IDs
      // We don't auto-fix them as it could cause data loss

      // Perform repairs
      const [qtResult, wzResult, qtAddResult, wzAddResult] = await Promise.all([
        qtFixes.length > 0
          ? this.parkRepairService.fixMismatchedQueueTimesIds(qtFixes)
          : Promise.resolve({
              fixedQtMismatches: 0,
              fixedWzMismatches: 0,
              addedQtIds: 0,
              addedWzIds: 0,
              mergedDuplicates: 0,
              errors: [],
            }),
        wzFixes.length > 0
          ? this.parkRepairService.fixMismatchedWartezeitenIds(wzFixes)
          : Promise.resolve({
              fixedQtMismatches: 0,
              fixedWzMismatches: 0,
              addedQtIds: 0,
              addedWzIds: 0,
              mergedDuplicates: 0,
              errors: [],
            }),
        qtAdditions.length > 0
          ? this.parkRepairService.addMissingQueueTimesIds(qtAdditions)
          : Promise.resolve({
              fixedQtMismatches: 0,
              fixedWzMismatches: 0,
              addedQtIds: 0,
              addedWzIds: 0,
              mergedDuplicates: 0,
              errors: [],
            }),
        wzAdditions.length > 0
          ? this.parkRepairService.addMissingWartezeitenIds(wzAdditions)
          : Promise.resolve({
              fixedQtMismatches: 0,
              fixedWzMismatches: 0,
              addedQtIds: 0,
              addedWzIds: 0,
              mergedDuplicates: 0,
              errors: [],
            }),
      ]);

      repairResult = {
        fixedQtMismatches:
          qtResult.fixedQtMismatches + wzResult.fixedQtMismatches,
        fixedWzMismatches:
          qtResult.fixedWzMismatches + wzResult.fixedWzMismatches,
        addedQtIds: qtAddResult.addedQtIds,
        addedWzIds: wzAddResult.addedWzIds,
        mergedDuplicates: 0, // Merges require manual confirmation
        errors: [
          ...qtResult.errors,
          ...wzResult.errors,
          ...qtAddResult.errors,
          ...wzAddResult.errors,
        ],
      };
    }

    return {
      validation: {
        mismatchedQtIds: validationReport.mismatchedQtIds.length,
        mismatchedWzIds: validationReport.mismatchedWzIds.length,
        missingQtIds: validationReport.missingQtIds.length,
        missingWzIds: validationReport.missingWzIds.length,
        duplicates: validationReport.duplicates.length,
        summary: validationReport.summary,
      },
      repair: repairResult
        ? {
            ...repairResult,
            errors: repairResult.errors.length,
          }
        : undefined,
      report: {
        mismatchedQtIds: validationReport.mismatchedQtIds.map((m) => ({
          parkId: m.parkId,
          parkName: m.parkName,
          currentQtId: m.currentQtId,
          reason: m.reason,
        })),
        mismatchedWzIds: validationReport.mismatchedWzIds.map((m) => ({
          parkId: m.parkId,
          parkName: m.parkName,
          currentWzId: m.currentWzId,
          reason: m.reason,
        })),
        missingQtIds: validationReport.missingQtIds.map((m) => ({
          parkId: m.parkId,
          parkName: m.parkName,
          suggestedQtId: m.suggestedQtId,
        })),
        missingWzIds: validationReport.missingWzIds.map((m) => ({
          parkId: m.parkId,
          parkName: m.parkName,
          suggestedWzId: m.suggestedWzId,
        })),
        duplicates: validationReport.duplicates.map((d) => ({
          park1: d.park1,
          park2: d.park2,
          score: d.score,
          reason: d.reason,
        })),
      },
    };
  }

  /**
   * Merge duplicate parks
   *
   * Identifies and merges duplicate parks, or merges specific parks if IDs are provided.
   */
  @Post("merge-duplicate-parks")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Merge duplicate parks",
    description:
      "Identifies duplicate parks automatically or merges specific parks if park1Id and park2Id are provided. " +
      "Winner is determined by priority (Wiki-ID, more Entity-IDs, more Child-Entities, older park).",
  })
  @ApiBody({
    schema: {
      type: "object",
      properties: {
        park1Id: {
          type: "string",
          description: "First park ID (optional, for manual merge)",
        },
        park2Id: {
          type: "string",
          description: "Second park ID (optional, for manual merge)",
        },
        autoDetect: {
          type: "boolean",
          description: "Automatically detect and merge all duplicates",
          default: false,
        },
      },
    },
  })
  @ApiResponse({
    status: 200,
    description: "Merge operation completed",
  })
  async mergeDuplicateParks(
    @Body()
    body: {
      park1Id?: string;
      park2Id?: string;
      autoDetect?: boolean;
    } = {},
  ): Promise<{
    message: string;
    merged: number;
    results: Array<{
      winnerId: string;
      winnerName: string;
      loserId: string;
      loserName: string;
      migratedAttractions: number;
      migratedShows: number;
      migratedRestaurants: number;
      migratedScheduleEntries: number;
      migratedMappings: number;
    }>;
    errors: Array<{ parkId: string; error: string }>;
  }> {
    const results: Array<{
      winnerId: string;
      winnerName: string;
      loserId: string;
      loserName: string;
      migratedAttractions: number;
      migratedShows: number;
      migratedRestaurants: number;
      migratedScheduleEntries: number;
      migratedMappings: number;
    }> = [];
    const errors: Array<{ parkId: string; error: string }> = [];

    if (body.autoDetect) {
      // Auto-detect duplicates
      const duplicates = await this.parkValidatorService.findDuplicates();

      if (duplicates.length === 0) {
        return {
          message: "No duplicates found",
          merged: 0,
          results: [],
          errors: [],
        };
      }

      // Use repair service to handle merges
      const mergePairs: Array<{ winnerId: string; loserId: string }> = [];
      const parkRepo = this.parkValidatorService.getParkRepository();

      for (const duplicate of duplicates) {
        // Determine winner based on priority
        const park1 = await parkRepo.findOne({
          where: { id: duplicate.park1.id },
        });
        const park2 = await parkRepo.findOne({
          where: { id: duplicate.park2.id },
        });

        if (!park1 || !park2) {
          errors.push({
            parkId: duplicate.park1.id || duplicate.park2.id,
            error: "Park not found",
          });
          continue;
        }

        // Simple priority: Wiki-ID > more Entity-IDs > older
        let winnerId = duplicate.park1.id;
        let loserId = duplicate.park2.id;

        if (park2.wikiEntityId && !park1.wikiEntityId) {
          winnerId = duplicate.park2.id;
          loserId = duplicate.park1.id;
        } else if (
          (park1.wikiEntityId && park2.wikiEntityId) ||
          (!park1.wikiEntityId && !park2.wikiEntityId)
        ) {
          // Count Entity-IDs
          const count1 =
            (park1.wikiEntityId ? 1 : 0) +
            (park1.queueTimesEntityId ? 1 : 0) +
            (park1.wartezeitenEntityId ? 1 : 0);
          const count2 =
            (park2.wikiEntityId ? 1 : 0) +
            (park2.queueTimesEntityId ? 1 : 0) +
            (park2.wartezeitenEntityId ? 1 : 0);

          if (count2 > count1) {
            winnerId = duplicate.park2.id;
            loserId = duplicate.park1.id;
          } else if (count1 === count2 && park2.createdAt < park1.createdAt) {
            // Older park wins if counts are equal
            winnerId = duplicate.park2.id;
            loserId = duplicate.park1.id;
          }
        }

        mergePairs.push({ winnerId, loserId });
      }

      // Use repair service to perform merges
      const repairResult =
        await this.parkRepairService.repairDuplicates(mergePairs);

      // Convert repair result to response format
      // Note: repairDuplicates doesn't return detailed migration counts per merge
      // We'll use the duplicate info for names
      for (let i = 0; i < mergePairs.length; i++) {
        const pair = mergePairs[i];
        const duplicateInfo = duplicates.find(
          (d) =>
            (d.park1.id === pair.winnerId && d.park2.id === pair.loserId) ||
            (d.park2.id === pair.winnerId && d.park1.id === pair.loserId),
        );

        const winnerName =
          duplicateInfo?.park1.id === pair.winnerId
            ? duplicateInfo.park1.name
            : duplicateInfo?.park2.name || "Unknown";
        const loserName =
          duplicateInfo?.park1.id === pair.loserId
            ? duplicateInfo.park1.name
            : duplicateInfo?.park2.name || "Unknown";

        // Check if this merge was successful (no error for this pair)
        const hasError = repairResult.errors.some(
          (e) => e.parkId === pair.loserId,
        );

        if (!hasError) {
          // Note: We don't have detailed migration counts from repairDuplicates
          // The merge service logs them, but repairDuplicates doesn't return them
          // For now, we'll use placeholder values
          results.push({
            winnerId: pair.winnerId,
            winnerName,
            loserId: pair.loserId,
            loserName,
            migratedAttractions: 0, // Would need to enhance repairDuplicates to return this
            migratedShows: 0,
            migratedRestaurants: 0,
            migratedScheduleEntries: 0,
            migratedMappings: 0,
          });
        }
      }

      // Add errors from repair result
      errors.push(...repairResult.errors);
    } else if (body.park1Id && body.park2Id) {
      // Manual merge - determine winner
      const parkRepo = this.parkValidatorService.getParkRepository();
      const park1 = await parkRepo.findOne({
        where: { id: body.park1Id },
      });
      const park2 = await parkRepo.findOne({
        where: { id: body.park2Id },
      });

      if (!park1 || !park2) {
        return {
          message: "One or both parks not found",
          merged: 0,
          results: [],
          errors: [
            {
              parkId: body.park1Id || body.park2Id,
              error: "Park not found",
            },
          ],
        };
      }

      // Determine winner
      let winnerId = body.park1Id;
      let loserId = body.park2Id;

      if (park2.wikiEntityId && !park1.wikiEntityId) {
        winnerId = body.park2Id;
        loserId = body.park1Id;
      } else if (
        (park1.wikiEntityId && park2.wikiEntityId) ||
        (!park1.wikiEntityId && !park2.wikiEntityId)
      ) {
        const count1 =
          (park1.wikiEntityId ? 1 : 0) +
          (park1.queueTimesEntityId ? 1 : 0) +
          (park1.wartezeitenEntityId ? 1 : 0);
        const count2 =
          (park2.wikiEntityId ? 1 : 0) +
          (park2.queueTimesEntityId ? 1 : 0) +
          (park2.wartezeitenEntityId ? 1 : 0);

        if (count2 > count1) {
          winnerId = body.park2Id;
          loserId = body.park1Id;
        } else if (count1 === count2 && park2.createdAt < park1.createdAt) {
          winnerId = body.park2Id;
          loserId = body.park1Id;
        }
      }

      try {
        const mergeResult = await this.parkMergeService.mergeParks(
          winnerId,
          loserId,
        );

        if (mergeResult.success) {
          results.push({
            winnerId: mergeResult.winnerId,
            winnerName: mergeResult.winnerName,
            loserId: mergeResult.loserId,
            loserName: mergeResult.loserName,
            migratedAttractions: mergeResult.migratedAttractions,
            migratedShows: mergeResult.migratedShows,
            migratedRestaurants: mergeResult.migratedRestaurants,
            migratedScheduleEntries: mergeResult.migratedScheduleEntries,
            migratedMappings: mergeResult.migratedMappings,
          });
        } else {
          errors.push({
            parkId: loserId,
            error: mergeResult.errors.join(", "),
          });
        }
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        errors.push({ parkId: loserId, error: errorMessage });
      }
    } else {
      return {
        message:
          "Either autoDetect=true or both park1Id and park2Id must be provided",
        merged: 0,
        results: [],
        errors: [],
      };
    }

    return {
      message: `Merged ${results.length} duplicate park(s)`,
      merged: results.length,
      results,
      errors,
    };
  }
}
