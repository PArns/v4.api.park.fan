import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Park } from "../../parks/entities/park.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { AnalyticsService } from "../../analytics/analytics.service";
import { Inject } from "@nestjs/common";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import Redis from "ioredis";

/**
 * Occupancy Calculation Processor
 *
 * Handles pre-computation of P90 sliding window values for parks and attractions.
 *
 * Benefits:
 * - Faster crowd level calculations (no on-the-fly computation)
 * - Reduced Redis cache misses
 * - Consistent baseline values across all endpoints
 *
 * Schedule:
 * - On bootstrap (immediate)
 * - Daily at 3am (after percentile aggregation)
 */
@Processor("occupancy-calculation")
export class OccupancyCalculationProcessor {
  private readonly logger = new Logger(OccupancyCalculationProcessor.name);

  constructor(
    @InjectRepository(Park)
    private parkRepository: Repository<Park>,
    @InjectRepository(Attraction)
    private attractionRepository: Repository<Attraction>,
    private analyticsService: AnalyticsService,
    @Inject(REDIS_CLIENT) private redis: Redis,
  ) {}

  /**
   * Pre-compute P90 sliding window values for all parks and attractions
   *
   * This job:
   * 1. Fetches all active parks
   * 2. For each park, calculates P90 with confidence
   * 3. Fetches all attractions in each park
   * 4. For each attraction, calculates P90 with confidence
   * 5. Stores results in Redis cache
   */
  @Process("precompute-p90-sliding-window")
  async handlePrecomputeP90(_job: Job): Promise<void> {
    this.logger.log("📊 Pre-computing P90 sliding window values...");

    try {
      const startTime = Date.now();
      let parkCount = 0;
      let attractionCount = 0;

      // Get all parks
      const parks = await this.parkRepository.find({
        select: ["id", "name", "timezone"],
      });

      this.logger.log(`   Found ${parks.length} parks to process`);

      for (const park of parks) {
        try {
          // Pre-compute park P90
          const parkP90 =
            await this.analyticsService.get90thPercentileWithConfidence(
              park.id,
              "park",
              park.timezone,
            );

          this.logger.debug(
            `   ✓ Park [${park.name}]: P90=${parkP90.p90}min, P50=${parkP90.p50}min, confidence=${parkP90.confidence}, samples=${parkP90.sampleCount}`,
          );
          parkCount++;

          // Get all attractions in this park
          const attractions = await this.attractionRepository.find({
            where: { parkId: park.id },
            select: ["id", "name"],
          });

          // Pre-compute attraction P90s
          for (const attraction of attractions) {
            try {
              const attractionP90 =
                await this.analyticsService.get90thPercentileWithConfidence(
                  attraction.id,
                  "attraction",
                  park.timezone,
                );

              if (attractionP90.p90 > 0 || attractionP90.p50 > 0) {
                this.logger.debug(
                  `     ✓ Attraction [${attraction.name}]: P90=${attractionP90.p90}min, P50=${attractionP90.p50}min, confidence=${attractionP90.confidence}`,
                );
              }
              attractionCount++;
            } catch (error) {
              this.logger.warn(
                `     ✗ Failed to compute P90 for attraction [${attraction.name}]: ${error}`,
              );
            }
          }
        } catch (error) {
          this.logger.warn(
            `   ✗ Failed to process park [${park.name}]: ${error}`,
          );
        }
      }

      const duration = ((Date.now() - startTime) / 1000).toFixed(2);
      this.logger.log(
        `✅ P90 pre-computation complete: ${parkCount} parks, ${attractionCount} attractions in ${duration}s`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to pre-compute P90 values: ${errorMessage}`,
        error instanceof Error ? error.stack : undefined,
      );
      throw error;
    }
  }
}
