import { Injectable, Logger, Inject } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";

/**
 * Popularity Service
 *
 * Tracks request counts for parks and attractions using Redis Sorted Sets.
 * This data is used to prioritize cache pre-warming for the most visited entities.
 */
@Injectable()
export class PopularityService {
  private readonly logger = new Logger(PopularityService.name);

  // Redis Keys
  private readonly PARK_POPULARITY_KEY = "popularity:parks";
  private readonly ATTRACTION_POPULARITY_KEY = "popularity:attractions";

  // Retention: 7 days (sliding window handled by periodic pruning or just letting it grow)
  // For simplicity, we use a single set and could reset it weekly if needed.

  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  /**
   * Record a hit for a park
   */
  async recordParkHit(parkId: string): Promise<void> {
    try {
      await this.redis.zincrby(this.PARK_POPULARITY_KEY, 1, parkId);
    } catch (err) {
      this.logger.debug(`Failed to record park hit for ${parkId}: ${err}`);
    }
  }

  /**
   * Record a hit for an attraction
   */
  async recordAttractionHit(attractionId: string): Promise<void> {
    try {
      await this.redis.zincrby(this.ATTRACTION_POPULARITY_KEY, 1, attractionId);
    } catch (err) {
      this.logger.debug(
        `Failed to record attraction hit for ${attractionId}: ${err}`,
      );
    }
  }

  /**
   * Get top N popular parks
   */
  async getTopParks(limit: number = 50): Promise<string[]> {
    try {
      return await this.redis.zrevrange(this.PARK_POPULARITY_KEY, 0, limit - 1);
    } catch (err) {
      this.logger.warn(`Failed to fetch top parks: ${err}`);
      return [];
    }
  }

  /**
   * Get top N popular attractions
   */
  async getTopAttractions(limit: number = 200): Promise<string[]> {
    try {
      return await this.redis.zrevrange(
        this.ATTRACTION_POPULARITY_KEY,
        0,
        limit - 1,
      );
    } catch (err) {
      this.logger.warn(`Failed to fetch top attractions: ${err}`);
      return [];
    }
  }

  /**
   * Reset popularity scores (e.g. weekly)
   */
  async resetScores(): Promise<void> {
    await this.redis.del(
      this.PARK_POPULARITY_KEY,
      this.ATTRACTION_POPULARITY_KEY,
    );
    this.logger.log("Popularity scores reset.");
  }
}
