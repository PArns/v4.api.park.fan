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

  // Time-decay (applied once per day by the decay-popularity cron).
  // Every score is multiplied by DECAY_FACTOR, so a hit's weight halves every
  // ~6.6 days and an entry not hit for ~1-2 weeks fades out of the ranking.
  // This keeps the ranking trend-sensitive without ever re-querying the full
  // request history.
  private readonly DECAY_FACTOR = 0.9;
  // After decaying, drop entries that fell below half a hit — they are stale
  // one-offs and only bloat the sorted set.
  private readonly PRUNE_BELOW = 0.5;

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
   * Record hits for multiple parks in a single round-trip.
   * Used by endpoints that serve several parks at once (favorites, nearby).
   */
  async recordParkHits(parkIds: string[]): Promise<void> {
    if (parkIds.length === 0) return;
    try {
      const pipeline = this.redis.pipeline();
      for (const id of parkIds) {
        pipeline.zincrby(this.PARK_POPULARITY_KEY, 1, id);
      }
      await pipeline.exec();
    } catch (err) {
      this.logger.debug(`Failed to record park hits: ${err}`);
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
   * Get top N popular parks with their request counts (descending).
   */
  async getTopParksWithScores(
    limit: number = 50,
  ): Promise<Array<{ id: string; requests: number }>> {
    try {
      const flat = await this.redis.zrevrange(
        this.PARK_POPULARITY_KEY,
        0,
        limit - 1,
        "WITHSCORES",
      );
      const result: Array<{ id: string; requests: number }> = [];
      for (let i = 0; i < flat.length; i += 2) {
        result.push({ id: flat[i], requests: Number(flat[i + 1]) });
      }
      return result;
    } catch (err) {
      this.logger.warn(`Failed to fetch top parks with scores: ${err}`);
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
   * Apply exponential time-decay to both rankings.
   *
   * Scales every score by DECAY_FACTOR in a single O(N) ZUNIONSTORE (writing
   * back onto the same key) and prunes entries that decayed below PRUNE_BELOW.
   * Run once per day by the decay-popularity cron.
   */
  async applyDecay(): Promise<void> {
    try {
      await this.decaySet(this.PARK_POPULARITY_KEY);
      await this.decaySet(this.ATTRACTION_POPULARITY_KEY);
      this.logger.log(
        `Popularity decay applied (factor ${this.DECAY_FACTOR}).`,
      );
    } catch (err) {
      this.logger.warn(`Popularity decay failed: ${err}`);
    }
  }

  private async decaySet(key: string): Promise<void> {
    const pipeline = this.redis.pipeline();
    pipeline.zunionstore(key, 1, key, "WEIGHTS", this.DECAY_FACTOR);
    pipeline.zremrangebyscore(key, "-inf", `(${this.PRUNE_BELOW}`);
    await pipeline.exec();
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
