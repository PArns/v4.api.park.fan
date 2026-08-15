import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { IsNull, Not, Repository } from "typeorm";
import { Redis } from "ioredis";
import { Attraction } from "../entities/attraction.entity";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { RevalidationService } from "../../common/revalidation/revalidation.service";
import { invalidateParkCaches } from "../../common/cache/park-cache-invalidation";

export interface RetirementRequest {
  attractionId: string;
  /** The day it stopped existing, where a source states one. */
  retiredAt: string;
  /** Why, and the URL it was established from. */
  reason: string;
}

export interface RetirementResult {
  attractionId: string;
  name: string;
  parkId: string;
  retiredAt: string;
}

/**
 * Retires attractions that no longer exist, and cleans up after itself.
 *
 * Retirement is a plain column write, so on its own it would leave the park's
 * cached payload and the frontend's advertised slug in place for up to 24h plus
 * the CDN's stale-while-revalidate window — the same trap the attraction merge
 * had to solve. Doing the write through here keeps eviction and revalidation
 * attached to it, which matters because ~73 retirements are queued and doing
 * that by hand 73 times is how one gets forgotten.
 */
@Injectable()
export class AttractionRetirementService {
  private readonly logger = new Logger(AttractionRetirementService.name);

  constructor(
    @InjectRepository(Attraction)
    private readonly attractionRepository: Repository<Attraction>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly revalidationService: RevalidationService,
  ) {}

  async retire(requests: RetirementRequest[]): Promise<RetirementResult[]> {
    const results: RetirementResult[] = [];
    const touchedParks = new Set<string>();

    for (const request of requests) {
      const attraction = await this.attractionRepository.findOne({
        where: { id: request.attractionId },
      });
      if (!attraction) {
        this.logger.warn(`Attraction ${request.attractionId} not found`);
        continue;
      }

      await this.attractionRepository.update(attraction.id, {
        retiredAt: new Date(request.retiredAt),
        retiredReason: request.reason,
      });

      touchedParks.add(attraction.parkId);
      results.push({
        attractionId: attraction.id,
        name: attraction.name,
        parkId: attraction.parkId,
        retiredAt: request.retiredAt,
      });
      this.logger.log(
        `🪦 Retired "${attraction.name}" as of ${request.retiredAt} — ${request.reason}`,
      );
    }

    for (const parkId of touchedParks) {
      await invalidateParkCaches(this.redis, parkId).catch((e) =>
        this.logger.warn(
          `Cache eviction failed for park ${parkId}: ${(e as Error)?.message ?? e}`,
        ),
      );
    }

    if (results.length > 0) {
      // The park page deduplicates at read time; the sitemap does not. Without
      // this the removed slug keeps being advertised.
      await this.revalidationService
        .revalidateTags(["geo", "parks", "attractions"])
        .catch((e) =>
          this.logger.warn(
            `Revalidation failed: ${(e as Error)?.message ?? e}`,
          ),
        );
    }

    return results;
  }

  /** Undo — a retirement is a claim about the world, and claims can be wrong. */
  async unretire(attractionId: string): Promise<boolean> {
    const attraction = await this.attractionRepository.findOne({
      where: { id: attractionId },
    });
    if (!attraction || attraction.retiredAt === null) return false;

    await this.attractionRepository.update(attractionId, {
      retiredAt: null,
      retiredReason: null,
    });
    await invalidateParkCaches(this.redis, attraction.parkId).catch(() => {});
    await this.revalidationService
      .revalidateTags(["geo", "parks", "attractions"])
      .catch(() => {});

    this.logger.log(`↩️  Un-retired "${attraction.name}"`);
    return true;
  }

  /** Everything currently retired, newest first — the audit view. */
  async listRetired(): Promise<Attraction[]> {
    return this.attractionRepository.find({
      where: { retiredAt: Not(IsNull()) },
      relations: ["park"],
      order: { retiredAt: "DESC" },
    });
  }
}
