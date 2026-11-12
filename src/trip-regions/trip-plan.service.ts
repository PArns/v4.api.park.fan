import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { CacheKeys } from "../common/cache/cache-keys";
import { TRIP_REGION_RESPONSE_TTL } from "../common/cache/cache-ttl";
import { safeJsonParse } from "../common/utils/json.util";
import { SingleFlight } from "../common/utils/single-flight.util";
import { PredictionLeadSnapshotService } from "../ml/services/prediction-lead-snapshot.service";
import { TripRegionService } from "./trip-region.service";
import {
  normalizePlanQuery,
  optimizePlan,
  planCacheQuery,
  plannedLeads,
  planResponse,
  type NormalizedPlanQuery,
} from "./trip-plan.assembly";
import type { TripPlanQueryDto } from "./dto/trip-region-query.dto";
import type { TripPlanDto } from "./dto/trip-region-response.dto";

export type { NormalizedPlanQuery } from "./trip-plan.assembly";

/**
 * `GET /v1/trip-regions/:region/plan`: reads the region calendar for the trip
 * once, runs `trip-plan.assembly.ts` on it and adds the measured forecast error
 * at the planned visits' leads. Cached an hour by the normalized query.
 * Contract: docs/frontend/trip-regions.md.
 */
@Injectable()
export class TripPlanService {
  private readonly logger = new Logger(TripPlanService.name);
  private readonly flight = new SingleFlight();

  constructor(
    private readonly regions: TripRegionService,
    private readonly leadSnapshots: PredictionLeadSnapshotService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /** Validates the query against the region; 400 with the reason otherwise. */
  async normalize(
    slug: string,
    query: TripPlanQueryDto,
  ): Promise<NormalizedPlanQuery> {
    const region = this.regions.region(slug);
    const parks = await this.regions.resolveParks(region);
    return normalizePlanQuery(
      region,
      parks.map((p) => p.config.slug),
      query,
      this.regions.today(region),
    );
  }

  async plan(slug: string, query: TripPlanQueryDto): Promise<TripPlanDto> {
    const q = await this.normalize(slug, query);
    const key = CacheKeys.tripRegionPlan(slug, planCacheQuery(q));
    const cached = safeJsonParse<TripPlanDto>(await this.redis.get(key));
    if (cached) return cached;
    return this.flight.run(key, async () => {
      const built = await this.build(slug, q);
      await this.redis
        .set(key, JSON.stringify(built), "EX", TRIP_REGION_RESPONSE_TTL)
        .catch(() => undefined);
      return built;
    });
  }

  private async build(
    slug: string,
    q: NormalizedPlanQuery,
  ): Promise<TripPlanDto> {
    const region = this.regions.region(slug);
    const today = this.regions.today(region);
    const calendar = await this.regions.internalCalendar(region, q.from, q.to);
    const trip = optimizePlan(region, q, calendar);
    const leadMae = new Map<number, number | null>();
    await Promise.all(
      plannedLeads(trip, today).map(async (lead) => {
        const mae = await this.leadSnapshots
          .getLeadTimeMae(lead)
          .catch((err: Error) => {
            this.logger.warn(`Lead-time error unavailable: ${err.message}`);
            return null;
          });
        leadMae.set(lead, mae);
      }),
    );
    return planResponse(region, q, calendar, trip, {
      computedAt: new Date().toISOString(),
      today,
      leadMae,
    });
  }
}
