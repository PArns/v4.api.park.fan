import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { CacheKeys } from "../common/cache/cache-keys";
import { TRIP_ACCURACY_TTL } from "../common/cache/cache-ttl";
import {
  addIsoDays,
  getCurrentDateInTimezone,
} from "../common/utils/date.util";
import { safeJsonParse } from "../common/utils/json.util";
import { SingleFlight } from "../common/utils/single-flight.util";
import { PredictionLeadSnapshotService } from "../ml/services/prediction-lead-snapshot.service";
import { TripRegionService } from "./trip-region.service";
import {
  ACCURACY_LEAD_DAYS,
  ACCURACY_LOOKBACK_DAYS,
  regionForecastAccuracy,
} from "./trip-accuracy";
import type { TripAccuracyDto } from "./dto/trip-region-response.dto";

/**
 * `GET /v1/trip-regions/:region/accuracy`: the region's measured forecast
 * error ({@link regionForecastAccuracy}), read once a day per region from the
 * lead-time archive and kept in Redis under the region's date. Contract:
 * docs/frontend/trip-regions.md#how-accurate-the-forecast-was-get-v1trip-regionsregionaccuracy.
 */
@Injectable()
export class TripAccuracyService {
  private readonly logger = new Logger(TripAccuracyService.name);
  private readonly flight = new SingleFlight();

  constructor(
    private readonly regions: TripRegionService,
    private readonly snapshots: PredictionLeadSnapshotService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  async accuracy(slug: string): Promise<TripAccuracyDto> {
    const region = this.regions.region(slug);
    const key = CacheKeys.tripRegionAccuracy(
      region.slug,
      this.regions.today(region),
    );
    const cached = safeJsonParse<TripAccuracyDto>(
      await this.redis.get(key).catch(() => null),
    );
    if (cached) return cached;
    return this.flight.run(key, async () => {
      const parks = (await this.regions.resolveParks(region)).map(
        ({ park }) => ({
          id: park.id,
          // The park's own "yesterday": its last day that can be measured.
          yesterday: addIsoDays(getCurrentDateInTimezone(park.timezone), -1),
        }),
      );
      if (parks.length === 0)
        return { region: region.slug, forecastAccuracy: null };
      const days = parks.map((p) => p.yesterday).sort();
      let rows;
      try {
        rows = await this.snapshots.parkDayErrors(
          parks.map((p) => p.id),
          ACCURACY_LEAD_DAYS,
          addIsoDays(days[0], -(ACCURACY_LOOKBACK_DAYS - 1)),
          days[days.length - 1],
        );
      } catch (err) {
        // Not cached: an unreadable archive is no finding about the forecast.
        this.logger.warn(
          `Forecast accuracy for ${region.slug} unavailable: ${(err as Error).message}`,
        );
        return { region: region.slug, forecastAccuracy: null };
      }
      const body: TripAccuracyDto = {
        region: region.slug,
        forecastAccuracy: regionForecastAccuracy({
          parks,
          rows,
          minScored: PredictionLeadSnapshotService.MIN_SCORED,
        }),
      };
      await this.redis
        .set(key, JSON.stringify(body), "EX", TRIP_ACCURACY_TTL)
        .catch(() => undefined);
      return body;
    });
  }
}
