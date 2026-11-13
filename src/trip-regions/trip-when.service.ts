import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { CacheKeys } from "../common/cache/cache-keys";
import { TRIP_WHEN_TTL } from "../common/cache/cache-ttl";
import { safeJsonParse } from "../common/utils/json.util";
import { SingleFlight } from "../common/utils/single-flight.util";
import { HolidaysService } from "../holidays/holidays.service";
import { WeatherService } from "../parks/weather.service";
import { TripRegionService } from "./trip-region.service";
import { findTripDates, type TripWhen } from "./trip-when";
import {
  CLIMATE_REFERENCE_PARK,
  normalizeWhenQuery,
  whenCacheQuery,
  whenRange,
} from "./trip-when.assembly";
import {
  countryOfCode,
  defaultSchoolBreakCountries,
  resolveWithin,
  schoolBreakRegions,
  schoolRowsSpan,
  type SchoolRowLike,
  type TripSchoolBreaks,
} from "./trip-school-breaks";
import type { TripWhenQueryDto } from "./dto/trip-when.dto";

/**
 * `GET /v1/trip-regions/:region/when` and `/school-breaks`: reads the region
 * calendar for the whole range once (the month caches and last-year
 * snapshots, no ML), the holiday rows and the reference park's climate
 * normals, and hands them to `trip-when.assembly.ts`, `trip-when.ts` and
 * `trip-school-breaks.ts`. `/when` is cached six hours by its normalized
 * query. Contract: docs/frontend/trip-regions-when.md.
 */
@Injectable()
export class TripWhenService {
  private readonly logger = new Logger(TripWhenService.name);
  private readonly flight = new SingleFlight();

  constructor(
    private readonly regions: TripRegionService,
    private readonly holidays: HolidaysService,
    private readonly weather: WeatherService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  async when(slug: string, query: TripWhenQueryDto): Promise<TripWhen> {
    const region = await this.regions.regionWithTickets(slug);
    const today = this.regions.today(region);
    const { from, to } = whenRange(query.from, query.to, today);
    const within = query.within
      ? resolveWithin(
          query.within,
          await this.breaks([countryOfCode(query.within)], from, to),
        )
      : undefined;
    const resolved = await this.regions.resolveParks(region);
    const q = normalizeWhenQuery(
      region,
      resolved.map((p) => p.config.slug),
      query,
      today,
      within,
    );

    const key = CacheKeys.tripRegionWhen(region.slug, whenCacheQuery(q));
    const cached = safeJsonParse<TripWhen>(await this.redis.get(key));
    if (cached) return cached;
    return this.flight.run(key, async () => {
      const calendar = await this.regions.internalCalendar(region, from, to);
      const reference = resolved.find(
        (p) => p.config.slug === CLIMATE_REFERENCE_PARK,
      );
      const climate = reference
        ? await this.weather
            .getClimateNormals(reference.park.id)
            .catch((err: Error) => {
              this.logger.warn(`Climate normals unavailable: ${err.message}`);
              return null;
            })
        : null;
      const result = findTripDates({
        region,
        calendar,
        query: q,
        climate,
        climatePark: CLIMATE_REFERENCE_PARK,
        clock: () => performance.now(),
      });
      const built: TripWhen = {
        ...result,
        meta: { ...result.meta, computedAt: new Date().toISOString() },
      };
      await this.redis
        .set(key, JSON.stringify(built), "EX", TRIP_WHEN_TTL)
        .catch(() => undefined);
      return built;
    });
  }

  /** `GET /v1/trip-regions/:region/school-breaks` */
  async schoolBreaks(
    slug: string,
    fromQuery?: string,
    toQuery?: string,
    countriesQuery?: string,
  ): Promise<TripSchoolBreaks> {
    const region = this.regions.region(slug);
    const today = this.regions.today(region);
    const { from, to } = whenRange(fromQuery, toQuery, today);
    const countries = countriesQuery
      ? [...new Set(countriesQuery.toUpperCase().split(","))]
      : defaultSchoolBreakCountries(
          await this.holidays.getSchoolHolidayCoverage(),
          today,
        );
    return this.breaks(countries, from, to);
  }

  private async breaks(
    countries: string[],
    from: string,
    to: string,
  ): Promise<TripSchoolBreaks> {
    const span = schoolRowsSpan(from, to);
    const rows = new Map<string, SchoolRowLike[]>();
    for (const country of countries) {
      rows.set(
        country,
        await this.holidays.getHolidays(country, span.from, span.to),
      );
    }
    return schoolBreakRegions(
      countries,
      rows,
      await this.holidays.getSchoolHolidayCoverage(),
      from,
      to,
    );
  }
}
