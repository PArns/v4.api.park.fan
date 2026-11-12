import { BadRequestException, Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { CacheKeys } from "../common/cache/cache-keys";
import { TRIP_WHEN_TTL } from "../common/cache/cache-ttl";
import { safeJsonParse } from "../common/utils/json.util";
import { SingleFlight } from "../common/utils/single-flight.util";
import { addIsoDays, isRealIsoDate } from "../common/utils/date.util";
import type { SchoolCoverage } from "../common/utils/holiday.utils";
import { normalizeRegionCode } from "../common/utils/region.util";
import { HolidaysService } from "../holidays/holidays.service";
import { WeatherService } from "../parks/weather.service";
import type { RegionConfig } from "./regions";
import { TripRegionService } from "./trip-region.service";
import {
  normalizePlanQuery,
  planCacheQuery,
} from "./trip-plan.assembly";
import {
  findTripDates,
  WHEN_DEFAULT_DAYS,
  WHEN_DEFAULT_GAP,
  WHEN_DEFAULT_LIMIT,
  WHEN_MAX_RANGE_DAYS,
  type TripWhen,
  type WhenWithin,
} from "./trip-when";
import {
  DEFAULT_SCHOOL_BREAK_COUNTRIES,
  MIN_COVERAGE_DAYS,
  SCHOOL_BREAK_REGIONS,
  schoolBreakPeriods,
  storedCodes,
  type SchoolBreakRegion,
  type TripSchoolBreaks,
} from "./trip-school-breaks";
import type { TripPlanQueryDto } from "./dto/trip-region-query.dto";
import type { TripWhenQueryDto } from "./dto/trip-when.dto";

/** The park whose climate normals stand for the region's months. */
export const CLIMATE_REFERENCE_PARK = "magic-kingdom-park";

const daysBetween = (from: string, to: string): number =>
  Math.round(
    (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) /
      86_400_000,
  );

/**
 * The last day a region's own or its country's nationwide school rows reach;
 * null when neither has a row. Unlike the calendar's coverage it never falls
 * back to another region of the country: a code with no rows of its own has
 * no known breaks, not the neighbours'.
 */
function coveredThrough(
  coverage: SchoolCoverage,
  country: string,
  code: string,
): string | null {
  const region = normalizeRegionCode(code);
  const dates = [
    region ? coverage.regional.get(`${country}|${region}`) : undefined,
    coverage.national.get(country),
  ].filter((d): d is string => !!d);
  return dates.length > 0 ? dates.sort()[dates.length - 1] : null;
}

/**
 * `GET /v1/trip-regions/:region/when` and `/school-breaks`: reads the region
 * calendar for the whole range once (month caches and last-year snapshots,
 * no ML), resolves `within` through the holiday table, and hands both to
 * `trip-when.ts`. Cached six hours by the normalized query. Contract:
 * docs/frontend/trip-regions-when.md.
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

  /** The search range: real dates, from today to a year ahead, at most 366 days. */
  private range(
    region: RegionConfig,
    fromQuery: string | undefined,
    toQuery: string | undefined,
  ): { from: string; to: string; today: string } {
    const today = this.regions.today(region);
    const last = addIsoDays(today, WHEN_MAX_RANGE_DAYS - 1);
    const from = fromQuery ?? today;
    const fallback = addIsoDays(from, WHEN_MAX_RANGE_DAYS - 1);
    const to = toQuery ?? (fallback < last ? fallback : last);
    for (const [name, value] of [
      ["from", from],
      ["to", to],
    ] as const) {
      if (!isRealIsoDate(value)) {
        throw new BadRequestException(`${name} is not a date the calendar has`);
      }
    }
    if (from < today) {
      throw new BadRequestException(`from must not be before today (${today})`);
    }
    if (to < from) throw new BadRequestException("to must not be before from");
    if (to > last) throw new BadRequestException(`to must not be after ${last}`);
    if (daysBetween(from, to) + 1 > WHEN_MAX_RANGE_DAYS) {
      throw new BadRequestException(
        `The search range is at most ${WHEN_MAX_RANGE_DAYS} days`,
      );
    }
    return { from, to, today };
  }

  async when(slug: string, query: TripWhenQueryDto): Promise<TripWhen> {
    const region = this.regions.region(slug);
    const { from, to, today } = this.range(region, query.from, query.to);
    const days = query.days ?? WHEN_DEFAULT_DAYS;
    if (daysBetween(from, to) + 1 < days) {
      throw new BadRequestException(
        `The search range is shorter than a ${days}-day trip`,
      );
    }
    const resolved = await this.regions.resolveParks(region);
    // The plan's own validation and defaults, on the range's first window.
    const plan = normalizePlanQuery(
      region,
      resolved.map((p) => p.config.slug),
      this.planQuery(query, from, addIsoDays(from, days - 1)),
      today,
    );
    const within = query.within
      ? await this.within(query.within, from, to)
      : undefined;
    const limit = query.limit ?? WHEN_DEFAULT_LIMIT;
    const gap = query.gap ?? WHEN_DEFAULT_GAP;

    const key = CacheKeys.tripRegionWhen(
      region.slug,
      [
        from,
        to,
        days,
        limit,
        gap,
        within?.code ?? "-",
        planCacheQuery(plan),
      ].join("|"),
    );
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
        query: { from, to, days, limit, gap, plan, within },
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

  /**
   * The plan's query for one window. `parties` is the old name of `fireworks`:
   * `avoid` kept the party nights out, which is `want`.
   */
  private planQuery(
    query: TripWhenQueryDto,
    from: string,
    to: string,
  ): TripPlanQueryDto {
    const fireworks =
      query.fireworks ??
      (query.parties === undefined
        ? undefined
        : query.parties === "avoid"
          ? "want"
          : "any");
    return {
      from,
      to,
      parks: query.parks,
      hotel: query.hotel,
      area: query.area,
      arrival: query.arrival,
      departure: query.departure,
      blocks: query.blocks,
      fireworks,
      split: query.split,
      rest: query.rest,
      hop: query.hop,
    };
  }

  /** A `within` code resolved through `/school-breaks`; 400 for a code it does not list. */
  private async within(
    code: string,
    from: string,
    to: string,
  ): Promise<WhenWithin> {
    const country = code.slice(0, 2).toUpperCase();
    const { regions } = await this.breaks([country], from, to);
    const found = regions.find(
      (r) => r.code.toUpperCase() === code.toUpperCase(),
    );
    if (!found) {
      const known = regions.map((r) => r.code).join(", ");
      throw new BadRequestException(
        known
          ? `within: ${code} is not a school-holiday region; ${country} has ${known}`
          : `within: no school-holiday regions for ${country}`,
      );
    }
    return found;
  }

  /** `GET /v1/trip-regions/:region/school-breaks` */
  async schoolBreaks(
    slug: string,
    fromQuery?: string,
    toQuery?: string,
    countriesQuery?: string,
  ): Promise<TripSchoolBreaks> {
    const region = this.regions.region(slug);
    const { from, to, today } = this.range(region, fromQuery, toQuery);
    if (countriesQuery) {
      const countries = [
        ...new Set(countriesQuery.toUpperCase().split(",")),
      ];
      return this.breaks(countries, from, to);
    }
    const coverage = await this.holidays.getSchoolHolidayCoverage();
    const horizon = addIsoDays(today, MIN_COVERAGE_DAYS);
    const covered = DEFAULT_SCHOOL_BREAK_COUNTRIES.filter(
      (c) => (coverage.countryAny.get(c) ?? "") >= horizon,
    );
    return this.breaks(covered, from, to);
  }

  /** Every region of the countries, its breaks in [from, to] and how far its data reaches. */
  private async breaks(
    countries: string[],
    from: string,
    to: string,
  ): Promise<TripSchoolBreaks> {
    const coverage = await this.holidays.getSchoolHolidayCoverage();
    const out: SchoolBreakRegion[] = [];
    for (const country of countries) {
      // A day each side, so a break that ends the day before `from` still joins its weekend.
      const rows = (
        await this.holidays.getHolidays(
          country,
          addIsoDays(from, -3),
          addIsoDays(to, 3),
        )
      ).filter((r) => r.holidayType === "school");
      const listed =
        SCHOOL_BREAK_REGIONS[country] ??
        storedCodes(rows).map((code) => ({ code, name: code }));
      for (const { code, name } of listed) {
        out.push({
          code,
          name,
          country,
          through: coveredThrough(coverage, country, code),
          periods: schoolBreakPeriods(rows, code, from, to),
        });
      }
    }
    return { regions: out };
  }
}
