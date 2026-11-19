import {
  Inject,
  Injectable,
  Logger,
  NotFoundException,
  Optional,
} from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { CacheKeys } from "../common/cache/cache-keys";
import { TRIP_REGION_RESPONSE_TTL } from "../common/cache/cache-ttl";
import { safeJsonParse } from "../common/utils/json.util";
import { SingleFlight } from "../common/utils/single-flight.util";
import { DbJobBudget, mapWithDbBudget } from "../common/utils/db-job-budget";
import {
  addIsoDays,
  eachIsoDate,
  getCurrentDateInTimezone,
  isoDaysBetween,
} from "../common/utils/date.util";
import { parseDateRange } from "../common/utils/date-parsing.util";
import { frontendParkPath } from "../common/utils/frontend-url.util";
import { ParksService } from "../parks/parks.service";
import { CalendarService } from "../parks/services/calendar.service";
import { ParkSeasonService } from "../parks/services/park-season.service";
import { HolidaysService } from "../holidays/holidays.service";
import { resolveCuratedPark } from "../parks/utils/curated-park-facts.util";
import { toPublicSeason } from "../parks/park-seasons.controller";
import type { Park } from "../parks/entities/park.entity";
import {
  findRegion,
  TRIP_REGIONS,
  type RegionConfig,
  type RegionParkConfig,
} from "./regions";
import { MAX_TRIP_DAYS } from "./trip-optimizer";
import { lastYearMeasured, type ScheduleRowLike } from "./trip-calendar.util";
import {
  monthOf,
  readLastYearMonths,
  writeLastYearMonth,
  type LastYearMonth,
} from "./last-year-snapshot";
import {
  lastYearReferenceDays,
  lastYearReferenceMonths,
  lastYearSeasonsFrom,
  loadLastYearDayFacts,
  type LastYearDayFacts,
} from "./last-year-reference";
import {
  assembleRegionCalendar,
  calendarMonths,
  describeRegion,
  joinCalendarMonths,
  MAX_CALENDAR_DAYS,
  publicCalendar,
  referenceRange,
  summarizeRegion,
  validateWindow,
  type CalendarMonthPart,
  type InternalRegionCalendar,
  type ParkCalendarSource,
  type RegionParkRecord,
} from "./trip-region.assembly";
import { regionBestDays, type TripBestDays } from "./trip-best-days";
import type {
  TripCalendarDto,
  TripRegionDto,
  TripRegionListDto,
} from "./dto/trip-region-response.dto";
import type { TripTicketCatalogDto } from "./dto/trip-tickets.dto";
import { TicketCatalogService } from "./ticket-catalog.service";
import { noteTripCache, noteTripShape } from "./metrics/trip-request-trace";
import {
  seedCatalog,
  ticketCatalogResponse,
  withCatalog,
} from "./ticket-catalog";

export {
  MAX_CALENDAR_DAYS,
  MAX_DAYS_AHEAD,
  type InternalCalendarDay,
  type InternalRegionCalendar,
} from "./trip-region.assembly";

/** How long a resolved list of a region's parks is reused. */
const RESOLVE_TTL_MS = 10 * 60 * 1000;

/** Parks of one region built at once: each is a calendar read, cold a build. */
const REGION_PARK_BUDGET = new DbJobBudget(4);

export interface ResolvedRegionPark {
  config: RegionParkConfig;
  park: Park;
}

/** A database park as the assembly reads it. */
function toRecord({ config, park }: ResolvedRegionPark): RegionParkRecord {
  return {
    config,
    id: park.id,
    slug: park.slug,
    name: resolveCuratedPark(park).name,
    path: frontendParkPath(park) ?? "",
    latitude: Number(park.latitude),
    longitude: Number(park.longitude),
    timezone: park.timezone,
    destinationId: park.destinationId ?? null,
    destinationName: park.destination?.name ?? null,
  };
}

/**
 * The trip regions: the region itself, its calendar and its best days, read
 * from the database and Redis and assembled by `trip-region.assembly.ts`. The
 * plan is `TripPlanService`'s. Contract: docs/frontend/trip-regions.md.
 */
@Injectable()
export class TripRegionService {
  private readonly logger = new Logger(TripRegionService.name);
  private readonly resolved = new Map<
    string,
    { at: number; parks: ResolvedRegionPark[] }
  >();
  private readonly bestDaysFlight = new SingleFlight();
  /** The calendar months being built in this process, by key, for a second reader to wait for. */
  private readonly monthFlights = new Map<
    string,
    Promise<InternalRegionCalendar>
  >();

  constructor(
    private readonly parksService: ParksService,
    private readonly calendarService: CalendarService,
    private readonly parkSeasonService: ParkSeasonService,
    private readonly holidaysService: HolidaysService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    // Absent only where a spec builds the service by hand: it then plans with the seed.
    @Optional() private readonly tickets?: TicketCatalogService,
  ) {}

  /** The region a slug names; 404 when there is none. */
  region(slug: string): RegionConfig {
    const region = findRegion(slug);
    if (!region) throw new NotFoundException(`No trip region "${slug}"`);
    return region;
  }

  /**
   * The region with its ticket catalog on it, as the plan, `/when`, the
   * region and tickets routes and Parky read it; 404 for an unknown region.
   */
  async regionWithTickets(slug: string): Promise<RegionConfig> {
    const region = this.region(slug);
    return this.tickets
      ? this.tickets.withTickets(region)
      : withCatalog(region, seedCatalog(region));
  }

  /** `GET /v1/trip-regions/:region/tickets` */
  async ticketCatalog(slug: string): Promise<TripTicketCatalogDto> {
    return ticketCatalogResponse(
      await this.regionWithTickets(slug),
    ) as TripTicketCatalogDto;
  }

  /** The region's today, in its own timezone. */
  today(region: RegionConfig): string {
    return getCurrentDateInTimezone(region.timezone);
  }

  /** Checks a window against the region's today; 400 otherwise. */
  validateWindow(
    region: RegionConfig,
    from: string,
    to: string,
    maxDays: number,
  ): void {
    validateWindow(from, to, maxDays, this.today(region));
  }

  /**
   * The region's parks as the database has them, in the order `regions.ts`
   * lists them. A park that is missing or retired is left out.
   */
  async resolveParks(region: RegionConfig): Promise<ResolvedRegionPark[]> {
    const cached = this.resolved.get(region.slug);
    if (cached && Date.now() - cached.at < RESOLVE_TTL_MS) return cached.parks;
    const found = await Promise.all(
      region.parks.map(async (config) => ({
        config,
        park: await this.parksService.findByGeographicPath(
          config.continent,
          config.country,
          config.city,
          config.slug,
        ),
      })),
    );
    const parks: ResolvedRegionPark[] = [];
    for (const { config, park } of found) {
      if (!park) {
        this.logger.warn(
          `Trip region ${region.slug}: park ${config.city}/${config.slug} not found`,
        );
        continue;
      }
      if (park.retiredAt) continue;
      parks.push({ config, park });
    }
    this.resolved.set(region.slug, { at: Date.now(), parks });
    return parks;
  }

  /** `GET /v1/trip-regions`: every region, in the order `regions.ts` lists them. */
  async list(): Promise<TripRegionListDto> {
    const regions = await Promise.all(
      Object.values(TRIP_REGIONS).map(async (region) =>
        summarizeRegion(
          region,
          (await this.resolveParks(region)).map(toRecord),
          MAX_TRIP_DAYS,
        ),
      ),
    );
    return { regions };
  }

  /** `GET /v1/trip-regions/:region` */
  async describe(slug: string): Promise<TripRegionDto> {
    const region = await this.regionWithTickets(slug);
    const parks = await this.resolveParks(region);
    return describeRegion(region, parks.map(toRecord));
  }

  /** `GET /v1/trip-regions/:region/calendar` */
  async calendar(
    slug: string,
    fromQuery?: string,
    toQuery?: string,
  ): Promise<TripCalendarDto> {
    const region = this.region(slug);
    const from = fromQuery ?? this.today(region);
    const to = toQuery ?? addIsoDays(from, 30);
    this.validateWindow(region, from, to, MAX_CALENDAR_DAYS);
    return publicCalendar(await this.internalCalendar(region, from, to));
  }

  /**
   * `GET /v1/trip-regions/:region/best-days`: an hour in Redis by its window
   * and `limit`, built from the region calendar as the plan is.
   */
  async bestDays(
    slug: string,
    fromQuery: string | undefined,
    toQuery: string | undefined,
    limit: number,
  ): Promise<TripBestDays> {
    const region = this.region(slug);
    const today = this.today(region);
    const from = fromQuery ?? today;
    const to = toQuery ?? addIsoDays(from, 30);
    validateWindow(from, to, MAX_CALENDAR_DAYS, today);
    noteTripShape({ rangeDays: isoDaysBetween(from, to) + 1 });
    const key = CacheKeys.tripRegionBestDays(
      region.slug,
      today,
      from,
      to,
      limit,
    );
    const cached = safeJsonParse<TripBestDays>(await this.redis.get(key));
    if (cached) {
      noteTripCache("cache", "hit");
      return cached;
    }
    noteTripCache("cache", "joined");
    return this.bestDaysFlight.run(key, async () => {
      noteTripCache("cache", "miss");
      const calendar = await this.internalCalendar(region, from, to);
      const built = regionBestDays(region, calendar, limit);
      await this.redis
        .set(key, JSON.stringify(built), "EX", TRIP_REGION_RESPONSE_TTL)
        .catch(() => undefined);
      return built;
    });
  }

  /**
   * The region calendar with the plan's fields, joined from its months
   * (`calendarMonths`), each an hour in Redis. The months a window misses are
   * built together from one read; a month another request is building is
   * waited for.
   */
  async internalCalendar(
    region: RegionConfig,
    from: string,
    to: string,
  ): Promise<InternalRegionCalendar> {
    const today = this.today(region);
    if (from < today) {
      // Every route refuses a past day; a caller that asks anyway is not kept.
      noteTripCache("calendarCache", "miss");
      const [built] = await this.buildMonths(region, today, [
        { month: monthOf(from), from, to },
      ]);
      return built;
    }
    const parts = calendarMonths(from, to, today);
    const keys = parts.map((p) =>
      CacheKeys.tripRegionCalendarMonth(region.slug, today, p.month),
    );
    const stored = await this.redis
      .mget(...keys)
      .catch(() => keys.map(() => null));
    const found = stored.map((v) => safeJsonParse<InternalRegionCalendar>(v));
    const missing = parts.filter(
      (_, i) => !found[i] && !this.monthFlights.has(keys[i]),
    );
    if (missing.length > 0) {
      noteTripCache("calendarCache", "miss");
      this.buildAndKeep(region, today, missing);
    } else if (found.some((m) => !m)) {
      noteTripCache("calendarCache", "joined");
    } else {
      noteTripCache("calendarCache", "hit");
    }
    const months = await Promise.all(
      keys.map((key, i) => found[i] ?? this.monthFlights.get(key)),
    );
    return joinCalendarMonths(months as InternalRegionCalendar[], from, to);
  }

  /** Builds `parts` from one read and keeps each month an hour, waitable while it builds. */
  private buildAndKeep(
    region: RegionConfig,
    today: string,
    parts: CalendarMonthPart[],
  ): void {
    const build = this.buildMonths(region, today, parts);
    parts.forEach((part, i) => {
      const key = CacheKeys.tripRegionCalendarMonth(
        region.slug,
        today,
        part.month,
      );
      const month = build.then(async (built) => {
        await this.redis
          .set(key, JSON.stringify(built[i]), "EX", TRIP_REGION_RESPONSE_TTL)
          .catch(() => undefined);
        return built[i];
      });
      this.monthFlights.set(key, month);
      month.finally(() => this.monthFlights.delete(key)).catch(() => undefined);
    });
  }

  /**
   * The region calendar of each part, assembled from one read of the parks
   * over all of them and their reference span.
   */
  private async buildMonths(
    region: RegionConfig,
    today: string,
    parts: CalendarMonthPart[],
  ): Promise<InternalRegionCalendar[]> {
    const from = parts[0].from;
    const to = parts[parts.length - 1].to;
    const parks = await this.resolveParks(region);
    // Read with the reference span, so each day is priced against the
    // park's usual day around it, whatever window was asked for.
    const reference = referenceRange(from, to);
    const { fromDate, toDate } = parseDateRange(reference.from, reference.to, {
      timezone: region.timezone,
    });
    // The window's seasons, and the year before it, whose editions a day
    // inside a season reads last year from.
    const seasonRows = await this.parkSeasonService.forParksInRange(
      parks.map((p) => p.park.id),
      lastYearSeasonsFrom(from),
      to,
    );
    const facts = new Map<
      string,
      Promise<Map<string, LastYearDayFacts> | null>
    >();

    const sources: ParkCalendarSource[] = await mapWithDbBudget(
      parks,
      async (rp) => {
        const [calendar, rows, lastYear, dayFacts] = await Promise.all([
          this.calendarService.buildCalendarResponse(
            rp.park,
            fromDate,
            toDate,
            "none",
          ),
          this.parksService
            .getSchedule(rp.park.id, fromDate, toDate)
            .catch((err: Error) => {
              this.logger.warn(
                `Schedule unavailable for ${rp.park.slug}: ${err.message}`,
              );
              return [];
            }),
          this.lastYearFor(
            rp.park,
            lastYearReferenceMonths(from, to, today),
            today,
          ),
          this.lastYearFactsFor(rp.park, from, to, facts),
        ]);
        // Without holidays the park's days read 52 weeks back.
        const lastYearReferences = dayFacts
          ? lastYearReferenceDays(eachIsoDate(from, to), dayFacts, {
              before: today,
              seasons: seasonRows.filter((s) => s.parkId === rp.park.id),
              measuredOpen: (day) =>
                lastYearMeasured(lastYear.get(monthOf(day))?.[day]),
            })
          : new Map<string, string>();
        return {
          park: toRecord(rp),
          calendar,
          rows: rows as ScheduleRowLike[],
          lastYear,
          lastYearReferences,
        };
      },
      REGION_PARK_BUDGET,
    );

    const forecastUntil = await this.redis
      .mget(...parks.map((p) => CacheKeys.tripForecastUntil(p.park.id)))
      .catch(() => [] as Array<string | null>);

    const computedAt = new Date().toISOString();
    return parts.map((part) =>
      assembleRegionCalendar({
        region,
        from: part.from,
        to: part.to,
        today,
        computedAt,
        sources,
        seasons: seasonRows
          .filter((s) => s.startDate <= part.to && s.endDate >= part.from)
          .map((s) => ({ ...toPublicSeason(s), parkId: s.parkId })),
        forecastUntil: forecastUntil as Array<string | null>,
      }),
    );
  }

  /**
   * One park's holiday facts for its reference days a year back
   * (`loadLastYearDayFacts`). Null where the park has no country or its
   * holidays cannot be read. Parks at one location share a read through
   * `facts`.
   */
  private lastYearFactsFor(
    park: Park,
    from: string,
    to: string,
    facts: Map<string, Promise<Map<string, LastYearDayFacts> | null>>,
  ): Promise<Map<string, LastYearDayFacts> | null> {
    const key = [park.countryCode, park.regionCode, park.timezone].join("|");
    let pending = facts.get(key);
    if (!pending) {
      pending = loadLastYearDayFacts(
        this.holidaysService,
        park,
        from,
        to,
      ).catch((err: Error) => {
        this.logger.warn(
          `Holidays unavailable for ${park.slug}, last year reads 52 weeks back: ${err.message}`,
        );
        return null;
      });
      facts.set(key, pending);
    }
    return pending;
  }

  /**
   * The last-year snapshots a window needs, from Redis; a month the warmup
   * has not written yet is built here once and kept.
   */
  private async lastYearFor(
    park: Park,
    months: string[],
    today: string,
  ): Promise<Map<string, LastYearMonth>> {
    if (months.length === 0) return new Map();
    const found = await readLastYearMonths(this.redis, park.id, months);
    for (const ym of months) {
      if (found.has(ym)) continue;
      const built = await writeLastYearMonth(
        this.calendarService,
        this.redis,
        park,
        ym,
        today,
      ).catch((err: Error) => {
        this.logger.warn(
          `Last-year month ${ym} unavailable for ${park.slug}: ${err.message}`,
        );
        return null;
      });
      if (built) found.set(ym, built);
    }
    return found;
  }
}
