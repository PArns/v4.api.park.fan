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
  getCurrentDateInTimezone,
  isoDaysBetween,
} from "../common/utils/date.util";
import { parseDateRange } from "../common/utils/date-parsing.util";
import { frontendParkPath } from "../common/utils/frontend-url.util";
import { ParksService } from "../parks/parks.service";
import { CalendarService } from "../parks/services/calendar.service";
import { ParkSeasonService } from "../parks/services/park-season.service";
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
import type { ScheduleRowLike } from "./trip-calendar.util";
import {
  readLastYearMonths,
  writeLastYearMonth,
  type LastYearMonth,
} from "./last-year-snapshot";
import {
  assembleRegionCalendar,
  describeRegion,
  lastYearMonthsNeeded,
  MAX_CALENDAR_DAYS,
  publicCalendar,
  referenceRange,
  summarizeRegion,
  validateWindow,
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
  private readonly calendarFlight = new SingleFlight();

  constructor(
    private readonly parksService: ParksService,
    private readonly calendarService: CalendarService,
    private readonly parkSeasonService: ParkSeasonService,
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

  /** `GET /v1/trip-regions/:region/best-days` */
  async bestDays(
    slug: string,
    fromQuery: string | undefined,
    toQuery: string | undefined,
    limit: number,
  ): Promise<TripBestDays> {
    const region = this.region(slug);
    const from = fromQuery ?? this.today(region);
    const to = toQuery ?? addIsoDays(from, 30);
    this.validateWindow(region, from, to, MAX_CALENDAR_DAYS);
    noteTripShape({ rangeDays: isoDaysBetween(from, to) + 1 });
    const calendar = await this.internalCalendar(region, from, to);
    return regionBestDays(region, calendar, limit);
  }

  /** The region calendar with the plan's fields, an hour in Redis. */
  async internalCalendar(
    region: RegionConfig,
    from: string,
    to: string,
  ): Promise<InternalRegionCalendar> {
    const key = CacheKeys.tripRegionCalendar(region.slug, from, to);
    const cached = safeJsonParse<InternalRegionCalendar>(
      await this.redis.get(key),
    );
    if (cached) {
      noteTripCache("calendarCache", "hit");
      return cached;
    }
    noteTripCache("calendarCache", "joined");
    return this.calendarFlight.run(key, async () => {
      noteTripCache("calendarCache", "miss");
      const built = await this.buildCalendar(region, from, to);
      await this.redis
        .set(key, JSON.stringify(built), "EX", TRIP_REGION_RESPONSE_TTL)
        .catch(() => undefined);
      return built;
    });
  }

  private async buildCalendar(
    region: RegionConfig,
    from: string,
    to: string,
  ): Promise<InternalRegionCalendar> {
    const today = this.today(region);
    const parks = await this.resolveParks(region);
    // Read with the reference span, so each day is priced against the
    // park's usual day around it, whatever window was asked for.
    const reference = referenceRange(from, to);
    const { fromDate, toDate } = parseDateRange(reference.from, reference.to, {
      timezone: region.timezone,
    });

    const sources: ParkCalendarSource[] = await mapWithDbBudget(
      parks,
      async (rp) => {
        const [calendar, rows] = await Promise.all([
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
        ]);
        const lastYear = await this.lastYearFor(
          rp.park,
          lastYearMonthsNeeded(
            calendar.days.filter((d) => d.date >= from && d.date <= to),
            today,
          ),
          today,
        );
        return {
          park: toRecord(rp),
          calendar,
          rows: rows as ScheduleRowLike[],
          lastYear,
        };
      },
      REGION_PARK_BUDGET,
    );

    const [seasons, forecastUntil] = await Promise.all([
      this.parkSeasonService.forParksInRange(
        parks.map((p) => p.park.id),
        from,
        to,
      ),
      this.redis
        .mget(...parks.map((p) => CacheKeys.tripForecastUntil(p.park.id)))
        .catch(() => [] as Array<string | null>),
    ]);

    return assembleRegionCalendar({
      region,
      from,
      to,
      today,
      computedAt: new Date().toISOString(),
      sources,
      seasons: seasons.map((s) => ({ ...toPublicSeason(s), parkId: s.parkId })),
      forecastUntil: forecastUntil as Array<string | null>,
    });
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
