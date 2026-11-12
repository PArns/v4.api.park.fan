import {
  BadRequestException,
  Inject,
  Injectable,
  Logger,
  NotFoundException,
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
  isRealIsoDate,
} from "../common/utils/date.util";
import { parseDateRange } from "../common/utils/date-parsing.util";
import { frontendParkPath } from "../common/utils/frontend-url.util";
import { ParksService } from "../parks/parks.service";
import { CalendarService } from "../parks/services/calendar.service";
import { ParkSeasonService } from "../parks/services/park-season.service";
import { resolveCuratedPark } from "../parks/utils/curated-park-facts.util";
import { toPublicSeason } from "../parks/park-seasons.controller";
import type { Park } from "../parks/entities/park.entity";
import type { CalendarDay } from "../parks/dto/integrated-calendar.dto";
import { SERVING_TFT_DAYS } from "../ml/serving-horizons.constants";
import {
  findRegion,
  type RegionConfig,
  type RegionParkConfig,
} from "./regions";
import { buildLegs, regionMatrix } from "./travel";
import {
  buildParkDay,
  lastYearDateFor,
  readDaySlots,
  scheduleRowDate,
  toPublicParkDay,
  type ScheduleRowLike,
  type SeasonLike,
  type TripParkDayInternal,
} from "./trip-calendar.util";
import {
  monthOf,
  readLastYearMonths,
  writeLastYearMonth,
  type LastYearMonth,
} from "./last-year-snapshot";
import type {
  TripCalendarDto,
  TripRegionDto,
} from "./dto/trip-region-response.dto";

/** The calendar route's window: at most this many days. */
export const MAX_CALENDAR_DAYS = 92;
/** Nothing is planned or shown further ahead than this. */
export const MAX_DAYS_AHEAD = 365;

/** How long a resolved list of a region's parks is reused. */
const RESOLVE_TTL_MS = 10 * 60 * 1000;

/** Parks of one region built at once: each is a calendar read, cold a build. */
const REGION_PARK_BUDGET = new DbJobBudget(4);

export interface ResolvedRegionPark {
  config: RegionParkConfig;
  park: Park;
}

export interface InternalCalendarDay {
  date: string;
  isHoliday: boolean;
  isSchoolVacation: boolean;
  schoolVacationUnknown?: boolean;
  holidays?: string[];
  parks: Record<string, TripParkDayInternal>;
}

/** The region calendar as the plan reads it; the route serves a projection. */
export interface InternalRegionCalendar {
  meta: TripCalendarDto["meta"];
  parks: string[];
  days: InternalCalendarDay[];
  seasons: TripCalendarDto["seasons"];
}

const stripMarks = (name: string): string =>
  name.replace(/[®™©]/g, "").replace(/\s+/g, " ").trim();

/**
 * The trip regions: the region itself (parks, groups, hotel areas, travel) and
 * its calendar, every park's days side by side. The plan is `TripPlanService`'s.
 * Contract: docs/frontend/trip-regions.md.
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
  ) {}

  /** The region a slug names; 404 when there is none. */
  region(slug: string): RegionConfig {
    const region = findRegion(slug);
    if (!region) throw new NotFoundException(`No trip region "${slug}"`);
    return region;
  }

  /** The region's today, in its own timezone. */
  today(region: RegionConfig): string {
    return getCurrentDateInTimezone(region.timezone);
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

  // ── GET /v1/trip-regions/:region ─────────────────────────────────────────

  async describe(slug: string): Promise<TripRegionDto> {
    const region = this.region(slug);
    const parks = await this.resolveParks(region);
    const present = new Set(parks.map((p) => p.config.slug));
    const points = new Set([
      ...present,
      ...region.hotelAreas.map((h) => `hotel:${h.key}`),
    ]);
    const matrix = regionMatrix(region);

    return {
      slug: region.slug,
      name: region.name,
      timezone: region.timezone,
      groups: region.groups
        .filter((g) => parks.some((p) => p.config.group === g.key))
        .map((g) => ({
          key: g.key,
          name: this.groupName(
            g.name,
            parks.filter((p) => p.config.group === g.key).map((p) => p.park),
          ),
          kind: g.kind,
        })),
      parks: parks.map(({ config, park }) => ({
        id: park.id,
        slug: park.slug,
        name: resolveCuratedPark(park).name,
        path: frontendParkPath(park) ?? "",
        group: config.group,
        type: config.type,
        headliner: config.headliner,
        latitude: Number(park.latitude),
        longitude: Number(park.longitude),
        timezone: park.timezone,
      })),
      hotelAreas: region.hotelAreas.map((area) => ({
        key: area.key,
        name: area.name,
        latitude: area.latitude,
        longitude: area.longitude,
        perks: [...new Set(region.perks[area.hotel].map((r) => r.perk))],
      })),
      travel: {
        source: matrix.source,
        computedOn: matrix.computedOn,
        attribution: matrix.attribution,
        basis: matrix.basis,
        legs: buildLegs(region).filter(
          (leg) => points.has(leg.from) && points.has(leg.to),
        ),
      },
    };
  }

  /**
   * A group's name: the `destinations` row its parks share when they share one
   * (the operator's own name, marks stripped), else the curated name.
   */
  private groupName(curated: string, parks: Park[]): string {
    const ids = new Set(parks.map((p) => p.destinationId));
    const destination = parks[0]?.destination;
    if (ids.size === 1 && destination?.name) {
      return stripMarks(destination.name);
    }
    return curated;
  }

  // ── GET /v1/trip-regions/:region/calendar ────────────────────────────────

  /**
   * Checks a window against the region's today: real dates, in order, from
   * today to a year ahead, at most `maxDays` long. 400 otherwise.
   */
  validateWindow(
    region: RegionConfig,
    from: string,
    to: string,
    maxDays: number,
  ): void {
    for (const [name, value] of [
      ["from", from],
      ["to", to],
    ] as const) {
      if (!isRealIsoDate(value)) {
        throw new BadRequestException(`${name} is not a date the calendar has`);
      }
    }
    const today = this.today(region);
    if (from < today) {
      throw new BadRequestException(`from must not be before today (${today})`);
    }
    if (to < from) throw new BadRequestException("to must not be before from");
    const last = addIsoDays(today, MAX_DAYS_AHEAD);
    if (to > last) {
      throw new BadRequestException(`to must not be after ${last}`);
    }
    if (addIsoDays(from, maxDays - 1) < to) {
      throw new BadRequestException(`The window is at most ${maxDays} days`);
    }
  }

  async calendar(
    slug: string,
    fromQuery?: string,
    toQuery?: string,
  ): Promise<TripCalendarDto> {
    const region = this.region(slug);
    const from = fromQuery ?? this.today(region);
    const to = toQuery ?? addIsoDays(from, 30);
    this.validateWindow(region, from, to, MAX_CALENDAR_DAYS);
    const internal = await this.internalCalendar(region, from, to);
    return {
      ...internal,
      days: internal.days.map((day) => ({
        ...day,
        parks: Object.fromEntries(
          Object.entries(day.parks).map(([s, d]) => [s, toPublicParkDay(d)]),
        ),
      })),
    };
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
    if (cached) return cached;
    return this.calendarFlight.run(key, async () => {
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
    const { fromDate, toDate } = parseDateRange(from, to, {
      timezone: region.timezone,
    });

    const perPark = await mapWithDbBudget(
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
        const lastYear = await this.lastYearFor(rp.park, calendar.days, today);
        return { rp, calendar, rows: rows as ScheduleRowLike[], lastYear };
      },
      REGION_PARK_BUDGET,
    );

    const [seasonRows, forecastUntil] = await Promise.all([
      this.parkSeasonService.forParksInRange(
        parks.map((p) => p.park.id),
        from,
        to,
      ),
      this.redis
        .mget(...parks.map((p) => CacheKeys.tripForecastUntil(p.park.id)))
        .catch(() => [] as Array<string | null>),
    ]);
    const slugById = new Map(parks.map((p) => [p.park.id, p.config.slug]));
    const seasonsByPark = new Map<string, SeasonLike[]>();
    for (const s of seasonRows) {
      const list = seasonsByPark.get(s.parkId) ?? [];
      list.push(s);
      seasonsByPark.set(s.parkId, list);
    }

    const earlyKindOf = new Map(
      region.groups.map((g) => [g.key, g.earlyKind] as const),
    );
    const dates: string[] = [];
    for (let d = from; d <= to; d = addIsoDays(d, 1)) dates.push(d);

    const cells = new Map<string, Map<string, TripParkDayInternal>>();
    const coverage: Record<string, string | null> = {};
    for (const { rp, calendar, rows, lastYear } of perPark) {
      const tz = rp.park.timezone || region.timezone;
      const earlyKind = earlyKindOf.get(rp.config.group) ?? "early_entry";
      const rowsByDate = new Map<string, ScheduleRowLike[]>();
      for (const row of rows) {
        const d = scheduleRowDate(row, tz);
        rowsByDate.set(d, [...(rowsByDate.get(d) ?? []), row]);
      }
      const hasEarlySlots = [...rowsByDate.values()].some(
        (dayRows) => !!readDaySlots(dayRows, tz, earlyKind).early,
      );
      const byDate = new Map(calendar.days.map((d) => [d.date, d]));
      const publishedHoursUntil = calendar.meta.scheduleCoverage?.to ?? null;
      coverage[rp.config.slug] = publishedHoursUntil;
      const seasons = seasonsByPark.get(rp.park.id) ?? [];
      const parkCells = new Map<string, TripParkDayInternal>();
      for (const date of dates) {
        const ly = lastYearDateFor(date);
        parkCells.set(
          date,
          buildParkDay({
            date,
            today,
            timezone: tz,
            day: byDate.get(date),
            rows: rowsByDate.get(date) ?? [],
            seasons,
            publishedHoursUntil,
            earlyKind,
            hasEarlySlots,
            longRangeAfterDays: SERVING_TFT_DAYS,
            lastYear: lastYear.get(monthOf(ly))?.[ly],
          }),
        );
      }
      cells.set(rp.config.slug, parkCells);
    }

    // The day's holiday flags are the region's: read them off its first park.
    const flagsFrom = perPark[0]?.calendar.days ?? [];
    const flagsByDate = new Map<string, CalendarDay>(
      flagsFrom.map((d) => [d.date, d]),
    );
    const days: InternalCalendarDay[] = dates.map((date) => {
      const flags = flagsByDate.get(date);
      const holidays = [...new Set((flags?.events ?? []).map((e) => e.name))];
      return {
        date,
        isHoliday: flags?.isHoliday ?? false,
        isSchoolVacation: flags?.isSchoolVacation ?? false,
        ...(flags?.schoolVacationUnknown && { schoolVacationUnknown: true }),
        ...(holidays.length > 0 && { holidays }),
        parks: Object.fromEntries(
          parks.map((p) => [
            p.config.slug,
            cells.get(p.config.slug)?.get(date) as TripParkDayInternal,
          ]),
        ),
      };
    });

    const until = (forecastUntil as Array<string | null>)
      .filter((d): d is string => !!d && isRealIsoDate(d))
      .sort();
    return {
      meta: {
        region: region.slug,
        timezone: region.timezone,
        from,
        to,
        computedAt: new Date().toISOString(),
        ...(until.length > 0 && { forecastUntil: until[until.length - 1] }),
        publishedHoursUntil: coverage,
      },
      parks: parks.map((p) => p.config.slug),
      days,
      seasons: seasonRows.map((s) => ({
        ...toPublicSeason(s),
        parkSlug: slugById.get(s.parkId),
      })),
    };
  }

  /**
   * The last-year snapshots a window needs: for each day without a forecast,
   * the same weekday a year back. Read from Redis; a month the warmup has not
   * written yet is built here once and kept.
   */
  private async lastYearFor(
    park: Park,
    days: CalendarDay[],
    today: string,
  ): Promise<Map<string, LastYearMonth>> {
    const months = new Set<string>();
    for (const day of days) {
      if (day.date < today) continue;
      if (day.headlinerForecast && !day.headlinerForecast.actual) continue;
      const ly = lastYearDateFor(day.date);
      if (ly < today) months.add(monthOf(ly));
    }
    if (months.size === 0) return new Map();
    const found = await readLastYearMonths(this.redis, park.id, [...months]);
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
