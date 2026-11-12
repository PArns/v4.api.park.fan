/**
 * The region and its calendar, assembled from data the caller has already
 * fetched. Pure: `TripRegionService` feeds it from the database and Redis, the
 * dev shim (`scripts/trip-regions/dev-server.ts`) from production's public
 * routes, and both get the same answer for the same inputs. Contract:
 * docs/frontend/trip-regions.md.
 */
import { BadRequestException } from "@nestjs/common";
import { addIsoDays, isRealIsoDate } from "../common/utils/date.util";
import { SERVING_TFT_DAYS } from "../ml/serving-horizons.constants";
import type { PublicParkSeason } from "../parks/park-seasons.controller";
import type { RegionConfig, RegionParkConfig } from "./regions";
import { buildLegs, regionMatrix } from "./travel";
import {
  buildParkDay,
  lastYearDateFor,
  readDaySlots,
  scheduleRowDate,
  toPublicParkDay,
  type CalendarDayLike,
  type ScheduleRowLike,
  type TripParkDay,
  type TripParkDayInternal,
} from "./trip-calendar.util";
import { monthOf, type LastYearMonth } from "./last-year-snapshot";
import type {
  TripCalendarDto,
  TripRegionDto,
} from "./dto/trip-region-response.dto";

/** The calendar route's window: at most this many days. */
export const MAX_CALENDAR_DAYS = 92;
/** Nothing is planned or shown further ahead than this. */
export const MAX_DAYS_AHEAD = 365;

/** A region park as the database (or production's park payload) has it. */
export interface RegionParkRecord {
  config: RegionParkConfig;
  id: string;
  slug: string;
  /** The display name (curated where an editor wrote one). */
  name: string;
  path: string;
  latitude: number;
  longitude: number;
  timezone: string;
  destinationId?: string | null;
  destinationName?: string | null;
}

/** A calendar day with the region-level flags the assembly reads. */
export interface RegionCalendarDayLike extends CalendarDayLike {
  isHoliday?: boolean;
  isSchoolVacation?: boolean;
  schoolVacationUnknown?: boolean;
  events?: Array<{ name: string }>;
}

/** One park's inputs for a window. */
export interface ParkCalendarSource {
  park: RegionParkRecord;
  calendar: {
    meta: { scheduleCoverage?: { to: string | null } | null };
    days: RegionCalendarDayLike[];
  };
  rows: ScheduleRowLike[];
  lastYear: Map<string, LastYearMonth>;
}

/** A curated season as the public seasons route serves it, with its park. */
export type RegionSeasonSource = PublicParkSeason & { parkId: string };

export interface InternalCalendarDay {
  date: string;
  isHoliday: boolean;
  isSchoolVacation: boolean;
  schoolVacationUnknown?: boolean;
  holidays?: string[];
  /** Per group, the open park with the lowest headliner wait that day. */
  best?: Record<string, string>;
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
 * Checks a window against the region's today: real dates, in order, from
 * today to a year ahead, at most `maxDays` long. 400 otherwise.
 */
export function validateWindow(
  from: string,
  to: string,
  maxDays: number,
  today: string,
): void {
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
  const last = addIsoDays(today, MAX_DAYS_AHEAD);
  if (to > last) throw new BadRequestException(`to must not be after ${last}`);
  if (addIsoDays(from, maxDays - 1) < to) {
    throw new BadRequestException(`The window is at most ${maxDays} days`);
  }
}

/**
 * A group's name: the `destinations` row its parks share when they share one
 * (the operator's own name, marks stripped), else the curated name.
 */
function groupName(curated: string, parks: RegionParkRecord[]): string {
  const ids = new Set(parks.map((p) => p.destinationId ?? null));
  const name = parks[0]?.destinationName;
  if (ids.size === 1 && !ids.has(null) && name) return stripMarks(name);
  return curated;
}

/** `GET /v1/trip-regions/:region`. */
export function describeRegion(
  region: RegionConfig,
  parks: RegionParkRecord[],
): TripRegionDto {
  const points = new Set([
    ...parks.map((p) => p.config.slug),
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
        name: groupName(
          g.name,
          parks.filter((p) => p.config.group === g.key),
        ),
        kind: g.kind,
      })),
    parks: parks.map((p) => ({
      id: p.id,
      slug: p.slug,
      name: p.name,
      path: p.path,
      group: p.config.group,
      type: p.config.type,
      headliner: p.config.headliner,
      latitude: p.latitude,
      longitude: p.longitude,
      timezone: p.timezone,
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
 * The past months a window's days without a forecast read last year from:
 * the same weekday a year back, for each day from today on.
 */
export function lastYearMonthsNeeded(
  days: CalendarDayLike[],
  today: string,
): string[] {
  const months = new Set<string>();
  for (const day of days) {
    if (day.date < today) continue;
    if (day.headlinerForecast && !day.headlinerForecast.actual) continue;
    const ly = lastYearDateFor(day.date);
    if (ly < today) months.add(monthOf(ly));
  }
  return [...months].sort();
}

/**
 * The order parks are compared in on one day: a figure first, then the lower
 * unrounded wait, then more hours for the day guest, then the lower level.
 */
const LEVEL_ORDER = [
  "very_low",
  "low",
  "moderate",
  "high",
  "very_high",
  "extreme",
];
const levelRank = (level: string): number => {
  const i = LEVEL_ORDER.indexOf(level);
  return i < 0 ? LEVEL_ORDER.length : i;
};

/** The unrounded wait a day is ranked by, falling back to the shown one. */
export const rankWait = (d: TripParkDayInternal): number | undefined =>
  d.waitExact ?? d.headlinerWait;

/**
 * Per resort, the theme park of the day to pick: OPERATING, with a forecast
 * (`forecast` or `long_range`; a `last_year` figure is last year's day, not a
 * statement about this one), the lowest unrounded headliner wait; ties go to
 * more day-guest hours, then the lower crowd level. Day trips are no resort
 * and have none; a resort with no such park is left out.
 */
export function bestOfDay(
  region: RegionConfig,
  parks: Record<string, TripParkDayInternal>,
): Record<string, string> {
  const best: Record<string, string> = {};
  for (const group of region.groups) {
    if (group.kind !== "resort") continue;
    const candidates = region.parks
      .filter((p) => p.group === group.key && p.type === "theme_park")
      .map((p) => ({ slug: p.slug, day: parks[p.slug] }))
      .filter(
        (c) =>
          c.day?.status === "OPERATING" &&
          (c.day.basis === "forecast" || c.day.basis === "long_range") &&
          rankWait(c.day) !== undefined,
      )
      .sort(
        (a, b) =>
          (rankWait(a.day) as number) - (rankWait(b.day) as number) ||
          (b.day.dayHours ?? 0) - (a.day.dayHours ?? 0) ||
          levelRank(a.day.crowdLevel) - levelRank(b.day.crowdLevel) ||
          a.slug.localeCompare(b.slug),
      );
    if (candidates.length > 0) best[group.key] = candidates[0].slug;
  }
  return best;
}

export interface AssembleCalendarInput {
  region: RegionConfig;
  from: string;
  to: string;
  today: string;
  computedAt: string;
  sources: ParkCalendarSource[];
  seasons: RegionSeasonSource[];
  /** Per park, the last forecast day the warmup recorded (null when cold). */
  forecastUntil: Array<string | null>;
}

/** `GET /v1/trip-regions/:region/calendar`, with the plan's fields. */
export function assembleRegionCalendar(
  input: AssembleCalendarInput,
): InternalRegionCalendar {
  const { region, from, to, today, sources } = input;
  const slugById = new Map(sources.map((s) => [s.park.id, s.park.slug]));
  const seasonsByPark = new Map<string, RegionSeasonSource[]>();
  for (const s of input.seasons) {
    seasonsByPark.set(s.parkId, [...(seasonsByPark.get(s.parkId) ?? []), s]);
  }
  const earlyKindOf = new Map(
    region.groups.map((g) => [g.key, g.earlyKind] as const),
  );
  const dates: string[] = [];
  for (let d = from; d <= to; d = addIsoDays(d, 1)) dates.push(d);

  const cells = new Map<string, Map<string, TripParkDayInternal>>();
  const coverage: Record<string, string | null> = {};
  for (const { park, calendar, rows, lastYear } of sources) {
    const tz = park.timezone || region.timezone;
    const earlyKind = earlyKindOf.get(park.config.group) ?? "early_entry";
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
    coverage[park.config.slug] = publishedHoursUntil;
    const seasons = seasonsByPark.get(park.id) ?? [];
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
    cells.set(park.config.slug, parkCells);
  }

  // The day's holiday flags are the region's: read them off its first park.
  const flagsByDate = new Map(
    (sources[0]?.calendar.days ?? []).map((d) => [d.date, d]),
  );
  const days: InternalCalendarDay[] = dates.map((date) => {
    const flags = flagsByDate.get(date);
    const holidays = [...new Set((flags?.events ?? []).map((e) => e.name))];
    const parks = Object.fromEntries(
      sources.map((s) => [
        s.park.config.slug,
        cells.get(s.park.config.slug)?.get(date) as TripParkDayInternal,
      ]),
    );
    const best = bestOfDay(region, parks);
    return {
      date,
      isHoliday: flags?.isHoliday ?? false,
      isSchoolVacation: flags?.isSchoolVacation ?? false,
      ...(flags?.schoolVacationUnknown && { schoolVacationUnknown: true }),
      ...(holidays.length > 0 && { holidays }),
      ...(Object.keys(best).length > 0 && { best }),
      parks,
    };
  });

  const until = input.forecastUntil
    .filter((d): d is string => !!d && isRealIsoDate(d))
    .sort();
  return {
    meta: {
      region: region.slug,
      timezone: region.timezone,
      from,
      to,
      computedAt: input.computedAt,
      ...(until.length > 0 && { forecastUntil: until[until.length - 1] }),
      publishedHoursUntil: coverage,
    },
    parks: sources.map((s) => s.park.config.slug),
    days,
    seasons: input.seasons.map(({ parkId, ...season }) => ({
      ...season,
      parkSlug: slugById.get(parkId),
    })),
  };
}

/** The route's projection: the plan's internal fields stay home. */
export function publicCalendar(
  internal: InternalRegionCalendar,
): TripCalendarDto {
  return {
    ...internal,
    days: internal.days.map((day) => ({
      ...day,
      parks: Object.fromEntries(
        Object.entries(day.parks).map(([s, d]): [string, TripParkDay] => [
          s,
          toPublicParkDay(d),
        ]),
      ),
    })),
  };
}
