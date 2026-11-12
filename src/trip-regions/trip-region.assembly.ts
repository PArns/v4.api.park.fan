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
import { dayCost, medianDayHours, type PlanCell } from "./trip-optimizer";
import { buildLegs, regionMatrix } from "./travel";
import {
  buildParkDay,
  lastYearDateFor,
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
  /** Per resort, the park of the day by `dayCost`. */
  best?: Record<string, string>;
  /** Per resort in `best`, whether the pick has the lowest raw wait too. */
  bestWhy?: Record<string, BestWhy>;
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

/** A calendar cell as the cost model reads it (`dayCost`). */
export function planCellOf(c: TripParkDayInternal): PlanCell {
  return {
    status: c.status,
    basis: c.basis,
    ...(c.headlinerWait != null && { headlinerWait: c.headlinerWait }),
    ...(c.waitExact != null && { exactWait: c.waitExact }),
    ...(c.crowdLevel && { crowdLevel: c.crowdLevel }),
    ...(c.dayHours != null && { dayHours: c.dayHours }),
    ...(c.early && { early: { kind: c.early.kind } }),
    ...(c.evening && { evening: { kind: c.evening.kind } }),
    ...(c.weather && { weather: c.weather }),
  };
}

/** A day is "closes early" when the day guests leave this long before the usual close. */
export const CLOSES_EARLY_MINUTES = 120;

/** Minutes after the day's own midnight of an ISO close (a close past midnight is over 24 h). */
function closeMinute(date: string, iso: string): number {
  const [closeDate, time] = iso.split("T");
  const [h, m] = time.split(":").map(Number);
  const days = Math.round(
    (Date.parse(`${closeDate}T00:00:00Z`) - Date.parse(`${date}T00:00:00Z`)) /
      86_400_000,
  );
  return days * 1440 + h * 60 + m;
}

/**
 * Marks the days of one park whose day-guest close is at least
 * {@link CLOSES_EARLY_MINUTES} before the park's median close over the
 * window's OPERATING days.
 */
function markEarlyCloses(cells: Map<string, TripParkDayInternal>): void {
  const closes = [...cells]
    .filter(([, c]) => c.status === "OPERATING" && c.closesAt)
    .map(([date, c]) => ({
      c,
      minute: closeMinute(date, c.closesAt as string),
    }));
  if (closes.length === 0) return;
  const sorted = closes.map((x) => x.minute).sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  const usual =
    sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
  for (const { c, minute } of closes) {
    if (minute <= usual - CLOSES_EARLY_MINUTES) {
      c.closesEarly = true;
      c.dayGuestCloseAt = c.closesAt;
    }
  }
}

/** A park's median day-guest hours over a window, for `dayCost`. */
export function windowMedianHours(
  days: Array<{ parks: Record<string, TripParkDayInternal> }>,
  slug: string,
): number | undefined {
  return medianDayHours(
    days.map((d) => d.parks[slug]).filter((c): c is TripParkDayInternal => !!c),
  );
}

export type BestWhy = "shortest_wait" | "closes_early_elsewhere";

/**
 * Per resort, the theme park of the day to pick: OPERATING, with a forecast
 * (`forecast` or `long_range`; a `last_year` figure is last year's day, not a
 * statement about this one), the lowest {@link dayCost} (the unrounded wait,
 * dearer where the day guests must leave early); ties go to the longer
 * day-guest day, then the lower crowd level. `why` says whether the pick also
 * has the lowest raw wait, or won because that park closes early. Day trips
 * are no resort; a resort with no such park is left out.
 */
export function bestOfDay(
  region: RegionConfig,
  parks: Record<string, TripParkDayInternal>,
  medianHours: Map<string, number | undefined>,
): { best: Record<string, string>; why: Record<string, BestWhy> } {
  const best: Record<string, string> = {};
  const why: Record<string, BestWhy> = {};
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
      .map((c) => ({
        ...c,
        cost: dayCost(planCellOf(c.day), {
          type: "theme_park",
          medianHours: medianHours.get(c.slug),
        }),
      }))
      .sort(
        (a, b) =>
          a.cost - b.cost ||
          (b.day.dayHours ?? 0) - (a.day.dayHours ?? 0) ||
          levelRank(a.day.crowdLevel) - levelRank(b.day.crowdLevel) ||
          a.slug.localeCompare(b.slug),
      );
    if (candidates.length === 0) continue;
    const pick = candidates[0];
    const lowestRaw = Math.min(
      ...candidates.map((c) => rankWait(c.day) as number),
    );
    best[group.key] = pick.slug;
    why[group.key] =
      (rankWait(pick.day) as number) <= lowestRaw
        ? "shortest_wait"
        : "closes_early_elsewhere";
  }
  return { best, why };
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
          longRangeAfterDays: SERVING_TFT_DAYS,
          lastYear: lastYear.get(monthOf(ly))?.[ly],
        }),
      );
    }
    markEarlyCloses(parkCells);
    cells.set(park.config.slug, parkCells);
  }

  const medianHours = new Map(
    sources.map((src) => [
      src.park.config.slug,
      medianDayHours([...(cells.get(src.park.config.slug)?.values() ?? [])]),
    ]),
  );
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
    const { best, why } = bestOfDay(region, parks, medianHours);
    return {
      date,
      isHoliday: flags?.isHoliday ?? false,
      isSchoolVacation: flags?.isSchoolVacation ?? false,
      ...(flags?.schoolVacationUnknown && { schoolVacationUnknown: true }),
      ...(holidays.length > 0 && { holidays }),
      ...(Object.keys(best).length > 0 && { best, bestWhy: why }),
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
