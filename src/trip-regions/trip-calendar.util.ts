/**
 * How one park's day in the region calendar is read from what the API already
 * has: the calendar day (status, crowd level, headliner forecast, weather), the
 * park's schedule rows for that date (the before-opening and evening slots), the
 * curated seasons, and the same weekday a year back.
 *
 * Pure: the service gathers the inputs, these functions decide. Every rule here
 * is a rule of the contract (docs/frontend/trip-regions.md), and the specs pin
 * them on the shapes the Orlando feed really sends.
 */
import { formatInTimeZone, fromZonedTime } from "date-fns-tz";
import {
  addIsoDays,
  isoDaysBetween,
  lastYearDays,
} from "../common/utils/date.util";
import { median } from "../common/utils/stats.util";

export type TripDayStatus = "OPERATING" | "CLOSED" | "UNKNOWN";
export type TripBasis = "forecast" | "long_range" | "last_year" | "none";
/** The before-opening slot's name, the region's own (`RegionGroupConfig.earlyKind`). */
export type EarlyKind = string;
export type EveningKind = "extended_evening" | "ticketed_event";

/** The schedule types that describe a slot beside the day guests' hours. */
const SLOT_TYPES = new Set(["TICKETED_EVENT", "EXTRA_HOURS"]);

/** An evening this long or longer is a separate event, not an extension. */
export const TICKETED_EVENING_MIN_HOURS = 3;

export interface TripSlot<K extends string = string> {
  kind: K;
  opensAt: string;
  closesAt: string;
  /** `season`: the feed lost the row and a curated season supplies it. */
  source?: "season";
}

/** One park on one day, as the calendar route serves it. */
export interface TripParkDay {
  status: TripDayStatus;
  hoursPublished: boolean;
  opensAt?: string;
  closesAt?: string;
  early?: TripSlot<EarlyKind>;
  evening?: TripSlot<EveningKind>;
  crowdLevel: string;
  /**
   * The figure every trip surface ranks and shows, on the measured scale
   * (`basis` says which): within the TFT horizon (`SERVING_TFT_DAYS`, 60
   * days) the forecast times its lead factor ({@link forecastLeadFactor}),
   * past it the same weekday a year back as measured.
   */
  headlinerWait?: number;
  basis: TripBasis;
  /**
   * The served forecast as the park's own calendar shows it, unscaled, where
   * there is one. It differs from `headlinerWait` on every forecast day.
   */
  forecastWait?: number;
  /** The same weekday a year back as measured, where there is one, from today on. */
  lastYearWait?: number;
  seasons?: string[];
  /** The curated closure season that makes the day CLOSED. */
  closedBy?: string;
  /**
   * The day guests must leave at least two hours before the park's median
   * close in the window (a party night, a short winter day), with that close.
   */
  closesEarly?: true;
  dayGuestCloseAt?: string;
  /**
   * `extreme_heat`: a theme park's day whose high (forecast, else the climate
   * normal) passes the plan's heat threshold (`HeatModel` in the optimizer).
   */
  warnings?: Array<"extreme_heat">;
}

/** What the plan reads beside the public fields; never sent. */
export interface TripParkDayInternal extends TripParkDay {
  dayHours?: number;
  /** `headlinerWait` before rounding, for ranking days; never sent. */
  waitExact?: number;
  /** `forecastWait` before rounding; never sent. */
  forecastExact?: number;
  /** `lastYearWait` before rounding; never sent. */
  lastYearExact?: number;
  /**
   * The park's usual day-guest hours around this date (the median over its
   * open days within `REFERENCE_DAYS`), which `dayCost` reads a short day
   * against. Never sent.
   */
  usualHours?: number;
  weather?: {
    tempMax?: number;
    precipitationMm?: number;
    icon?: number;
    /** `climate_normal` past the forecast's reach (the calendar's own marker). */
    basis?: "forecast" | "climate_normal";
  };
}

/** A schedule row as `ParksService.getSchedule` returns it. */
export interface ScheduleRowLike {
  date: string | Date;
  scheduleType: string;
  openingTime: Date | string | null;
  closingTime: Date | string | null;
  description?: string | null;
}

/** A curated season as the calendar needs it. */
export interface SeasonLike {
  id: string;
  kind: string;
  startDate: string;
  endDate: string;
  dates: string[] | null;
  status: string;
  separateTicket: boolean;
  opensAt: string | null;
  closesAt: string | null;
}

/** The subset of a `CalendarDay` this module reads. */
export interface CalendarDayLike {
  date: string;
  status: TripDayStatus;
  crowdLevel: string;
  hours?: { openingTime: string; closingTime: string; isInferred: boolean };
  headlinerForecast?: { avgWait: number; exactWait?: number; actual?: boolean };
  weather?: {
    tempMax: number;
    rainChance: number;
    precipitationMm?: number;
    icon: number;
    basis?: "forecast" | "climate_normal";
  };
}

/** The measured figures of a past day, as the last-year snapshot keeps them. */
export interface LastYearEntry {
  /** The calendar's crowd level that day; `closed` when the park was. */
  c: string;
  /** Mean of the headliners' measured day-P90, rounded to five. */
  w?: number;
  /** The same mean unrounded, for ranking (absent in older snapshots). */
  x?: number;
}

const round5 = (min: number): number => Math.round(min / 5) * 5;
/** One decimal: the unrounded figures a ranking reads, and the averages `/when` shows. */
export const round1 = (min: number): number => Math.round(min * 10) / 10;

/**
 * What a TFT day figure is multiplied by to sit on the measured scale, by
 * lead (days from the region's today): realised day P90 ÷ predicted, over
 * 2.5 M comparisons in the 45 days to 2026-09-03, all rides pooled
 * (docs/ml/long-range-forecasting.md §2). The model reads low and more so
 * further out; a last-year day is measured, so without this a December
 * window would beat a June one on its basis alone. Replaced by factors
 * measured per region from `prediction_lead_snapshots` once their 30- and
 * 60-day buckets hold enough served rows.
 */
export const TFT_LEAD_FACTORS: ReadonlyArray<{
  upToLead: number;
  factor: number;
}> = [
  { upToLead: 1, factor: 1.1 },
  { upToLead: 3, factor: 1.11 },
  { upToLead: 7, factor: 1.12 },
  { upToLead: 14, factor: 1.16 },
  { upToLead: 30, factor: 1.21 },
  { upToLead: 60, factor: 1.29 },
];

/** {@link TFT_LEAD_FACTORS} at a lead; today counts as lead 1, past 60 the last row. */
export function forecastLeadFactor(lead: number): number {
  const row = TFT_LEAD_FACTORS.find((r) => lead <= r.upToLead);
  return (row ?? TFT_LEAD_FACTORS[TFT_LEAD_FACTORS.length - 1]).factor;
}

/** An instant as ISO 8601 with the park's own offset. */
export function isoInZone(instant: Date | string, timezone: string): string {
  return formatInTimeZone(
    typeof instant === "string" ? new Date(instant) : instant,
    timezone,
    "yyyy-MM-dd'T'HH:mm:ssXXX",
  );
}

/** A schedule row's day as `YYYY-MM-DD` (TypeORM hands DATE columns out as strings). */
export function scheduleRowDate(
  row: ScheduleRowLike,
  timezone: string,
): string {
  return typeof row.date === "string"
    ? row.date.slice(0, 10)
    : formatInTimeZone(row.date, timezone, "yyyy-MM-dd");
}

const asDate = (value: Date | string | null): Date | null =>
  value == null ? null : typeof value === "string" ? new Date(value) : value;

/** Whether a curated season runs on a date: its `dates` when listed, else the range. */
export function seasonCovers(season: SeasonLike, date: string): boolean {
  if (season.dates && season.dates.length > 0) {
    return season.dates.includes(date);
  }
  return season.startDate <= date && date <= season.endDate;
}

/**
 * The seasons a day lists: those running on the date, a cancelled one left
 * out. The region calendar's cells and the MCP's days both read it.
 */
export function seasonsOn<T extends SeasonLike>(
  seasons: readonly T[],
  date: string,
): T[] {
  return seasons.filter(
    (season) => season.status !== "cancelled" && seasonCovers(season, date),
  );
}

/** A season that states something: not last year's pattern, not called off. */
const isStatement = (season: SeasonLike): boolean =>
  season.status === "confirmed" || season.status === "announced";

/**
 * The day guests' hours and the slots beside them, read off one date's
 * schedule rows by their time of day against the OPERATING row: a slot that
 * ends by the opening is the early one, a slot that starts at or after the day
 * guests' close is the evening. Without an OPERATING row nothing can be placed.
 */
export function readDaySlots(
  rows: ScheduleRowLike[],
  timezone: string,
  earlyKind: EarlyKind,
): {
  open?: Date;
  close?: Date;
  early?: TripSlot<EarlyKind>;
  evening?: TripSlot<EveningKind>;
} {
  const operating = rows.find(
    (r) => r.scheduleType === "OPERATING" && r.openingTime && r.closingTime,
  );
  const open = asDate(operating?.openingTime ?? null);
  const close = asDate(operating?.closingTime ?? null);
  if (!open || !close) return {};

  let early: { row: ScheduleRowLike; start: Date; end: Date } | undefined;
  let evening: { row: ScheduleRowLike; start: Date; end: Date } | undefined;
  for (const row of rows) {
    if (!SLOT_TYPES.has(row.scheduleType)) continue;
    const start = asDate(row.openingTime);
    const end = asDate(row.closingTime);
    if (!start || !end || end <= start) continue;
    if (end.getTime() <= open.getTime()) {
      if (!early || start < early.start) early = { row, start, end };
    } else if (start.getTime() >= close.getTime()) {
      if (!evening || end > evening.end) evening = { row, start, end };
    }
  }

  const hours = (a: Date, b: Date) => (b.getTime() - a.getTime()) / 3_600_000;
  return {
    open,
    close,
    ...(early && {
      early: {
        kind: earlyKind,
        opensAt: isoInZone(early.start, timezone),
        closesAt: isoInZone(early.end, timezone),
      },
    }),
    ...(evening && {
      evening: {
        kind:
          hours(evening.start, evening.end) >= TICKETED_EVENING_MIN_HOURS
            ? "ticketed_event"
            : "extended_evening",
        opensAt: isoInZone(evening.start, timezone),
        closesAt: isoInZone(evening.end, timezone),
      },
    }),
  };
}

/**
 * The evening a curated season supplies where the feed has none: a separate
 * ticket, stated (confirmed or announced), running that date, with times that
 * start at or after the day guests' close. `24:00` and a close before the
 * opening run into the next day.
 */
export function seasonEvening(
  seasons: SeasonLike[],
  date: string,
  dayClose: Date,
  timezone: string,
): TripSlot<"ticketed_event"> | undefined {
  for (const season of seasons) {
    if (!season.separateTicket || !isStatement(season)) continue;
    if (!season.opensAt || !season.closesAt || !seasonCovers(season, date)) {
      continue;
    }
    const start = fromZonedTime(`${date}T${season.opensAt}:00`, timezone);
    if (start.getTime() < dayClose.getTime()) continue;
    const endDay =
      season.closesAt === "24:00" || season.closesAt <= season.opensAt
        ? addIsoDays(date, 1)
        : date;
    const endTime = season.closesAt === "24:00" ? "00:00" : season.closesAt;
    const end = fromZonedTime(`${endDay}T${endTime}:00`, timezone);
    return {
      kind: "ticketed_event",
      opensAt: isoInZone(start, timezone),
      closesAt: isoInZone(end, timezone),
      source: "season",
    };
  }
  return undefined;
}

/** A day's holiday flags, as the region calendar and the plan's day info carry them. */
interface HolidayFlags {
  isHoliday?: boolean;
  isSchoolVacation?: boolean;
}

/** Monday to Friday and no public holiday: the `weekday` reason of a plan day and a best day. */
export function isPlainWeekday(
  weekday: number,
  isHoliday: boolean | undefined,
): boolean {
  return weekday >= 1 && weekday <= 5 && !isHoliday;
}

/** The first day after a holiday or school break that is neither: the `holiday_ends` reason. */
export function endsHolidays(
  previous: HolidayFlags | undefined,
  day: HolidayFlags | undefined,
): boolean {
  return (
    !!previous &&
    !!(previous.isHoliday || previous.isSchoolVacation) &&
    !day?.isHoliday &&
    !day?.isSchoolVacation
  );
}

/** A day "closes early" when the day guests leave this long before the usual close. */
export const CLOSES_EARLY_MINUTES = 120;

/**
 * Minutes after the day's own midnight of a park-local ISO close
 * (`2026-11-24T18:00:00-05:00`); a close past midnight reads over 24 h.
 */
export function closeMinute(date: string, closesAt: string): number {
  const [closeDate, time] = closesAt.split("T");
  const [h, m] = time.split(":").map(Number);
  return isoDaysBetween(date, closeDate) * 1440 + h * 60 + m;
}

/** A park's usual close over some open days: the median of their closes. */
export function usualCloseMinute(
  days: Array<{ date: string; closesAt?: string }>,
): number | undefined {
  return median(
    days
      .filter((d): d is { date: string; closesAt: string } => !!d.closesAt)
      .map((d) => closeMinute(d.date, d.closesAt)),
  );
}

/** The day guests leave at least {@link CLOSES_EARLY_MINUTES} before the usual close. */
export function closesEarly(
  date: string,
  closesAt: string | undefined,
  usual: number | undefined,
): boolean {
  if (!closesAt || usual === undefined) return false;
  return closeMinute(date, closesAt) <= usual - CLOSES_EARLY_MINUTES;
}

/** A past calendar day reduced to what `last_year` needs. */
export function toLastYearEntry(day: CalendarDayLike): LastYearEntry {
  if (day.status === "CLOSED" || day.crowdLevel === "closed") {
    return { c: "closed" };
  }
  const measured =
    day.headlinerForecast?.actual === true ? day.headlinerForecast : undefined;
  const w = measured ? round5(measured.avgWait) : undefined;
  const x = measured?.exactWait;
  return {
    c: day.crowdLevel,
    ...(w != null && { w }),
    ...(x != null && { x }),
  };
}

export interface ParkDayInput {
  date: string;
  /** The region's today. */
  today: string;
  timezone: string;
  day: CalendarDayLike | undefined;
  rows: ScheduleRowLike[];
  /** The park's seasons overlapping the window. */
  seasons: SeasonLike[];
  /** `scheduleCoverage.to`: the park's last published operating day. */
  publishedHoursUntil: string | null;
  earlyKind: EarlyKind;
  /** Days ahead past which the forecast is the long-range model's. */
  longRangeAfterDays: number;
  /** The same weekday a year back, from the last-year snapshot. */
  lastYear?: LastYearEntry;
}

/** The day a year back that `last_year` reads for a date (lastYearDays' weekday). */
export function lastYearDateFor(date: string): string {
  return lastYearDays(date).sameWeekday;
}

/** One park's day for the region calendar. */
export function buildParkDay(input: ParkDayInput): TripParkDayInternal {
  const { date, today, timezone, day, rows, seasons } = input;
  const genuineClosedRow = rows.some(
    (r) => r.scheduleType === "CLOSED" && r.description !== "Gap-filled",
  );
  const pastCoverage =
    input.publishedHoursUntil === null || date > input.publishedHoursUntil;

  let status: TripDayStatus = day?.status ?? "UNKNOWN";
  // Past the last published day the calendar's seasonal rule calls an
  // off-season day CLOSED. Nobody said so; only a row the feed sent does.
  if (status === "CLOSED" && pastCoverage && !genuineClosedRow) {
    status = "UNKNOWN";
  }

  const daySeasons = seasonsOn(seasons, date);
  let closedBy: string | undefined;
  if (status !== "OPERATING") {
    // A curated closure is a stated fact (the operator's page), so it may close
    // a day the feed is silent about. It never overrides an OPERATING row.
    const closure = daySeasons.find(
      (s) => s.kind === "closure" && isStatement(s),
    );
    if (closure) {
      status = "CLOSED";
      closedBy = closure.id;
    }
  }

  const hoursPublished =
    !closedBy &&
    ((status === "OPERATING" && !!day?.hours && !day.hours.isInferred) ||
      (status === "CLOSED" && (!pastCoverage || genuineClosedRow)));

  const out: TripParkDayInternal = {
    status,
    hoursPublished,
    crowdLevel: "unknown",
    basis: "none",
  };
  const seasonIds = daySeasons.map((s) => s.id);
  if (seasonIds.length > 0) out.seasons = seasonIds;
  if (closedBy) out.closedBy = closedBy;

  if (status === "CLOSED") {
    out.crowdLevel = "closed";
    return out;
  }

  if (status === "OPERATING") {
    const slots = readDaySlots(rows, timezone, input.earlyKind);
    if (slots.open && slots.close) {
      out.opensAt = isoInZone(slots.open, timezone);
      out.closesAt = isoInZone(slots.close, timezone);
      out.dayHours =
        Math.round(
          ((slots.close.getTime() - slots.open.getTime()) / 3_600_000) * 100,
        ) / 100;
      if (slots.early) out.early = slots.early;
      out.evening =
        slots.evening ?? seasonEvening(seasons, date, slots.close, timezone);
      if (!out.evening) delete out.evening;
    } else if (day?.hours && !day.hours.isInferred) {
      out.opensAt = isoInZone(day.hours.openingTime, timezone);
      out.closesAt = isoInZone(day.hours.closingTime, timezone);
    }
  }

  // Both figures sit side by side; one of them is the day's, on the measured
  // scale. Within TFT's reach that is the forecast times its lead factor; past
  // it, the same weekday a year back as measured, because the long-range level
  // reads about 40 % low and nearly flat against it (PAR-1319); the long-range
  // forecast stands in only where nothing was measured a year back.
  const forecast =
    day?.headlinerForecast && !day.headlinerForecast.actual
      ? day.headlinerForecast
      : undefined;
  const level = day?.crowdLevel;
  const rated = !!level && level !== "unknown" && level !== "closed";
  const lastYear =
    input.lastYear &&
    input.lastYear.c !== "closed" &&
    (input.lastYear.w != null || input.lastYear.c !== "unknown")
      ? input.lastYear
      : undefined;
  const future = date >= today;
  if (future && forecast) {
    out.forecastWait = round5(forecast.avgWait);
    if (forecast.exactWait != null) out.forecastExact = forecast.exactWait;
  }
  if (future && lastYear?.w != null) {
    out.lastYearWait = lastYear.w;
    if (lastYear.x != null) out.lastYearExact = lastYear.x;
  }
  const lead = isoDaysBetween(today, date);
  const forecastDay = future && (!!forecast || rated);
  if (forecastDay && lead <= input.longRangeAfterDays) {
    if (forecast) {
      const exact =
        (forecast.exactWait ?? forecast.avgWait) * forecastLeadFactor(lead);
      out.headlinerWait = round5(exact);
      out.waitExact = round1(exact);
    }
    out.crowdLevel = level ?? "unknown";
    out.basis = "forecast";
  } else if (lastYear) {
    out.crowdLevel = lastYear.c;
    if (lastYear.w != null) out.headlinerWait = lastYear.w;
    if (lastYear.x != null) out.waitExact = lastYear.x;
    out.basis = "last_year";
  } else if (forecastDay) {
    if (forecast) {
      out.headlinerWait = out.forecastWait;
      if (out.forecastExact != null) out.waitExact = out.forecastExact;
    }
    out.crowdLevel = level ?? "unknown";
    out.basis = "long_range";
  }

  if (day?.weather) {
    out.weather = {
      tempMax: day.weather.tempMax,
      precipitationMm: day.weather.precipitationMm ?? day.weather.rainChance,
      icon: day.weather.icon,
      ...(day.weather.basis && { basis: day.weather.basis }),
    };
  }
  return out;
}

/** The public shape: the plan's internal fields stay home. */
export function toPublicParkDay(day: TripParkDayInternal): TripParkDay {
  const {
    dayHours: _dayHours,
    weather: _weather,
    waitExact: _waitExact,
    forecastExact: _forecastExact,
    lastYearExact: _lastYearExact,
    usualHours: _usualHours,
    ...rest
  } = day;
  return rest;
}
