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
import { addIsoDays, lastYearDays } from "../common/utils/date.util";

export type TripDayStatus = "OPERATING" | "CLOSED" | "UNKNOWN";
export type TripBasis = "forecast" | "long_range" | "last_year" | "none";
export type EarlyKind = "early_entry" | "early_park_admission";
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
  headlinerWait?: number;
  basis: TripBasis;
  seasons?: string[];
  /** The curated closure season that makes the day CLOSED. */
  closedBy?: string;
  /**
   * The day guests must leave at least two hours before the park's median
   * close in the window (a party night, a short winter day), with that close.
   */
  closesEarly?: true;
  dayGuestCloseAt?: string;
}

/** What the plan reads beside the public fields; never sent. */
export interface TripParkDayInternal extends TripParkDay {
  dayHours?: number;
  /** `headlinerWait` before rounding, for ranking days; never sent. */
  waitExact?: number;
  /**
   * The park's usual day-guest hours around this date (the median over its
   * open days within `REFERENCE_DAYS`), which `dayCost` reads a short day
   * against. Never sent.
   */
  usualHours?: number;
  weather?: { tempMax?: number; precipitationMm?: number; icon?: number };
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

/** A day "closes early" when the day guests leave this long before the usual close. */
export const CLOSES_EARLY_MINUTES = 120;

/**
 * Minutes after the day's own midnight of a park-local ISO close
 * (`2026-11-24T18:00:00-05:00`); a close past midnight reads over 24 h.
 */
export function closeMinute(date: string, closesAt: string): number {
  const [closeDate, time] = closesAt.split("T");
  const [h, m] = time.split(":").map(Number);
  const days = Math.round(
    (Date.parse(`${closeDate}T00:00:00Z`) - Date.parse(`${date}T00:00:00Z`)) /
      86_400_000,
  );
  return days * 1440 + h * 60 + m;
}

/** A park's usual close over some open days: the median of their closes. */
export function usualCloseMinute(
  days: Array<{ date: string; closesAt?: string }>,
): number | undefined {
  const minutes = days
    .filter((d): d is { date: string; closesAt: string } => !!d.closesAt)
    .map((d) => closeMinute(d.date, d.closesAt))
    .sort((a, b) => a - b);
  if (minutes.length === 0) return undefined;
  const mid = Math.floor(minutes.length / 2);
  return minutes.length % 2
    ? minutes[mid]
    : (minutes[mid - 1] + minutes[mid]) / 2;
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

const daysBetween = (from: string, to: string): number =>
  Math.round(
    (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) /
      86_400_000,
  );

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

  const daySeasons = seasons.filter((s) => seasonCovers(s, date));
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
  const seasonIds = daySeasons
    .filter((s) => s.status !== "cancelled")
    .map((s) => s.id);
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

  // A future day the calendar rates is a forecast; its headliner figure comes
  // with it where the model named the rides.
  const forecast = day?.headlinerForecast;
  const level = day?.crowdLevel;
  const rated = !!level && level !== "unknown" && level !== "closed";
  if (date >= today && ((forecast && !forecast.actual) || rated)) {
    if (forecast && !forecast.actual) {
      out.headlinerWait = round5(forecast.avgWait);
      if (forecast.exactWait != null) out.waitExact = forecast.exactWait;
    }
    out.crowdLevel = level ?? "unknown";
    out.basis =
      daysBetween(today, date) > input.longRangeAfterDays
        ? "long_range"
        : "forecast";
  } else if (
    input.lastYear &&
    input.lastYear.c !== "closed" &&
    (input.lastYear.w != null || input.lastYear.c !== "unknown")
  ) {
    out.crowdLevel = input.lastYear.c;
    if (input.lastYear.w != null) out.headlinerWait = input.lastYear.w;
    if (input.lastYear.x != null) out.waitExact = input.lastYear.x;
    out.basis = "last_year";
  }

  if (day?.weather) {
    out.weather = {
      tempMax: day.weather.tempMax,
      precipitationMm: day.weather.precipitationMm ?? day.weather.rainChance,
      icon: day.weather.icon,
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
    usualHours: _usualHours,
    ...rest
  } = day;
  return rest;
}
