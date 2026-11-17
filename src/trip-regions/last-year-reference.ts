/**
 * The day a year back that stands for a day ahead: the measured day a trip
 * day reads (`basis: "last_year"`, `lastYearWait`) and the day Parky's
 * `get_park_history` compares a date with.
 *
 * 52 weeks back keeps the weekday and lets every moving holiday slip: Easter
 * Monday 2027 (29 March) read Monday 30 March 2026, an ordinary Monday, and
 * Monday 5 April 2027 read Easter Monday 2026. So the day is picked by what it
 * is ({@link lastYearReferenceDay}). Pure but for {@link loadLastYearDayFacts},
 * which reads the holiday table. Contract: docs/frontend/trip-regions.md.
 */
import {
  addIsoDays,
  eachIsoDate,
  lastYearDays,
} from "../common/utils/date.util";
import {
  dayHolidayFlags,
  localPublicHolidayNames,
  markUnknownSchoolVacation,
  schoolCoverageThrough,
  type HolidayLocation,
} from "../common/utils/holiday.utils";
import type { HolidayRow, HolidaysService } from "../holidays/holidays.service";
import {
  isStatement,
  seasonCovers,
  type SeasonLike,
} from "./trip-calendar.util";

/** Weeks either side of 52 weeks back that a reference day may move to. */
export const LAST_YEAR_WEEKS_EITHER_SIDE = 3;

/**
 * Days either side of 52 weeks back that the same public holiday is looked
 * for: Easter Sunday falls between 22 March and 25 April, 34 days apart, and a
 * year's one or two days of drift add the rest.
 */
export const LAST_YEAR_HOLIDAY_REACH_DAYS = 36;

/** What the lookup reads about one day. */
export interface LastYearDayFacts {
  isHoliday: boolean;
  isBridgeDay: boolean;
  isSchoolVacation: boolean;
  /** `isSchoolVacation` is false only because no school data reaches the day. */
  schoolVacationUnknown?: boolean;
  /** The English names of the day's public holidays in the park's region. */
  holidayNames?: string[];
}

/** A curated season as the lookup reads it. */
export type LastYearSeason = Pick<
  SeasonLike,
  "kind" | "startDate" | "endDate" | "dates" | "status"
> & { name: string | null };

/** What narrows a park's reference day beyond its holidays. */
export interface LastYearReferenceOptions {
  /** The park's first day not measured yet, its today: a reference day lies before it. */
  before?: string;
  /** The park's curated seasons on the days and a year before them. */
  seasons?: readonly LastYearSeason[];
}

type DayKind = "holiday" | "bridge" | "school" | "plain";

/** The kinds of day a day of each kind reads a year back, the first found wins. */
const READS: Record<DayKind, DayKind[]> = {
  holiday: ["holiday", "plain"],
  bridge: ["bridge", "plain"],
  school: ["school", "plain"],
  plain: ["plain"],
};

/** A bridge day is never a holiday or a break day (`calculateHolidayInfo`). */
function kindOf(day: LastYearDayFacts, schoolKnown: boolean): DayKind {
  if (day.isHoliday) return "holiday";
  if (day.isBridgeDay) return "bridge";
  if (day.isSchoolVacation && schoolKnown) return "school";
  return "plain";
}

/** 52 weeks back, then a week earlier and later, out to three weeks. */
function sameWeekdays(date: string): string[] {
  const centre = lastYearDays(date).sameWeekday;
  const out = [centre];
  for (let week = 1; week <= LAST_YEAR_WEEKS_EITHER_SIDE; week++) {
    out.push(addIsoDays(centre, -7 * week), addIsoDays(centre, 7 * week));
  }
  return out;
}

/** The day last year carrying one of `names`, nearest 52 weeks back. */
function sameHoliday(
  date: string,
  names: readonly string[],
  facts: ReadonlyMap<string, LastYearDayFacts>,
  measured: (day: string) => boolean,
): string | undefined {
  const centre = lastYearDays(date).sameWeekday;
  for (let off = 0; off <= LAST_YEAR_HOLIDAY_REACH_DAYS; off++) {
    for (const day of new Set([
      addIsoDays(centre, -off),
      addIsoDays(centre, off),
    ])) {
      if (!measured(day)) continue;
      if (facts.get(day)?.holidayNames?.some((n) => names.includes(n))) {
        return day;
      }
    }
  }
  return undefined;
}

/**
 * For each stated season running on `date`, whether a day lies in its edition
 * a year earlier: the same kind and name, ended before this one began. The
 * nights a season lists are compared with the nights listed the year before
 * only; a season without its list says every day of its range, and matching
 * that to last year's nights would put every day on an event night.
 */
function lastEditionTests(
  date: string,
  seasons: readonly LastYearSeason[],
): Array<(day: string) => boolean> {
  const tests: Array<(day: string) => boolean> = [];
  for (const season of seasons) {
    if (!isStatement(season) || !seasonCovers(season, date)) continue;
    for (const edition of seasons) {
      if (
        edition.kind !== season.kind ||
        edition.name !== season.name ||
        edition.status === "cancelled" ||
        edition.endDate >= season.startDate
      ) {
        continue;
      }
      tests.push(
        season.dates?.length && edition.dates?.length
          ? (day) => seasonCovers(edition, day)
          : (day) => edition.startDate <= day && day <= edition.endDate,
      );
    }
  }
  return tests;
}

/**
 * The day a year back that stands for `date`:
 *
 * 1. a public holiday reads the same holiday last year, by name, the one
 *    nearest 52 weeks back;
 * 2. a bridge day or a school-break day reads the day of the same weekday
 *    nearest 52 weeks back, at most three weeks off, that was one too;
 * 3. failing that, and on every other day, 52 weeks back where that was
 *    neither a holiday, a bridge day nor a break day, else the nearest such day
 *    of the same weekday at most three weeks off; failing that, 52 weeks back.
 *
 * A day whose school break is not known (`schoolVacationUnknown`) tells no
 * break day from a plain one a year back. Inside a stated season with an
 * edition a year earlier, a day of that edition goes first among the days
 * found in step 2 or 3. A day without facts reads 52 weeks back.
 */
export function lastYearReferenceDay(
  date: string,
  facts: ReadonlyMap<string, LastYearDayFacts>,
  options: LastYearReferenceOptions = {},
): string {
  const plain = lastYearDays(date).sameWeekday;
  const day = facts.get(date);
  if (!day) return plain;
  const { before } = options;
  const measured = (d: string) => !before || d < before;

  if (day.holidayNames?.length) {
    const same = sameHoliday(date, day.holidayNames, facts, measured);
    if (same) return same;
  }

  const inEdition = lastEditionTests(date, options.seasons ?? []);
  const candidates = sameWeekdays(date).filter(measured);
  const schoolKnown = !day.schoolVacationUnknown;
  for (const wanted of READS[kindOf(day, true)]) {
    const found = candidates.filter((d) => {
      const f = facts.get(d);
      return !!f && kindOf(f, schoolKnown) === wanted;
    });
    if (found.length > 0) {
      return found.find((d) => inEdition.some((test) => test(d))) ?? found[0];
    }
  }
  return plain;
}

/** {@link lastYearReferenceDay} for each of `dates`. */
export function lastYearReferenceDays(
  dates: readonly string[],
  facts: ReadonlyMap<string, LastYearDayFacts>,
  options: LastYearReferenceOptions = {},
): Map<string, string> {
  return new Map(
    dates.map((date) => [date, lastYearReferenceDay(date, facts, options)]),
  );
}

/**
 * The days whose facts {@link lastYearReferenceDay} reads for the days `from`
 * to `to`: those days, and the year back as far as a reference day can sit.
 */
export function lastYearFactDays(from: string, to: string): string[] {
  const reach = Math.max(
    LAST_YEAR_HOLIDAY_REACH_DAYS,
    7 * LAST_YEAR_WEEKS_EITHER_SIDE,
  );
  const back = eachIsoDate(
    addIsoDays(lastYearDays(from).sameWeekday, -reach),
    addIsoDays(lastYearDays(to).sameWeekday, reach),
  );
  return [...new Set([...back, ...eachIsoDate(from, to)])].sort();
}

/**
 * The first day a park's seasons must be read from for days from `from` on:
 * the earliest a reference day can sit, so last year's editions are among them.
 */
export function lastYearSeasonsFrom(from: string): string {
  return addIsoDays(
    lastYearDays(from).sameWeekday,
    -7 * LAST_YEAR_WEEKS_EITHER_SIDE,
  );
}

/**
 * The facts of `days` by the calendar's own rule (`dayHolidayFlags`), with
 * every public holiday's name and the break left unknown past `schoolThrough`
 * (`schoolCoverageThrough`; null when no school break is stored for the park).
 * `holidays` must reach three days past both ends of `days`.
 */
export function lastYearDayFacts(
  days: string[],
  holidays: HolidayRow[],
  park: HolidayLocation,
  schoolThrough: string | null,
): Map<string, LastYearDayFacts> {
  const flags = dayHolidayFlags(days, holidays, park);
  const names = localPublicHolidayNames(holidays, park);
  const rows = days.map((date): LastYearDayFacts & { date: string } => {
    const f = flags.get(date);
    const holidayNames = f?.isHoliday ? names.get(date) : undefined;
    return {
      date,
      isHoliday: !!f?.isHoliday,
      isBridgeDay: !!f?.isBridgeDay,
      isSchoolVacation: !!f?.isSchoolVacation,
      ...(holidayNames && { holidayNames }),
    };
  });
  markUnknownSchoolVacation(rows, schoolThrough);
  return new Map(rows.map(({ date, ...facts }) => [date, facts]));
}

/**
 * One park's facts for the days `from` to `to` from the holiday table. Null
 * for a park without a country, whose days read 52 weeks back. A failed
 * holiday read throws; without the school coverage every day outside a stored
 * break is unknown.
 */
export async function loadLastYearDayFacts(
  holidays: Pick<HolidaysService, "getHolidays" | "getSchoolHolidayCoverage">,
  park: {
    countryCode: string | null;
    regionCode: string | null;
    timezone: string;
  },
  from: string,
  to: string,
): Promise<Map<string, LastYearDayFacts> | null> {
  if (!park.countryCode) return null;
  const days = lastYearFactDays(from, to);
  const location: HolidayLocation = {
    countryCode: park.countryCode,
    regionCode: park.regionCode ?? null,
    timezone: park.timezone,
  };
  const [rows, coverage] = await Promise.all([
    holidays.getHolidays(
      location.countryCode,
      addIsoDays(days[0], -3),
      addIsoDays(days[days.length - 1], 3),
    ),
    holidays.getSchoolHolidayCoverage().catch(() => null),
  ]);
  const schoolThrough = coverage
    ? schoolCoverageThrough(coverage, location.countryCode, location.regionCode)
    : null;
  return lastYearDayFacts(days, rows, location, schoolThrough);
}
