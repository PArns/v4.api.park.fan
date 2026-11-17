import { roundServedWait } from "../pcn-serving.constants";
import { PARK_SEASON_BINDING_KINDS } from "../../parks/entities/park-season.entity";
import {
  dayHolidayFlags,
  type HolidayLocation,
} from "../../common/utils/holiday.utils";
import { isCountryWeekendDate } from "../../date-features/constants/weekend-rules.constant";
import type { HolidayRow } from "../../holidays/holidays.service";

/**
 * The season level (PAR-1343): on a day of a park season that is running, a
 * ride's served daily level is what the ride measured on the days of that
 * season so far, not what a model that has never heard of the season says.
 *
 * Why it exists. No model reads `park_seasons`. On 2026-11-14, the first day
 * of Phantasialand's Wintertraum, the calendar read "very low" for every day
 * to 18 December with a headliner peak of 10–25 minutes; the day itself
 * averaged 35. The models had a closed park behind them and nothing that said
 * a season had begun.
 *
 * What it is: the weekday average, restricted to the season. The reference is
 * the median of the ride's measured daily P90 (TFT's own target statistic)
 * over the season's days already observed of the same day type, once that has
 * {@link MIN_SEASON_DAYS} days. Fewer than that and the model's number stays.
 *
 * The two day types are a day off and a working day ({@link seasonDayTypes}):
 * the country's weekend, a public holiday, a school-break day and a bridge
 * day are days off. A Tuesday of the Christmas break queues like a Saturday,
 * not like a Tuesday in November. A weekday past the park's school-holiday
 * data is neither: it could be a break day.
 *
 * Never all of the season's days together: a season's first days are mostly
 * its opening weekend, so their median is a day off's, and it would set every
 * working day of the season to it.
 *
 * shortcut: a season's first days have no reference, because only the running
 * edition is read; read the previous edition of the same season once past
 * editions are curated.
 */
export const MIN_SEASON_DAYS = 3;

/**
 * The kinds that count and the statuses that are facts. `expected` is a
 * projection from last year and `cancelled` is not happening.
 */
export const SEASON_LEVEL_KINDS = PARK_SEASON_BINDING_KINDS;
export const SEASON_LEVEL_STATUSES = ["confirmed", "announced"] as const;

export interface RunningSeason {
  id: string;
  /** Park-local `YYYY-MM-DD`, both inclusive. */
  startDate: string;
  endDate: string;
  /** The season's own days when it lists them; otherwise every day in range. */
  dates: string[] | null;
}

export interface SeasonReference {
  seasonId: string;
  attractionId: string;
  /** Measured on days off (true) or on working days (false), {@link seasonDayTypes}. */
  dayOff: boolean;
  /** Observed days behind `level`. */
  days: number;
  level: number;
}

/** What a row the season level replaced carries after its model's own label. */
export const SEASON_LEVEL_SUFFIX = "+season";

/** Where a served daily level came from: {@link servedLevelSource}. */
export type ServedLevelSource = "tft" | "catboost" | "season";

/**
 * Which source a served daily row came from, for the readers that keep the
 * served level's error apart per source (lead snapshots, forward archive) and
 * for `/plan/day`'s H5 composer, which routes each source its own way. A
 * season-level row is its own source: filed under the model it replaced, it
 * would be scored as that model, and a `modelVersion === "tft"` test misses
 * it (`tft+season`).
 */
export function servedLevelSource(
  modelVersion: string | null | undefined,
): ServedLevelSource {
  if (modelVersion?.endsWith(SEASON_LEVEL_SUFFIX)) return "season";
  return modelVersion === "tft" ? "tft" : "catboost";
}

/** The first running season that counts `day`, or undefined. */
export function seasonOn(
  seasons: readonly RunningSeason[],
  day: string,
): RunningSeason | undefined {
  return seasons.find((s) =>
    s.dates && s.dates.length > 0
      ? s.dates.includes(day)
      : day >= s.startDate && day <= s.endDate,
  );
}

/** A season's days by type; every day in neither list is a working day. */
export interface SeasonDayTypes {
  daysOff: string[];
  /**
   * Weekdays past the park's school-holiday data that no other rule makes a
   * day off: a break there is not known (PAR-1049), so the day is neither
   * measured into a reference nor served one.
   */
  unknownDays: string[];
}

/**
 * The types of `dates` (park-local `YYYY-MM-DD`). Days off are the country's
 * weekend (`getWeekendDaysForCountry`) and every public holiday, school-break
 * day and bridge day by the calendar's own rule ({@link dayHolidayFlags}), so
 * a season day is a day off exactly where the calendar marks one. `holidays`
 * must reach three days past both ends of `dates`, as `dayHolidayFlags` reads
 * them. `schoolThrough` is the last day the park's school breaks are stored
 * for (`schoolCoverageThrough`), null when they are stored for none.
 */
export function seasonDayTypes(
  dates: readonly string[],
  holidays: HolidayRow[],
  park: HolidayLocation,
  schoolThrough: string | null,
): SeasonDayTypes {
  const flags = dayHolidayFlags([...dates], holidays, park);
  const types: SeasonDayTypes = { daysOff: [], unknownDays: [] };
  for (const day of dates) {
    const flag = flags.get(day);
    if (
      isCountryWeekendDate(day, park.countryCode) ||
      (!!flag && (flag.isHoliday || flag.isSchoolVacation || flag.isBridgeDay))
    ) {
      types.daysOff.push(day);
    } else if (schoolThrough === null || day > schoolThrough) {
      types.unknownDays.push(day);
    }
  }
  return types;
}

const referenceKey = (
  seasonId: string,
  attractionId: string,
  dayOff: boolean,
) => `${seasonId}|${attractionId}|${dayOff}`;

/**
 * `predictions` with the season level on the rows it applies to; every other
 * row is returned as it came.
 *
 * @param dayTypes - The season's days off and unknown days ({@link seasonDayTypes}).
 * @param dayOf - The row's park-local `YYYY-MM-DD`.
 */
export function applySeasonLevel<
  T extends {
    attractionId: string;
    predictedWaitTime: number;
    modelVersion?: string;
    crowdLevel?: unknown;
  },
>(
  predictions: T[],
  seasons: readonly RunningSeason[],
  references: readonly SeasonReference[],
  dayTypes: SeasonDayTypes,
  dayOf: (prediction: T) => string,
): T[] {
  if (seasons.length === 0 || references.length === 0) return predictions;
  const byKey = new Map(
    references
      .filter((r) => r.days >= MIN_SEASON_DAYS)
      .map((r) => [referenceKey(r.seasonId, r.attractionId, r.dayOff), r]),
  );
  const off = new Set(dayTypes.daysOff);
  const unknown = new Set(dayTypes.unknownDays);
  return predictions.map((prediction) => {
    const day = dayOf(prediction);
    const season = seasonOn(seasons, day);
    if (!season || unknown.has(day)) return prediction;
    const reference = byKey.get(
      referenceKey(season.id, prediction.attractionId, off.has(day)),
    );
    if (!reference) return prediction;
    return {
      ...prediction,
      predictedWaitTime: roundServedWait(reference.level),
      // A measured level rates nothing by itself; the calendar rates the day.
      crowdLevel: "unknown",
      modelVersion: `${prediction.modelVersion ?? "catboost"}${SEASON_LEVEL_SUFFIX}`,
    };
  });
}
