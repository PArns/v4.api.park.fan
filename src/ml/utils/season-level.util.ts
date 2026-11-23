import { roundServedWait } from "../pcn-serving.constants";
import { PARK_SEASON_BINDING_KINDS } from "../../parks/entities/park-season.entity";
import {
  dayHolidayFlags,
  holidayDay,
  influencingHolidayTest,
  schoolCoverageThrough,
  type HolidayLocation,
  type SchoolCoverage,
} from "../../common/utils/holiday.utils";
import type { RegionInfluence } from "../../common/region-influences";
import { addIsoDays } from "../../common/utils/date.util";
import { median } from "../../common/utils/stats.util";
import { isCountryWeekendDate } from "../../date-features/constants/weekend-rules.constant";
import type { HolidayRow } from "../../holidays/holidays.service";

/**
 * The season level (PAR-1343): on a day of a park season that is running, a
 * ride's served daily level follows what the ride measured on the days of that
 * season so far, not what a model that has never heard of the season says.
 *
 * Why it exists. No model reads `park_seasons`. On 2026-11-14, the first day
 * of Phantasialand's Wintertraum, the calendar read "very low" for every day
 * to 18 December with a headliner peak of 10–25 minutes; the day itself
 * averaged 35. The models had a closed park behind them and nothing that said
 * a season had begun.
 *
 * What it is. Every season day has a type ({@link seasonDayTypes}). Per ride
 * and type, the type's most recent measured days give a level, the median of
 * the measured daily P90 (TFT's own target statistic), and a ratio, the median
 * of that P90 over TFT's forecast of the day; {@link applySeasonLevel} says
 * which row is served which. A type needs this many measured days of its own;
 * with fewer, the row is the model's number, raised at most to a quieter
 * type's level ({@link SEASON_FLOOR_TYPES}).
 *
 * Never all of the season's days together: a season's first days are mostly
 * its opening weekend, so their median is a day off's, and it would set every
 * working day of the season to it (PAR-1375).
 *
 * shortcut: a season's first days have no reference, because only the running
 * edition is read; read the previous edition of the same season once past
 * editions are curated.
 */
export const MIN_SEASON_DAYS = 3;

/**
 * How many of a type's measured days a reference reads, the latest first. A
 * median over all of them held a season that builds down: Wintertraum's
 * November days would have outvoted its Advent days for weeks.
 */
export const SEASON_RECENT_DAYS = 6;

/**
 * The smallest TFT forecast a ratio divides by: TFT's panel drops waits under
 * 5 minutes (`NF_MIN_WAIT`), so a forecast below it says "quiet", and a ratio
 * over it would multiply that noise into every served day.
 */
export const MIN_RATIO_FORECAST = 5;

/**
 * The bounds a served ratio is held to. The ratio is measured on forecasts
 * TFT made before it had seen the season and applied to its later, informed
 * runs, and nothing has scored it: unbounded, a ride TFT forecast at 6 that
 * measured 30 carried 5 into every day of its type, 160 minutes on a Saturday
 * TFT put at 32.
 */
export const SEASON_RATIO_MIN = 0.5;
export const SEASON_RATIO_MAX = 3;

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

/** A season day's type, {@link seasonDayTypes}. */
export type SeasonDayType = "off" | "neighbour" | "eve" | "working";

/**
 * The types that floor a type without a reference of its own: its row is
 * raised to the highest of their levels, never lowered. Each queues below the
 * type it floors, so a floor says "at least this", never "like this", and no
 * day is served a busier type's level.
 */
export const SEASON_FLOOR_TYPES: Readonly<
  Record<SeasonDayType, readonly SeasonDayType[]>
> = {
  off: ["eve", "neighbour", "working"],
  neighbour: ["working"],
  eve: ["working"],
  working: [],
};

/**
 * How a season row is served (`SEASON_LEVEL_MODE`). `level`, the default,
 * serves every row the measured median, one number per type; `ratio` scales a
 * TFT row by what the ride measured against TFT's forecasts, held to
 * {@link SEASON_RATIO_MIN}–{@link SEASON_RATIO_MAX} and to the highest P90 the
 * type measured.
 */
export type SeasonLevelMode = "ratio" | "level";

export interface SeasonReference {
  seasonId: string;
  attractionId: string;
  dayType: SeasonDayType;
  /** Measured days behind `level`: the type's latest, at most {@link SEASON_RECENT_DAYS}. */
  days: number;
  level: number;
  /**
   * The highest measured P90 among the days behind `level` and `ratio`; a
   * ratio row is served no more.
   */
  maxPeak: number;
  /**
   * Days behind `ratio`: the type's latest measured days with a TFT forecast
   * of at least {@link MIN_RATIO_FORECAST}, at most {@link SEASON_RECENT_DAYS}.
   */
  ratioDays: number;
  /**
   * Median of the measured P90 over TFT's freshest forecast made before the
   * day, as measured: the bounds apply where it is served. Null where no day
   * has one.
   */
  ratio: number | null;
}

/**
 * What a row served a measured level carries after its model's own label:
 * its type's level, or a quieter type's as a floor.
 */
export const SEASON_LEVEL_SUFFIX = "+season";

/**
 * What a TFT row scaled by the season ratio carries instead. It is a forecast
 * times a ratio, not a measured level, so `/plan/day` gates it like TFT
 * (`h5LevelFor`) and the evaluation keeps it apart.
 */
export const SEASON_RATIO_SUFFIX = "+season-ratio";

/** Where a served daily level came from: {@link servedLevelSource}. */
export type ServedLevelSource = "tft" | "catboost" | "season";

/**
 * Which source a served daily row came from, for the readers that keep the
 * served level's error apart per source (lead snapshots, forward archive) and
 * for `/plan/day`'s H5 composer, which routes each source its own way. A
 * season row is its own source, a ratio row included: filed under the model it
 * replaced, it would be scored as that model, and a `modelVersion === "tft"`
 * test misses it (`tft+season`). Which kind of season row:
 * {@link isSeasonRatioRow}.
 */
export function servedLevelSource(
  modelVersion: string | null | undefined,
): ServedLevelSource {
  if (
    modelVersion?.endsWith(SEASON_LEVEL_SUFFIX) ||
    isSeasonRatioRow(modelVersion)
  ) {
    return "season";
  }
  return modelVersion === "tft" ? "tft" : "catboost";
}

/** Whether a served daily row is TFT's number scaled by the season ratio. */
export function isSeasonRatioRow(
  modelVersion: string | null | undefined,
): boolean {
  return modelVersion?.endsWith(SEASON_RATIO_SUFFIX) ?? false;
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

/** A season's days by type; every day in none of the lists is a working day. */
export interface SeasonDayTypes {
  daysOff: string[];
  /**
   * Working days on which only an influencing region has a public holiday or
   * a school break.
   */
  neighbourDays: string[];
  /** Working days whose next calendar day is one of the park's own days off. */
  eves: string[];
  /**
   * Days whose type rests on a school break nobody has stored yet (PAR-1049;
   * {@link seasonDayTypes}): measured into no reference and served none.
   */
  unknownDays: string[];
}

/** The park fields {@link seasonDayTypes} reads. */
export type SeasonPark = HolidayLocation & {
  influencingRegions?: readonly RegionInfluence[] | null;
};

/** How far the school breaks behind a park's day types are stored. */
export interface SeasonSchoolCoverage {
  /** The park's own region (`schoolCoverageThrough`); null for none. */
  park: string | null;
  /**
   * The earliest edge among the influencing regions whose stored school
   * breaks reach the park's today (`schoolCoverageThrough` each); null when
   * none does.
   */
  neighbours: string | null;
}

/**
 * {@link SeasonSchoolCoverage} of a park on its park-local `today`. An
 * influencing region whose stored school breaks end before today is left out,
 * as one that stores none: counted, its edge would make every working day of
 * every running season unknown, as Mexico's (2026-07-15) did at the US parks
 * it influences. Its stored breaks still make neighbour days; one it has not
 * published reads as no break.
 */
export function seasonSchoolCoverage(
  coverage: SchoolCoverage,
  park: Pick<SeasonPark, "countryCode" | "regionCode" | "influencingRegions">,
  today: string,
): SeasonSchoolCoverage {
  const neighbours = (park.influencingRegions ?? [])
    .map((r) => schoolCoverageThrough(coverage, r.countryCode, r.regionCode))
    .filter(
      (through): through is string => through !== null && through >= today,
    )
    .sort();
  return {
    park: park.countryCode
      ? schoolCoverageThrough(coverage, park.countryCode, park.regionCode)
      : null,
    neighbours: neighbours[0] ?? null,
  };
}

/** A neighbour's row that makes a neighbour day: a public holiday or a school break. */
const NEIGHBOUR_DAY_TYPES: ReadonlySet<HolidayRow["holidayType"]> = new Set([
  "public",
  "bank",
  "school",
]);

/**
 * The types of `dates` (park-local `YYYY-MM-DD`), the first that matches:
 *
 * 1. A **day off** is the park's own: the country's weekend
 *    (`getWeekendDaysForCountry`), or a public holiday, school-break day or
 *    bridge day of its own region by the calendar's rule
 *    ({@link dayHolidayFlags}).
 * 2. A **neighbour day** has a public holiday or a school break in one of the
 *    park's influencing regions only ({@link influencingHolidayTest}). It
 *    lifts a day by about a fifth (docs/ml/level-drivers.md §3), not to a
 *    weekend's level, and at Phantasialand 25 of the 30 weekdays from
 *    28 September to 6 November 2026 have an own or a neighbour's break:
 *    pooled with the days off, a Halloween season there would have one type.
 * 3. An **eve** is a working day whose next calendar day is an own day off:
 *    Friday before a Saturday–Sunday weekend, Thursday before a
 *    Friday–Saturday one, the day before a holiday or a break.
 * 4. A **working day** is every other day.
 *
 * A day whose type rests on a break nobody has stored is **unknown**: past
 * the park's own school data (`coverage.park`) and not a day off anyway; past
 * an influencing region's (`coverage.neighbours`) with no stored neighbour row
 * that makes it a neighbour day; and the working day before a day past the
 * park's own data, whose eve depends on it.
 *
 * `holidays` holds the rows of the park's country and of every influencing
 * country, from three days before the first date to four past the last:
 * `dayHolidayFlags` reads three days either side of a day, and an eve reads
 * the day after.
 */
export function seasonDayTypes(
  dates: readonly string[],
  holidays: HolidayRow[],
  park: SeasonPark,
  coverage: SeasonSchoolCoverage,
): SeasonDayTypes {
  const withNextDays = [
    ...new Set(dates.flatMap((day) => [day, addIsoDays(day, 1)])),
  ];
  const flags = dayHolidayFlags(withNextDays, holidays, park);
  const isNeighbours = influencingHolidayTest(park.influencingRegions ?? []);
  const neighbourHolidays = new Set(
    holidays
      .filter((h) => NEIGHBOUR_DAY_TYPES.has(h.holidayType))
      .filter(isNeighbours)
      .map(holidayDay),
  );
  const ownDayOff = (day: string): boolean => {
    const flag = flags.get(day);
    return (
      isCountryWeekendDate(day, park.countryCode) ||
      (!!flag && (flag.isHoliday || flag.isSchoolVacation || flag.isBridgeDay))
    );
  };
  const pastOwnData = (day: string): boolean =>
    coverage.park === null || day > coverage.park;

  const types: SeasonDayTypes = {
    daysOff: [],
    neighbourDays: [],
    eves: [],
    unknownDays: [],
  };
  for (const day of dates) {
    const next = addIsoDays(day, 1);
    if (ownDayOff(day)) types.daysOff.push(day);
    else if (pastOwnData(day)) types.unknownDays.push(day);
    else if (neighbourHolidays.has(day)) types.neighbourDays.push(day);
    else if (coverage.neighbours !== null && day > coverage.neighbours) {
      types.unknownDays.push(day);
    } else if (ownDayOff(next)) types.eves.push(day);
    else if (pastOwnData(next)) types.unknownDays.push(day);
  }
  return types;
}

/** A day's type by {@link SeasonDayTypes}, as a lookup; null for an unknown day. */
function seasonDayTypeOf(
  dayTypes: SeasonDayTypes,
): (day: string) => SeasonDayType | null {
  const byDay = new Map<string, SeasonDayType | null>([
    ...dayTypes.unknownDays.map((day) => [day, null] as const),
    ...dayTypes.eves.map((day) => [day, "eve"] as const),
    ...dayTypes.neighbourDays.map((day) => [day, "neighbour"] as const),
    ...dayTypes.daysOff.map((day) => [day, "off"] as const),
  ]);
  return (day) => {
    const type = byDay.get(day);
    return type === undefined ? "working" : type;
  };
}

const referenceKey = (
  seasonId: string,
  attractionId: string,
  dayType: SeasonDayType,
) => `${seasonId}|${attractionId}|${dayType}`;

/** A ride's measured day of a running season, as `loadSeasonLevelInputs` reads it. */
export interface MeasuredSeasonDay {
  seasonId: string;
  attractionId: string;
  /** Park-local `YYYY-MM-DD`. */
  day: string;
  /** The day's measured P90. */
  peak: number;
  /** TFT's freshest forecast of the day made before it; null without one. */
  forecast: number | null;
}

/**
 * The references of `measured`, per season, ride and day type. A day of
 * unknown type counts for none. The level is the median peak of the type's
 * latest {@link SEASON_RECENT_DAYS} days; the ratio is the median of peak ÷
 * forecast over the type's latest {@link SEASON_RECENT_DAYS} days with a
 * forecast of at least {@link MIN_RATIO_FORECAST}, so a day TFT forecast
 * nothing for, or almost nothing, is passed over rather than counted.
 */
export function seasonReferences(
  measured: readonly MeasuredSeasonDay[],
  dayTypes: SeasonDayTypes,
): SeasonReference[] {
  const typeOf = seasonDayTypeOf(dayTypes);
  const groups = new Map<
    string,
    Pick<SeasonReference, "seasonId" | "attractionId" | "dayType"> & {
      days: MeasuredSeasonDay[];
    }
  >();
  for (const m of measured) {
    const dayType = typeOf(m.day);
    if (!dayType) continue;
    const key = referenceKey(m.seasonId, m.attractionId, dayType);
    const group = groups.get(key) ?? {
      seasonId: m.seasonId,
      attractionId: m.attractionId,
      dayType,
      days: [],
    };
    group.days.push(m);
    groups.set(key, group);
  }
  return [...groups.values()].map(({ days, ...key }) => {
    const latest = [...days].sort((a, b) => b.day.localeCompare(a.day));
    const levelDays = latest.slice(0, SEASON_RECENT_DAYS);
    const ratioDays = latest
      .filter((m) => m.forecast !== null && m.forecast >= MIN_RATIO_FORECAST)
      .slice(0, SEASON_RECENT_DAYS);
    return {
      ...key,
      days: levelDays.length,
      level: median(levelDays.map((m) => m.peak)) ?? 0,
      maxPeak: Math.max(...[...levelDays, ...ratioDays].map((m) => m.peak)),
      ratioDays: ratioDays.length,
      ratio: median(ratioDays.map((m) => m.peak / m.forecast!)) ?? null,
    };
  });
}

/**
 * `predictions` with the season level on the rows it applies to; every other
 * row is returned as it came. Only references of the day's own season and
 * ride with {@link MIN_SEASON_DAYS} days are read.
 *
 * - **The day's type has a reference:** the row is served its level
 *   (`+season`). In `ratio` mode a TFT row whose ratio has
 *   {@link MIN_SEASON_DAYS} days is served its own number × the ratio
 *   instead, the ratio held to {@link SEASON_RATIO_MIN}–{@link SEASON_RATIO_MAX}
 *   and the product to the reference's `maxPeak` (`+season-ratio`). A
 *   CatBoost row (days 61+, rides TFT has no row for) gets the level in either
 *   mode: its number was never measured against.
 * - **It has none:** the row is raised to the highest level of the types
 *   that floor it ({@link SEASON_FLOOR_TYPES}) where that is above the
 *   model's number (`+season`), and stays the model's otherwise.
 * - **An unknown day** stays the model's number.
 *
 * A replaced row has no band and no confidence: both describe the model's own
 * number, and nothing has measured the one served in its place.
 *
 * @param dayTypes - The season's day types ({@link seasonDayTypes}).
 * @param dayOf - The row's park-local `YYYY-MM-DD`.
 */
export function applySeasonLevel<
  T extends {
    attractionId: string;
    predictedWaitTime: number;
    modelVersion?: string;
    crowdLevel?: unknown;
    confidence?: number | null;
    uncertaintyMinutes?: number | null;
  },
>(
  predictions: T[],
  seasons: readonly RunningSeason[],
  references: readonly SeasonReference[],
  dayTypes: SeasonDayTypes,
  dayOf: (prediction: T) => string,
  mode: SeasonLevelMode,
): T[] {
  if (seasons.length === 0 || references.length === 0) return predictions;
  const byKey = new Map(
    references
      .filter((r) => r.days >= MIN_SEASON_DAYS)
      .map((r) => [referenceKey(r.seasonId, r.attractionId, r.dayType), r]),
  );
  const typeOf = seasonDayTypeOf(dayTypes);
  const replaced = (prediction: T, wait: number, suffix: string): T => ({
    ...prediction,
    predictedWaitTime: roundServedWait(wait),
    // A measured level rates nothing by itself; the calendar rates the day.
    crowdLevel: "unknown",
    uncertaintyMinutes: null,
    confidence: null,
    modelVersion: `${prediction.modelVersion ?? "catboost"}${suffix}`,
  });
  return predictions.map((prediction) => {
    const day = dayOf(prediction);
    const season = seasonOn(seasons, day);
    const dayType = season ? typeOf(day) : null;
    if (!season || !dayType) return prediction;
    const referenceOf = (type: SeasonDayType) =>
      byKey.get(referenceKey(season.id, prediction.attractionId, type));

    const own = referenceOf(dayType);
    if (own) {
      if (
        mode === "ratio" &&
        servedLevelSource(prediction.modelVersion) === "tft" &&
        own.ratio !== null &&
        own.ratioDays >= MIN_SEASON_DAYS
      ) {
        const ratio = Math.min(
          SEASON_RATIO_MAX,
          Math.max(SEASON_RATIO_MIN, own.ratio),
        );
        return replaced(
          prediction,
          Math.min(prediction.predictedWaitTime * ratio, own.maxPeak),
          SEASON_RATIO_SUFFIX,
        );
      }
      return replaced(prediction, own.level, SEASON_LEVEL_SUFFIX);
    }

    const floors = SEASON_FLOOR_TYPES[dayType].flatMap((type) => {
      const reference = referenceOf(type);
      return reference ? [reference.level] : [];
    });
    if (floors.length === 0) return prediction;
    const floor = Math.max(...floors);
    return roundServedWait(floor) > prediction.predictedWaitTime
      ? replaced(prediction, floor, SEASON_LEVEL_SUFFIX)
      : prediction;
  });
}
