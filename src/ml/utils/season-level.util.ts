import { roundServedWait } from "../pcn-serving.constants";
import { PARK_SEASON_BINDING_KINDS } from "../../parks/entities/park-season.entity";
import {
  dayHolidayFlags,
  holidayDay,
  influencingHolidayTest,
  type HolidayLocation,
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
 * which row is served which. A type needs this many measured days of its own,
 * and fewer leave the model's number.
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
export type SeasonDayType = "off" | "eve" | "working";

/**
 * How a season row is served (`SEASON_LEVEL_MODE`). `ratio` scales a TFT row
 * by what the ride measured against TFT's forecasts, which keeps TFT's
 * day-to-day structure (weekday, holidays, weather); `level` serves every row
 * the measured median, one number per type.
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
   * Days behind `ratio`: the type's latest measured days with a TFT forecast
   * of at least {@link MIN_RATIO_FORECAST}, at most {@link SEASON_RECENT_DAYS}.
   */
  ratioDays: number;
  /**
   * Median of the measured P90 over TFT's freshest forecast made before the
   * day; null where no day has one.
   */
  ratio: number | null;
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

/** A season's days by type; every day in none of the lists is a working day. */
export interface SeasonDayTypes {
  daysOff: string[];
  /** Working days whose next calendar day is a day off. */
  eves: string[];
  /**
   * Weekdays past the park's school-holiday data that no other rule makes a
   * day off, and the working day before one: a break there is not known
   * (PAR-1049), so the day is neither measured into a reference nor served one.
   */
  unknownDays: string[];
}

/** The park fields {@link seasonDayTypes} reads. */
export type SeasonPark = HolidayLocation & {
  influencingRegions?: readonly RegionInfluence[] | null;
};

/** A neighbour's row that makes a day off: a public holiday or a school break. */
const NEIGHBOUR_DAY_OFF_TYPES: ReadonlySet<HolidayRow["holidayType"]> = new Set(
  ["public", "bank", "school"],
);

/**
 * The types of `dates` (park-local `YYYY-MM-DD`).
 *
 * - A **day off** is the country's weekend (`getWeekendDaysForCountry`), a
 *   public holiday, school-break day or bridge day by the calendar's own rule
 *   ({@link dayHolidayFlags}), or a public holiday or school-break day in one
 *   of the park's influencing regions ({@link influencingHolidayTest}): NL and
 *   BE carry Phantasialand, and TFT and CatBoost read their holidays too.
 * - An **eve** is a working day whose next calendar day is a day off: Friday
 *   before a Saturday–Sunday weekend, Thursday before a Friday–Saturday one,
 *   the day before a holiday or a break. Evening-driven seasons queue higher
 *   on it than on the rest of the week.
 * - A **working day** is every other day up to `schoolThrough`, the last day
 *   the park's own school breaks are stored for (`schoolCoverageThrough`, null
 *   for none). A weekday past it is unknown, and so is a working day before
 *   one, whose type depends on that unknown day.
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
  schoolThrough: string | null,
): SeasonDayTypes {
  const withNextDays = [
    ...new Set(dates.flatMap((day) => [day, addIsoDays(day, 1)])),
  ];
  const flags = dayHolidayFlags(withNextDays, holidays, park);
  const isNeighbours = influencingHolidayTest(park.influencingRegions ?? []);
  const neighbourDaysOff = new Set(
    holidays
      .filter((h) => NEIGHBOUR_DAY_OFF_TYPES.has(h.holidayType))
      .filter(isNeighbours)
      .map(holidayDay),
  );
  const kindOf = (day: string): "off" | "unknown" | "working" => {
    const flag = flags.get(day);
    if (
      isCountryWeekendDate(day, park.countryCode) ||
      neighbourDaysOff.has(day) ||
      (!!flag && (flag.isHoliday || flag.isSchoolVacation || flag.isBridgeDay))
    ) {
      return "off";
    }
    return schoolThrough === null || day > schoolThrough
      ? "unknown"
      : "working";
  };

  const types: SeasonDayTypes = { daysOff: [], eves: [], unknownDays: [] };
  for (const day of dates) {
    const kind = kindOf(day);
    if (kind === "off") {
      types.daysOff.push(day);
      continue;
    }
    if (kind === "unknown") {
      types.unknownDays.push(day);
      continue;
    }
    const next = kindOf(addIsoDays(day, 1));
    if (next === "off") types.eves.push(day);
    else if (next === "unknown") types.unknownDays.push(day);
  }
  return types;
}

/** A day's type by {@link SeasonDayTypes}, as a lookup; null for an unknown day. */
function seasonDayTypeOf(
  dayTypes: SeasonDayTypes,
): (day: string) => SeasonDayType | null {
  const off = new Set(dayTypes.daysOff);
  const eves = new Set(dayTypes.eves);
  const unknown = new Set(dayTypes.unknownDays);
  return (day) =>
    off.has(day)
      ? "off"
      : eves.has(day)
        ? "eve"
        : unknown.has(day)
          ? null
          : "working";
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
    const ratios = latest
      .flatMap((m) =>
        m.forecast !== null && m.forecast >= MIN_RATIO_FORECAST
          ? [m.peak / m.forecast]
          : [],
      )
      .slice(0, SEASON_RECENT_DAYS);
    return {
      ...key,
      days: levelDays.length,
      level: median(levelDays.map((m) => m.peak)) ?? 0,
      ratioDays: ratios.length,
      ratio: median(ratios) ?? null,
    };
  });
}

/**
 * `predictions` with the season level on the rows it applies to; every other
 * row is returned as it came.
 *
 * In `ratio` mode a TFT row is served its own number times the reference's
 * ratio, once the ratio has {@link MIN_SEASON_DAYS} days. Every other row is
 * served the reference's level: a CatBoost row (days 61+, rides TFT has no row
 * for), whose number the ratio was not measured against; a TFT row whose ratio
 * is short of days; every row in `level` mode. Only the reference of the day's
 * own type is read.
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
  return predictions.map((prediction) => {
    const day = dayOf(prediction);
    const season = seasonOn(seasons, day);
    const dayType = season ? typeOf(day) : null;
    if (!season || !dayType) return prediction;
    const reference = byKey.get(
      referenceKey(season.id, prediction.attractionId, dayType),
    );
    if (!reference) return prediction;
    const ratio =
      mode === "ratio" &&
      servedLevelSource(prediction.modelVersion) === "tft" &&
      reference.ratioDays >= MIN_SEASON_DAYS
        ? reference.ratio
        : null;
    return {
      ...prediction,
      predictedWaitTime: roundServedWait(
        ratio !== null ? prediction.predictedWaitTime * ratio : reference.level,
      ),
      // A measured level rates nothing by itself; the calendar rates the day.
      crowdLevel: "unknown",
      modelVersion: `${prediction.modelVersion ?? "catboost"}${SEASON_LEVEL_SUFFIX}`,
    };
  });
}
