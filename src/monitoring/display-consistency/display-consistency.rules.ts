/**
 * Which of the places that show a park-day must agree, and the comparison.
 *
 * A park-day is shown on many surfaces, each with its own read path. Two of
 * them must show the same value only where the code makes them the same
 * statistic; everywhere else they measure different things and are allowed to
 * differ. Every pair below names which, and why. The reasoning per pair is in
 * docs/analytics/display-consistency.md.
 */

/** A place a visitor, or Parky, reads a park-day. */
export type DisplaySurface =
  /** `/calendar` (`includeHourly=none`): the grid, the day dialog, the month page summary. */
  | "calendar"
  /** The same day's `predictedCrowdLevel`, which the frontend rates the cells on. */
  | "calendar_predicted"
  /** The same response's `todayCrowdLevel`: today measured so far. */
  | "calendar_so_far"
  /** `/best-days`, the snapshot the month calendar paints from. */
  | "best_days"
  /** `/plan/day`, the trip planner's day, through its 15-minute cache. */
  | "plan_day"
  /** `/v1/trip-regions/:region/calendar`, a forecast day (`basis` forecast or long_range). */
  | "trip_region"
  /** The region calendar's day where it shows the same weekday a year back. */
  | "trip_region_last_year"
  /** The park payload's live level (`analytics.statistics.crowdLevel`), while it is open. */
  | "park_live"
  /** The live level the park lists read (`park:occupancy:{id}`): discovery, favourites, nearby, search. */
  | "park_lists_live"
  /** Parky's `get_park_live` and the park card: `crowd.level`. */
  | "parky_live"
  /** Parky's `get_park_live` and the park card: `todayExpected`. */
  | "parky_today_expected"
  /** Parky's chat tool `get_days`. */
  | "parky_get_days"
  /** Parky's chat tool `get_park_day`, which builds the day without the `/plan/day` cache. */
  | "parky_get_park_day"
  /** The stamp Parky's day tip would be written with now. */
  | "parky_tip"
  /** The stamp of each tip served now, per locale, from its 12-hour cache. */
  | "parky_tip_served";

/** What a value is. */
export type DisplayMeasure =
  /** A crowd level, `closed` included. */
  | "crowd_level"
  /** The headliners' expected peak wait for the day, in minutes. */
  | "headliner_wait"
  /** One ride's expected peak wait for the day, in minutes; keyed by the ride's id. */
  | "ride_wait"
  /** Parky's stamp: good, meh or bad. */
  | "verdict";

/** One value one surface shows for one park-day. */
export interface SurfaceValue {
  /** The park-local day. */
  date: string;
  surface: DisplaySurface;
  measure: DisplayMeasure;
  /** As the surface serves it; null or `unknown` when it shows none. */
  value: string | number | null;
  /** Tells apart several values of one surface and measure: a ride id, a locale. */
  key?: string;
  /** When the surface computed the value, where it says so. */
  asOf?: string;
}

/** Two surfaces, and whether they must show the same value. */
export interface SurfacePair {
  a: DisplaySurface;
  b: DisplaySurface;
  measure: DisplayMeasure;
  /** `equal`: one statistic, so a difference is a mismatch. `may_differ`: two statistics. */
  rule: "equal" | "may_differ";
  /** For `equal` the one statistic both carry; for `may_differ` what each carries. */
  statistic: string;
  /** A field that ignores the day's status: `closed` on either side is not compared. */
  ignoreClosed?: boolean;
  /** Minutes compared on the calendar's five-minute grid, which only one side rounds to. */
  roundTo5?: boolean;
}

const DAY_LEVEL =
  "the day's forecast level: predicted headliner day peak ÷ typical-day peak (CalendarService.buildPredictedCrowdLevels)";
const HEADLINER_WAIT =
  "the mean predicted headliner day peak, rounded to 5 (CalendarService.buildHeadlinerForecasts, headlinerForecast.avgWait)";

export const SURFACE_PAIRS: readonly SurfacePair[] = [
  {
    a: "calendar",
    b: "calendar_predicted",
    measure: "crowd_level",
    rule: "equal",
    statistic: `${DAY_LEVEL}; crowdLevel is predictedCrowdLevel on a day that is not closed (buildCalendarDay)`,
    ignoreClosed: true,
  },
  {
    a: "calendar",
    b: "best_days",
    measure: "crowd_level",
    rule: "equal",
    statistic: `${DAY_LEVEL}; the snapshot is calendar.days.map(projectDay) (BestDaysService)`,
  },
  {
    a: "calendar",
    b: "plan_day",
    measure: "crowd_level",
    rule: "equal",
    statistic: `${DAY_LEVEL}; context.crowdLevel is the calendar day's (PlanDayService.loadDay)`,
  },
  {
    a: "calendar",
    b: "trip_region",
    measure: "crowd_level",
    rule: "equal",
    statistic: `${DAY_LEVEL}; buildParkDay copies the calendar day's level on a forecast day`,
  },
  {
    a: "calendar",
    b: "parky_get_days",
    measure: "crowd_level",
    rule: "equal",
    statistic: `${DAY_LEVEL}; get_days reads the calendar (calendarDays, compactDay)`,
  },
  {
    a: "calendar",
    b: "parky_get_park_day",
    measure: "crowd_level",
    rule: "equal",
    statistic: `${DAY_LEVEL}; get_park_day reads /plan/day's context (buildPlanDay)`,
  },
  {
    a: "plan_day",
    b: "parky_get_park_day",
    measure: "crowd_level",
    rule: "equal",
    statistic: `${DAY_LEVEL}; both from buildPlanDay, /plan/day through its 15-minute cache`,
  },
  {
    a: "calendar",
    b: "parky_today_expected",
    measure: "crowd_level",
    rule: "equal",
    statistic: `${DAY_LEVEL}; todayExpected reads today's calendar day (parkLiveSummary)`,
  },
  {
    a: "park_live",
    b: "park_lists_live",
    measure: "crowd_level",
    rule: "equal",
    statistic:
      "the live level: Σ latest headliner waits ÷ Σ their P50 (calculateParkOccupancy, crowdLevel); the payload through getParkStatistics' 5-minute cache, the lists through park:occupancy:{id}",
  },
  {
    a: "park_live",
    b: "parky_live",
    measure: "crowd_level",
    rule: "equal",
    statistic:
      "the live level of the park payload; get_park_live reads the same payload (get_park)",
  },
  {
    a: "calendar",
    b: "parky_tip",
    measure: "verdict",
    rule: "equal",
    statistic:
      "Parky's stamp of the day's forecast level (parkyVerdict of the calendar day, dayVerdict)",
  },
  {
    a: "parky_tip",
    b: "parky_tip_served",
    measure: "verdict",
    rule: "equal",
    statistic:
      "Parky's stamp of the day's forecast level; a served tip keeps the stamp it was written with for 12 hours",
  },
  {
    a: "calendar",
    b: "parky_get_days",
    measure: "headliner_wait",
    rule: "equal",
    statistic: `${HEADLINER_WAIT}; get_days' expectedPeakWaitMinutes`,
  },
  {
    a: "calendar",
    b: "parky_today_expected",
    measure: "headliner_wait",
    rule: "equal",
    statistic: `${HEADLINER_WAIT}; todayExpected.expectedPeakWaitMinutes`,
  },
  {
    a: "calendar",
    b: "trip_region",
    measure: "headliner_wait",
    rule: "equal",
    statistic: `${HEADLINER_WAIT}; the region calendar's forecastWait`,
  },
  {
    a: "calendar",
    b: "plan_day",
    measure: "ride_wait",
    rule: "equal",
    statistic:
      "one ride's served daily prediction (MLService.getServingDailyPredictions): the calendar's headliner waitTime, rounded to 5, against /plan/day's dayPeak, unrounded",
    roundTo5: true,
  },
  {
    a: "calendar",
    b: "plan_day",
    measure: "headliner_wait",
    rule: "may_differ",
    statistic:
      "the calendar averages every headliner with a daily prediction; /plan/day's dayPeak mean covers only the headliners it draws a curve for, and takes a curve's maximum where a ride has no daily row",
  },
  {
    a: "calendar",
    b: "park_live",
    measure: "crowd_level",
    rule: "may_differ",
    statistic:
      "the day's forecast against the typical-day peak, and the live level against P50 right now",
  },
  {
    a: "calendar",
    b: "calendar_so_far",
    measure: "crowd_level",
    rule: "may_differ",
    statistic:
      "the whole day's forecast, and today's measured day value so far (calculateCrowdLevelForDate)",
  },
  {
    a: "park_live",
    b: "calendar_so_far",
    measure: "crowd_level",
    rule: "may_differ",
    statistic:
      "the live level against P50 right now, and the day so far against the typical-day peak",
  },
  {
    a: "calendar",
    b: "trip_region_last_year",
    measure: "crowd_level",
    rule: "may_differ",
    statistic:
      "the day's forecast, and the same weekday a year back as measured (basis last_year, PAR-1319)",
  },
];

/** A value of one side of a pair as a report names it. */
export interface PairSide {
  surface: DisplaySurface;
  value: string | number;
  asOf?: string;
}

/** Two surfaces that showed different values for one park-day. */
export interface DisplayDifference {
  date: string;
  measure: DisplayMeasure;
  key?: string;
  a: PairSide;
  b: PairSide;
  statistic: string;
}

export interface DisplayComparison {
  /** Pairs of known values compared, equal or not. */
  compared: number;
  /** Must-be-equal pairs that differ. */
  mismatches: DisplayDifference[];
  /** May-differ pairs that differ, for reading beside the mismatches. */
  differences: DisplayDifference[];
}

/** Whether a value says something: absent, null, `unknown` and a non-number say nothing. */
export function isKnownValue(
  value: SurfaceValue["value"] | undefined,
): value is string | number {
  if (value === null || value === undefined) return false;
  if (typeof value === "number") return Number.isFinite(value);
  return value !== "" && value !== "unknown";
}

function sameValue(
  a: string | number,
  b: string | number,
  roundTo5: boolean,
): boolean {
  if (typeof a === "number" && typeof b === "number") {
    return roundTo5 ? Math.round(a / 5) * 5 === Math.round(b / 5) * 5 : a === b;
  }
  return a === b;
}

/** Keys meet when they are equal, or when one side has none (one value for every key). */
function keysMeet(a?: string, b?: string): boolean {
  return a === undefined || b === undefined || a === b;
}

function side(value: SurfaceValue & { value: string | number }): PairSide {
  return {
    surface: value.surface,
    value: value.value,
    ...(value.asOf && { asOf: value.asOf }),
  };
}

/**
 * Compares every pair on every day both sides have a known value for. An
 * absent or `unknown` value never counts as a mismatch: it is a surface that
 * says nothing, not one that says something else.
 */
export function compareSurfaces(
  values: readonly SurfaceValue[],
  pairs: readonly SurfacePair[] = SURFACE_PAIRS,
): DisplayComparison {
  const known = values.filter(
    (v): v is SurfaceValue & { value: string | number } =>
      isKnownValue(v.value),
  );
  const result: DisplayComparison = {
    compared: 0,
    mismatches: [],
    differences: [],
  };
  for (const pair of pairs) {
    const left = known.filter(
      (v) => v.surface === pair.a && v.measure === pair.measure,
    );
    const right = known.filter(
      (v) => v.surface === pair.b && v.measure === pair.measure,
    );
    for (const a of left) {
      for (const b of right) {
        if (a.date !== b.date || !keysMeet(a.key, b.key)) continue;
        if (pair.ignoreClosed && (a.value === "closed" || b.value === "closed"))
          continue;
        result.compared++;
        if (sameValue(a.value, b.value, pair.roundTo5 === true)) continue;
        const key = a.key ?? b.key;
        const difference: DisplayDifference = {
          date: a.date,
          measure: pair.measure,
          ...(key !== undefined && { key }),
          a: side(a),
          b: side(b),
          statistic: pair.statistic,
        };
        if (pair.rule === "equal") result.mismatches.push(difference);
        else result.differences.push(difference);
      }
    }
  }
  const order = (x: DisplayDifference, y: DisplayDifference) =>
    x.date.localeCompare(y.date);
  result.mismatches.sort(order);
  result.differences.sort(order);
  return result;
}
