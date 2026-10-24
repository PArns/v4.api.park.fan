/**
 * Admin model comparison (PAR-855): the contract of
 *
 * - `GET /v1/admin/model-comparison?days=28&region=ALL|EU|NA|ASIA|OTHER`
 * - `GET /v1/admin/model-comparison/day/options?date=YYYY-MM-DD`
 * - `GET /v1/admin/model-comparison/day?parkId&date&attractionId&origin=morning`
 *
 * The admin frontend (PAR-856) is built against these types; change them only
 * additively. Semantics are in docs/ml/forward-archive.md §9.
 */

export type ModelComparisonGroupId = "live" | "today" | "planner" | "calendar";

export type ModelComparisonState =
  "today" | "better" | "worse" | "unclear" | "pending";

export type ModelComparisonCheck = "yes" | "no" | "running" | "open";

export interface ModelComparisonModelDto {
  id: string;
  /** German column label, e.g. "Wochentag-Durchschnitt". */
  label: string;
  /** One German sentence describing the model. */
  oneLiner: string;
}

/** One lead of a cell (PAR-1036): the per-lead figures the cell's numbers are the mean of. */
export interface ModelComparisonLeadDto {
  /** "h0-1", "d3", … */
  lead: string;
  n: number;
  /** Scored dates reached by each of the 10 best-covered parks at this lead. */
  days: number;
  parks: number;
  diff: number;
  modelMae: number;
  referenceMae: number;
}

export interface ModelComparisonLiveDto {
  /**
   * modelMae − referenceMae on identical slots / ride-days, in `metric.unit`.
   * NEGATIVE = the model is better than what is served. Since PAR-1035 every
   * cell of one question is paired on the SAME rows (where every candidate of
   * the question has a value), so the cells share `referenceMae` lead by lead.
   * Since PAR-1036 a multi-lead cell is the plain mean of its leads.
   */
  diff: number;
  /**
   * 95 % CI of `diff`. Since PAR-1036 (owner decision) the two-way park × date
   * block bootstrap (`ci = "park_date"`): parks AND dates resampled, so a day
   * that moved every park at once counts once. This is the interval the gate
   * reads. Before: park-cluster, now in `parkCiLow`/`parkCiHigh`.
   */
  ciLow: number;
  ciHigh: number;
  /** Which bootstrap `ciLow`/`ciHigh` are; absent = park-cluster (before PAR-1036). */
  ci?: "park_date";
  /** 95 % park-cluster CI (parks resampled), shown beside the gate's interval; null with < 2 parks. */
  parkCiLow?: number | null;
  parkCiHigh?: number | null;
  /** The weakest lead's scored dates (reached by each of 10 parks) and park count; 0 when a lead of the cell has no rows yet. */
  days: number;
  parks: number;
  /** Paired slots (or ride-days for best time / calendar). */
  n: number;
  modelMae: number;
  referenceMae: number;
  /** First scored target date for this model × question (all time, not just the window). */
  since: string | null;
  /** PAR-1036: `equal` = every lead weighs the same in `diff`, `modelMae`, `referenceMae` and the CI. */
  leadWeighting?: "equal";
  /** PAR-1036: per lead. The gate needs every lead of the cell to pass on its own. */
  leads?: ModelComparisonLeadDto[];
}

/**
 * A direct comparison beside the cells (PAR-1035/1037): two candidates on the
 * question's joint rows (Chronos-2 vs H5, H5 + TFT vs H5), or a daily-level
 * model against the weekday average on the ride-days that model levelled
 * (CatBoost, TFT — never the same days, so read them side by side). Not a
 * verdict: no cell state depends on it.
 */
export interface ModelComparisonPairDto {
  /** Model ids of `models`; `diff` = model − reference, negative = `model` better. */
  model: string;
  reference: string;
  /** German, what the comparison covers. */
  note: string;
  live: ModelComparisonLiveDto | null;
  /** Every lead with ≥ 10 parks × ≥ 14 dates. */
  enoughData: boolean;
}

export interface ModelComparisonOfflineDto {
  diff?: number;
  ciLow?: number;
  ciHigh?: number;
  /** Where the verdict comes from, e.g. "ml-bench PR #445 (full period)". */
  source: string;
  /** "YYYY-MM-DD..YYYY-MM-DD" of the offline backtest. */
  period?: string;
  /**
   * What the offline figure was measured against (PAR-1038): `served` = this
   * question's served model, else the bench reference (`persistence`,
   * `wt_med`, `clim`, `chronos2`, `hourly_profile`); null = not recorded.
   */
  reference?: string | null;
  /** `reference === "served"`: only then can an offline win make the cell `better`. */
  referenceIsServed?: boolean;
  /** `slot_mae`, `besttime_regret`, `daypeak_mae`, `hit_rate`; null = not recorded. */
  metric?: string | null;
  /** The ml-bench results directory of the run; null = not recorded. */
  runId?: string | null;
}

export interface ModelComparisonExpectedByDto {
  /** YYYY-MM-DD, Europe/Berlin calendar. */
  date: string;
  /** German, e.g. "frühestens 26.10. (noch 9 von 14 Tagen)". */
  label: string;
}

export interface ModelComparisonCellDto {
  state: ModelComparisonState;
  /** One plain German sentence (two short ones at most), generated from the numbers. */
  note: string;
  checks: {
    /** (O) offline over the whole year better: yes / no / running (a run is pending) / open (no run). */
    offline: ModelComparisonCheck;
    /** (L) live ≥ minDays and ≥ minParks with the CI below 0: yes / no / running (gate not met yet) / open (not archived). */
    live: ModelComparisonCheck;
    /** (S) visible in the app: the served model, or a confirmed live win of at least 1 minute. */
    visible: boolean;
  };
  /** Null when the model is not archived for this question, or has no paired data yet. */
  live: ModelComparisonLiveDto | null;
  offline: ModelComparisonOfflineDto | null;
  /** When the outstanding check is expected to report; null when nothing is outstanding. */
  expectedBy: ModelComparisonExpectedByDto | null;
}

export interface ModelComparisonQuestionDto {
  id: string;
  group: ModelComparisonGroupId;
  /** German, plain words ("Geplanter Tag ist in 2 bis 5 Tagen"). */
  label: string;
  /** Where the question appears in the app. */
  sub: string;
  /** German, the forecast distance concretely ("Prognose 2–5 Tage vorher, morgens um 06:05 gespeichert"). */
  leadLabel: string;
  /** Model id of the `today` cell — the reference of every other cell. */
  servedModel: string;
  /** `min` = minutes; `rate` = percentage points (D7: share of wrongly ordered day pairs, PAR-1037). */
  metric: { unit: "min" | "rate"; lowerIsBetter: boolean };
  /** Absent key = the model does not apply to this question. */
  cells: Record<string, ModelComparisonCellDto>;
  /** PAR-1035/1037: direct comparisons that are not "against served"; absent when there are none. */
  comparisons?: ModelComparisonPairDto[];
  /**
   * Paired rows (slots or ride-days) behind each archived cell, summed over
   * its leads, and `jointRows` — the rows every core candidate shared. A
   * Chronos-2 or old-composer cell below `jointRows` shows that shadow's
   * outage; it never removes rows from the other cells. Absent without data.
   */
  coverage?: { jointRows: number; cells: Record<string, number> };
}

export interface ModelComparisonResponseDto {
  generatedAt: string;
  window: { days: number; firstDate: string | null; lastDate: string | null };
  rule: { minDays: 14; minParks: 10 };
  models: ModelComparisonModelDto[];
  groups: { id: ModelComparisonGroupId; label: string }[];
  questions: ModelComparisonQuestionDto[];
  /** Counts over every non-`today` cell. */
  summary: { better: number; pending: number; unclear: number; worse: number };
}

export type ModelComparisonOriginId = "morning" | "d1" | "d3" | "d7";

/** A ride-day worth opening the day view on (PAR-855 follow-up). */
export interface ModelComparisonDaySuggestionDto {
  /** YYYY-MM-DD, park-local; equal to the options' `date`. */
  date: string;
  parkId: string;
  attractionId: string;
}

export interface ModelComparisonDayOptionsDto {
  /** The date `parks` describe: the requested one, else the newest of `dates`. */
  date: string;
  /**
   * COMPLETE target dates with archived 06:00 curves, newest first, at most 30:
   * a date is listed only once it is over in the timezone of every park, so
   * every ride listed for it can have its whole day of truth.
   */
  dates: string[];
  /**
   * Parks ordered by data richness (rides with OPERATING truth in at least
   * half of the operating window, then rides with any truth), not by name.
   * `rides`: headliners first, then by truth coverage; a ride with no
   * OPERATING truth that day is left out (and a park left without rides).
   */
  parks: {
    id: string;
    name: string;
    timezone: string;
    rides: { id: string; name: string; isHeadliner: boolean }[];
  }[];
  origins: { id: ModelComparisonOriginId; label: string }[];
  /**
   * The ride-day to open first, on `date`: a ride with truth in at least half
   * of its operating window and curves from as many models as any such ride,
   * taken in the order of `parks` / `rides`. Null when no ride qualifies.
   */
  suggested: ModelComparisonDaySuggestionDto | null;
}

export interface ModelComparisonDayDto {
  park: { id: string; name: string; timezone: string };
  ride: { id: string; name: string };
  date: string;
  origin: {
    id: ModelComparisonOriginId;
    label: string;
    /** ISO instant of the capture; null when nothing was archived for this origin. */
    at: string | null;
    /** Park-local, e.g. "10.10. 06:05 Uhr"; "keine Prognose gespeichert" when `at` is null. */
    localLabel: string;
  };
  /** The published park-wide OPERATING window of that day, park-local; null when none was published. */
  openingHours: { open: string; close: string } | null;
  /**
   * Park-local "HH:MM", 15-min steps across the operating window only: the
   * published hours; without them the first to the last slot with truth,
   * padded by one hour; without truth either, the span of the served curve.
   */
  slots: string[];
  /** Truth per slot: the STANDBY reading in force, OPERATING and ≥ 5 min; null = not operating / no reading. */
  truth: (number | null)[];
  /** Share of `slots` with a truth value, 0..1 (3 decimals); 0 = no measurements for this ride on this day. */
  truthCoverage: number;
  /** Hourly surfaces expanded to 15-min steps; null where the model had no value. */
  series: { model: string; label: string; values: (number | null)[] }[];
  /** MAE per model over the slots with truth and a value; ascending. */
  scores: { model: string; label: string; mae: number; n: number }[];
}
