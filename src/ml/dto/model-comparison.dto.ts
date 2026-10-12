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

export interface ModelComparisonLiveDto {
  /** modelMae − referenceMae on identical slots / ride-days, in `metric.unit`. NEGATIVE = the model is better than what is served. */
  diff: number;
  /** 95 % park-cluster bootstrap CI of `diff` (parks resampled with replacement). */
  ciLow: number;
  ciHigh: number;
  /** Distinct scored target dates and parks that contributed. */
  days: number;
  parks: number;
  /** Paired slots (or ride-days for best time / calendar). */
  n: number;
  modelMae: number;
  referenceMae: number;
  /** First scored target date for this model × question (all time, not just the window). */
  since: string | null;
}

export interface ModelComparisonOfflineDto {
  diff?: number;
  ciLow?: number;
  ciHigh?: number;
  /** Where the verdict comes from, e.g. "ml-bench PR #445 (full period)". */
  source: string;
  /** "YYYY-MM-DD..YYYY-MM-DD" of the offline backtest. */
  period?: string;
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
  metric: { unit: "min" | "rate"; lowerIsBetter: boolean };
  /** Absent key = the model does not apply to this question. */
  cells: Record<string, ModelComparisonCellDto>;
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
