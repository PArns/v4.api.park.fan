/**
 * Admin ML overview (PAR-861): the contract of `GET /v1/admin/ml-overview`.
 *
 * One page, three answers: what is up for decision, where is which model
 * better (the model comparison's summary), and is everything running. The
 * admin frontend (PAR-862) is built against these types; change them only
 * additively. Semantics: docs/ml/ml-overview.md.
 */

export type MlDecisionState =
  /** Every cell `better`, offline and live check green. */
  | "ready"
  /** Offline better, the live gate (14 days, 10 parks) not met yet. */
  | "waiting_live"
  /** Offline no sure difference — nothing to switch to yet. */
  | "open"
  /** Waits on other decisions (static, from the decisions file). */
  | "later"
  /** The owner's call; no measurement decides it (static). */
  | "owner";

export interface MlDecisionDto {
  id: string;
  /** Plain German. */
  title: string;
  /** Plain German, one or two sentences. */
  detail: string;
  state: MlDecisionState;
  /**
   * The least-measured cell of the decision: min live days / parks over its
   * cells (0 when a cell has no live figure yet). Null for a static
   * (`later` / `owner`) decision or one without matrix cells.
   */
  progress: {
    liveDays: number;
    liveParks: number;
    needDays: number;
    needParks: number;
  } | null;
  /** YYYY-MM-DD (Europe/Berlin) + German label, e.g. "frühestens 24.10.". Null when nothing is outstanding or nothing can be dated. */
  earliest: { date: string; label: string } | null;
  /** Matrix cells the decision rests on (for a link into the model comparison). */
  cells: { questionId: string; modelId: string }[];
  /**
   * PAR-1036: the fixed evaluation date (YYYY-MM-DD). Before it the decision
   * is never `ready`; from it on (`evaluated: true`) it is judged on the
   * matrix window ending the day before and no longer moves.
   */
  evaluateOn?: string;
  evaluated?: boolean;
  /**
   * PAR-1059: the frozen report written once on `evaluateOn` (after that
   * day's 12:00 UTC scoring). Present = `state` is the report's verdict and
   * never moves again. Absent before the job ran.
   */
  report?: MlDecisionReportDto;
}

/** One cell of a frozen decision report (PAR-1059), copied from the matrix. */
export interface MlDecisionReportCellDto {
  questionId: string;
  modelId: string;
  /** The matrix cell's state on the frozen window. */
  state: string;
  /** model − served, in the question's unit; negative = better. Null without live data. */
  diff: number | null;
  /** The gate's interval (park × date bootstrap). */
  ciLow: number | null;
  ciHigh: number | null;
  /** The park-cluster interval, shown beside it. */
  parkCiLow: number | null;
  parkCiHigh: number | null;
  parks: number;
  days: number;
  /** Paired slots / ride-days behind the cell. */
  n: number;
  /** The question's joint rows (every core candidate had a value); null without coverage. */
  jointRows: number | null;
  /** This cell's own rows (below `jointRows` for an optional shadow with gaps). */
  cellRows: number | null;
}

export interface MlDecisionReportCheckDto {
  id: "weekday_mix" | "holiday_share" | "autumn_only" | "window_end";
  ok: boolean;
  /** Plain German, what was measured. */
  label: string;
}

/**
 * A decision judged once on its `evaluateOn` (PAR-1059): the verdict and
 * everything it rested on, stored in `ml_decision_reports` and never
 * recomputed. Semantics: docs/ml/ml-overview.md §1a.
 */
export interface MlDecisionReportDto {
  decisionId: string;
  evaluateOn: string;
  /** ISO instant the report was frozen. */
  createdAt: string;
  /** The paired window (YYYY-MM-DD, inclusive); the intended end is evaluateOn − 1. */
  window: { firstDate: string | null; lastDate: string | null };
  /** The derived state on the frozen window (never `ready` with a failed check unnoticed: see `warning`). */
  verdict: MlDecisionState;
  cells: MlDecisionReportCellDto[];
  /** Distinct parks and dates among the paired park × date cells of the decision. */
  parks: number;
  days: number;
  /** Paired park × date cells, Monday first (index 0 = Mo … 6 = So). */
  weekdays: { weekday: string; count: number; share: number }[];
  /** Share (0..1) of paired park × date cells on a public holiday, school break or bridge day at the park. */
  holidayShare: number | null;
  /** Plain German, e.g. "Herbst" or "Herbst und Winter". */
  season: string;
  checks: MlDecisionReportCheckDto[];
  /** True when any check failed: the verdict must not be read as a silent green. */
  warning: boolean;
  /** Plain German, one line per failed check. */
  warnings: string[];
}

export type MlDriftStatus = "ok" | "drift" | "no_data";

export interface MlDriftWindowDto {
  /** Scored slots (rows) in the window. */
  n: number;
  mae: number | null;
  /** MAE of the weekday median (same ride, park-local weekday and hour, 4 weeks before) on the slots that have one. */
  baselineMae: number | null;
  /** mae / baselineMae on the same slots; below 1 = beats the weekday median. */
  skill: number | null;
  /** P(actual ≤ q0.8) / P(actual ≤ q0.95); null where no quantile is stored. */
  coverageQ80: number | null;
  coverageQ95: number | null;
}

/** One served model's drift watch (PAR-1059): last 7 days vs the 28 before. */
export interface MlDriftModelDto {
  /** `catboost` (raw, with quantiles), `pcn`, `chronos` (served). */
  model: string;
  label: string;
  status: MlDriftStatus;
  recent: MlDriftWindowDto;
  previous: MlDriftWindowDto;
  /** Plain German, one line per threshold crossed (empty when ok). */
  reasons: string[];
}

export interface MlDriftWatchDto {
  /** ISO instant of the daily computation. */
  computedAt: string;
  /** Worst of the models: drift > ok > no_data. */
  status: MlDriftStatus;
  models: MlDriftModelDto[];
}

export type MlComponentRole = "served" | "shadow" | "measurement";

export type MlComponentStatus =
  "ok" | "late" | "failed" | "warming" | "unknown";

export interface MlComponentDto {
  id: string;
  name: string;
  /** Plain German: what it provides in the app. */
  delivers: string;
  role: MlComponentRole;
  /** ISO instant of the last run that left data behind; null = none found. */
  lastRunAt: string | null;
  /** Plain German cadence, e.g. "alle 15 Min.", "täglich 03:00 UTC". */
  expected: string;
  status: MlComponentStatus;
  /** Plain German, e.g. "Version v20261011_0600". */
  note: string | null;
}

export interface MlOverviewDto {
  generatedAt: string;
  decisions: MlDecisionDto[];
  /** The model comparison's `summary` (28 days, all regions). */
  matrixSummary: {
    better: number;
    pending: number;
    unclear: number;
    worse: number;
  };
  components: MlComponentDto[];
  /** PAR-1059: the daily drift watch; absent until it first ran (or when it is older than 3 days). */
  drift?: MlDriftWatchDto;
}
