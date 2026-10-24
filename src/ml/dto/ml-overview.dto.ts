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
  cells: { questionId: string; modelId: string; expect?: "not_better" }[];
  /**
   * PAR-1036: the fixed evaluation date (YYYY-MM-DD). Before it the decision
   * is never `ready`; from it on (`evaluated: true`) it is judged on the
   * matrix window ending the day before and no longer moves.
   */
  evaluateOn?: string;
  evaluated?: boolean;
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
}
