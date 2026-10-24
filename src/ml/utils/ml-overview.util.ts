/**
 * The admin ML overview (PAR-861), pure parts: which decision stands where,
 * and whether a component ran when it should have.
 *
 * Decisions rest on cells of the model comparison matrix (PAR-855); the
 * service hands in that matrix, the decisions file and "today", and the spec
 * pins the state rule, the progress, the earliest date and the thresholds.
 * See docs/ml/ml-overview.md.
 */
import { addIsoDays } from "../../common/utils/date.util";
import {
  ModelComparisonCellDto,
  ModelComparisonResponseDto,
} from "../dto/model-comparison.dto";
import {
  MlComponentStatus,
  MlDecisionDto,
  MlDecisionState,
} from "../dto/ml-overview.dto";
import {
  MIN_DAYS,
  MIN_PARKS,
  cellFloorDate,
  dayMonthDe,
} from "./model-comparison.util";

export interface DecisionDef {
  id: string;
  title: string;
  detail: string;
  /** Static override: `later` (waits on `dependsOn`) or `owner`. */
  state?: MlDecisionState;
  dependsOn?: string[];
  /**
   * The fixed evaluation date (YYYY-MM-DD, Europe/Berlin; PAR-1036). Before
   * it the decision cannot be `ready`; from it on it is judged once, on the
   * matrix window ending the day before, and does not drift afterwards. The
   * owner moves it by editing ml-decisions.json.
   */
  evaluateOn?: string;
  cells: { questionId: string; modelId: string }[];
}

export interface DecisionFile {
  decisions: DecisionDef[];
}

const MS_MIN = 60_000;
const MS_HOUR = 60 * MS_MIN;

/**
 * When each component counts as late: the age of its newest run beyond which
 * something has gone wrong. Each is the cadence plus room for one slow or
 * skipped run, never tighter than the gaps measured on production
 * (2026-10-11, last 2-3 days).
 */
export const LATE_AFTER_MS = {
  /** Every 15 min, around the clock (max gap measured: 15 min). */
  pcn: 45 * MS_MIN,
  /** Daily 03:00 UTC; a run takes ~10 min, a slow night can push it hours. */
  tft: 30 * MS_HOUR,
  /** Daily 06:00 UTC training, ~1 h until the model is registered. */
  catboost: 30 * MS_HOUR,
  /**
   * Chronos-2 shadow: intraday origins every 2 h while a park is open (the
   * served `park_hourly` surface, same origins, has a max gap of 2 h), the
   * 06:00 grid once a day per park. Open parks around the world leave short
   * nightly gaps, hence 4 h.
   */
  chronos: 4 * MS_HOUR,
  /** H5 profiles: 06:00 / 07:00 park-local, max gap measured 6-7 h (no park at 06:00 local). */
  h5: 8 * MS_HOUR,
  /** Forward archive: hourly :05; max gap of any surface measured 1 h. */
  archive: 3 * MS_HOUR,
  /** Scoring: daily 12:00 UTC. */
  scoring: 26 * MS_HOUR,
} as const;

export type ComponentId = keyof typeof LATE_AFTER_MS;

/**
 * ok / late / unknown from the newest run. `failed` and `warming` come from
 * the caller (a service's own status) and win over the age.
 */
export function componentStatus(
  lastRunAt: Date | null,
  lateAfterMs: number,
  now: Date,
  override?: "failed" | "warming" | null,
): MlComponentStatus {
  if (override) return override;
  if (!lastRunAt) return "unknown";
  return now.getTime() - lastRunAt.getTime() > lateAfterMs ? "late" : "ok";
}

/** The matrix cells a decision rests on, without the served (`today`) ones. */
function decisionCells(
  def: DecisionDef,
  matrix: ModelComparisonResponseDto | null,
): ModelComparisonCellDto[] {
  if (!matrix) return [];
  const out: ModelComparisonCellDto[] = [];
  for (const ref of def.cells) {
    const q = matrix.questions.find((x) => x.id === ref.questionId);
    const cell = q?.cells[ref.modelId];
    if (cell && cell.state !== "today") out.push(cell);
  }
  return out;
}

/**
 * The derived state. `ready`: every cell better, offline and live green.
 * `waiting_live`: at least one cell is better offline (or its offline run is
 * still going) while its live gate is not met. Otherwise `open`.
 */
export function deriveDecisionState(
  cells: ModelComparisonCellDto[],
): MlDecisionState {
  if (cells.length === 0) return "open";
  if (
    cells.every(
      (c) =>
        c.state === "better" &&
        c.checks.offline === "yes" &&
        c.checks.live === "yes",
    )
  ) {
    return "ready";
  }
  const waiting = cells.some(
    (c) =>
      (c.checks.offline === "yes" || c.checks.offline === "running") &&
      (c.checks.live === "running" || c.checks.live === "open"),
  );
  return waiting ? "waiting_live" : "open";
}

/** The least-measured cell: min live days and parks (0 without live data). */
export function decisionProgress(
  cells: ModelComparisonCellDto[],
): MlDecisionDto["progress"] {
  if (cells.length === 0) return null;
  let days = Infinity;
  let parks = Infinity;
  for (const c of cells) {
    days = Math.min(days, c.live?.days ?? 0);
    parks = Math.min(parks, c.live?.parks ?? 0);
  }
  return {
    liveDays: days,
    liveParks: parks,
    needDays: MIN_DAYS,
    needParks: MIN_PARKS,
  };
}

/**
 * Earliest date the live gate can be met: today + the missing days (one
 * scored date per day). Days met but parks short: the next scoring, i.e.
 * tomorrow. Gate met: null — nothing is outstanding.
 */
export function earliestFromProgress(
  progress: MlDecisionDto["progress"],
  today: string,
  /** No date before it: ARCHIVE_START + the longest lead + 14 ({@link decisionFloor}). */
  floor?: string,
): MlDecisionDto["earliest"] {
  if (!progress) return null;
  const atLeast = (d: string) => (floor && floor > d ? floor : d);
  const missing = Math.max(0, progress.needDays - progress.liveDays);
  if (missing === 0) {
    if (progress.liveParks >= progress.needParks) return null;
    const date = atLeast(addIsoDays(today, 1));
    return {
      date,
      label: `frühestens ${dayMonthDe(date)} (es fehlen noch Parks)`,
    };
  }
  const date = atLeast(addIsoDays(today, missing));
  return { date, label: `frühestens ${dayMonthDe(date)}` };
}

/**
 * The earliest date any cell of the decision can pass the gate: the latest
 * {@link cellFloorDate} of its non-served cells. A lead that has no row yet
 * counts as 0 days in `progress`, so "today + 14" alone slides forward every
 * day for d60 / d90 cells (≈ 22.12.2026 and 21.01.2027 from the 09.10. start).
 */
export function decisionFloor(def: DecisionDef): string | undefined {
  const dates = def.cells
    .map((c) => cellFloorDate(c.questionId, c.modelId))
    .sort();
  return dates[dates.length - 1];
}

/**
 * Every decision of the file, in its order. A static `state` wins; a `later`
 * decision is dated after the latest of the decisions it depends on.
 */
export function buildDecisions(
  file: DecisionFile,
  matrix: ModelComparisonResponseDto | null,
  today: string,
  /** Per `evaluateOn` reached: the matrix of the window ending the day before. */
  frozen: Map<string, ModelComparisonResponseDto | null> = new Map(),
): MlDecisionDto[] {
  const derived = new Map<string, MlDecisionDto>();
  for (const def of file.decisions) {
    if (def.state) continue;
    const evaluated = !!def.evaluateOn && def.evaluateOn <= today;
    const cells = decisionCells(
      def,
      evaluated ? (frozen.get(def.evaluateOn!) ?? null) : matrix,
    );
    let state = deriveDecisionState(cells);
    // Before the evaluation date a decision is never ready: the gate is
    // looked at once, not every day until it happens to pass.
    if (!evaluated && def.evaluateOn && state === "ready") {
      state = "waiting_live";
    }
    const progress = decisionProgress(cells);
    let earliest: MlDecisionDto["earliest"] = null;
    if (!evaluated && state !== "ready") {
      earliest = earliestFromProgress(progress, today, decisionFloor(def));
      if (def.evaluateOn && (!earliest || earliest.date < def.evaluateOn)) {
        earliest = {
          date: def.evaluateOn,
          label: `ausgewertet am ${dayMonthDe(def.evaluateOn)}`,
        };
      }
    }
    derived.set(def.id, {
      id: def.id,
      title: def.title,
      detail: def.detail,
      state,
      progress,
      earliest,
      cells: def.cells,
      ...(def.evaluateOn ? { evaluateOn: def.evaluateOn, evaluated } : {}),
    });
  }
  return file.decisions.map((def) => {
    const hit = derived.get(def.id);
    if (hit) return hit;
    let earliest: MlDecisionDto["earliest"] = null;
    if (def.state === "later" && def.dependsOn?.length) {
      const dates = def.dependsOn
        .map((id) => derived.get(id)?.earliest?.date)
        .filter((d): d is string => !!d)
        .sort();
      const last = dates[dates.length - 1];
      if (last) {
        earliest = {
          date: last,
          label: `nicht vor ${dayMonthDe(last)} (erst wenn der Ersatz live bestätigt ist)`,
        };
      }
    }
    return {
      id: def.id,
      title: def.title,
      detail: def.detail,
      state: def.state as MlDecisionState,
      progress: null,
      earliest,
      cells: def.cells,
    };
  });
}

/** "v20261011_0600" → "Version v20261011_0600"; null without a version. */
export function versionNote(version: string | null | undefined): string | null {
  return version ? `Version ${version}` : null;
}
