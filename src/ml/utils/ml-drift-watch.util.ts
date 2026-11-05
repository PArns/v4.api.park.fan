/**
 * Drift watch (PAR-1059), pure part: per served model, the last 7 days
 * against the 28 before — quantile coverage and the MAE relative to the
 * weekday median. The thresholds are here and in docs/ml/ml-overview.md §2a.
 * No alert is sent (owner decision "vorerst keine Alarme"); the result is a
 * status on the ML overview.
 */
import {
  MlDriftModelDto,
  MlDriftStatus,
  MlDriftWatchDto,
  MlDriftWindowDto,
} from "../dto/ml-overview.dto";
import { fmtDe } from "./model-comparison.util";
import { MAX_PLAUSIBLE_WAIT_TIME } from "../../common/utils/wait-time.utils";
import { scoredSlotSql } from "../entities/prediction-accuracy.entity";

export const DRIFT_THRESHOLDS = {
  /** Fewer scored slots than this in either window: `no_data`, never a verdict. */
  minSlots: 1000,
  /** q0.8 coverage moving more than 5 percentage points. */
  coverageQ80Pp: 5,
  /** q0.95 coverage moving more than 3 points (a narrower tail, a tighter bound). */
  coverageQ95Pp: 3,
  /**
   * The MAE ÷ weekday-median ratio rising by more than 10 % (e.g. 0.80 →
   * 0.88). A ratio, not the MAE: a busier week raises every MAE, the
   * model's and the baseline's alike.
   */
  skillRise: 0.1,
} as const;

/** The window sums one SQL row carries per model and window. */
export interface DriftWindowSums {
  n: number;
  sae: number;
  /** Slots that have a weekday median, and both error sums on them. */
  nBase: number;
  saeOnBase: number;
  saeBase: number;
  /** Slots with a stored quantile, and how many came in at or under it. */
  nQ: number;
  hitQ80: number;
  hitQ95: number;
}

export const DRIFT_MODEL_LABELS: Record<string, string> = {
  catboost: "CatBoost (Stundenwerte, mit Quantilen)",
  pcn: "PCN (ausgeliefert)",
  chronos: "Chronos-2 (ausgeliefert)",
};

const r3 = (v: number) => Math.round(v * 1000) / 1000;

export function driftWindow(s: DriftWindowSums | undefined): MlDriftWindowDto {
  if (!s || s.n === 0) {
    return {
      n: 0,
      mae: null,
      baselineMae: null,
      skill: null,
      coverageQ80: null,
      coverageQ95: null,
    };
  }
  return {
    n: s.n,
    mae: r3(s.sae / s.n),
    baselineMae: s.nBase ? r3(s.saeBase / s.nBase) : null,
    skill: s.nBase && s.saeBase > 0 ? r3(s.saeOnBase / s.saeBase) : null,
    coverageQ80: s.nQ ? r3(s.hitQ80 / s.nQ) : null,
    coverageQ95: s.nQ ? r3(s.hitQ95 / s.nQ) : null,
  };
}

const pp = (v: number) =>
  `${fmtDe(Math.round(v * 1000) / 10, true)} Prozentpunkte`;

/** One model's status from its two windows. */
export function driftModel(
  model: string,
  recentSums: DriftWindowSums | undefined,
  previousSums: DriftWindowSums | undefined,
): MlDriftModelDto {
  const recent = driftWindow(recentSums);
  const previous = driftWindow(previousSums);
  const base = {
    model,
    label: DRIFT_MODEL_LABELS[model] ?? model,
    recent,
    previous,
  };
  const t = DRIFT_THRESHOLDS;
  if (recent.n < t.minSlots || previous.n < t.minSlots) {
    return { ...base, status: "no_data", reasons: [] };
  }
  const reasons: string[] = [];
  const cov = (
    a: number | null,
    b: number | null,
    limitPp: number,
    name: string,
  ) => {
    if (a === null || b === null) return;
    if (Math.abs(a - b) * 100 > limitPp) {
      reasons.push(
        `Abdeckung ${name}: ${pp(a - b)} gegenüber den 4 Wochen davor (erlaubt ±${limitPp})`,
      );
    }
  };
  cov(recent.coverageQ80, previous.coverageQ80, t.coverageQ80Pp, "q0,8");
  cov(recent.coverageQ95, previous.coverageQ95, t.coverageQ95Pp, "q0,95");
  if (
    recent.skill !== null &&
    previous.skill !== null &&
    recent.skill > previous.skill * (1 + t.skillRise)
  ) {
    reasons.push(
      `Fehler gegenüber dem Wochentag-Median: ${dec2(recent.skill)} statt ${dec2(previous.skill)} (erlaubt bis +${t.skillRise * 100} %)`,
    );
  }
  return { ...base, status: reasons.length ? "drift" : "ok", reasons };
}

const dec2 = (v: number) => v.toFixed(2).replace(".", ",");

const RANK: Record<MlDriftStatus, number> = { no_data: 0, ok: 1, drift: 2 };

export function buildDriftWatch(
  sums: Map<string, { recent?: DriftWindowSums; previous?: DriftWindowSums }>,
  computedAt: Date,
): MlDriftWatchDto {
  const models = Object.keys(DRIFT_MODEL_LABELS).map((m) =>
    driftModel(m, sums.get(m)?.recent, sums.get(m)?.previous),
  );
  const status = models.reduce<MlDriftStatus>(
    (w, m) => (RANK[m.status] > RANK[w] ? m.status : w),
    "no_data",
  );
  return { computedAt: computedAt.toISOString(), status, models };
}

export interface DriftSqlOptions {
  /** Days of the recent window; the daily watch reads 7. */
  recentDays?: number;
  /** Days judged (recent + previous); the daily watch reads 35. */
  judgedDays?: number;
  /** Days read, judged plus the 28 the weekday median looks back; 63 for the watch. */
  fetchDays?: number;
  /**
   * Adds the grouping key (`id`, `name`) and the sum of signed errors (`sbias`)
   * to every row, per park or per ride; absent = one row per model and window.
   */
  group?: "park" | "ride";
  /** Restricts to the rides of the park bound as `$2` (a uuid parameter, never a cast column). */
  parkFilter?: boolean;
  /** Restricts to the model bound as the next parameter (`$2`, or `$3` with a park). */
  modelFilter?: boolean;
}

/**
 * The drift watch's statement, shared by the daily watch and the MCP accuracy
 * tool so their figures cannot diverge: per served model and window, the
 * error sums, the weekday median baseline (same ride, park-local weekday and
 * hour, the 4 weeks before each slot) and quantile coverage. `$1` is the end
 * of the windows. With no options it is the watch's statement unchanged.
 */
export function driftSql(opts: DriftSqlOptions = {}): string {
  const days = (v: number) => Math.trunc(v);
  const recent = days(opts.recentDays ?? 7);
  const judged = days(opts.judgedDays ?? 35);
  const fetch = days(opts.fetchDays ?? 63);
  const grouped = opts.group !== undefined;
  const keyCols =
    opts.group === "ride"
      ? `att.id::text AS k, att.name AS nm,\n                `
      : `p.id::text AS k, p.name AS nm,\n                `;
  const biasCols = grouped
    ? `,\n                pa.predicted_wait_time - pa.actual_wait_time AS bias,\n                pa.served_wait_time - pa.actual_wait_time AS sbias`
    : "";
  const parkClause = opts.parkFilter
    ? `\n            AND att."parkId" = $2::uuid`
    : "";
  const modelParam = opts.parkFilter ? "$3" : "$2";
  const modelClause = opts.modelFilter
    ? `\n            AND m.model = ${modelParam}`
    : "";
  return `WITH r AS (
         SELECT pa.attraction_id AS a,
                ${grouped ? keyCols : ""}date_trunc('hour', pa.target_time AT TIME ZONE p.timezone) AS lt,
                pa.target_time >= $1::timestamptz - interval '${recent} days' AS recent,
                pa.target_time >= $1::timestamptz - interval '${judged} days' AS judged,
                pa.actual_wait_time AS actual, pa.absolute_error AS err,
                pa.served_source AS src, pa.served_absolute_error AS serr,
                pa.predicted_q80 AS q80, pa.predicted_q95 AS q95${biasCols}
           FROM prediction_accuracy pa
           JOIN attractions att ON att.id = pa.attraction_id
           JOIN parks p ON p.id = att."parkId"
          WHERE pa.comparison_status = 'COMPLETED'
            AND pa.target_time >= $1::timestamptz - interval '${fetch} days'
            AND pa.target_time < $1::timestamptz
            AND pa."predictionType" = 'hourly'${parkClause}
            AND ${scoredSlotSql("pa", MAX_PLAUSIBLE_WAIT_TIME)}
       ),
       -- One actual per ride-hour: r holds ~3.6 predictions per target hour,
       -- so joining r to itself multiplied rows (23 M, 61 s on production) and
       -- let a single week pass the two-week floor below.
       h AS (
         SELECT a, lt, avg(actual) AS actual, bool_or(judged) AS judged
           FROM r
          GROUP BY a, lt
       ),
       base AS (
         SELECT s.a, s.lt,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY h.actual) AS b
           FROM h s
          CROSS JOIN generate_series(1, 4) w
           -- Equi-join on (ride, hour): an IN list here became an OR join
           -- filter over every slot pair of a ride (review of PAR-1059).
           JOIN h ON h.a = s.a AND h.lt = s.lt - w * interval '7 days'
          WHERE s.judged
          GROUP BY s.a, s.lt
         HAVING count(*) >= 2
       ),
       e AS (
         SELECT ${grouped ? "r.k, r.nm, " : ""}r.recent, m.model, m.err, m.withq, r.actual, r.q80, r.q95, b.b${grouped ? ", m.bias" : ""}
           FROM r
           LEFT JOIN base b ON b.a = r.a AND b.lt = r.lt
          CROSS JOIN LATERAL (VALUES
                ('catboost', r.err, true${grouped ? ", r.bias" : ""}),
                (CASE WHEN r.src IN ('pcn', 'chronos') THEN r.src END, r.serr, false${grouped ? ", r.sbias" : ""})
          ) m(model, err, withq${grouped ? ", bias" : ""})
          WHERE r.judged AND m.model IS NOT NULL AND m.err IS NOT NULL${modelClause}
       )
       SELECT ${grouped ? "k AS id, nm AS name, " : ""}model, recent,
              count(*) AS n, sum(err) AS sae,${grouped ? " sum(bias) AS sbias," : ""}
              count(b) AS "nBase",
              sum(err) FILTER (WHERE b IS NOT NULL) AS "saeOnBase",
              sum(abs(actual - b)) AS "saeBase",
              count(*) FILTER (WHERE withq AND q80 IS NOT NULL AND q95 IS NOT NULL) AS "nQ",
              count(*) FILTER (WHERE withq AND q80 IS NOT NULL AND q95 IS NOT NULL AND actual <= q80) AS hit80,
              count(*) FILTER (WHERE withq AND q80 IS NOT NULL AND q95 IS NOT NULL AND actual <= q95) AS hit95
         FROM e
        GROUP BY ${grouped ? "k, nm, " : ""}model, recent`;
}
