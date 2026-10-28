import { Inject, Injectable } from "@nestjs/common";
import { InjectDataSource } from "@nestjs/typeorm";
import { DataSource } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { safeJsonParse } from "../../common/utils/json.util";
import { isCanonicalUuid } from "../../common/utils/uuid-param.util";
import { MAX_PLAUSIBLE_WAIT_TIME } from "../../common/utils/wait-time.utils";
import {
  StatementLimits,
  queryWithLimits,
} from "../../common/utils/statement-limits.util";
import { scoredSlotSql } from "../entities/prediction-accuracy.entity";
import { DriftWindowSums, driftWindow } from "../utils/ml-drift-watch.util";

/** The served models whose hourly forecasts `prediction_accuracy` scores. */
export const SCORED_MODELS = ["catboost", "pcn", "chronos"] as const;
export type ScoredModel = (typeof SCORED_MODELS)[number];

/** COMPLETED rows are kept 90 days (PredictionAccuracyProcessor, `cleanup-old`). */
export const ACCURACY_RETENTION_DAYS = 90;
/** The weekday median looks 4 weeks back from every judged slot. */
const BASELINE_DAYS = 28;
/** Two windows of `days` plus the baseline's look-back must fit the retention. */
export const MAX_BREAKDOWN_DAYS = Math.floor(
  (ACCURACY_RETENTION_DAYS - BASELINE_DAYS) / 2,
);
const CACHE_TTL_SECONDS = 30 * 60;

/**
 * A person waits at the tool, not a job: a statement past this is cancelled
 * and the caller is told to narrow it (one park, fewer days).
 */
const LIMITS: StatementLimits = {
  statementTimeoutMs: 45_000,
  lockTimeoutMs: 2_000,
  idleInTransactionTimeoutMs: 15_000,
};

interface SqlRow {
  id: string;
  name: string;
  model: string;
  recent: boolean;
  n: string | number;
  sae: string | number | null;
  sbias: string | number | null;
  nBase: string | number;
  saeOnBase: string | number | null;
  saeBase: string | number | null;
}

/** One window of one model at one park or ride. */
export interface ErrorWindow {
  /** Scored slots. */
  n: number;
  mae: number | null;
  /** Mean of (forecast − actual): positive = forecasts run too long. */
  bias: number | null;
  /** MAE of the weekday median on the slots that have one: the drift watch's baseline. */
  baselineMae: number | null;
  /** The model's MAE ÷ the baseline's on the same slots; below 1 beats the baseline. */
  skill: number | null;
}

export interface ErrorRow {
  id: string;
  name: string;
  model: ScoredModel;
  recent: ErrorWindow;
  previous: ErrorWindow;
  /** recent.mae − previous.mae; null when either window has no figure. */
  maeChange: number | null;
}

export interface ErrorBreakdown {
  grain: "park" | "ride";
  days: number;
  /** ISO instants: the recent window is [recentFrom, until), the previous [previousFrom, recentFrom). */
  window: { previousFrom: string; recentFrom: string; until: string };
  /** Every model over everything scored, as the drift watch pools it. */
  totals: Array<Pick<ErrorRow, "model" | "recent" | "previous" | "maeChange">>;
  rows: ErrorRow[];
}

const round = (v: number, digits = 2) => {
  const f = 10 ** digits;
  return Math.round(v * f) / f;
};

type Sums = DriftWindowSums & { sbias: number };
const empty = (): Sums => ({
  n: 0,
  sae: 0,
  nBase: 0,
  saeOnBase: 0,
  saeBase: 0,
  nQ: 0,
  hitQ80: 0,
  hitQ95: 0,
  sbias: 0,
});

function window(s: Sums | undefined): ErrorWindow {
  const w = driftWindow(s);
  return {
    n: w.n,
    mae: w.mae === null ? null : round(w.mae),
    bias: s && s.n ? round(s.sbias / s.n) : null,
    baselineMae: w.baselineMae === null ? null : round(w.baselineMae),
    skill: w.skill,
  };
}

const change = (recent: ErrorWindow, previous: ErrorWindow) =>
  recent.mae === null || previous.mae === null
    ? null
    : round(recent.mae - previous.mae);

/**
 * Where the served hourly forecasts miss: the error against the actual wait
 * per park (or per ride of one park), for the last `days` and the `days`
 * before, with the drift watch's baseline (the weekday median of the same
 * ride and hour, 4 weeks back). Reads `prediction_accuracy` only, through
 * its `(comparison_status, target_time)` index, under a statement timeout;
 * never `wait_time_predictions` or `queue_data`. The slot rule and the
 * baseline are those of `MlDecisionWatchService.computeDrift`, so the
 * totals match the ML overview's drift watch.
 */
@Injectable()
export class ForecastErrorBreakdownService {
  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  async breakdown(
    opts: { days: number; parkId?: string; model?: ScoredModel },
    now: Date = new Date(),
  ): Promise<ErrorBreakdown> {
    if (opts.parkId !== undefined && !isCanonicalUuid(opts.parkId)) {
      throw new Error("parkId must be a lowercase uuid");
    }
    const days = Math.min(
      Math.max(Math.trunc(opts.days), 1),
      MAX_BREAKDOWN_DAYS,
    );
    // Hour-aligned so the cache key is shared by calls within the hour.
    const until = new Date(Math.floor(now.getTime() / 3_600_000) * 3_600_000);
    const key = `ml-error-breakdown:v1:${days}:${opts.parkId ?? "-"}:${opts.model ?? "-"}:${until.toISOString()}`;
    const hit = safeJsonParse<ErrorBreakdown>(
      await this.redis.get(key).catch(() => null),
    );
    if (hit) return hit;

    const dayMs = 86_400_000;
    const recentFrom = new Date(until.getTime() - days * dayMs);
    const previousFrom = new Date(until.getTime() - 2 * days * dayMs);
    const fetchFrom = new Date(previousFrom.getTime() - BASELINE_DAYS * dayMs);
    const rows = await queryWithLimits<SqlRow[]>(
      this.dataSource,
      this.sql(opts.parkId !== undefined, opts.model !== undefined),
      [
        until.toISOString(),
        recentFrom.toISOString(),
        previousFrom.toISOString(),
        fetchFrom.toISOString(),
        ...(opts.parkId !== undefined ? [opts.parkId] : []),
        ...(opts.model !== undefined ? [opts.model] : []),
      ],
      LIMITS,
    );

    const cells = new Map<
      string,
      {
        id: string;
        name: string;
        model: ScoredModel;
        recent?: Sums;
        previous?: Sums;
      }
    >();
    const totals = new Map<ScoredModel, { recent: Sums; previous: Sums }>();
    const add = (to: Sums, row: SqlRow) => {
      to.n += Number(row.n);
      to.sae += Number(row.sae ?? 0);
      to.sbias += Number(row.sbias ?? 0);
      to.nBase += Number(row.nBase);
      to.saeOnBase += Number(row.saeOnBase ?? 0);
      to.saeBase += Number(row.saeBase ?? 0);
    };
    for (const row of rows) {
      const model = row.model as ScoredModel;
      const cell = cells.get(`${row.id}|${model}`) ?? {
        id: row.id,
        name: row.name,
        model,
      };
      const side = row.recent ? "recent" : "previous";
      add((cell[side] ??= empty()), row);
      cells.set(`${row.id}|${model}`, cell);
      const total = totals.get(model) ?? { recent: empty(), previous: empty() };
      add(total[side], row);
      totals.set(model, total);
    }

    const result: ErrorBreakdown = {
      grain: opts.parkId !== undefined ? "ride" : "park",
      days,
      window: {
        previousFrom: previousFrom.toISOString(),
        recentFrom: recentFrom.toISOString(),
        until: until.toISOString(),
      },
      totals: [...totals.entries()].map(([model, t]) => {
        const recent = window(t.recent);
        const previous = window(t.previous);
        return { model, recent, previous, maeChange: change(recent, previous) };
      }),
      rows: [...cells.values()].map((cell) => {
        const recent = window(cell.recent);
        const previous = window(cell.previous);
        return {
          id: cell.id,
          name: cell.name,
          model: cell.model,
          recent,
          previous,
          maeChange: change(recent, previous),
        };
      }),
    };
    await this.redis
      .set(key, JSON.stringify(result), "EX", CACHE_TTL_SECONDS)
      .catch(() => undefined);
    return result;
  }

  /**
   * The drift watch's statement with a grouping key. $1 until, $2 recent
   * start, $3 previous start, $4 first row read (previous start − 28 days),
   * then the park and the model when given. The park is compared as a uuid
   * parameter so `uq_pa_attraction_target` stays usable.
   */
  private sql(byPark: boolean, byModel: boolean): string {
    const key = byPark
      ? "att.id::text AS k, att.name AS nm"
      : "p.id::text AS k, p.name AS nm";
    const parkIdx = 5;
    const modelIdx = byPark ? 6 : 5;
    return `WITH r AS (
         SELECT pa.attraction_id AS a, ${key},
                date_trunc('hour', pa.target_time AT TIME ZONE p.timezone) AS lt,
                pa.target_time >= $2::timestamptz AS recent,
                pa.target_time >= $3::timestamptz AS judged,
                pa.actual_wait_time AS actual,
                pa.predicted_wait_time - pa.actual_wait_time AS bias, pa.absolute_error AS err,
                pa.served_source AS src, pa.served_absolute_error AS serr,
                pa.served_wait_time - pa.actual_wait_time AS sbias
           FROM prediction_accuracy pa
           JOIN attractions att ON att.id = pa.attraction_id
           JOIN parks p ON p.id = att."parkId"
          WHERE pa.comparison_status = 'COMPLETED'
            AND pa.target_time >= $4::timestamptz
            AND pa.target_time < $1::timestamptz
            AND pa."predictionType" = 'hourly'
            ${byPark ? `AND att."parkId" = $${parkIdx}::uuid` : ""}
            AND ${scoredSlotSql("pa", MAX_PLAUSIBLE_WAIT_TIME)}
       ),
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
           JOIN h ON h.a = s.a AND h.lt = s.lt - w * interval '7 days'
          WHERE s.judged
          GROUP BY s.a, s.lt
         HAVING count(*) >= 2
       ),
       e AS (
         SELECT r.k, r.nm, r.recent, m.model, m.err, m.bias, r.actual, b.b
           FROM r
           LEFT JOIN base b ON b.a = r.a AND b.lt = r.lt
          CROSS JOIN LATERAL (VALUES
                ('catboost', r.err, r.bias),
                (CASE WHEN r.src IN ('pcn', 'chronos') THEN r.src END, r.serr, r.sbias)
          ) m(model, err, bias)
          WHERE r.judged AND m.model IS NOT NULL AND m.err IS NOT NULL
            ${byModel ? `AND m.model = $${modelIdx}` : ""}
       )
       SELECT k AS id, nm AS name, model, recent,
              count(*) AS n, sum(err) AS sae, sum(bias) AS sbias,
              count(b) AS "nBase",
              sum(err) FILTER (WHERE b IS NOT NULL) AS "saeOnBase",
              sum(abs(actual - b)) AS "saeBase"
         FROM e
        GROUP BY k, nm, model, recent`;
  }
}
