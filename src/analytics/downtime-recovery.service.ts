import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { DataSource, IsNull, Repository } from "typeorm";
import type { OutageSignal } from "./entities/attraction-outage.entity";
import {
  DowntimeRecoveryCurve,
  MIN_PARK_CURVE_SAMPLE,
  RECOVERY_ELAPSED_BUCKETS,
} from "./entities/downtime-recovery-curve.entity";

/**
 * Build the conditional recovery curves, one statement, once a night.
 *
 * The statistics and the reasoning behind them are on
 * {@link DowntimeRecoveryCurve}. This is the mechanics.
 *
 * ## Why it is one statement and not a loop
 *
 * A bucket's numbers are three aggregates over the same rows with different
 * predicates, so the whole table is a single grouped scan of
 * `attraction_outages` crossed with eleven bucket edges. Looping the buckets in
 * TypeScript would read the table eleven times to compute what one pass gives,
 * and the table is the output of the nightly reconstruction that just ran.
 */
@Injectable()
export class DowntimeRecoveryService {
  private readonly logger = new Logger(DowntimeRecoveryService.name);

  constructor(
    private readonly dataSource: DataSource,
    @InjectRepository(DowntimeRecoveryCurve)
    private readonly curves: Repository<DowntimeRecoveryCurve>,
  ) {}

  /**
   * Recompute every curve from `attraction_outages`.
   *
   * Delete-then-insert over the whole table rather than an upsert: a park that
   * stops producing intervals must lose its curve, not keep a stale one, and
   * "the rows that should no longer exist" is not something an upsert expresses.
   *
   * @param windowDays - How far back intervals are read. The curve describes
   *   recent behaviour, so a window much longer than the profile's would let a
   *   ride's pre-refurbishment era set the expectation for today.
   */
  async rebuild(windowDays = 180): Promise<void> {
    const generatedAt = new Date();
    const since = new Date(
      generatedAt.getTime() - windowDays * 24 * 60 * 60 * 1000,
    );

    const rows: CurveRow[] = await this.dataSource.query(RECOVERY_CURVE_SQL, [
      since,
      [...RECOVERY_ELAPSED_BUCKETS],
      MIN_PARK_CURVE_SAMPLE,
    ]);

    const toSave = rows.map((row) => ({
      id: `${row.parkId ?? "pooled"}:${row.signal}:${row.elapsedMinutes}`,
      parkId: row.parkId,
      signal: row.signal,
      elapsedMinutes: Number(row.elapsedMinutes),
      atRisk: Number(row.atRisk),
      recoveryWithin30: fixed3(row.recoveryWithin30),
      recoveryWithin60: fixed3(row.recoveryWithin60),
      remainingP25: intOrNull(row.remainingP25),
      remainingMedian: intOrNull(row.remainingMedian),
      remainingP75: intOrNull(row.remainingP75),
      generatedAt,
    }));

    await this.dataSource.transaction(async (manager) => {
      // `delete({})` throws "Empty criteria(s) are not allowed for the delete
      // method" — TypeORM refuses it precisely because it looks like a
      // filtered delete and is not. The whole-table form has to say so.
      // This threw on every nightly run, BullMQ retried three times, and the
      // error never reached the log, so the reconstruction looked healthy while
      // the curves stayed empty.
      await manager
        .createQueryBuilder()
        .delete()
        .from(DowntimeRecoveryCurve)
        .execute();
      for (let i = 0; i < toSave.length; i += 500) {
        await manager
          .getRepository(DowntimeRecoveryCurve)
          .insert(toSave.slice(i, i + 500));
      }
    });

    const pooled = toSave.filter((r) => r.parkId === null).length;
    this.logger.log(
      `📉 Recovery curves rebuilt: ${toSave.length} rows ` +
        `(${toSave.length - pooled} park-level, ${pooled} pooled)`,
    );
  }

  /**
   * The curve to read for one park, park-level where it is dense enough.
   *
   * Returns both, because the choice is per bucket rather than per park: a park
   * can carry its own numbers at fifteen minutes and fall back to the pooled
   * ones at four hours, where only a handful of its spells ever reach.
   */
  async getCurve(parkId: string): Promise<RecoveryCurveSet> {
    const [park, pooled] = await Promise.all([
      this.curves.find({ where: { parkId }, order: { elapsedMinutes: "ASC" } }),
      this.curves.find({
        where: { parkId: IsNull() },
        order: { elapsedMinutes: "ASC" },
      }),
    ]);
    return { park, pooled };
  }
}

export interface RecoveryCurveSet {
  park: DowntimeRecoveryCurve[];
  pooled: DowntimeRecoveryCurve[];
}

interface CurveRow {
  parkId: string | null;
  signal: OutageSignal;
  elapsedMinutes: number | string;
  atRisk: number | string;
  recoveryWithin30: number | string | null;
  recoveryWithin60: number | string | null;
  remainingP25: number | string | null;
  remainingMedian: number | string | null;
  remainingP75: number | string | null;
}

function fixed3(value: number | string | null): string | null {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(3) : null;
}

function intOrNull(value: number | string | null): number | null {
  if (value === null || value === undefined) return null;
  const n = Math.round(Number(value));
  return Number.isFinite(n) ? n : null;
}

/**
 * Kaplan-Meier, in one statement.
 *
 * Parameters: `$1` window start, `$2` bucket edges, `$3` the park-level sample
 * floor.
 *
 * Four things here are load-bearing and easy to "simplify" wrongly:
 *
 * - **The pooled curve is the same events with the park key dropped** (the
 *   `UNION ALL`), never a second query with a different `WHERE`. A serving path
 *   that falls back from a park curve to the pooled one must not silently
 *   change the population under it.
 * - **The survival product is a running sum of logs.** The plain product
 *   underflows over a tail this long; the log form does not.
 * - **Both the probability and the quantiles are read off the same estimator.**
 *   `rec30` is `1 - S(T+30)/S(T)` rather than a separate actuarial count, so the
 *   two numbers a reader sees side by side cannot disagree about one curve.
 *   The earlier actuarial form gave 0.505 against this one's 0.542 at T=5 — both
 *   defensible, but only one of them matches the median printed beside it.
 * - **`operating_minutes`, never `wall_minutes`.** The curve is read against an
 *   elapsed figure computed the same way, and a spell spanning a night has hours
 *   of wall time in which nobody could have repaired or observed anything. This
 *   single choice is what takes censoring from 68 % to 15.6 %.
 *
 * A quantile that the curve never reaches stays NULL. Past about two hours the
 * upper quartile stops resolving, and that is the honest output — a stored large
 * number would read as knowledge.
 */
const RECOVERY_CURVE_SQL = `
  WITH ev_raw AS (
    SELECT a."parkId"          AS park_id,
           o.signal            AS signal,
           o.operating_minutes AS mins,
           (o.end_reason IN ('recovered', 'reclassified')) AS observed
      FROM attraction_outages o
      JOIN attractions a ON a.id = o."attractionId"
     WHERE o.started_at >= $1::timestamptz
       AND NOT o.likely_works_period
       AND o.operating_minutes IS NOT NULL
       -- The quality half of duration_usable, WITHOUT its end-reason half.
       --
       -- This is the correction to a fix that overshot. duration_usable
       -- requires end_reason IN ('recovered','reclassified') — the very
       -- predicate observed uses one line above — so filtering on it made
       -- observed true for every surviving row and removed censoring from the
       -- estimator entirely. Verified against production: 23 748 rows with
       -- duration_usable, all observed; the 4964 censored ones vanished
       -- instead of being held at risk.
       --
       -- That biases exactly the wrong way. An outage still running at
       -- measurement time is disproportionately a LONG one, so dropping it
       -- makes recovery look faster than it is — the failure §6a exists to
       -- avoid.
       --
       -- What is still excluded is the part about measurement quality rather
       -- than outcome: a works period is not an outage, a start-censored spell
       -- has a duration that begins before we were looking, and a spell whose
       -- minutes are mostly carried heartbeat is not a measurement. Those
       -- cannot be rescued by censoring, because their TIME is wrong, not just
       -- their ending.
       -- Reported outages only. A closed_gap interval is stored as recovered by
       -- construction — the statement that produces it only emits one once the
       -- ride is running again — so its curve would have zero censoring and be
       -- survivorship-biased by definition. Nothing reads it (the serving path
       -- hard-codes "no estimate" for that signal), so this is dead work that
       -- would become a live bug the moment someone wired it up.
       AND o.signal = 'down'
       AND NOT o.likely_works_period
       AND NOT o.start_censored
       AND o.operating_minutes > 0
       AND o.observed_operating_minutes::numeric / o.operating_minutes >= 0.5
  ),
  -- The pooled curve is the SAME events with the park key dropped, never a
  -- second query with a different WHERE: a serving path that falls back must
  -- not silently change the population under it.
  ev AS (
    SELECT park_id, signal, mins, observed FROM ev_raw
    UNION ALL
    SELECT NULL::uuid, signal, mins, observed FROM ev_raw
  ),
  -- Kaplan-Meier needs, per distinct duration: how many ended there, and how
  -- many were still at risk when it arrived.
  agg AS (
    SELECT park_id, signal, mins,
           COUNT(*) FILTER (WHERE observed)::int AS d,
           COUNT(*)::int                         AS leaving
      FROM ev GROUP BY park_id, signal, mins
  ),
  risk AS (
    SELECT park_id, signal, mins, d,
           SUM(leaving) OVER (PARTITION BY park_id, signal ORDER BY mins DESC
                              ROWS UNBOUNDED PRECEDING) AS n_at_risk
      FROM agg
  ),
  km AS (
    SELECT park_id, signal, mins, n_at_risk,
           -- Product of (1 - d/n) as a running sum of logs: the product form
           -- underflows on long tails, the log form does not.
           EXP(SUM(LN(GREATEST(1.0 - d::numeric / NULLIF(n_at_risk, 0), 1e-12)))
               OVER (PARTITION BY park_id, signal ORDER BY mins
                     ROWS UNBOUNDED PRECEDING)) AS surv
      FROM risk
  ),
  buckets AS (SELECT UNNEST($2::int[]) AS t),
  -- S(T): the survival still standing when the bucket edge is reached.
  anchors AS (
    SELECT b.t, k.park_id, k.signal,
           (SELECT k2.surv FROM km k2
             WHERE k2.park_id IS NOT DISTINCT FROM k.park_id
               AND k2.signal = k.signal AND k2.mins <= b.t
             ORDER BY k2.mins DESC LIMIT 1) AS s_at_t,
           (SELECT k3.n_at_risk FROM km k3
             WHERE k3.park_id IS NOT DISTINCT FROM k.park_id
               AND k3.signal = k.signal AND k3.mins >= b.t
             ORDER BY k3.mins ASC LIMIT 1) AS at_risk
      FROM buckets b CROSS JOIN (SELECT DISTINCT park_id, signal FROM km) k
  ),
  -- Conditional quantiles: the first duration at which the surviving fraction
  -- has fallen to (1-q) of what it was at T. NULL when it never does — which is
  -- the honest answer past roughly four hours, and better than a large number
  -- that reads as knowledge.
  quantiles AS (
    SELECT a.t, a.park_id, a.signal, a.at_risk, a.s_at_t,
           (SELECT MIN(k.mins) - a.t FROM km k
             WHERE k.park_id IS NOT DISTINCT FROM a.park_id AND k.signal = a.signal
               AND k.mins >= a.t AND k.surv <= a.s_at_t * 0.75) AS rem_p25,
           (SELECT MIN(k.mins) - a.t FROM km k
             WHERE k.park_id IS NOT DISTINCT FROM a.park_id AND k.signal = a.signal
               AND k.mins >= a.t AND k.surv <= a.s_at_t * 0.50) AS rem_p50,
           (SELECT MIN(k.mins) - a.t FROM km k
             WHERE k.park_id IS NOT DISTINCT FROM a.park_id AND k.signal = a.signal
               AND k.mins >= a.t AND k.surv <= a.s_at_t * 0.25) AS rem_p75,
           -- Recovery share over a horizon is 1 - S(T+H)/S(T), read off the
           -- same estimator, so the probability and the quantiles cannot
           -- disagree about the same curve.
           1 - COALESCE((SELECT k.surv FROM km k
             WHERE k.park_id IS NOT DISTINCT FROM a.park_id AND k.signal = a.signal
               AND k.mins <= a.t + 30
             ORDER BY k.mins DESC LIMIT 1), a.s_at_t) / NULLIF(a.s_at_t, 0) AS rec30,
           1 - COALESCE((SELECT k.surv FROM km k
             WHERE k.park_id IS NOT DISTINCT FROM a.park_id AND k.signal = a.signal
               AND k.mins <= a.t + 60
             ORDER BY k.mins DESC LIMIT 1), a.s_at_t) / NULLIF(a.s_at_t, 0) AS rec60
      FROM anchors a
     WHERE a.s_at_t IS NOT NULL AND a.at_risk IS NOT NULL
  )
  SELECT park_id                       AS "parkId",
         signal                        AS "signal",
         t                             AS "elapsedMinutes",
         at_risk                       AS "atRisk",
         ROUND(rec30::numeric, 3)      AS "recoveryWithin30",
         ROUND(rec60::numeric, 3)      AS "recoveryWithin60",
         rem_p25                       AS "remainingP25",
         rem_p50                       AS "remainingMedian",
         rem_p75                       AS "remainingP75"
    FROM quantiles
   WHERE park_id IS NULL OR at_risk >= $3::int
   ORDER BY park_id NULLS FIRST, signal, t
`;

/**
 * The statement, exported for the spec beside it.
 *
 * Not exported for any other use: the service is the only thing that should run
 * it. A spec asserting on the SQL text is worth the export, because the two
 * predicates it pins were once made accidentally identical and nothing noticed.
 */
export const RECOVERY_CURVE_SQL_FOR_TEST = RECOVERY_CURVE_SQL;
