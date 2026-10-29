import { MAX_PLAUSIBLE_WAIT_TIME } from "../../common/utils/wait-time.utils";
import { scoredSlotSql } from "../entities/prediction-accuracy.entity";
import { driftSql } from "./ml-drift-watch.util";

/** The statement `MlDecisionWatchService.computeDrift` ran before it was shared. */
const WATCH_SQL = `WITH r AS (
         SELECT pa.attraction_id AS a,
                date_trunc('hour', pa.target_time AT TIME ZONE p.timezone) AS lt,
                pa.target_time >= $1::timestamptz - interval '7 days' AS recent,
                pa.target_time >= $1::timestamptz - interval '35 days' AS judged,
                pa.actual_wait_time AS actual, pa.absolute_error AS err,
                pa.served_source AS src, pa.served_absolute_error AS serr,
                pa.predicted_q80 AS q80, pa.predicted_q95 AS q95
           FROM prediction_accuracy pa
           JOIN attractions att ON att.id = pa.attraction_id
           JOIN parks p ON p.id = att."parkId"
          WHERE pa.comparison_status = 'COMPLETED'
            AND pa.target_time >= $1::timestamptz - interval '63 days'
            AND pa.target_time < $1::timestamptz
            AND pa."predictionType" = 'hourly'
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
         SELECT r.recent, m.model, m.err, m.withq, r.actual, r.q80, r.q95, b.b
           FROM r
           LEFT JOIN base b ON b.a = r.a AND b.lt = r.lt
          CROSS JOIN LATERAL (VALUES
                ('catboost', r.err, true),
                (CASE WHEN r.src IN ('pcn', 'chronos') THEN r.src END, r.serr, false)
          ) m(model, err, withq)
          WHERE r.judged AND m.model IS NOT NULL AND m.err IS NOT NULL
       )
       SELECT model, recent,
              count(*) AS n, sum(err) AS sae,
              count(b) AS "nBase",
              sum(err) FILTER (WHERE b IS NOT NULL) AS "saeOnBase",
              sum(abs(actual - b)) AS "saeBase",
              count(*) FILTER (WHERE withq AND q80 IS NOT NULL AND q95 IS NOT NULL) AS "nQ",
              count(*) FILTER (WHERE withq AND q80 IS NOT NULL AND q95 IS NOT NULL AND actual <= q80) AS hit80,
              count(*) FILTER (WHERE withq AND q80 IS NOT NULL AND q95 IS NOT NULL AND actual <= q95) AS hit95
         FROM e
        GROUP BY model, recent`;

describe("driftSql", () => {
  it("without options is the daily watch's statement, character for character", () => {
    expect(driftSql()).toBe(WATCH_SQL);
  });

  it("groups by park, restricts to a park uuid parameter and a model", () => {
    const sql = driftSql({
      recentDays: 14,
      judgedDays: 28,
      fetchDays: 56,
      group: "ride",
      parkFilter: true,
      modelFilter: true,
    });
    expect(sql).toContain(`att."parkId" = $2::uuid`);
    expect(sql).toContain("m.model = $3");
    expect(sql).toContain("interval '56 days'");
    expect(sql).toContain("att.id::text AS k");
  });
});
