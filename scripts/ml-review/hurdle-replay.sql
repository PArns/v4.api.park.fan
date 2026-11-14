-- PAR-1335: what would a hurdle buy on rides that rarely have a queue?
--
--   scripts/prod-psql.sh < scripts/ml-review/hurdle-replay.sql
--
-- Read-only. Raw CatBoost hourly, scored slots (real zeros included), last 14
-- days. p_queue is a ride's share of scored slots with a wait >= 5 in the 14
-- days BEFORE those (ex ante). The hurdle serves 0 where p_queue < 0.5 — the
-- median of a wait that is absent more often than not — and the stored
-- forecast everywhere else.
--
-- Read `rarely_queues = t` for the gain (quiet) and the price (mid, busy), and
-- `rarely_queues = f` to confirm nothing else moves. Segments are by actual
-- wait, as on the PCN board. The same query is on
-- docs/ml/accuracy-scoring-rule.md.
WITH scored AS (
  SELECT attraction_id, target_time,
         predicted_wait_time AS pred, actual_wait_time AS actual
  FROM prediction_accuracy
  WHERE "predictionType" = 'hourly' AND comparison_status = 'COMPLETED'
    AND score_class = 'scored' AND actual_wait_time <= 400
    AND target_time >= now() - interval '28 days'
), ride AS (
  SELECT attraction_id, avg((actual >= 5)::int) AS p_queue
  FROM scored
  WHERE target_time < now() - interval '14 days'
  GROUP BY 1 HAVING count(*) >= 50
), replay AS (
  SELECT s.pred, s.actual, r.p_queue < 0.5 AS rarely_queues,
         CASE WHEN r.p_queue < 0.5 THEN 0 ELSE s.pred END AS hurdle
  FROM scored s JOIN ride r USING (attraction_id)
  WHERE s.target_time >= now() - interval '14 days'
)
SELECT rarely_queues,
       CASE WHEN actual >= 60 THEN 'busy' WHEN actual >= 30 THEN 'mid' ELSE 'quiet' END AS segment,
       count(*) AS n,
       round(avg(abs(pred - actual)), 2)   AS mae_served,
       round(avg(pred - actual), 2)        AS bias_served,
       round(avg(abs(hurdle - actual)), 2) AS mae_hurdle,
       round(avg(hurdle - actual), 2)      AS bias_hurdle
FROM replay
GROUP BY 1, 2 ORDER BY 1, 2;

-- How many rides and how much of the scored volume the rule touches.
WITH scored AS (
  SELECT attraction_id, target_time, actual_wait_time AS actual
  FROM prediction_accuracy
  WHERE "predictionType" = 'hourly' AND comparison_status = 'COMPLETED'
    AND score_class = 'scored' AND actual_wait_time <= 400
    AND target_time >= now() - interval '28 days'
), ride AS (
  SELECT attraction_id, avg((actual >= 5)::int) AS p_queue, count(*) AS n
  FROM scored
  WHERE target_time < now() - interval '14 days'
  GROUP BY 1 HAVING count(*) >= 50
)
SELECT width_bucket(p_queue, 0, 1.0001, 10) AS p_queue_decile,
       count(*) AS rides, sum(n) AS slots
FROM ride GROUP BY 1 ORDER BY 1;
