-- PAR-1116: is there enough curated park_seasons history to feed a model, and
-- how much busier is a park on its event days?
--
--   scripts/prod-psql.sh < scripts/ml-review/park-season-coverage.sql
--
-- Read-only. An event day is a day of a season of a binding kind (halloween,
-- christmas, winter, summer_nights, special_event) that is not cancelled: one
-- of its `dates` when it lists them, else every day between its bounds. Only
-- past days count, from the first queue_data row (2025-12-24).

-- 0. Inventory: what is curated at all, and in which shape. A season is either
-- a list of single days (`dates`), a plain range (every day between its
-- bounds counts, which over-claims a weekends-only event), or a range whose
-- bounds are only projected (`expected`). A model may read the first as fact,
-- the second with care, the third not as fact. `separate_ticket` and event
-- hours (`opens_at` / `closes_at`) mark evening events that do not load the
-- daytime queues the way an all-day season does.
SELECT kind, status,
       CASE WHEN dates IS NOT NULL THEN 'single days' ELSE 'range' END AS shape,
       separate_ticket,
       (opens_at IS NOT NULL OR closes_at IS NOT NULL) AS has_event_hours,
       count(*) AS seasons,
       count(DISTINCT park_id) AS parks,
       count(*) FILTER (WHERE end_date < current_date) AS ended,
       count(*) FILTER (WHERE start_date <= current_date AND end_date >= current_date) AS running,
       count(*) FILTER (WHERE start_date > current_date) AS ahead,
       count(*) FILTER (WHERE jsonb_array_length(COALESCE(attraction_ids, '[]'::jsonb)) > 0) AS with_bound_rides
FROM park_seasons
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3;

-- 0b. The same per park, for the kinds that drive waits: which parks a model
-- could learn from, and since when.
SELECT p.name AS park, s.kind,
       count(*) AS seasons,
       min(s.start_date) AS first_start, max(s.end_date) AS last_end,
       count(*) FILTER (WHERE s.dates IS NOT NULL) AS as_single_days,
       count(*) FILTER (WHERE s.status = 'expected') AS expected_only
FROM park_seasons s JOIN parks p ON p.id = s.park_id
WHERE s.kind IN ('halloween', 'christmas', 'winter', 'summer_nights', 'special_event')
  AND s.status <> 'cancelled'
GROUP BY 1, 2 ORDER BY 2, 1;

-- 1. Coverage: seasons, parks and past event days per kind.
WITH season_days AS (
  SELECT s.park_id, s.kind, d::date AS day
  FROM park_seasons s
  CROSS JOIN LATERAL (
    SELECT jsonb_array_elements_text(s.dates)::date AS d WHERE s.dates IS NOT NULL
    UNION ALL
    SELECT generate_series(s.start_date, s.end_date, interval '1 day')::date
    WHERE s.dates IS NULL
  ) AS days
  WHERE s.kind IN ('halloween', 'christmas', 'winter', 'summer_nights', 'special_event')
    AND s.status <> 'cancelled'
)
SELECT kind,
       count(DISTINCT park_id) AS parks,
       count(DISTINCT (park_id, day)) FILTER (WHERE day < current_date AND day >= '2025-12-24') AS past_event_days,
       count(DISTINCT (park_id, day)) FILTER (WHERE day >= current_date) AS future_event_days
FROM season_days
GROUP BY 1 ORDER BY 3 DESC;

-- 2. Lift: the headliner day value (mean over headliners of the day's highest
-- hourly P90) on event days against the same park's other days of the same
-- month and day type. A ratio near 1 says the season carries no level signal.
WITH season_days AS (
  SELECT DISTINCT s.park_id, s.kind, d::date AS day
  FROM park_seasons s
  CROSS JOIN LATERAL (
    SELECT jsonb_array_elements_text(s.dates)::date AS d WHERE s.dates IS NOT NULL
    UNION ALL
    SELECT generate_series(s.start_date, s.end_date, interval '1 day')::date
    WHERE s.dates IS NULL
  ) AS days
  WHERE s.kind IN ('halloween', 'christmas', 'winter', 'summer_nights', 'special_event')
    AND s.status <> 'cancelled'
), ride_day AS (
  SELECT h."parkId" AS park_id, (q.hour AT TIME ZONE p.timezone)::date AS day,
         q."attractionId", max(q.p90) AS peak
  FROM headliner_attractions h
  JOIN parks p ON p.id = h."parkId"
  JOIN queue_data_aggregates q
    ON q."attractionId" = h."attractionId"::text
  WHERE h."parkId" IN (SELECT park_id FROM season_days)
    AND q.hour >= '2025-12-24' AND q."sampleCount" >= 2
  GROUP BY 1, 2, 3
), park_day AS (
  SELECT park_id, day, avg(peak) AS day_value
  FROM ride_day GROUP BY 1, 2 HAVING avg(peak) > 0
), labelled AS (
  SELECT pd.park_id, pd.day, pd.day_value,
         date_trunc('month', pd.day) AS month,
         extract(isodow FROM pd.day) >= 6 AS weekend,
         sd.kind
  FROM park_day pd
  LEFT JOIN season_days sd ON sd.park_id = pd.park_id AND sd.day = pd.day
), cells AS (
  -- One cell per park, month and day type that holds both kinds of day.
  SELECT e.kind, e.park_id, e.month, e.weekend,
         avg(e.day_value) AS event_value, count(*) AS event_days,
         (SELECT avg(o.day_value) FROM labelled o
           WHERE o.park_id = e.park_id AND o.month = e.month
             AND o.weekend = e.weekend AND o.kind IS NULL) AS other_value,
         (SELECT count(*) FROM labelled o
           WHERE o.park_id = e.park_id AND o.month = e.month
             AND o.weekend = e.weekend AND o.kind IS NULL) AS other_days
  FROM labelled e
  WHERE e.kind IS NOT NULL
  GROUP BY 1, 2, 3, 4
)
SELECT kind,
       count(DISTINCT park_id) AS parks,
       sum(event_days) AS event_days,
       sum(other_days) AS comparison_days,
       round(avg(event_value)::numeric, 1) AS mean_event_value,
       round(avg(other_value)::numeric, 1) AS mean_other_value,
       round((sum(event_value * event_days) / nullif(sum(other_value * event_days), 0))::numeric, 2) AS lift
FROM cells
WHERE other_days >= 2
GROUP BY 1 ORDER BY 3 DESC;
