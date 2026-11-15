import { DataSource } from "typeorm";
import { queryWithLimits } from "../../common/utils/statement-limits.util";
import {
  SEASON_LEVEL_KINDS,
  SEASON_LEVEL_STATUSES,
  type RunningSeason,
  type SeasonReference,
} from "./season-level.util";

/** One read each; a slow one costs the season level, never the response. */
const LIMITS = { statementTimeoutMs: 20_000, lockTimeoutMs: 2_000 };

export interface SeasonLevelInputs {
  seasons: RunningSeason[];
  references: SeasonReference[];
}

/**
 * The park's running seasons and, per ride, what it measured on their days so
 * far — the two inputs of `applySeasonLevel`.
 *
 * A season runs when today (park-local) lies inside its bounds. The measured
 * day value is TFT's target statistic (`nf-service` `fetch_daily_peak_panel`):
 * the P90 of the day's OPERATING STANDBY waits of at least 5 minutes, no
 * heartbeat rows, at least three readings. Today is left out: a running day's
 * P90 is not a day's P90.
 */
export async function loadSeasonLevelInputs(
  dataSource: DataSource,
  parkId: string,
  timezone: string,
): Promise<SeasonLevelInputs> {
  const seasons = await queryWithLimits<RunningSeason[]>(
    dataSource,
    `SELECT id, start_date::text AS "startDate", end_date::text AS "endDate", dates
       FROM park_seasons
      WHERE park_id = $1::uuid
        AND kind = ANY($2::text[])
        AND status = ANY($3::text[])
        AND start_date <= (now() AT TIME ZONE $4)::date
        AND end_date >= (now() AT TIME ZONE $4)::date
      ORDER BY start_date, id`,
    [parkId, [...SEASON_LEVEL_KINDS], [...SEASON_LEVEL_STATUSES], timezone],
    LIMITS,
  );
  if (seasons.length === 0) return { seasons, references: [] };

  const rows = await queryWithLimits<
    Array<{
      seasonId: string;
      attractionId: string;
      weekend: boolean | null;
      days: number;
      level: number;
    }>
  >(
    dataSource,
    `WITH observed AS (
       SELECT s.id AS season_id, d::date AS day
         FROM park_seasons s
        CROSS JOIN LATERAL (
          SELECT jsonb_array_elements_text(s.dates)::date AS d
           WHERE jsonb_array_length(COALESCE(s.dates, '[]'::jsonb)) > 0
          UNION ALL
          SELECT generate_series(s.start_date, s.end_date, interval '1 day')::date
           WHERE jsonb_array_length(COALESCE(s.dates, '[]'::jsonb)) = 0
        ) AS days
        WHERE s.id = ANY($1::uuid[])
          AND d::date < (now() AT TIME ZONE $3)::date
     ), peaks AS (
       SELECT qd."attractionId" AS attraction_id,
              (qd.timestamp AT TIME ZONE $3)::date AS day,
              percentile_cont(0.9) WITHIN GROUP (ORDER BY qd."waitTime") AS peak
         FROM queue_data qd
         JOIN attractions a ON a.id = qd."attractionId"
        WHERE a."parkId" = $2::uuid
          AND qd.timestamp >= (SELECT min(day) FROM observed)::timestamp AT TIME ZONE $3
          AND qd.timestamp < now()
          AND NOT COALESCE(qd.is_heartbeat, false)
          AND qd.status = 'OPERATING'
          AND qd."queueType" = 'STANDBY'
          AND qd."waitTime" >= 5
        GROUP BY 1, 2
       HAVING count(*) >= 3
     ), measured AS (
       SELECT o.season_id, p.attraction_id, p.peak,
              extract(isodow FROM p.day) >= 6 AS weekend
         FROM peaks p
         JOIN observed o ON o.day = p.day
     )
     SELECT season_id AS "seasonId", attraction_id AS "attractionId", weekend,
            count(*)::int AS days,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY peak)::float AS level
       FROM measured GROUP BY 1, 2, 3
     UNION ALL
     SELECT season_id, attraction_id, NULL::boolean,
            count(*)::int,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY peak)::float
       FROM measured GROUP BY 1, 2`,
    [seasons.map((s) => s.id), parkId, timezone],
    LIMITS,
  );
  return {
    seasons,
    references: rows.map((r) => ({ ...r, level: Number(r.level) })),
  };
}
