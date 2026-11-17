import { DataSource } from "typeorm";
import { queryWithLimits } from "../../common/utils/statement-limits.util";
import { addIsoDays, eachIsoDate } from "../../common/utils/date.util";
import type { HolidayRow } from "../../holidays/holidays.service";
import {
  SEASON_LEVEL_KINDS,
  SEASON_LEVEL_STATUSES,
  seasonDayTypes,
  seasonReferences,
  type MeasuredSeasonDay,
  type RunningSeason,
  type SeasonDayTypes,
  type SeasonPark,
  type SeasonReference,
  type SeasonSchoolCoverage,
} from "./season-level.util";

/** One read each; a slow one costs the season level, never the response. */
const LIMITS = { statementTimeoutMs: 20_000, lockTimeoutMs: 2_000 };

export interface SeasonLevelInputs {
  seasons: RunningSeason[];
  /** The types of every running season's days (`seasonDayTypes`). */
  dayTypes: SeasonDayTypes;
  references: SeasonReference[];
}

/** What the season level reads about the park's holidays. */
export interface SeasonHolidaySource {
  /**
   * The holiday rows of the park's country and of every country among its
   * influencing regions, from `from` to `to`, both included.
   */
  holidays(from: string, to: string): Promise<HolidayRow[]>;
  /**
   * How far the school breaks of the park's region and of its influencing
   * regions are stored (`seasonSchoolCoverage`).
   */
  schoolCoverage(): Promise<SeasonSchoolCoverage>;
}

/**
 * The live parks with a season of the season level running on their own today, the same
 * definition {@link loadSeasonLevelInputs} reads for one park.
 */
export async function parksWithRunningSeason(
  dataSource: DataSource,
): Promise<string[]> {
  const rows = await queryWithLimits<Array<{ parkId: string }>>(
    dataSource,
    `SELECT DISTINCT s.park_id AS "parkId"
       FROM park_seasons s
       JOIN parks p ON p.id = s.park_id
      WHERE s.kind = ANY($1::text[])
        AND s.status = ANY($2::text[])
        AND p.retired_at IS NULL
        AND s.start_date <= (now() AT TIME ZONE p.timezone)::date
        AND s.end_date >= (now() AT TIME ZONE p.timezone)::date`,
    [[...SEASON_LEVEL_KINDS], [...SEASON_LEVEL_STATUSES]],
    LIMITS,
  );
  return rows.map((row) => row.parkId);
}

/**
 * The park's running seasons, the types of their days and, per ride and day
 * type, what the ride measured on their latest days — the inputs of
 * `applySeasonLevel`.
 *
 * A season runs when today (park-local) lies inside its bounds on the day of
 * the read; the caller keeps the inputs from one calendar warmup to the next.
 * The measured day value is TFT's target statistic (`nf-service`
 * `fetch_daily_peak_panel`): the P90 of the day's OPERATING STANDBY waits of
 * at least 5 minutes, no heartbeat rows, at least three readings. Today is
 * left out: a running day's P90 is not a day's P90. Beside it, TFT's freshest
 * forecast of the day with a `forecast_date` before it, one lookup on the
 * primary key of `tft_forecasts` per measured day; `seasonReferences` turns
 * both into the references.
 *
 * A failed holiday read fails the whole load, because without it every
 * holiday would count as a working day.
 */
export async function loadSeasonLevelInputs(
  dataSource: DataSource,
  park: SeasonPark & { id: string },
  holidaySource: SeasonHolidaySource,
): Promise<SeasonLevelInputs> {
  const { id: parkId, timezone } = park;
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
  if (seasons.length === 0) {
    return {
      seasons,
      dayTypes: { daysOff: [], neighbourDays: [], eves: [], unknownDays: [] },
      references: [],
    };
  }

  const bounds = seasons
    .flatMap((s) => [s.startDate, s.endDate, ...(s.dates ?? [])])
    .sort();
  const [first, last] = [bounds[0], bounds[bounds.length - 1]];
  const dayTypes = seasonDayTypes(
    eachIsoDate(first, last),
    await holidaySource.holidays(addIsoDays(first, -3), addIsoDays(last, 4)),
    park,
    await holidaySource.schoolCoverage(),
  );

  const rows = await queryWithLimits<MeasuredSeasonDay[]>(
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
     )
     SELECT o.season_id AS "seasonId", p.attraction_id AS "attractionId",
            p.day::text AS day, p.peak::float AS peak,
            f.predicted_peak::float AS forecast
       FROM peaks p
       JOIN observed o ON o.day = p.day
       LEFT JOIN LATERAL (
         SELECT tf.predicted_peak
           FROM tft_forecasts tf
          WHERE tf.attraction_id = p.attraction_id
            AND tf.target_date = p.day
            AND tf.forecast_date < p.day
          ORDER BY tf.forecast_date DESC
          LIMIT 1
       ) f ON true`,
    [seasons.map((s) => s.id), parkId, timezone],
    LIMITS,
  );
  return {
    seasons,
    dayTypes,
    references: seasonReferences(
      rows.map((r) => ({
        ...r,
        peak: Number(r.peak),
        forecast: r.forecast === null ? null : Number(r.forecast),
      })),
      dayTypes,
    ),
  };
}
