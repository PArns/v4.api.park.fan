/**
 * The nightly VACUUM window (PAR-868).
 *
 * On 2026-10-12 at 06:15 UTC the kernel OOM-killed the CatBoost training (the
 * 06:00 `ml-training/train-model` job, an ml-service subprocess that peaks at
 * ~13 GB) on the shared 29 GB host. Postgres then ran with ten autovacuum
 * workers, each allowed `maintenance_work_mem` = 2 GB. The owner's call: put
 * the vacuuming of the high-churn tables BEFORE the training, so autovacuum has
 * nothing large left to do inside it.
 *
 * Everything time-related is UTC on purpose: the training cron is UTC, and the
 * point of the window is to end before it.
 */

const MINUTE_MS = 60 * 1000;

/** Cron of the job: 04:50 UTC, after the 04:00–04:45 batch has started. */
export const DB_VACUUM_CRON = "50 4 * * *";

/** First minute of the day (UTC) a table may be started. */
export const VACUUM_WINDOW_START_MINUTE = 4 * 60 + 50; // 04:50
/** No table is STARTED at or after this minute (UTC). */
export const VACUUM_LAST_START_MINUTE = 5 * 60 + 50; // 05:50
/**
 * No VACUUM is still RUNNING at this minute (UTC): each statement's timeout is
 * capped so it ends here at the latest, two minutes before the 06:00 training.
 */
export const VACUUM_HARD_DEADLINE_MINUTE = 5 * 60 + 58; // 05:58

/** How long the job waits for a table lock before skipping the table. */
export const VACUUM_LOCK_TIMEOUT = "5s";

/**
 * Work memory of the vacuum session. A manual VACUUM uses
 * `maintenance_work_mem` (2 GB on production), not `autovacuum_work_mem`
 * (256 MB since 2026-10-12) — so without this the job would bring back the
 * very allocation it exists to keep away from the training.
 */
export const VACUUM_MAINTENANCE_WORK_MEM = "256MB";

export interface VacuumTarget {
  /** Table name in `public`. */
  table: string;
  /** Upper bound of one VACUUM on this table, before the deadline cap. */
  maxStatementMs: number;
  /** Why it is on the list — the measurement it was picked by. */
  reason: string;
}

/**
 * The tables, in the order they run: the worst offender first, so a short
 * window still gets the one that matters. Measured read-only on production on
 * 2026-10-12 (`pg_stat_user_tables`, stats never reset):
 *
 * | table               | live    | dead   | autovacuums | size  |
 * |---------------------|---------|--------|-------------|-------|
 * | pcn_forecasts       | 122.1 M | 9.46 M | 143         | 28 GB |
 * | prediction_accuracy | 16.2 M  | 1.11 M | 5 320       | 6.3 GB|
 * | shape_forecasts     | 102.6 M | 2.40 M | 119         | 19 GB | (retired with PAR-870, not vacuumed)
 * | tft_forecasts       | 24.4 M  | 0.49 M | 37          | 3.4 GB|
 * | holidays            | 0.20 M  | 34.8 k | 192         | 164 MB|
 *
 * Deliberately NOT on it:
 * - Hypertables (`wait_time_predictions`, 166 k dead over 14 chunks;
 *   `queue_data`, 44 k). Their dead rows sit in a handful of small uncompressed
 *   chunks that autovacuum handles cheaply, and a manual VACUUM does not yield
 *   to a conflicting lock the way autovacuum does — so a TimescaleDB
 *   compression policy landing on a chunk mid-VACUUM would queue behind it, the
 *   lock-queue shape of the 2026-09-28 outage. The policies' 12-hour schedules
 *   drift, so "they run at another time" is not a guarantee.
 * - `forecast_archive_*`, `prediction_lead_snapshots`, `catboost_daily_forecasts`:
 *   0–5.5 k dead tuples, insert-only or nearly so. Nothing for VACUUM to win.
 */
export const VACUUM_TARGETS: readonly VacuumTarget[] = [
  {
    table: "pcn_forecasts",
    maxStatementMs: 30 * MINUTE_MS,
    reason: "9.46 M dead / 122 M live, ~10^7 rows rewritten a day",
  },
  {
    table: "prediction_accuracy",
    maxStatementMs: 15 * MINUTE_MS,
    reason: "1.11 M dead, 5 320 autovacuums (2.37 B updates)",
  },
  {
    table: "tft_forecasts",
    maxStatementMs: 10 * MINUTE_MS,
    reason: "0.49 M dead / 24 M live, 24 M updates",
  },
  {
    table: "holidays",
    maxStatementMs: 5 * MINUTE_MS,
    reason: "14.8 % dead (34.8 k), 22 M updates",
  },
];

function minuteOfDayUtc(now: Date): number {
  return now.getUTCHours() * 60 + now.getUTCMinutes();
}

/**
 * Whether a new table may be started at `now`: only inside [04:50, 05:50) UTC.
 * Outside the window covers both the hard stop and a run Bull delivers late (a
 * retry, a stalled job after a deploy), which must not drift into 06:00.
 */
export function mayStartVacuum(now: Date): boolean {
  const m = minuteOfDayUtc(now);
  return m >= VACUUM_WINDOW_START_MINUTE && m < VACUUM_LAST_START_MINUTE;
}

/**
 * The `statement_timeout` for one VACUUM started at `now`: the table's own
 * bound, capped so the statement ends by 05:58 UTC. Never below one second
 * (Postgres reads 0 as "no limit").
 */
export function vacuumStatementTimeoutMs(
  now: Date,
  maxStatementMs: number,
): number {
  const deadline = Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate(),
    0,
    VACUUM_HARD_DEADLINE_MINUTE,
  );
  const untilDeadline = deadline - now.getTime();
  return Math.max(1000, Math.min(maxStatementMs, untilDeadline));
}

/** Double-quotes an identifier the way `quote_ident` does. */
export function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}
