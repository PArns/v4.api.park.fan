/**
 * The hypertables this schema has, as data.
 *
 * `TimescaleInitService` turns these into `create_hypertable` and
 * `ALTER TABLE … SET (timescaledb.compress …)` at boot — production's schema is
 * whatever this list says, because the repo owns no migration for it.
 *
 * It is a list rather than a sequence of calls because the E2E schema needs the
 * same shape and had been keeping its own copy. `synchronize: true` in
 * `test/global-setup.ts` makes every one of these a PLAIN table, so the suite
 * has to convert them itself, and a hand-written list there drifted from this
 * one twice: PAR-172 found `show_live_data` and `restaurant_live_data` plain
 * while the merge's decompression cap was lifted for them, and PAR-234 found
 * the remaining four. Both times the tables existed, so every `to_regclass`
 * guard passed and the cases measured a plain table.
 *
 * This is the same rule `test/helpers/ml-forecast-tables.ts` follows for the
 * DDL of the tables no entity owns — read the definition from whoever issues
 * it, never copy it. There the writers are Python files and it takes a text
 * scan; here the writer is this module, so the test imports it.
 *
 * Adding a hypertable means adding a line here, and nothing else: the service
 * creates it in production, `global-setup.ts` creates it in the test schema,
 * and `park-merge.e2e-spec.ts` asserts its shape.
 */
export interface HypertableSpec {
  /** Physical table name. */
  readonly table: string;
  /** Partitioning column. Has to be part of the table's primary key. */
  readonly timeColumn: string;
  /**
   * `chunk_time_interval`, as a Postgres interval literal. These are the values
   * production runs (measured 2026-09-17 against
   * `timescaledb_information.dimensions`), so a change here repartitions new
   * chunks on the next deploy.
   */
  readonly chunkInterval: string;
  /** Chunks older than this are compressed by the policy. */
  readonly compressAfterDays: number;
  /** What the table holds — shown in the boot log. */
  readonly description: string;
  /**
   * `timescaledb.compress_segmentby`. Among the tables a merge writes, only
   * `wait_time_predictions` has one, and the absence on the others is
   * load-bearing rather than a gap: segmenting by a column a merge REWRITES
   * lets TimescaleDB decompress that one segment instead of every batch the
   * rows sit in, which is an easier case than the one production runs. The
   * site statistic's tables have one because no merge writes them.
   */
  readonly segmentBy?: string;
  /** `timescaledb.compress_orderby`, column plus direction. */
  readonly orderBy?: string;
}

export const HYPERTABLES: ReadonlyArray<HypertableSpec> = [
  {
    table: "queue_data",
    timeColumn: "timestamp",
    chunkInterval: "1 day",
    compressAfterDays: 30,
    description: "Hourly wait time data",
  },
  {
    table: "forecast_data",
    timeColumn: "createdAt",
    chunkInterval: "1 day",
    compressAfterDays: 7,
    description: "Forecast predictions",
  },
  {
    table: "weather_data",
    timeColumn: "date",
    chunkInterval: "7 days",
    compressAfterDays: 60,
    description: "Daily weather data",
  },
  {
    table: "show_live_data",
    timeColumn: "timestamp",
    chunkInterval: "1 day",
    compressAfterDays: 30,
    description: "Show live status and showtimes",
  },
  {
    table: "restaurant_live_data",
    timeColumn: "timestamp",
    chunkInterval: "1 day",
    compressAfterDays: 30,
    description: "Dining availability and wait times",
  },
  {
    table: "queue_data_aggregates",
    timeColumn: "hour",
    chunkInterval: "7 days",
    compressAfterDays: 30,
    description: "Hourly percentile aggregates",
  },
  {
    table: "wait_time_predictions",
    timeColumn: "createdAt",
    chunkInterval: "7 days",
    compressAfterDays: 14,
    description: "ML Predictions",
    segmentBy: "attractionId",
    orderBy: "predictedTime ASC",
  },
  // The site statistic (PAR-1151): insert-only, one row per dimension
  // combination and flush window, bucketed by the UTC hour. Week chunks, so a
  // chunk is compressed a week after it closes and dropped whole after 400
  // days by the nightly `site-analytics` / `drop-old-chunks` job, never
  // deleted from.
  {
    table: "site_pageviews",
    timeColumn: "bucket",
    chunkInterval: "7 days",
    compressAfterDays: 7,
    description: "Site statistic: page views",
    segmentBy: "locale",
    orderBy: "bucket DESC",
  },
  {
    table: "site_events",
    timeColumn: "bucket",
    chunkInterval: "7 days",
    compressAfterDays: 7,
    description: "Site statistic: events",
    segmentBy: "name",
    orderBy: "bucket DESC",
  },
  {
    table: "site_analytics_flushes",
    timeColumn: "bucket",
    chunkInterval: "30 days",
    compressAfterDays: 30,
    description: "Site statistic: written flush windows",
  },
  // Google Search Console (PAR-1160): insert-only, a batch per import of a
  // day, the reports read the newest batch of each day. Month chunks,
  // compressed 35 days after the day (a day is imported again for the first
  // few days) and dropped whole past 500 days by the nightly
  // `search-console` / `drop-old-chunks` job. One compressed segment per
  // batch, so the reports' `batch_id = ANY(…)` skips the batches that a
  // re-import replaced.
  {
    table: "search_console_rows",
    timeColumn: "day",
    chunkInterval: "30 days",
    compressAfterDays: 35,
    description: "Search Console: queries and pages by day",
    segmentBy: "batch_id",
    orderBy: "page ASC",
  },
];

/** The site statistic's hypertables, whose chunks its nightly job drops. */
export const SITE_ANALYTICS_HYPERTABLES = [
  "site_pageviews",
  "site_events",
  "site_analytics_flushes",
] as const;

/**
 * `wait_time_predictions` chunks whose whole range is older than this many days
 * are dropped by the nightly `cleanup-old` job
 * (`MLService.dropExpiredPredictionChunks`).
 *
 * It is a backstop: the row cleanup already removes hourly rows after 9 days
 * and daily rows 90 days past their target, so a chunk this old is near-empty.
 * It used to be a TimescaleDB retention policy and is not one any more —
 * `drop_chunks` locks every table the hypertable references (`attractions`)
 * ACCESS EXCLUSIVE, and the policy had no way to give up on that lock quickly.
 * See docs/troubleshooting/timescaledb-lock-queues.md.
 */
export const PREDICTION_CHUNK_RETENTION_DAYS = 90;

/**
 * Past `PREDICTION_CHUNK_RETENTION_DAYS` plus this many days, a chunk that the
 * nightly job still could not drop is logged at error level: one missed night
 * in the busy 03:30 window is expected, two weeks of misses are not.
 */
export const PREDICTION_CHUNK_OVERDUE_DAYS = 14;
