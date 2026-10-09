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
   * `timescaledb.compress_segmentby`. Only `wait_time_predictions` has one in
   * production, and the absence elsewhere is load-bearing rather than a gap:
   * segmenting by a column a merge REWRITES lets TimescaleDB decompress that
   * one segment instead of every batch the rows sit in, which is an easier case
   * than the one production runs.
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
];

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
 * See docs/troubleshooting/db-health-runbook.md §0b.
 */
export const PREDICTION_CHUNK_RETENTION_DAYS = 90;

/**
 * Past `PREDICTION_CHUNK_RETENTION_DAYS` plus this many days, a chunk that the
 * nightly job still could not drop is logged at error level: one missed night
 * in the busy 03:30 window is expected, two weeks of misses are not.
 */
export const PREDICTION_CHUNK_OVERDUE_DAYS = 14;
