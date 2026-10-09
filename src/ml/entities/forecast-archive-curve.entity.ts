import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryColumn,
} from "typeorm";

/**
 * Which served surface a curve was read from. Stored as a smallint to keep the
 * key narrow; the names are what the scores and the admin board use.
 */
export const ARCHIVE_SURFACES = {
  /** `MLService.getParkPredictions(…, "hourly")` — the served 15-min curve
   *  (CatBoost, PCN-overridden + persistence-blended where PCN answered). */
  park_hourly: 1,
  /** `PlanDayService.buildPlanDay` — the planner's hourly curve per ride. */
  plan_day: 2,
} as const;
export type ArchiveSurface = keyof typeof ARCHIVE_SURFACES;

export const ARCHIVE_ORIGIN_KINDS = {
  /** The fixed park-local 06:00 origin (UC2/UC3). */
  daily: 1,
  /** The 2-hourly origins during the operating day (UC1, short UC2). */
  intraday: 2,
  /**
   * The sparse long leads (d10 … d90) of the 06:00 origin's planner, captured
   * an hour later (07:00 park-local) so the Europe burst is spread over two
   * hours. Leads are still counted from the origin's park-local DATE, which
   * is the same day.
   */
  long: 3,
} as const;
export type ArchiveOriginKind = keyof typeof ARCHIVE_ORIGIN_KINDS;

/**
 * One character per slot in {@link ForecastArchiveCurve.sources}. Single
 * characters rather than an array of names because a curve has up to 192 of
 * them and the name is the same on most.
 */
export const ARCHIVE_SOURCE_CODES = {
  p: "pcn_blend",
  c: "catboost",
  m: "measured",
  /** Composed by the old composer: hourly P50 profile stretched to the level. */
  k: "composed",
  /** Composed by H5 (PAR-834), plain profile — no TFT level for the day. */
  h: "composed_h5",
  /** Composed by H5 scaled by the TFT level ÷ the ride's 56-day P90. */
  t: "composed_h5_tft",
  l: "climatology",
  o: "observed",
} as const;
export type ArchiveSourceCode = keyof typeof ARCHIVE_SOURCE_CODES;
export type ArchiveSourceName =
  (typeof ARCHIVE_SOURCE_CODES)[ArchiveSourceCode];

/**
 * The forward archive of SERVED intraday curves (PAR-831).
 *
 * Every other forecast table keeps only the freshest answer per target:
 * `wait_time_predictions` deletes the future rows on each run and
 * `prediction_accuracy` keeps the last prediction per slot, so the curve a
 * visitor saw three days before their visit is gone by the time the day can be
 * scored. `prediction_lead_snapshots` solved this for the DAILY number; this
 * table does it for the curve, as served — the same `getParkPredictions` /
 * `buildPlanDay` calls the endpoints make, never a re-implementation.
 *
 * ONE ROW PER (origin, surface, ride, park-local target date), with the slots as
 * an array starting at `slotStart`, `slotMinutes` apart. Per-slot rows would be
 * ~76 rows per ride per origin, each paying a 24-byte tuple header and a key of
 * 30+ bytes for a 2-byte wait — the array is roughly a tenth of that.
 *
 * NOT A HYPERTABLE. Sized in docs/ml/forward-archive.md at ~29 MB a day with
 * its indexes, kept 14 days past the TARGET date (~0.8 GB steady state, most
 * of it the d10-d90 rows waiting for their day); a plain DELETE keeps
 * retention free of the `drop_chunks` lock on referenced tables that took the
 * API down in October (db-health-runbook §0b). For the same reason there is NO
 * foreign key: a FK to `attractions` would put this table into that lock path.
 */
@Entity("forecast_archive_curves")
// Scoring reads one park's rows for one target date.
@Index("idx_fac_park_target", ["parkId", "targetDate"])
// Retention is by target date (a d90 row must live until its day is scored).
@Index("idx_fac_target", ["targetDate"])
export class ForecastArchiveCurve {
  /** When the served curve was read. Shared by every row of one park capture. */
  @PrimaryColumn({ name: "origin_at", type: "timestamptz" })
  originAt: Date;

  @PrimaryColumn({ name: "attraction_id", type: "uuid" })
  attractionId: string;

  /** {@link ARCHIVE_SURFACES}. */
  @PrimaryColumn({ type: "smallint" })
  surface: number;

  /** Park-local calendar day the slots belong to. */
  @PrimaryColumn({ name: "target_date", type: "date" })
  targetDate: string;

  @Column({ name: "park_id", type: "uuid" })
  parkId: string;

  /** {@link ARCHIVE_ORIGIN_KINDS}. */
  @Column({ name: "origin_kind", type: "smallint" })
  originKind: number;

  /** `targetDate` minus the origin's park-local date, in days. */
  @Column({ name: "lead_days", type: "smallint" })
  leadDays: number;

  /** Instant of the first slot. Slot i starts at slotStart + i·slotMinutes. */
  @Column({ name: "slot_start", type: "timestamptz" })
  slotStart: Date;

  /**
   * Native resolution of the surface: 15 for park_hourly; 60 for plan_day,
   * or 15 for a plan_day ride the H5 composer served in quarter-hours
   * (PAR-834).
   */
  @Column({ name: "slot_minutes", type: "smallint" })
  slotMinutes: number;

  /** Served q50 wait per slot (NULL = the surface had no value for it). */
  @Column({ type: "smallint", array: true })
  waits: (number | null)[];

  /** One {@link ARCHIVE_SOURCE_CODES} character per slot. */
  @Column({ type: "text" })
  sources: string;

  /**
   * park_hourly only: served `uncertaintyMinutes` per slot — CatBoost's
   * q95 − q50, the upper half-width of the band. NULL when no slot had one,
   * and always NULL on plan_day (see `peakBand`).
   */
  @Column({ type: "smallint", array: true, nullable: true })
  bands: (number | null)[] | null;

  /** plan_day only: the ride's served `dayPeak`. */
  @Column({ name: "day_peak", type: "smallint", nullable: true })
  dayPeak: number | null;

  /** plan_day only: the served `expectedError` (typical |dayPeak error|). */
  @Column({ name: "expected_error", type: "smallint", nullable: true })
  expectedError: number | null;

  /**
   * Model version as served (park_hourly: the CatBoost version, without the
   * `+pcn` suffix — PCN is recorded per slot in `sources`).
   */
  @Column({
    name: "model_version",
    type: "varchar",
    length: 64,
    nullable: true,
  })
  modelVersion: string | null;

  /**
   * The ride's q90 of time-weighted 15-min waits inside the published
   * windows over the 56 days before the origin (from
   * `attraction_hourly_history`). Kept as the number rather than the
   * busy flag so the threshold (45 min in BENCH-SPEC) can move later. It is the
   * EX-ANTE busy segment: known at the origin, never the realised value.
   */
  @Column({ name: "ride_q90_56d", type: "smallint", nullable: true })
  rideQ90: number | null;

  @Column({ name: "is_headliner", type: "boolean", default: false })
  isHeadliner: boolean;

  /**
   * The ride's STANDBY wait IN FORCE at the origin — the last change-log row
   * at or before it, OPERATING, no older than 3 h (the truth's own staleness
   * rule; `queue_data` writes only on change, so "a row in the last 30 min"
   * missed half the operating rides). NULL when there was none. The anchor of
   * decision metric D1.
   */
  @Column({ name: "live_wait", type: "smallint", nullable: true })
  liveWait: number | null;

  /** Minutes between that reading and the origin. */
  @Column({ name: "live_age_min", type: "smallint", nullable: true })
  liveAgeMin: number | null;

  /**
   * plan_day rows of an INTRADAY origin only: the frontend's next-best-ride
   * "later" value — the maximum of today's plan hours whose start lies in
   * [origin, origin + 120 min] before the close hour (`nextRideLater`) — and
   * the start of that hour. NULL when no hour qualified.
   */
  @Column({ name: "later_wait", type: "smallint", nullable: true })
  laterWait: number | null;

  @Column({ name: "later_at", type: "timestamptz", nullable: true })
  laterAt: Date | null;

  /**
   * plan_day only: the served `uncertaintyMinutes` — a ONE-sided upper
   * half-width around `dayPeak` (the p95 of the signed residual
   * `actual − predicted peak`, forecast-accuracy.service.ts), NOT around each
   * hour. Scored per ride-day as (truth P90 − dayPeak) ≤ band and never pooled
   * with the per-slot band.
   */
  @Column({ name: "peak_band", type: "smallint", nullable: true })
  peakBand: number | null;

  /**
   * plan_day only: which model produced the day LEVEL the curve is built on —
   * `tft` or `catboost` (the row `getServingDailyPredictions` served for that
   * ride and day, matched the way `PlanDayService.dayLevels` matches it), or
   * `climatology` on a climatology day. NULL when the ride had no level.
   * Internal: the planner payload does not expose it.
   */
  @Column({
    name: "level_source",
    type: "varchar",
    length: 12,
    nullable: true,
  })
  levelSource: string | null;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt: Date;
}
