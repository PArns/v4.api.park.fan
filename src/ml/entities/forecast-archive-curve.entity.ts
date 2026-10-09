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
  k: "composed",
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
 * NOT A HYPERTABLE. Sized in docs/ml/forward-archive.md at ~20 MB a day with
 * its indexes, kept 35 days (~0.7 GB steady state); a plain DELETE keeps
 * retention free of the `drop_chunks` lock on referenced tables that took the
 * API down in October (db-health-runbook §0b). For the same reason there is NO
 * foreign key: a FK to `attractions` would put this table into that lock path.
 */
@Entity("forecast_archive_curves")
// Scoring reads one park's rows for one target date.
@Index("idx_fac_park_target", ["parkId", "targetDate"])
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

  /** Native resolution of the surface: 15 for park_hourly, 60 for plan_day. */
  @Column({ name: "slot_minutes", type: "smallint" })
  slotMinutes: number;

  /** Served q50 wait per slot (NULL = the surface had no value for it). */
  @Column({ type: "smallint", array: true })
  waits: (number | null)[];

  /** One {@link ARCHIVE_SOURCE_CODES} character per slot. */
  @Column({ type: "text" })
  sources: string;

  /**
   * Served `uncertaintyMinutes` per slot — the upper half-width of the band, as
   * the payload carried it. NULL when no slot had one. On plan_day this is the
   * ride's single band repeated, because the planner serves one per ride.
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
   * The ride's q90 of 15-min average waits over the 56 days before the origin
   * (from `attraction_hourly_history`). Kept as the number rather than the
   * busy flag so the threshold (45 min in BENCH-SPEC) can move later. It is the
   * EX-ANTE busy segment: known at the origin, never the realised value.
   */
  @Column({ name: "ride_q90_56d", type: "smallint", nullable: true })
  rideQ90: number | null;

  @Column({ name: "is_headliner", type: "boolean", default: false })
  isHeadliner: boolean;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt: Date;
}
