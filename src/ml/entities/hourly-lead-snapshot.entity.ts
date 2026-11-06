import { Entity, PrimaryColumn, Column, CreateDateColumn } from "typeorm";
import type { BandLeadBucket } from "../utils/catboost-band-calibration.util";

/**
 * HourlyLeadSnapshot Entity (PAR-658)
 *
 * How wrong an HOURLY prediction turns out to be, as a function of how far
 * ahead it was made: the hourly counterpart of {@link PredictionLeadSnapshot}.
 *
 * `prediction_accuracy` cannot answer it. `recordPredictions` upserts on
 * `(attraction_id, target_time)`, and the 15-minute run rewrites that row until
 * the slot arrives, so the lead it keeps is that of the last run that still
 * predicted the slot. Measured over 30 days (4.67 M completed hourly rows), the
 * 0.7 % with a lead over 24 h were 345 rides at an actual wait of 0.03 min:
 * slots of parks that then stayed CLOSED, which `predict.py` drops from every
 * later run. That remainder is selected by "closed later", not a sample.
 *
 * So this table is written FORWARD, at write time: each run copies the
 * full-hour slots that sit inside one of the sampling windows
 * (`HourlyLeadSnapshotService.SAMPLE_LEADS`), one row per slot and lead
 * bucket, first run wins. The buckets are the band calibration's
 * (`bandLeadBucket`), so that fit and the CatBoost-vs-PCN comparison can read
 * the same rows.
 *
 * NO ACTUAL HERE. A slot's realised wait is already computed once, by the
 * comparison job, on the slot's `prediction_accuracy` row: a reader joins on
 * `(attraction_id, target_time)` and filters with `scoredSlotSql`. Both tables
 * are written from the same rows in the same call, so the partner row exists,
 * and it is kept 90 days against this table's 35.
 *
 * A hypertable on `target_time` (src/database/hypertables.ts), compressed
 * after 7 days and dropped whole by the nightly `cleanup-old` job after
 * `HOURLY_LEAD_SNAPSHOT_RETENTION_DAYS`. No foreign key, so `drop_chunks`
 * locks nothing but its own chunks.
 */
@Entity("hourly_lead_snapshots")
export class HourlyLeadSnapshot {
  @PrimaryColumn({ name: "attraction_id", type: "uuid" })
  attractionId: string;

  /** The slot predicted; also the hypertable's partitioning column. */
  @PrimaryColumn({ name: "target_time", type: "timestamptz" })
  targetTime: Date;

  /** `h0-2` / `h2-6` / `h6-24` / `h24+`, from `bandLeadBucket(leadMinutes)`. */
  @PrimaryColumn({ name: "lead_bucket", type: "varchar", length: 8 })
  leadBucket: BandLeadBucket;

  /** The run that made the prediction (`wait_time_predictions.createdAt`). */
  @Column({ name: "prediction_time", type: "timestamptz" })
  predictionTime: Date;

  /** `target_time − prediction_time` in whole minutes, kept so a reader never
   *  has to assume where inside its bucket a row was sampled. */
  @Column({ name: "lead_minutes", type: "smallint" })
  leadMinutes: number;

  /** Raw CatBoost q0.5, as `prediction_accuracy.predicted_wait_time`. */
  @Column({ name: "predicted_wait_time", type: "int" })
  predictedWaitTime: number;

  /** CatBoost's band of the same run, the calibration's denominator. */
  @Column({ name: "uncertainty_minutes", type: "smallint", nullable: true })
  uncertaintyMinutes: number | null;

  @Column({ name: "model_version", type: "varchar" })
  modelVersion: string;

  /**
   * What the reader was served for the slot at that moment, and which model
   * produced it (`pcn`, `chronos`, `catboost`). NULL below
   * `SERVED_ACCURACY_MIN_LEAD_MS` (2 h), the rule `prediction_accuracy`
   * follows, so `h0-2` rows carry the raw CatBoost answer only.
   */
  @Column({ name: "served_wait_time", type: "int", nullable: true })
  servedWaitTime: number | null;

  @Column({
    name: "served_source",
    type: "varchar",
    length: 16,
    nullable: true,
  })
  servedSource: "pcn" | "chronos" | "catboost" | null;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt: Date;
}
