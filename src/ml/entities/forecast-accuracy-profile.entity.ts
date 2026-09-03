import { Entity, PrimaryColumn, Column } from "typeorm";

/**
 * How wrong a daily forecast typically is, by how far ahead it was made and how
 * busy it says the day will be.
 *
 * A planner has to be able to say "give or take a quarter of an hour", and it
 * cannot get that from one number: the error depends on both axes and by a lot.
 * Measured against the realised day-P90 over 45 days (2.5 M comparisons):
 *
 * ```
 *   predicted        <=7d    8-30d   31-60d
 *   >= 60 min        21.9     23.9     25.0
 *   30-59 min        13.4     15.4     16.6
 *   <  30 min         9.0     10.9     13.0
 * ```
 *
 * A single per-day figure would understate a headliner's error by ten minutes
 * and overstate a quiet ride's by four. Hence two axes.
 *
 * WHY THE BAND IS THE **PREDICTED** LEVEL and never the realised one. Grouping by
 * the outcome is the trap that makes any well-calibrated model look badly biased
 * — conditioning on the result reproduces regression to the mean — and it is the
 * reason `shape_comparisons`' bias column cannot be read at face value. The
 * predicted level is also the only one available when the answer is served.
 *
 * WHY IT IS MEASURED RATHER THAN CONFIGURED. These numbers move with the model,
 * the season and the parks in the set. A constant in the code would be right on
 * the day it was written and quietly wrong afterwards, and nobody would notice,
 * because a confidence figure has no failing test.
 *
 * Beyond the TFT horizon there is no row at all, and that absence is the point:
 * past 60 days the only forecast is CatBoost's, whose accuracy at that distance
 * has never been measurable (`deduplicatePredictions` keeps only the last one, so
 * of its daily rows for past days 72,795 sit at lead 1 and 179 at 8-30 days).
 * `prediction_lead_snapshots` is recording that forward now. Until it reports, a
 * planner is told the day is *unmeasured* rather than given a number nobody
 * checked.
 */
@Entity("forecast_accuracy_profile")
export class ForecastAccuracyProfile {
  /** `quiet` (<30), `mid` (30-59) or `busy` (>=60) — of the PREDICTED wait. */
  @PrimaryColumn({ name: "predicted_band", type: "varchar", length: 8 })
  predictedBand: string;

  /** `d1`, `d7`, `d30` or `d60` — the upper edge of the lead bucket, in days. */
  @PrimaryColumn({ name: "lead_bucket", type: "varchar", length: 8 })
  leadBucket: string;

  /** Comparisons behind the figure. Published, so a caller can weigh it. */
  @Column({ name: "sample_size", type: "int" })
  sampleSize: number;

  /**
   * Mean absolute error in minutes.
   *
   * A typical miss, not a bound: roughly half of days land further out than this,
   * which is why it is served as "give or take" and never as a range that
   * contains the answer.
   */
  @Column({ name: "mae", type: "real" })
  mae: number;

  /** Mean realised wait in the bucket, so a reader can size the error. */
  @Column({ name: "mean_actual", type: "real" })
  meanActual: number;

  @Column({ name: "computed_at", type: "timestamptz" })
  computedAt: Date;
}
