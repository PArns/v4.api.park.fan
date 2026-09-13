import { Entity, PrimaryColumn, Column } from "typeorm";

/**
 * How wrong a daily forecast typically is, by how far ahead it was made and how
 * busy it says the day will be.
 *
 * A planner has to be able to say "give or take a quarter of an hour", and it
 * cannot get that from one number: the error depends on both axes and by a lot.
 * Measured against the realised day-P90 over 45 days, 2.5 M comparisons
 * (2026-09-11, six lead buckets — the two middle columns were added by PAR-17):
 *
 * ```
 *   predicted       <=1d    <=3d    <=7d   <=14d   <=30d   <=60d
 *   >= 60 min       21.5    21.6    22.9    24.3    25.1    25.5
 *   30-59 min       12.9    13.2    13.6    14.7    15.5    16.5
 *   <  30 min        8.6     8.7     9.0     9.9    10.7    12.5
 * ```
 *
 * A single per-day figure would understate a headliner's error by ten minutes
 * and overstate a quiet ride's by four. Hence two axes.
 *
 * READ THE ROWS, NOT JUST THE TREND. Each band widens by about the same four
 * minutes from one day out to sixty — but that is +19 % on a busy ride and +45 %
 * on a quiet one, so "the band grows with distance" is a statement about minutes
 * and not about proportions. A multiplier applied to the prediction would get
 * the quiet end badly wrong in one direction and the busy end in the other.
 *
 * WHY THE BAND IS THE **PREDICTED** LEVEL and never the realised one. Grouping by
 * the outcome is the trap that makes any well-calibrated model look badly biased
 * — conditioning on the result reproduces regression to the mean — and it is the
 * reason `shape_comparisons`' bias column cannot be read at face value. The
 * predicted level is also the only one available when the answer is served.
 *
 * WHY THERE IS NO PER-RIDE AXIS. Not a decision about effort — a ceiling.
 * Measured on 2026-09-11 over the same 45-day window: of 2,643 rides, **none**
 * clears even 100 comparisons in all six buckets, and the average ride's thinnest
 * bucket holds 18. The reason is arithmetic rather than sparsity: the `d1` bucket
 * spans exactly ONE lead distance, so a ride can contribute at most one
 * comparison per target day — 45 in a 45-day window, and 45 is also the observed
 * maximum.
 *
 * Worse, feasibility runs opposite to usefulness. `d30` and `d60` do clear 500
 * for 716 and 1,055 rides, but only by pooling 16 and 30 distinct lead distances:
 * "this ride's error at 30 days" would really be a mean over a 16-day-wide span.
 * The near buckets a planner reads most are precisely the ones that cannot carry
 * a per-ride figure, and widening the window until they could would average two
 * model versions into one number — the thing the 45-day choice above exists to
 * avoid. So the second axis is the predicted band, which every ride shares.
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

  /**
   * `d1`, `d3`, `d7`, `d14`, `d30` or `d60` — the upper edge of the bucket, in
   * days. The same six distances the forward archive samples.
   */
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

  /**
   * The 95th percentile of the SIGNED residual `actual - predicted`, in minutes:
   * how far above the served number the day can land before it is a one-in-twenty
   * day. This is the band, and {@link mae} is not — see below.
   *
   * MEASURED, NOT A MULTIPLE OF THE MAE. The two are different statistics of the
   * same residual distribution and the ratio between them is not constant: on
   * 2026-09-12 it runs 2.7x on `quiet|d1` and 2.1x on `busy|d1`, because the
   * quiet end's residuals are the more skewed. A band derived by scaling the MAE
   * would be wrong at one end whichever factor were chosen — the same reason the
   * class docblock gives for not scaling the prediction itself.
   *
   * ONE-SIDED, AND UPWARDS. A visitor plans against the queue being longer than
   * promised, not shorter, and the residual distribution is not symmetric: at
   * `quiet|d60` it reaches +35.7 above and only -9.5 below. Serving the upper
   * half-width also matches what CatBoost's `uncertaintyMinutes` claims to be
   * (its top trained quantile minus its served median), so the two sit on the
   * same axis even where they disagree on width.
   *
   * OUT-OF-SAMPLE CALIBRATED, which is the whole reason it can be published.
   * Fitted on target days -45..-15 and checked against -15..-1 (2026-09-13,
   * production), every one of the 18 cells covers between **92.3 % and 97.9 %**
   * of realised days against a nominal 95 %. The MAE-wide band, by comparison,
   * covers 67.8 % to 88.7 % — a "typical miss" and not a bound, exactly as
   * {@link mae} says of itself.
   *
   * NULLABLE, AND THE REASON IS THE DEPLOY AND NOT THE STATISTIC. `rebuild()`
   * always writes it, so a freshly built row always has one. But this column is
   * being added to a table that has held rows since PAR-17, and the schema comes
   * from `synchronize` rather than a migration: `ALTER TABLE ... ADD COLUMN real
   * NOT NULL` against a non-empty table is rejected outright by Postgres
   * (`column "uncertainty_p95" of relation ... contains null values`, verified on
   * a throwaway instance against the 18 rows production holds), and the API would
   * not have finished booting. A `DEFAULT 0` would boot and be worse: it would
   * publish a zero-wide band, which reads as certainty rather than as absence.
   *
   * So between the deploy and that night's rebuild the existing rows carry NULL,
   * every caller treats that as "no cell" — the same path as a bucket that has
   * no row at all — and `uncertaintyMinutes` stays absent for one night rather
   * than becoming a number nobody measured.
   */
  @Column({ name: "uncertainty_p95", type: "real", nullable: true })
  uncertaintyP95: number | null;

  /** Mean realised wait in the bucket, so a reader can size the error. */
  @Column({ name: "mean_actual", type: "real" })
  meanActual: number;

  @Column({ name: "computed_at", type: "timestamptz" })
  computedAt: Date;
}
