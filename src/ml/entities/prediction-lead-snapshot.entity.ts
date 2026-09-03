import {
  Entity,
  PrimaryColumn,
  Column,
  Index,
  CreateDateColumn,
} from "typeorm";

/**
 * PredictionLeadSnapshot Entity
 *
 * How wrong a DAILY prediction turns out to be, as a function of how far ahead it
 * was made. Nothing measures that today, and — this is the point of the table —
 * nothing can measure it retroactively.
 *
 * Two reasons the answer is not already in the database:
 *
 * 1. Daily predictions are never scored. `prediction-accuracy.service.ts` says so
 *    in its own type doc: a prediction type that is not compared reports
 *    `tracked: false`, "e.g. daily predictions, which span up to 365 days and are
 *    never compared, so 0% would read as broken". `prediction_accuracy` therefore
 *    holds hourly rows only, which reach 24 hours ahead.
 *
 * 2. The predictions themselves do not survive to be scored later.
 *    `MLService.deduplicatePredictions` deletes every daily row with
 *    `predictedTime` in `[now, now+60d]` and `createdAt >= now-13d` before each
 *    nightly run. At a daily cadence a prediction for day X is deleted and
 *    rewritten on every run up to X, so when X finally arrives only the last one
 *    is left — lead time about a day. Long-lead rows that do turn up are the
 *    residue of runs that failed, not a sample.
 *
 * So this table is written FORWARD: each nightly run copies its prediction for a
 * few fixed lead distances, and a later job fills in what actually happened. The
 * far buckets say nothing until they have been running for that many days, which
 * is a property of the question, not a shortcoming of the design.
 *
 * WHAT IT IS COMPARED AGAINST. `actualWait` is the day's P90 for that ride, the
 * same statistic `AnalyticsService.getHeadlinerDailyPeaks` computes for the
 * calendar's past days — and the comment there explains why it is the right one:
 * the forecast side of the same field is a day-peak proxy, because predict.py
 * collapses the peak-window hours to a per-day MAX. Scoring against a daily
 * average instead would measure the change of statistic, not the model's error.
 *
 * WHY ONLY HEADLINERS. An error curve needs statistical mass, not coverage. Every
 * ride at every lead distance would be roughly 44,000 × 60 × 6 rows against the
 * ~1,000 headliners × 60 × 6 this samples, on a question that is about the model
 * rather than about any one ride. Headliners are also the rides whose predictions
 * anyone reads.
 *
 * NOT A HYPERTABLE, deliberately. It is small, it is written once per row and
 * updated once, and it must survive exactly the compression and de-duplication
 * that make `wait_time_predictions` unable to answer this question.
 */
@Entity("prediction_lead_snapshots")
// The scoring job's query: rows for a target date that has passed and has no
// actual yet. `targetDate` leads because it is the selective half.
@Index("idx_pls_target_scored", ["targetDate", "scoredAt"])
// The read path's query: the mean error at one lead distance, over scored rows.
// `leadDays` leads because it is what the question filters on, and there are six
// values of it.
@Index("idx_pls_lead_scored", ["leadDays", "scoredAt"])
export class PredictionLeadSnapshot {
  /**
   * `uuid`, matching every other attraction reference in the schema.
   *
   * TypeORM infers `varchar` from the TypeScript type, which is how this became
   * the one table whose attraction id would need a cast to join — the exact trap
   * `PlanDayService.downYesterday` documents from the other side, where
   * `queue_data_aggregates.attractionId` being text made `::text` look like the
   * house style and produced `operator does not exist: text = uuid`.
   */
  @PrimaryColumn({ name: "attraction_id", type: "uuid" })
  attractionId: string;

  /** Park-local calendar day the prediction is about (YYYY-MM-DD). */
  @PrimaryColumn({ name: "target_date", type: "date" })
  targetDate: string;

  /**
   * Whole days between the run that made the prediction and `targetDate`, and one
   * of the fixed buckets the snapshot job samples. Part of the key so one target
   * day accumulates one row per distance as the runs walk towards it.
   */
  @PrimaryColumn({ name: "lead_days", type: "smallint" })
  leadDays: number;

  @Column({ name: "predicted_wait_time", type: "int" })
  predictedWaitTime: number;

  /** The model's own band width at prediction time, when it reported one. */
  @Column({ name: "uncertainty_minutes", type: "smallint", nullable: true })
  uncertaintyMinutes: number | null;

  /**
   * The day's realised P90 for this ride. NULL until the scoring job has run for
   * a target date that has passed — and it stays NULL when the day produced no
   * usable readings at all (closed, out of season, feed gap), which is why
   * `scoredAt` exists separately rather than being inferred from this being set.
   */
  @Column({ name: "actual_wait_time", type: "int", nullable: true })
  actualWaitTime: number | null;

  @Column({ name: "absolute_error", type: "int", nullable: true })
  absoluteError: number | null;

  /**
   * When scoring last looked at this row. Distinguishes "not scored yet" from
   * "scored, and there was nothing to compare against" — without it the job would
   * re-examine every unscorable row every night forever.
   */
  @Column({ name: "scored_at", type: "timestamptz", nullable: true })
  scoredAt: Date | null;

  @Column({ name: "model_version" })
  modelVersion: string;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt: Date;
}
