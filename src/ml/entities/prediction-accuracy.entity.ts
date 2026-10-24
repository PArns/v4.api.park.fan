import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  JoinColumn,
  Index,
  Unique,
  CreateDateColumn,
} from "typeorm";
import { Attraction } from "../../attractions/entities/attraction.entity";

/** See {@link PredictionAccuracy.scoreClass}. */
export type ScoreClass =
  "scored" | "unplanned_closure" | "ride_not_operating" | "no_wait_signal";

/**
 * {@link scoredSlotSql} for a row already in memory — the same rule, for
 * readers that load rows rather than aggregate them.
 */
export function isScoredSlot(
  row: Pick<
    PredictionAccuracy,
    "comparisonStatus" | "actualWaitTime" | "wasUnplannedClosure" | "scoreClass"
  >,
  maxWait: number,
): boolean {
  if (row.comparisonStatus !== "COMPLETED") return false;
  if (row.actualWaitTime === null || row.actualWaitTime === undefined) {
    return false;
  }
  if (row.actualWaitTime > maxWait) return false;
  if (row.scoreClass === "scored") return true;
  return (
    (row.scoreClass === null || row.scoreClass === undefined) &&
    row.wasUnplannedClosure === false &&
    row.actualWaitTime > 0
  );
}

/**
 * The one predicate for "this row is a forecast error" (PAR-912), as SQL over
 * a `prediction_accuracy` alias. Rows compared since PAR-912 carry their
 * verdict; rows compared before it do not, and for them only a positive wait
 * outside an unplanned closure is trusted — their zeros mix status-only feeds,
 * closed rides and the rare real 0-minute wait, and nothing on the row can tell
 * them apart. So the transition drops the old real zeros too (about 9 % of the
 * old zero rows, measured 2026-10-17) rather than keeping the 91 % that are not
 * waits. Works without a backfill; the old rows age out with the 90-day
 * retention.
 *
 * Includes `actual_wait_time <= MAX_PLAUSIBLE_WAIT_TIME` via the `maxWait`
 * argument because every caller needs the sentinel cut as well.
 */
export function scoredSlotSql(alias: string, maxWait: number): string {
  const a = alias;
  return `(${a}.comparison_status = 'COMPLETED'
    AND ${a}.actual_wait_time IS NOT NULL
    AND ${a}.actual_wait_time <= ${Math.trunc(maxWait)}
    AND (${a}.score_class = 'scored'
         OR (${a}.score_class IS NULL
             AND ${a}."wasUnplannedClosure" = false
             AND ${a}.actual_wait_time > 0)))`;
}

/**
 * PredictionAccuracy Entity
 *
 * Tracks prediction vs reality for model monitoring and improvement
 * Used for:
 * - Model performance tracking (compareWithActuals job)
 * - Pre-aggregation into attraction_accuracy_stats
 *
 * Note: API now reads from attraction_accuracy_stats for display,
 * this table is only used by background jobs.
 *
 * Two predictions per row since PAR-854: `predictedWaitTime` (raw CatBoost,
 * for the model boards and the ride day curve) and `servedWaitTime` (what the
 * reader saw, for the per-ride badge). Both are scored against the same actual.
 */
@Entity("prediction_accuracy")
@Unique("uq_pa_attraction_target", ["attractionId", "targetTime"])
// Composite index for the hourly aggregate-stats query:
//   WHERE comparison_status = 'COMPLETED' AND target_time >= $1
// Also covers getAttractionAccuracyStats per-attraction aggregate and cleanup-old job.
@Index("idx_pa_status_target", ["comparisonStatus", "targetTime"])
// Removed unused/redundant indexes:
// - @Index("idx_pa_attraction_target", ...) - redundant with unique constraint above
// - @Index(["modelVersion", "createdAt"]) - never queried (0 scans)
// - @Index(["targetTime"]) - covered by composite index above
// - @Index("idx_pa_target_actual", ...) - barely used (40 scans)
export class PredictionAccuracy {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  // No standalone index: attraction_id is the leading column of the
  // uq_pa_attraction_target unique index, which already serves attraction_id
  // lookups. A separate single-column index had 0 scans and only added write
  // cost to every accuracy upsert.
  @Column({ name: "attraction_id" })
  attractionId: string;

  @ManyToOne(() => Attraction)
  @JoinColumn({ name: "attraction_id" })
  attraction: Attraction;

  @Column({ name: "prediction_time", type: "timestamptz" })
  predictionTime: Date;

  @Column({ name: "target_time", type: "timestamptz" })
  targetTime: Date;

  @Column({ name: "predicted_wait_time", type: "int" })
  predictedWaitTime: number;

  @Column({ name: "actual_wait_time", type: "int", nullable: true })
  actualWaitTime: number | null; // Filled when actual data becomes available

  @Column({ name: "absolute_error", type: "int", nullable: true })
  absoluteError: number | null; // |predicted - actual|

  @Column({ name: "percentage_error", type: "float", nullable: true })
  percentageError: number | null; // (|predicted - actual| / actual) * 100

  @Column()
  modelVersion: string;

  /**
   * The wait a reader was SERVED for this slot when the row was written
   * (PAR-854) — `predictedWaitTime` after the PCN champion-swap override and
   * the §7.7 persistence blend, captured at generation time with the lead it
   * had then. `predictedWaitTime` stays the raw CatBoost answer, because the
   * admin boards compare CatBoost against PCN and must not see PCN on both
   * sides.
   *
   * NULL on every row written before PAR-854. Those scored CatBoost while users
   * were shown PCN, so the per-ride badge reads only rows that carry this
   * column. (The ride day-curve band reads `absoluteError` — that curve draws
   * CatBoost's own stored forecast; see `AttractionAccuracyStats.rawMae`.)
   */
  @Column({ name: "served_wait_time", type: "int", nullable: true })
  servedWaitTime: number | null;

  /** Which model produced {@link servedWaitTime}: `pcn` (override + blend),
   *  `chronos` (the Chronos-2 30–120 min window, PAR-911) or `catboost` (flags
   *  off, no forecast for the slot, or stale). NULL exactly when
   *  `servedWaitTime` is. A varchar, so the new value needs no migration. */
  @Column({
    name: "served_source",
    type: "varchar",
    length: 16,
    nullable: true,
  })
  servedSource: "pcn" | "chronos" | "catboost" | null;

  /** |servedWaitTime − actualWaitTime|, filled by the comparison job beside
   *  `absoluteError`. An unplanned closure charges the full served wait, as it
   *  does the raw one. */
  @Column({ name: "served_absolute_error", type: "int", nullable: true })
  servedAbsoluteError: number | null;

  /**
   * The model's own q0.8 and q0.95 for the slot, in minutes (PAR-912), written
   * beside `predictedWaitTime` (the served q0.5) by the same upsert, so the three
   * always come from one run. q0.8 drives the crowd level and q0.95 the
   * uncertainty band; storing them makes their coverage measurable live —
   * P(actual ≤ q) on the slots that are scored at all (`scoreClass = 'scored'`).
   *
   * Raw quantiles rounded to whole minutes, NOT the posted-wait rounding
   * (`predictedWaitTime` is rounded to 5 with a floor of 5). On a quiet slot the
   * served median can therefore sit above q0.8 — that is the rounding, not a
   * crossing. NULL when the model has no quantile head, and on every row written
   * before PAR-912. No index: nothing filters on them, and this table takes ~13M
   * upserts a day.
   */
  @Column({ name: "predicted_q80", type: "int", nullable: true })
  predictedQ80: number | null;

  @Column({ name: "predicted_q95", type: "int", nullable: true })
  predictedQ95: number | null;

  /**
   * What the comparison decided this slot IS (PAR-912), written by
   * `compareWithActuals` beside `comparisonStatus = 'COMPLETED'`:
   *
   * - `scored` — the ride was OPERATING with a wait: a forecast error. A real
   *   0-minute wait counts, if the ride has published a wait above 0 in the
   *   seven days before (otherwise see `no_wait_signal`).
   * - `unplanned_closure` — DOWN, or CLOSED after the ride had already run that
   *   park-local day. The existing rule: `wasUnplannedClosure`, actual 0, full
   *   error — and no accuracy figure averages it.
   * - `ride_not_operating` — REFURBISHMENT, or CLOSED before the ride's first
   *   OPERATING reading of the day: the ride was not scheduled to run (closed
   *   for the day, for the season, not open yet). Actual and errors stay NULL.
   * - `no_wait_signal` — OPERATING with a wait of 0 on a ride that has not
   *   published a wait above 0 in seven days (status-only feeds, free-flow
   *   attractions). That 0 is a feed's placeholder, not a measured wait, so
   *   actual and errors stay NULL — docs/rules/absent-facts.md.
   *
   * NULL on rows compared before PAR-912 (and on PENDING/MISSED rows). Readers
   * that must also be right for those use {@link scoredSlotSql}.
   */
  @Column({ name: "score_class", type: "varchar", length: 24, nullable: true })
  scoreClass: ScoreClass | null;

  @Column({
    type: "enum",
    enum: ["hourly", "daily"],
  })
  predictionType: "hourly" | "daily";

  // Features used (for debugging which features led to errors)
  @Column({ type: "jsonb", nullable: true })
  features: Record<string, unknown>;

  // Track unplanned closures (predicted open, but actually closed)
  @Column({ type: "boolean", default: false })
  wasUnplannedClosure: boolean;

  // No standalone index: comparison_status is the leading column of
  // idx_pa_status_target (comparison_status, target_time), which already serves
  // status-only lookups. A separate single-column index on a low-cardinality
  // enum added write cost for no read benefit.
  @Column({
    name: "comparison_status",
    type: "enum",
    enum: ["PENDING", "COMPLETED", "MISSED"],
    default: "PENDING",
  })
  comparisonStatus: "PENDING" | "COMPLETED" | "MISSED";

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;
}
