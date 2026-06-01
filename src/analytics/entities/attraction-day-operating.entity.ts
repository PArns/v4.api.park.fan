import { Entity, PrimaryColumn, Column, Index } from "typeorm";

/**
 * Per-(attraction, park-local day) "was operating" rollup.
 *
 * Precomputed nightly so the seasonal/refurbishment detection job
 * (`queue-percentile.processor` → `detect-seasonal`) can read a small table instead of
 * re-scanning ~60 days of the raw `queue_data` hypertable on every run (that multi-CTE
 * scan + cross-join was the top slow-query, peaking at ~227 s).
 *
 * Semantics MUST match the detection logic it replaces: a row exists iff the attraction had
 * **any** `status = 'OPERATING'` record (any queueType) on that park-local day. This is why
 * the STANDBY-only, count≥3 `queue_data_aggregates` table can NOT be used here — it would
 * miss boarding-group-only or sparse days and produce false refurbishment flags.
 */
@Entity("attraction_day_operating")
@Index(["parkId", "opDay"])
export class AttractionDayOperating {
  @PrimaryColumn("uuid")
  attractionId: string;

  @PrimaryColumn({ type: "date", name: "op_day" })
  opDay: string;

  @Column("uuid")
  parkId: string;

  @Column({ type: "timestamptz", default: () => "now()" })
  updatedAt: Date;
}
