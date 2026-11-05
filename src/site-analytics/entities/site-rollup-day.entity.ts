import { Column, Entity, PrimaryColumn } from "typeorm";

/**
 * One cuboid of one day that the nightly job has rolled up: the marker the
 * reports read to know which days they may take from the daily tables. It is
 * inserted in the same transaction as the cuboid's rows, so a marker never
 * stands for rows that are not there, and a day without one is read from the
 * hourly rows. A zero-row day has its marker too. Insert-only, one small row
 * per day and cuboid, kept.
 */
@Entity("site_rollup_days")
export class SiteRollupDay {
  /** `pageviews` or `events`. */
  @PrimaryColumn({ type: "varchar", length: 10 })
  source: string;

  @PrimaryColumn({ type: "smallint" })
  cuboid: number;

  /** `YYYY-MM-DD`, UTC. */
  @PrimaryColumn({ type: "date" })
  day: string;

  /** The cuboid's rows for that day. */
  @Column({ type: "integer", default: 0 })
  rows: number;

  @Column({ name: "rolled_at", type: "timestamptz", default: () => "now()" })
  rolledAt: Date;
}
