import { Column, Entity, PrimaryColumn } from "typeorm";

/**
 * Unique visitors of one finished UTC day, all languages (`scope = 'all'`) and
 * per language, written by the nightly snapshot from that day's Redis
 * HyperLogLog. Today is read from Redis live. An estimate (HyperLogLog's
 * standard error is 0.81 %), and a reader who comes back on another day
 * counts again on that day.
 */
@Entity("site_visitor_days")
export class SiteVisitorDay {
  /** `YYYY-MM-DD`, UTC. */
  @PrimaryColumn({ type: "date" })
  day: string;

  /** `all`, or one of the six languages. */
  @PrimaryColumn({ type: "varchar", length: 3 })
  scope: string;

  @Column({ type: "integer", default: 0 })
  visitors: number;
}
