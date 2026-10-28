import { Column, Entity, PrimaryColumn } from "typeorm";

/**
 * Unique visitors of one finished UTC hour, all languages (`scope = 'all'`)
 * and per language, written once by the nightly snapshot from that day's hour
 * HyperLogLogs (insert-only), beside the day's own in `site_visitor_days`, so
 * an hourly series of a past day has `visitors`. The hours of today are read
 * from Redis live. An estimate, like the day's; a reader who comes back in
 * another hour counts again in it, so the hours of a day do not add up to its
 * visitors. 168 rows a day, kept.
 */
@Entity("site_visitor_hours")
export class SiteVisitorHour {
  /** The hour's start, UTC. */
  @PrimaryColumn({ type: "timestamptz" })
  hour: Date;

  /** `all`, or one of the six languages. */
  @PrimaryColumn({ type: "varchar", length: 3 })
  scope: string;

  @Column({ type: "integer", default: 0 })
  visitors: number;
}
