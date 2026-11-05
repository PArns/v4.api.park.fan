import { Column, Entity, PrimaryColumn } from "typeorm";

/**
 * Hits of one finished UTC day that the collector dropped as a bot's (an
 * empty user agent counts as one): the `dropped:bot` field of the day's drop
 * counts in Redis, written once by the nightly snapshot (insert-only). Days
 * not written yet are read from Redis live. One row a day, kept.
 */
@Entity("site_bot_days")
export class SiteBotDay {
  /** `YYYY-MM-DD`, UTC. */
  @PrimaryColumn({ type: "date" })
  day: string;

  @Column({ type: "integer", default: 0 })
  hits: number;
}
