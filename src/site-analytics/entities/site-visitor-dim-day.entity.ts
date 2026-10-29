import { Column, Entity, PrimaryColumn } from "typeorm";

/**
 * Unique visitors of one finished UTC day with one value of one dimension
 * (`country`, `device`, `browser`, `os` or `channel`), all languages
 * (`scope = 'all'`) or one language, written once by the nightly snapshot from
 * that day's Redis HyperLogLog (insert-only), beside the day's own in
 * `site_visitor_days`. Today is read from Redis live. An estimate, like the
 * day's; a reader who started visits from two channels counts under both. A
 * few hundred rows a day in a plain table, which the nightly job deletes past
 * `DAILY_RETENTION_DAYS` like the other visitor counts.
 * docs/frontend/site-analytics.md.
 */
@Entity("site_visitor_dim_days")
export class SiteVisitorDimDay {
  /** `YYYY-MM-DD`, UTC. */
  @PrimaryColumn({ type: "date" })
  day: string;

  /**
   * `all` or one of the six languages; for readers from a data centre `dca`,
   * or `d` and the language.
   */
  @PrimaryColumn({ type: "varchar", length: 3 })
  scope: string;

  /** One of `VISITOR_DIMS`. */
  @PrimaryColumn({ type: "varchar", length: 16 })
  dim: string;

  /** The dimension's value as the rows carry it: `de`, `mobile`, `safari`, `search`. */
  @PrimaryColumn({ type: "varchar", length: 64 })
  value: string;

  @Column({ type: "integer", default: 0 })
  visitors: number;
}
