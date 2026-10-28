import { Column, Entity, PrimaryColumn } from "typeorm";
import type { TrafficDim } from "../site-analytics.constants";
import type { SiteLocale } from "../site-analytics.schema";

/**
 * Page views and visits per dimension value, language and UTC day: how often
 * `/de/parks/…/phantasialand` was viewed, how often from Safari, from the
 * Netherlands, as a visit's first page. One row per value a dimension took
 * that day, never one per reader: no row carries an address, a User-Agent, a
 * visit or a time of day.
 *
 * `visits` is the number of visits that reached this value at least once that
 * day in that language; for `entry` and `referrer`, which exist only on a
 * visit's first page view, it equals `views`.
 *
 * The day is UTC: a reader's day, not a park's.
 */
@Entity("site_traffic_days")
export class SiteTrafficDay {
  @PrimaryColumn({ type: "varchar", length: 12 })
  dim: TrafficDim;

  /** `YYYY-MM-DD`, UTC. */
  @PrimaryColumn({ type: "date" })
  day: string;

  @PrimaryColumn({ type: "varchar", length: 2 })
  locale: SiteLocale;

  /** A path, a page type, a referrer host, a browser family, a country … */
  @PrimaryColumn({ type: "varchar", length: 820 })
  value: string;

  @Column({ type: "integer", default: 0 })
  views: number;

  @Column({ type: "integer", default: 0 })
  visits: number;
}
