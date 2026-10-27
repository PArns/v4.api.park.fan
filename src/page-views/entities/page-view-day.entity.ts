import { Column, Entity, Index, PrimaryColumn } from "typeorm";
import type { PageViewKind, PageViewLocale } from "../page-view.constants";

/**
 * How often one page was viewed in one language on one UTC day.
 *
 * A count, not a log: no row carries an address, a session or a time of day,
 * so there is nothing here that could identify a reader. The table grows by
 * at most one row per page and language that was viewed that day, a few
 * thousand rows a day at today's traffic.
 *
 * The day is UTC rather than a park's zone: a view is a reader's, not a
 * park's, and readers sit in every zone. A ranking over "the last 7 days" is
 * seven UTC days, today included.
 */
@Entity("page_view_days")
@Index("idx_page_view_days_kind_locale_day", ["kind", "locale", "day"])
export class PageViewDay {
  @PrimaryColumn({ type: "varchar", length: 16 })
  kind: PageViewKind;

  /** `translationKey` for a post, the UUID for a park or an attraction. */
  @PrimaryColumn({ type: "varchar", length: 120 })
  key: string;

  @PrimaryColumn({ type: "varchar", length: 8 })
  locale: PageViewLocale;

  /** `YYYY-MM-DD`, UTC. */
  @PrimaryColumn({ type: "date" })
  day: string;

  @Column({ type: "integer", default: 0 })
  views: number;
}
