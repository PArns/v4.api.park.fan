import { Column, Entity, PrimaryColumn } from "typeorm";
import type {
  SiteLocale,
  SitePageType,
  SiteViewport,
} from "../site-analytics.schema";

/**
 * How often one event happened with one set of properties, on one kind of
 * page, at one viewport, in one language, on one UTC day. The properties sit
 * in `p1`…`p3` in the order `SITE_EVENTS` declares them, `''` where an event
 * has fewer. A count, not a log: nothing here names a reader, and the entity
 * an event was about lives in `site_event_entity_days`, so this table stays
 * one row per combination rather than one per park.
 */
@Entity("site_event_days")
export class SiteEventDay {
  @PrimaryColumn({ type: "varchar", length: 32 })
  name: string;

  /** `YYYY-MM-DD`, UTC. */
  @PrimaryColumn({ type: "date" })
  day: string;

  @PrimaryColumn({ type: "varchar", length: 2 })
  locale: SiteLocale;

  @PrimaryColumn({ name: "page_type", type: "varchar", length: 16 })
  pageType: SitePageType;

  @PrimaryColumn({ type: "varchar", length: 8 })
  viewport: SiteViewport;

  @PrimaryColumn({ type: "varchar", length: 60, default: "" })
  p1: string;

  @PrimaryColumn({ type: "varchar", length: 60, default: "" })
  p2: string;

  @PrimaryColumn({ type: "varchar", length: 60, default: "" })
  p3: string;

  @Column({ type: "integer", default: 0 })
  count: number;
}
