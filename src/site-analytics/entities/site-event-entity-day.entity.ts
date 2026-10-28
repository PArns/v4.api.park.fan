import { Column, Entity, Index, PrimaryColumn } from "typeorm";
import type { SiteEntityKind, SiteLocale } from "../site-analytics.schema";

/**
 * How often one event was about one park, ride or show, in one language, on
 * one UTC day. The index on `entity` is for the admin's merges, which move a
 * merged-away entity's rows onto the survivor
 * (`src/parks/utils/merge-dependencies.ts`).
 */
@Entity("site_event_entity_days")
@Index("idx_site_event_entity_days_entity", ["entity"])
export class SiteEventEntityDay {
  @PrimaryColumn({ type: "varchar", length: 32 })
  name: string;

  /** `YYYY-MM-DD`, UTC. */
  @PrimaryColumn({ type: "date" })
  day: string;

  @PrimaryColumn({ type: "varchar", length: 2 })
  locale: SiteLocale;

  /** The park's, ride's or show's id. */
  @PrimaryColumn({ type: "uuid" })
  entity: string;

  /**
   * Which of the three, as the event said: kept so an entity that is gone
   * since is still listed as what it was.
   */
  @Column({ type: "varchar", length: 10 })
  kind: SiteEntityKind;

  @Column({ type: "integer", default: 0 })
  count: number;
}
