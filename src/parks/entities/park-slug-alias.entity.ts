import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  JoinColumn,
  ManyToOne,
  PrimaryGeneratedColumn,
} from "typeorm";
import { Park } from "./park.entity";

/**
 * A geographic path a park used to be reachable at.
 *
 * Park names come from the upstream sources (ThemeParks Wiki wins — see
 * park-metadata.processor), and when a name changes the processor regenerates `park.slug`.
 * The park then answers only on its NEW path and every previously published URL 404s:
 * search-engine signals for that page are dropped, inbound links break, and our own sitemap
 * keeps pointing at the old path until it is rebuilt.
 *
 * Real example: upstream renamed "Attractiepark Toverland" → "Toverland" and
 * "Magic Kingdom Park" → "Disney Magic Kingdom", which silently 404'd those parks.
 *
 * Every row here lets the park lookup answer an old path with a 301 to the current one, so a
 * rename is a redirect instead of a dead end. Rows are written by ParkRenameService, the park
 * merge and, by hand, `POST /v1/admin/redirects` (PAR-1233).
 */
@Entity("park_slug_aliases")
// One park per historical path — the lookup relies on this being unambiguous.
@Index(["continentSlug", "countrySlug", "citySlug", "slug"], { unique: true })
export class ParkSlugAlias {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column()
  @Index()
  parkId: string;

  @ManyToOne(() => Park, { onDelete: "CASCADE" })
  @JoinColumn({ name: "parkId" })
  park: Park;

  @Column()
  continentSlug: string;

  @Column()
  countrySlug: string;

  @Column()
  citySlug: string;

  @Column()
  slug: string;

  /**
   * The admin (email) who added this alias by hand through
   * `POST /v1/admin/redirects` (PAR-1233); null for the rows the sync, a merge
   * or a location correction record. Only a hand-made alias may be deleted
   * through the admin, the others are the history of a URL that was live.
   */
  @Column({ type: "varchar", length: 320, nullable: true })
  createdBy: string | null;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;
}
