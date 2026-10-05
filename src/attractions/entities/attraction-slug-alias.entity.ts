import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  JoinColumn,
  ManyToOne,
  PrimaryGeneratedColumn,
} from "typeorm";
import { Attraction } from "./attraction.entity";

/**
 * A slug an attraction used to answer at, inside its park.
 *
 * The counterpart of `ParkSlugAlias` one level down, and missing until
 * PAR-687: an attraction slug is indexed, linked from blog posts and stored in
 * the media sidecars, so every change to one — a merge removing the loser's
 * `-2` slug, a slug corrected by hand — was a permanent 404 with nothing
 * recording where the ride went. A row here lets the attraction lookup answer
 * the old path with a 301 to the current one.
 *
 * Keyed on the attraction, not on the park: the park is read through the
 * attraction at lookup time, so a park merge that moves the ride to another
 * park row carries its old slugs along without a write of its own. The
 * attraction merge moves the loser's rows to the winner
 * (`ATTRACTION_DEPENDENCIES`) and adds the loser's own slug.
 */
@Entity("attraction_slug_aliases")
@Index(["attractionId", "slug"], { unique: true })
@Index(["slug"])
export class AttractionSlugAlias {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ type: "uuid" })
  attractionId: string;

  @ManyToOne(() => Attraction, { onDelete: "CASCADE" })
  @JoinColumn({ name: "attractionId" })
  attraction: Attraction;

  @Column()
  slug: string;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;
}
