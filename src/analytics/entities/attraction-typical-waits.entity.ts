import {
  Entity,
  Column,
  PrimaryColumn,
  ManyToOne,
  JoinColumn,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from "typeorm";
import { Park } from "../../parks/entities/park.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";
import type { TypicalWaitsDto } from "../../attractions/dto/attraction-response.dto";

/**
 * Attraction Typical-Waits Entity
 *
 * Precomputed typical (P50) vs busy (P90) peak-wait stats per headliner, so the
 * park response (and thus the statically-prerendered ride-page shell) can serve
 * them without a per-request percentile scan. Mirrors the rope-drop precompute:
 * a nightly job stores one row per displayable headliner; the park integration
 * reads them in one query and attaches them to the embedded attractions.
 *
 * The payload is the exact `TypicalWaitsDto` the detail endpoint serves, kept as
 * jsonb (no need to flatten — it is read whole and never queried by field). Only
 * `displayable` rows are stored.
 */
@Entity("attraction_typical_waits")
@Index("idx_attraction_typical_waits_park", ["parkId"])
export class AttractionTypicalWaits {
  @PrimaryColumn("uuid")
  attractionId: string;

  @Column("uuid")
  parkId: string;

  @ManyToOne(() => Attraction, { onDelete: "CASCADE" })
  @JoinColumn({ name: "attractionId" })
  attraction: Attraction;

  @ManyToOne(() => Park, { onDelete: "CASCADE" })
  @JoinColumn({ name: "parkId" })
  park: Park;

  /** Always true (we only persist displayable rows) — kept for clarity/filtering. */
  @Column({ type: "boolean", default: true })
  displayable: boolean;

  /** The full typical-waits payload served on the attraction detail endpoint. */
  @Column({ type: "jsonb" })
  data: TypicalWaitsDto;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
