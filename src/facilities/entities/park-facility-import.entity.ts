import { Column, Entity, JoinColumn, ManyToOne, PrimaryColumn } from "typeorm";
import { Park } from "../../parks/entities/park.entity";
import type { OsmElementType, OutlineRings } from "../facility.types";

/**
 * `pending`: no import has succeeded yet (no outline found, or it failed).
 * `imported`: rows are there that nobody approved yet; with `approvedAt` set,
 * the park was approved before and its approved rows are still served.
 * `approved`: everything is approved and served.
 */
export type FacilityReviewStatus = "pending" | "imported" | "approved";

/** How the last import attempt for a park ended. */
export type FacilityRunOutcome = "ok" | "no_outline" | "failed";

/**
 * The state of a park's facility import: which OSM outline it was cut with,
 * how the last attempt ended, and whether somebody has looked at the result.
 *
 * A park with `approvedAt` serves its rows that are neither hidden nor
 * `awaitingReview`. An import that brings something a visitor would newly see
 * marks those rows and puts the park back to `imported`, and the rest stays
 * served until the next approval takes the new ones too; a failed attempt
 * changes neither the rows nor the review state.
 */
@Entity("park_facility_imports")
export class ParkFacilityImport {
  @PrimaryColumn({ name: "park_id", type: "uuid" })
  parkId: string;

  @ManyToOne(() => Park, { onDelete: "CASCADE" })
  @JoinColumn({ name: "park_id" })
  park?: Park;

  @Column({ type: "text", default: "pending" })
  status: FacilityReviewStatus;

  @Column({ name: "boundary_osm_type", type: "text", nullable: true })
  boundaryOsmType: OsmElementType | null;

  @Column({ name: "boundary_osm_id", type: "bigint", nullable: true })
  boundaryOsmId: string | null;

  @Column({ name: "boundary_name", type: "text", nullable: true })
  boundaryName: string | null;

  /** The outline as it was at the last import, for the review plot. */
  @Column({ name: "boundary_rings", type: "jsonb", nullable: true })
  boundaryRings: OutlineRings | null;

  /** The last import that succeeded. */
  @Column({ name: "imported_at", type: "timestamptz", nullable: true })
  importedAt: Date | null;

  /** Overpass' `timestamp_osm_base`: the OSM state the import saw. */
  @Column({ name: "osm_base", type: "timestamptz", nullable: true })
  osmBase: Date | null;

  @Column({ name: "last_run", type: "text" })
  lastRun: FacilityRunOutcome;

  @Column({ name: "last_run_at", type: "timestamptz" })
  lastRunAt: Date;

  /** Why it failed, or the outlines found when none carried the park's name. */
  @Column({ name: "last_run_message", type: "text", nullable: true })
  lastRunMessage: string | null;

  /**
   * The last approval, kept while later points await review: set means the
   * park is served. Withdrawing clears it.
   */
  @Column({ name: "approved_at", type: "timestamptz", nullable: true })
  approvedAt: Date | null;

  /** The admin's e-mail; never leaves the admin routes. */
  @Column({ name: "approved_by", type: "text", nullable: true })
  approvedBy: string | null;
}
