import { Column, Entity, JoinColumn, ManyToOne, PrimaryColumn } from "typeorm";
import { Park } from "../../parks/entities/park.entity";
import type { OsmElementType } from "../facility.types";

export type FacilityReviewStatus = "imported" | "approved";

/**
 * The state of a park's facility import: which OSM boundary it was cut with,
 * when, and whether somebody has looked at the result.
 *
 * `imported` rows are never served. An import that changes anything puts an
 * approved park back to `imported`, so a point nobody saw cannot reach a
 * visitor through a re-import.
 */
@Entity("park_facility_imports")
export class ParkFacilityImport {
  @PrimaryColumn({ name: "park_id", type: "uuid" })
  parkId: string;

  @ManyToOne(() => Park, { onDelete: "CASCADE" })
  @JoinColumn({ name: "park_id" })
  park?: Park;

  @Column({ type: "text", default: "imported" })
  status: FacilityReviewStatus;

  @Column({ name: "boundary_osm_type", type: "text" })
  boundaryOsmType: OsmElementType;

  @Column({ name: "boundary_osm_id", type: "bigint" })
  boundaryOsmId: string;

  @Column({ name: "boundary_name", type: "text", nullable: true })
  boundaryName: string | null;

  @Column({ name: "imported_at", type: "timestamptz" })
  importedAt: Date;

  /** Overpass' `timestamp_osm_base`: the OSM state the import saw. */
  @Column({ name: "osm_base", type: "timestamptz", nullable: true })
  osmBase: Date | null;

  @Column({ name: "approved_at", type: "timestamptz", nullable: true })
  approvedAt: Date | null;

  /** The admin's e-mail; never leaves the admin routes. */
  @Column({ name: "approved_by", type: "text", nullable: true })
  approvedBy: string | null;
}
