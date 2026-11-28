import {
  Column,
  Entity,
  Index,
  JoinColumn,
  ManyToOne,
  PrimaryGeneratedColumn,
} from "typeorm";
import { Park } from "../../parks/entities/park.entity";
import type { FacilityType, OsmElementType } from "../facility.types";

/**
 * One facility point taken from OpenStreetMap.
 *
 * This table holds OSM-derived rows and nothing else, and that is a licence
 * decision, not a style one: the table is the database the ODbL applies to,
 * and everything outside it (parks, rides, waits) stays a separate database
 * beside it (ODbL 4.5 a). Nothing here is typed by hand. A wrong point is
 * corrected in OpenStreetMap and imported again; the one thing an admin sets
 * is `hidden`, which removes a row from the answer without changing it.
 * docs/frontend/facilities.md.
 */
@Entity("park_facilities_osm")
@Index(["parkId", "osmType", "osmId"], { unique: true })
export class ParkFacilityOsm {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @ManyToOne(() => Park, { onDelete: "CASCADE" })
  @JoinColumn({ name: "park_id" })
  park?: Park;

  @Column({ name: "park_id", type: "uuid" })
  parkId: string;

  @Column({ type: "text" })
  type: FacilityType;

  @Column({ type: "double precision" })
  latitude: number;

  @Column({ type: "double precision" })
  longitude: number;

  @Column({ type: "text", nullable: true })
  name: string | null;

  @Column({ type: "text", nullable: true })
  wheelchair: "yes" | "limited" | "no" | null;

  @Column({ name: "changing_table", type: "boolean", nullable: true })
  changingTable: boolean | null;

  @Column({ type: "boolean", nullable: true })
  fee: boolean | null;

  @Column({ name: "osm_type", type: "text" })
  osmType: OsmElementType;

  /** A string: node ids passed 2^31 years ago and pg returns bigint as text. */
  @Column({ name: "osm_id", type: "bigint" })
  osmId: string;

  @Column({ name: "osm_version", type: "int" })
  osmVersion: number;

  /** When the element was last edited in OSM: how stale the point may be. */
  @Column({ name: "osm_timestamp", type: "timestamptz" })
  osmTimestamp: Date;

  @Column({ name: "fetched_at", type: "timestamptz" })
  fetchedAt: Date;

  /** False for a building whose centre falls outside the outline it touches. */
  @Column({ name: "inside_outline", type: "boolean", default: true })
  insideOutline: boolean;

  /** Set in the admin, kept across imports. A hidden row is never served. */
  @Column({ type: "boolean", default: false })
  hidden: boolean;

  /**
   * New, or changed in something a client shows, since the park was last
   * approved: not served until the next approval clears it. The rest of an
   * approved park keeps being served meanwhile. Rows from before this column
   * existed take the default, served as they were.
   */
  @Column({ name: "awaiting_review", type: "boolean", default: false })
  awaitingReview: boolean;
}
