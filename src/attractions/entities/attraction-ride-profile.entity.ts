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
import { Attraction } from "./attraction.entity";

/**
 * A ride's measurements, imported from Wikidata, with the entity they came
 * from. Every field is independently nullable — Wikidata states what someone
 * bothered to enter, which for most rides is nothing.
 */
export interface RideStats {
  /** Top speed in km/h. */
  topSpeedKmh: number | null;
  /** Highest point in metres. */
  heightM: number | null;
  /** Track length in metres. */
  lengthM: number | null;
  /** Ride duration in seconds. */
  durationSeconds: number | null;
  source: "wikidata";
  /** Wikidata entity id, e.g. "Q319081". */
  sourceId: string;
}

/**
 * Attraction Ride Profile Entity
 *
 * The curated "what kind of ride is this, and what does it do" record that
 * connects an attraction to the frontend glossary. Every id stored here is a
 * **glossary term id** (park.fan `lib/glossary/data.ts`), so a ride page can
 * render its layout as links into the glossary and the glossary can list the
 * rides that feature a term — the link works in both directions off one table.
 *
 * Why a table and not columns on `attractions`: the attraction row is written
 * by the upstream syncs on every poll, this is hand-curated seed data with a
 * completely different lifecycle. Keeping it separate means a sync can never
 * clobber it and the seed can be re-applied independently.
 *
 * Lifecycle: written **only** by `RideProfileSeedService.apply()` from
 * `RIDE_PROFILE_SEED`. There is no upstream feed — updates happen by editing
 * the seed file and re-running the job (`POST /v1/admin/apply-ride-profiles`).
 *
 * The `elements` / `types` arrays are jsonb with GIN indexes so the reverse
 * lookup ("which rides have a zero-g roll") is a single indexed containment
 * query rather than a scan.
 */
@Entity("attraction_ride_profiles")
@Index("idx_ride_profile_park", ["parkId"])
// GIN indexes for the reverse (glossary → rides) lookup — created by
// RideProfileSeedService.onModuleInit(), not TypeORM sync.
@Index("idx_ride_profile_elements_gin", { synchronize: false })
@Index("idx_ride_profile_types_gin", { synchronize: false })
export class AttractionRideProfile {
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

  /**
   * Glossary term ids of the ride's track elements, **in ride order** —
   * `['lifthill', 'first-drop', 'vertical-loop', …]`. Order is meaningful and
   * is what the ride page renders as the layout walkthrough, so never sort it.
   * Empty for rides that have no track figures (dark rides, flat rides).
   */
  @Column({ type: "jsonb", default: () => "'[]'::jsonb" })
  elements: string[];

  /**
   * Glossary term ids describing what kind of ride this is — from the
   * `coasters` and `attractions` categories (`['launch-coaster',
   * 'terrain-coaster']`, `['dark-ride', 'omnimover']`). Unordered set.
   */
  @Column({ type: "jsonb", default: () => "'[]'::jsonb" })
  types: string[];

  /**
   * Manufacturer as a display name — always set when known ("Bolliger &
   * Mabillard", "Walt Disney Imagineering"). Free text on purpose: the
   * glossary only covers the best-known builders, and a ride by a builder we
   * have no term for should still say who built it.
   */
  @Column({ name: "manufacturer_name", type: "text", nullable: true })
  manufacturerName: string | null;

  /**
   * Glossary term id of the manufacturer (`manufacturers` category) when we
   * have one — null means "show `manufacturerName` as plain text, no link".
   */
  @Column({ name: "manufacturer_term_id", type: "text", nullable: true })
  manufacturerTermId: string | null;

  /** Manufacturer's own model name, e.g. "Blitz Coaster", "Inverted Coaster". */
  @Column({ type: "text", nullable: true })
  model: string | null;

  /** Year the ride opened to the public. */
  @Column({ name: "opened_year", type: "int", nullable: true })
  openedYear: number | null;

  /**
   * Inversion count as the manufacturer/park publishes it. Kept as its own
   * column rather than derived from `elements`, because the two legitimately
   * disagree: `elements` lists an element once even when the layout repeats
   * it, and non-inverting figures (a zero-g stall on some models) are counted
   * differently by different sources.
   */
  @Column({ type: "int", nullable: true })
  inversions: number | null;

  /**
   * Measured facts imported from Wikidata — speed, height, length, duration.
   *
   * Written by `RideStatsService`, NOT by the seed. The two writers share this
   * row on purpose (both answer "what is this ride") and stay out of each
   * other's way because the seed's `upsert` names only its own columns.
   *
   * jsonb rather than four columns: nothing queries these yet, they arrive and
   * change together, and the source is free to start stating one more.
   */
  @Column({ type: "jsonb", nullable: true })
  stats: RideStats | null;

  /** When the stats were last imported, so a re-run can skip fresh rows. */
  @Column({ name: "stats_updated_at", type: "timestamptz", nullable: true })
  statsUpdatedAt: Date | null;

  /** When the seed entry was last written, for spotting stale curation. */
  @Column({ name: "seeded_at", type: "timestamptz" })
  seededAt: Date;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
