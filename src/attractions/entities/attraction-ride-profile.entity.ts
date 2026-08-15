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

/** The four numbers a ride is measured by, metric. Independently nullable. */
export interface RideMeasurements {
  /** Top speed in km/h. */
  topSpeedKmh: number | null;
  /** Highest point in metres. */
  heightM: number | null;
  /** Track length in metres. */
  lengthM: number | null;
  /** Ride duration in seconds. */
  durationSeconds: number | null;
}

/**
 * A ride's measurements as served, with where each number came from.
 *
 * Two writers feed this: the Wikidata import, which is automatic and thin, and
 * the curation, which is hand-written and states what Wikidata does not. They
 * are stored apart and merged on read — see `mergeRideStats`.
 */
export interface RideStats extends RideMeasurements {
  /** "mixed" = some fields curated, some imported. */
  source: "wikidata" | "curated" | "mixed";
  /** Wikidata entity id, e.g. "Q319081". Null when no imported value survived. */
  sourceId: string | null;
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
 * by the upstream syncs on every poll, this is hand-curated data with a
 * completely different lifecycle. Keeping it separate means a sync can never
 * clobber it.
 *
 * Lifecycle: there is no upstream feed and no seed job — these rows ARE the
 * source of truth and are edited directly in the database. Nothing in this
 * codebase writes them, so a deploy can never overwrite the curation. The one
 * exception is `stats` below, which `RideStatsService` imports from Wikidata
 * into its own column.
 *
 * The `elements` / `types` arrays are jsonb with GIN indexes so the reverse
 * lookup ("which rides have a zero-g roll") is a single indexed containment
 * query rather than a scan.
 */
@Entity("attraction_ride_profiles")
@Index("idx_ride_profile_park", ["parkId"])
// GIN indexes for the reverse (glossary → rides) lookup — created by
// RideProfileService.onModuleInit(), not TypeORM sync.
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
   * Written by `RideStatsService`, NOT by hand. It shares this row with the
   * curated columns on purpose (both answer "what is this ride") and stays out
   * of their way by naming only its own column on write.
   *
   * jsonb rather than four columns: nothing queries these yet, they arrive and
   * change together, and the source is free to start stating one more.
   */
  @Column({ type: "jsonb", nullable: true })
  stats: RideStats | null;

  /** When the stats were last imported, so a re-run can skip fresh rows. */
  @Column({ name: "stats_updated_at", type: "timestamptz", nullable: true })
  statsUpdatedAt: Date | null;

  /**
   * The same four measurements, hand-curated — and a separate column for the
   * same reason `stats` is one: two writers, no shared cell. The curation owns
   * this, the importer owns `stats`, and neither can undo the other's work.
   *
   * It exists because Wikidata is thin where it matters most: of the coasters
   * we curate it states a top speed for well under a fifth, and misses nearly
   * every headliner people actually search for. These values are assembled the
   * way the rest of the curation is — from the park's own page, the
   * manufacturer, and the ride's Wikipedia articles cross-checked against
   * each other.
   *
   * On read the two are merged field by field with curated winning
   * (`mergeRideStats`), so a hand-checked speed is not displaced by an import
   * and a Wikidata duration still shows up next to it.
   */
  @Column({ name: "curated_stats", type: "jsonb", nullable: true })
  curatedStats: RideMeasurements | null;

  /** When the curation was last written, for spotting stale rows. */
  @Column({ name: "seeded_at", type: "timestamptz" })
  seededAt: Date;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
