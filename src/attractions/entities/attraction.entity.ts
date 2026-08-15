import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  CreateDateColumn,
  UpdateDateColumn,
  BeforeInsert,
  BeforeUpdate,
  Index,
  JoinColumn,
  OneToMany,
} from "typeorm";
import { Park } from "../../parks/entities/park.entity";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import { generateSlug } from "../../common/utils/slug.util";

/**
 * Attraction Entity
 *
 * Represents a ride/attraction within a park.
 * Examples: "Taron", "Space Mountain", "Flight of Passage"
 *
 * API Mapping (from GET /v1/entity/{id}/children):
 * - id → externalId
 * - name → name
 * - location.latitude → latitude
 * - location.longitude → longitude
 * - parentId → park (FK)
 * - slug → slug (from API or auto-generated)
 */
@Entity("attractions")
@Index(["parkId", "slug"], { unique: true }) // Slug unique per park
@Index(["parkId", "name"]) // Optimize deduplication and name-based filtering within a park
// GIN trigram indexes for fuzzy search — managed by SearchService.onModuleInit(), not TypeORM sync
@Index("idx_attraction_name_trgm", { synchronize: false })
@Index("idx_attraction_land_name_trgm", { synchronize: false })
export class Attraction {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ unique: true })
  externalId: string; // ThemeParks.wiki ID — unique: true already creates an index

  @Column({ name: "queue_times_entity_id", type: "text", nullable: true })
  @Index()
  queueTimesEntityId: string | null; // The ID from Queue-Times (e.g. "8")

  @Column()
  @Index()
  name: string;

  @Column()
  @Index()
  slug: string; // Unique per park, not globally

  @ManyToOne(() => Park, (park) => park.attractions)
  @JoinColumn({ name: "parkId" })
  park: Park;

  @Column()
  @Index("idx_attraction_park_id", ["parkId"])
  parkId: string;

  @Column({ type: "decimal", precision: 10, scale: 7, nullable: true })
  latitude: number;

  @Column({ type: "decimal", precision: 10, scale: 7, nullable: true })
  longitude: number;

  // Land/Area (from Queue-Times)
  @Column({ name: "land_name", type: "text", nullable: true })
  @Index() // B-tree: land name search/filter queries
  landName: string | null; // e.g., "Tomorrowland", "Adventureland"

  @Column({ name: "land_external_id", type: "text", nullable: true })
  landExternalId: string | null; // Queue-Times land ID

  @Column({ nullable: true })
  attractionType: string;

  // Minimum rider height in cm. Synced from ThemeParks.wiki entity documents
  // (which mirror the parks' own app APIs). Hand-curated fallbacks fill the
  // parks the wiki does not cover (Disney) and simply live in this column —
  // the sync only overwrites them once the wiki publishes a number of its own,
  // which is the intended order: the park's own sign wins.
  //
  // This cell belongs to the sync. A correction to a number the wiki already
  // publishes would be overwritten on the next run, so it goes in
  // `curated_minimum_height` instead.
  @Column({ name: "minimum_height", type: "int", nullable: true })
  minimumHeight: number | null;

  // Maximum rider height in cm (kiddie rides). Rare upstream (~2% of rides).
  @Column({ name: "maximum_height", type: "int", nullable: true })
  maximumHeight: number | null;

  // Whether riders may get wet (water rides). Null = unknown, not "dry".
  /**
   * Unit the operator publishes the height in. `minimumHeight` is always
   * centimetres; this only says how it should be shown. US parks publish
   * inches (52"), and converting that to "132 cm" on their ride pages would
   * contradict the number on the park's own signage.
   */
  @Column({
    name: "minimum_height_unit",
    type: "varchar",
    length: 2,
    nullable: true,
  })
  minimumHeightUnit: "cm" | "in" | null;

  /** Whether the ride may soak you, as ThemeParks.wiki reports it. */
  @Column({ name: "may_get_wet", type: "boolean", nullable: true })
  mayGetWet: boolean | null;

  /**
   * The same flag, hand-corrected — and a separate column for the same reason
   * `curated_stats` is one on the ride profile: two writers, no shared cell.
   *
   * The detail sync overwrites `may_get_wet` whenever the wiki publishes a
   * value that differs, so a correction written into that column survives only
   * until the next run. It exists because the wiki populates the flag for a
   * few dozen of ~7000 attractions and is occasionally wrong where it does —
   * Genting SkyWorlds' shot tower is flagged as a water ride and is not.
   *
   * Null means "nothing to correct"; read it as `curatedMayGetWet ?? mayGetWet`
   * and never write it from a sync.
   */
  @Column({ name: "curated_may_get_wet", type: "boolean", nullable: true })
  curatedMayGetWet: boolean | null;

  /**
   * The minimum height, hand-corrected — same two-writers rule as
   * `curated_may_get_wet`, and for a demonstrated reason.
   *
   * Upstream reads the parks' app fields, and those conflate "you must be this
   * tall" with "below this height you need an adult with you". Phantasialand's
   * Winni Splash is the worked example: the wiki publishes 100, while the
   * park's own Nutzungsbedingungen say children under 1.00 m may play *when
   * accompanied* — no minimum at all. Its neighbour Wavy Battle genuinely
   * forbids entry below 1.00 m and upstream carries nothing.
   *
   * Always centimetres. Null means "nothing to correct"; **0 means "no minimum
   * height"**, which is not really a sentinel — a 0 cm minimum excludes nobody
   * — and lets a correction say "upstream's number is wrong and the truth is
   * none". Resolve it through `resolveCuratedFacts`, never inline, and never
   * write it from a sync.
   */
  @Column({ name: "curated_minimum_height", type: "int", nullable: true })
  curatedMinimumHeight: number | null;

  /**
   * Whether the ride has a single-rider line at all — a static fact about the
   * queue layout, not a live reading.
   *
   * Deliberately NOT derived from the live `queues` array on each request: a
   * single-rider queue that is closed right now, or a park whose wait times we
   * cannot read, would make the ride look as though it never had one. The
   * `queues` array keeps answering "is it open and how long"; this answers
   * "does it exist".
   *
   * Seeded from observation — every attraction that has ever reported a
   * SINGLE_RIDER queue — and extended by hand for the rides whose feed does not
   * publish the queue even though the park runs one. No sync writes it.
   */
  @Column({ name: "has_single_rider", type: "boolean", nullable: true })
  hasSingleRider: boolean | null;

  // RCDB (rcdb.com) database ID for outbound links (https://rcdb.com/{id}.htm).
  // Originally from Wikidata property P2751 (CC0); edited by hand in the
  // database now, with no upstream writer — linking to RCDB is explicitly
  // permitted by their ToS, ingesting their data is not.
  //
  // One id must never sit on two attractions: it would point a ride page at a
  // different ride. Two did, from a name-based match across parks that hold a
  // ride of the same name. Check before adding one:
  //   SELECT rcdb_id, count(*) FROM attractions WHERE rcdb_id IS NOT NULL
  //    GROUP BY rcdb_id HAVING count(*) > 1;
  @Column({ name: "rcdb_id", type: "int", nullable: true })
  rcdbId: number | null;

  @Column({ name: "is_seasonal", default: false })
  isSeasonal: boolean;

  @Column({ name: "season_months", type: "jsonb", nullable: true })
  seasonMonths: number[] | null;

  // When true: attraction is shown as OPERATING whenever the park is OPERATING,
  // regardless of queue data status. Use for free-flow attractions (playgrounds,
  // water play areas, climbing structures) that have no traditional queue but
  // are physically accessible when the park is open.
  @Column({ name: "open_with_park", default: false })
  openWithPark: boolean;

  /**
   * When this attraction stopped existing. Null means it is still around.
   *
   * A ride that has been demolished is not "closed today" and it is not
   * "unknown" either — both of those describe a state it could come back from.
   * Before this column the API had no way to say the third thing, so Disney's
   * Dino-Sue, torn down with DinoLand in February 2026, sat in the park's
   * attraction list forever.
   *
   * The row and its queue_data are deliberately KEPT: the wait-time history is
   * load-bearing for baselines and models, and a ride page that says "operated
   * until February 2026" is worth more than a 404. What retirement changes is
   * visibility — a retired attraction leaves the park's live list, the
   * operating counts, search and favorites, while its own detail endpoint keeps
   * answering.
   *
   * No sync writes this, the same two-writers rule as the curated columns.
   */
  @Column({ name: "retired_at", type: "timestamptz", nullable: true })
  @Index("idx_attraction_retired_at", { where: "retired_at IS NULL" })
  retiredAt: Date | null;

  /**
   * Why, and on whose authority — the source URL belongs in here. A retirement
   * is a claim about the world, so it travels with its evidence.
   */
  @Column({ name: "retired_reason", type: "text", nullable: true })
  retiredReason: string | null;

  @OneToMany(() => QueueData, (queueData) => queueData.attraction)
  queueData: QueueData[];

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;

  @BeforeInsert()
  @BeforeUpdate()
  generateSlug(): void {
    if (this.name && !this.slug) {
      this.slug = generateSlug(this.name);
    }
  }
}
