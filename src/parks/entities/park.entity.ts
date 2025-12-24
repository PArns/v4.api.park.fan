import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  OneToMany,
  CreateDateColumn,
  UpdateDateColumn,
  BeforeInsert,
  BeforeUpdate,
  Index,
  JoinColumn,
} from "typeorm";
import { Destination } from "../../destinations/entities/destination.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { Show } from "../../shows/entities/show.entity";
import { Restaurant } from "../../restaurants/entities/restaurant.entity";
import { generateSlug } from "../../common/utils/slug.util";

/**
 * Park Entity
 *
 * Represents a theme park within a destination.
 * Examples: "Magic Kingdom", "EPCOT", "Phantasialand"
 *
 * API Mapping (from GET /v1/entity/{id}):
 * - id → externalId
 * - name → name
 * - location.latitude → latitude
 * - location.longitude → longitude
 * - timezone → timezone
 * - destinationId → destination (FK)
 * - slug → slug (from API or auto-generated)
 */
@Entity("parks")
@Index(["destinationId", "slug"], { unique: true }) // Slug unique per destination
export class Park {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ unique: true })
  @Index()
  externalId: string; // ThemeParks.wiki ID

  @Column()
  @Index()
  name: string;

  @Column()
  @Index()
  slug: string; // Unique per destination, not globally

  @ManyToOne(() => Destination, (destination) => destination.parks)
  @JoinColumn({ name: "destinationId" })
  destination: Destination;

  @Column({ nullable: true })
  destinationId: string;

  @OneToMany(() => Attraction, (attraction) => attraction.park)
  attractions: Attraction[];

  @OneToMany(() => Show, (show) => show.park)
  shows: Show[];

  @OneToMany(() => Restaurant, (restaurant) => restaurant.park)
  restaurants: Restaurant[];

  // Geographic data (enriched via geocoding)
  @Column({ nullable: true })
  continent: string;

  @Column({ nullable: true })
  @Index()
  continentSlug: string; // URL-safe: "north-america", "europe", "asia"

  @Column({ nullable: true })
  country: string;

  @Column({ nullable: true, length: 2 })
  @Index()
  countryCode: string; // ISO 3166-1 alpha-2 (e.g., 'DE', 'FR', 'US')

  @Column({ nullable: true })
  @Index()
  countrySlug: string; // URL-safe: "united-states", "germany", "china"

  // Region/State/Province (e.g. "Florida", "Baden-Württemberg")
  @Column({ nullable: true })
  region: string;

  // Region Code (ISO 3166-2 compatible, e.g. "FL", "BW", "CA")
  // Used for regional holiday filtering
  @Column({ nullable: true, length: 50 })
  @Index()
  regionCode: string;

  @Column({ nullable: true })
  city: string;

  @Column({ nullable: true })
  @Index()
  citySlug: string; // URL-safe: "orlando", "rust", "guangzhou"

  @Column({ type: "decimal", precision: 10, scale: 7, nullable: true })
  latitude: number;

  @Column({ type: "decimal", precision: 10, scale: 7, nullable: true })
  longitude: number;

  // Track when geocoding was last attempted (to avoid retrying failed attempts)
  @Column({ type: "timestamptz", nullable: true })
  geocodingAttemptedAt: Date | null;

  @Column()
  timezone: string;

  // Multi-country influence for ML predictions (holidays/school breaks)
  // Example: Europapark attracts visitors from DE, FR, CH
  @Column("simple-array", { nullable: true })
  influencingCountries: string[]; // ISO country codes: ['DE', 'FR', 'CH']

  // Radius in km to auto-detect neighboring countries (default: 200km)
  // Used as fallback if influencingCountries is not manually set
  @Column({ type: "int", default: 200 })
  influenceRadiusKm: number;

  // Multi-source tracking
  @Column({ name: "primary_data_source", default: "themeparks-wiki" })
  primaryDataSource: string; // 'themeparks-wiki', 'queue-times', 'multi-source'

  @Column({ name: "data_sources", type: "simple-array", nullable: true })
  dataSources: string[]; // ['themeparks-wiki', 'queue-times']

  // Explicit Source IDs (User Request)
  @Column({ name: "wiki_entity_id", type: "text", nullable: true })
  @Index()
  wikiEntityId: string | null; // The UUID from ThemeParks.wiki

  @Column({ name: "queue_times_entity_id", type: "text", nullable: true })
  @Index()
  queueTimesEntityId: string | null; // The ID from Queue-Times (e.g. "8")

  @Column({ name: "wartezeiten_entity_id", type: "text", nullable: true })
  @Index()
  wartezeitenEntityId: string | null; // The UUID from Wartezeiten.app

  // EXPERIMENTAL: Current crowd level from Wartezeiten.app (real-time)
  // ⚠️ NOTE: Data quality unverified - stored for analysis/comparison only
  // Range: 0-100 (float), updated every 5-10 minutes
  // TODO: Compare with ML predictions before exposing in API
  @Column({
    name: "current_crowd_level",
    type: "decimal",
    precision: 5,
    scale: 2,
    nullable: true,
  })
  currentCrowdLevel: number | null;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;

  @Column({ type: "int", default: 0 })
  metadataRetryCount: number;

  @BeforeInsert()
  @BeforeUpdate()
  generateSlug(): void {
    if (this.name && !this.slug) {
      this.slug = generateSlug(this.name);
    }
  }

  @BeforeInsert()
  @BeforeUpdate()
  generateGeographicSlugs(): void {
    // Generate continent slug
    if (this.continent && !this.continentSlug) {
      this.continentSlug = generateSlug(this.continent);
    }

    // Generate country slug
    if (this.country && !this.countrySlug) {
      this.countrySlug = generateSlug(this.country);
    }

    // Generate city slug
    if (this.city && !this.citySlug) {
      this.citySlug = generateSlug(this.city);
    }
  }
}
