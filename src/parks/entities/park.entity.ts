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
@Index(["continentSlug", "countrySlug", "citySlug", "slug"]) // Optimize findByGeographicPath queries
// GIN trigram indexes for fuzzy search — managed by SearchService.onModuleInit(), not TypeORM sync
@Index("idx_park_name_trgm", { synchronize: false })
@Index("idx_park_city_trgm", { synchronize: false })
@Index("idx_park_country_trgm", { synchronize: false })
@Index("idx_park_continent_trgm", { synchronize: false })
// GiST word-similarity index on `name` (the `<%` operator). Unlike the GIN
// indexes above it sits on the bare `name` column, so without this declaration
// TypeORM sync treats it as extraneous and drops it on every boot (it's then
// recreated by SearchService). The functional `_normalized` indexes need no
// such declaration — TypeORM ignores expression indexes.
@Index("idx_park_name_word_trgm", { synchronize: false })
export class Park {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ unique: true })
  externalId: string; // ThemeParks.wiki ID — unique: true already creates an index

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
  @Index() // B-tree: GROUP BY / ORDER BY in geo queries
  continent: string;

  @Column({ nullable: true })
  @Index()
  continentSlug: string; // URL-safe: "north-america", "europe", "asia"

  @Column({ nullable: true })
  @Index() // B-tree: equality + sort in search/geo queries
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
  @Index() // B-tree: equality + sort in search/geo queries
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
  // Structure: [{ countryCode: 'DE', regionCode: 'DE-NRW' }, { countryCode: 'NL', regionCode: null }]
  @Column("jsonb", { nullable: true })
  influencingRegions: { countryCode: string; regionCode: string | null }[];

  // Radius in km to auto-detect neighboring countries (default: 200km)
  // Used as fallback if influencingRegions is not manually set
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

  // ─── curated columns ──────────────────────────────────────────────────────
  // Everything above is written by a sync or a job. Everything here is written
  // only by a human, through the admin curation endpoints, and is merged over
  // its counterpart on read — see `resolveCuratedPark`. The rule is the one the
  // attraction heights already follow: two writers, no shared cell. Put a
  // correction in a synced column and the next run silently reverts it.

  /**
   * The park's name as it should be shown.
   *
   * The metadata sync writes `name` from ThemeParks.wiki, whose names are
   * legal-ish and long ("Disney's Hollywood Studios", "Universal Studios
   * Florida — a Universal Destinations & Experiences park"), while the name on
   * the signage and in every conversation about the place is shorter.
   *
   * Display only. `slug` and the geographic path are NOT regenerated from it,
   * because a park's URL is a published address: changing it is
   * `ParkRenameService`'s job, which writes a `park_slug_aliases` row so the
   * old path keeps redirecting. Renaming for display and renaming the address
   * are different decisions and stay different operations.
   */
  @Column({ name: "curated_name", type: "text", nullable: true })
  curatedName: string | null;

  /**
   * `THEME_PARK` / `WATER_PARK`, corrected.
   *
   * `park_type` is inferred upstream and gets combined resorts wrong — a water
   * park sharing a destination with a theme park is routinely filed as the
   * latter, which puts it in the wrong listings and gives it the wrong crowd
   * expectations.
   */
  @Column({ name: "curated_park_type", type: "text", nullable: true })
  curatedParkType: string | null;

  // Timezone is deliberately NOT curated here, and the reason is worth writing
  // down so it does not get "fixed" later. `timezone` is read at 206 call
  // sites — every schedule, every park-local date, the whole seasonality month
  // derivation. A curated column resolved only in the DTO would show the right
  // zone on the park page while the calendar underneath it stayed wrong, which
  // is worse than not offering the correction at all. Correcting a timezone
  // means correcting `timezone` itself, which is the sync's cell; that needs a
  // sync-side guard, not a second column.

  /**
   * Why this park's wait times cannot be read, or null when they can.
   *
   * The same fact `PARKS_WITHOUT_LIVE_WAIT_TIMES` holds in code, and for the
   * same reason: from outside, a park with no source and a park shut for the
   * night are byte-identical in the payload, so this can never be derived. The
   * column exists so the next Hansa-Park can be recorded by whoever noticed,
   * in the admin, instead of waiting for a deploy — the code list stays as the
   * seed and the fallback.
   *
   * Values are the `NoLiveWaitTimesReason` strings, which the frontend
   * translates, so they are contract: `in_park_app_only` | `not_published`.
   */
  @Column({
    name: "curated_no_wait_times_reason",
    type: "text",
    nullable: true,
  })
  curatedNoWaitTimesReason: string | null;

  // ─── the facts no feed carries ────────────────────────────────────────────
  // Everything below has exactly one writer, a human, and is here for the same
  // reason `has_single_rider` is on the attraction: neither ThemeParks.wiki nor
  // Queue-Times nor Wartezeiten.app states any of it, so there is no column to
  // correct and nothing to merge. A park's own address is not a wait time.
  //
  // They are grouped in the editor as Links / Kontakt / Eckdaten and reach the
  // frontend as one `info` object on the park detail payload, never on the
  // listings: the card overlay re-fetches its nine fields every five minutes
  // and a postal code has no business in that budget.

  /**
   * The park's own site.
   *
   * Deliberately one URL and not one per locale. Most parks answer a plain
   * `europapark.de` with the visitor's own language, the ones that do not are a
   * redirect away from it, and a six-column set would be six columns nobody
   * fills for 212 parks. If a park ever needs a language-specific address, that
   * is a season-style row, not a wider column here.
   */
  @Column({ name: "curated_website", type: "text", nullable: true })
  curatedWebsite: string | null;

  /** Where tickets are actually bought — often a different host from the site. */
  @Column({ name: "curated_tickets_url", type: "text", nullable: true })
  curatedTicketsUrl: string | null;

  /** The article, for a reader who wants the history rather than the queue. */
  @Column({ name: "curated_wikipedia_url", type: "text", nullable: true })
  curatedWikipediaUrl: string | null;

  @Column({ name: "curated_instagram_url", type: "text", nullable: true })
  curatedInstagramUrl: string | null;

  @Column({ name: "curated_facebook_url", type: "text", nullable: true })
  curatedFacebookUrl: string | null;

  @Column({ name: "curated_youtube_url", type: "text", nullable: true })
  curatedYoutubeUrl: string | null;

  /**
   * Street and house number.
   *
   * The geocoding fills `city`, `country` and coordinates and stops there, so
   * the one line somebody needs to type into a navigation system is the one
   * line we do not have. It also completes the `PostalAddress` in the park
   * page's structured data, which until now claimed a locality and no street.
   */
  @Column({ name: "curated_street_address", type: "text", nullable: true })
  curatedStreetAddress: string | null;

  @Column({ name: "curated_postal_code", type: "text", nullable: true })
  curatedPostalCode: string | null;

  /** Stored as it should be dialled, including the country code. */
  @Column({ name: "curated_phone", type: "text", nullable: true })
  curatedPhone: string | null;

  /** The year it opened to the public, not the year the company was founded. */
  @Column({ name: "curated_opened_year", type: "int", nullable: true })
  curatedOpenedYear: number | null;

  /**
   * Area in hectares — `double precision`, because a quarter of the parks worth
   * recording are under 30 ha and rounding those to whole hectares throws away
   * the digit that distinguishes them.
   */
  @Column({
    name: "curated_area_hectares",
    type: "double precision",
    nullable: true,
  })
  curatedAreaHectares: number | null;

  /**
   * What this park calls its paid queue-jump product.
   *
   * On the park and not on each ride, because it is a brand: Phantasialand
   * sells QuickPass across the whole park, Heide Park an Express Pass. Typing
   * it per attraction would mean typing it forty times and watching it drift
   * into "Quick Pass" on the eleventh. A ride whose product genuinely differs
   * overrides it in `attractions.fast_pass_name`.
   *
   * Empty means the API falls back to the neutral "Fast Pass" for any ride
   * flagged as having one — see `resolveFastPass`.
   */
  @Column({ name: "curated_fast_pass_name", type: "text", nullable: true })
  curatedFastPassName: string | null;

  /**
   * What the cheapest version of the product costs, when it is not priced per
   * ride.
   *
   * Almost every park sells one pass for the whole visit rather than one per
   * ride: Heide Park's Express Ticket starts at 25 €, Energylandia's ENERGY
   * PASS is 239 zł, Walibi's Fast Lane is a day product. Only Phantasialand and
   * Disney price per attraction, and both do it dynamically — so
   * `attractions.fast_pass_price` stays empty at almost every park, and this is
   * the number a visitor actually wants.
   *
   * A floor, not a price: the tiers above it (Gold, Unlimited, Ultimate) cost
   * more and change more often. Rendered as "ab 25 €" and never as "25 €".
   */
  @Column({
    name: "curated_fast_pass_price_from",
    type: "double precision",
    nullable: true,
  })
  curatedFastPassPriceFrom: number | null;

  /**
   * The glossary term that explains this park's product, e.g. `quick-pass`.
   *
   * A frontend glossary id, exactly like the ones on `attraction_ride_profiles`,
   * and stored for the same reason: the id is the identity, the name is a label
   * that gets renamed. It is what lets a ride page's "QuickPass" chip link to
   * the entry that explains what a QuickPass actually buys you.
   *
   * Nothing in this database can validate it — the glossary lives in the
   * frontend — so the admin checks it against the published id list on write,
   * and a wrong one does not error, it just links nowhere.
   */
  @Column({ name: "curated_fast_pass_term_id", type: "text", nullable: true })
  curatedFastPassTermId: string | null;

  /**
   * ISO-4217 code the park's prices are quoted in, e.g. `EUR`.
   *
   * A property of the park rather than of each price, and it is what makes the
   * per-ride fast-pass price servable at all: a bare "12" on a ride page is not
   * a price. Without this column the API withholds the number.
   */
  @Column({
    name: "curated_currency",
    type: "varchar",
    length: 3,
    nullable: true,
  })
  curatedCurrency: string | null;

  /**
   * Free-text note for whoever reads this row next.
   *
   * Not shown to visitors. It is where "the park's own site lists opening
   * hours only in a PDF" goes — the context that explains why the other
   * curated columns on this row say what they say.
   */
  @Column({ name: "curation_note", type: "text", nullable: true })
  curationNote: string | null;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;

  @Column({ name: "park_type", default: "THEME_PARK", nullable: true })
  parkType: string; // 'THEME_PARK' | 'WATER_PARK'

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
