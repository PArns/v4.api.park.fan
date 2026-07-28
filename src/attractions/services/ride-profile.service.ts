import {
  Inject,
  Injectable,
  Logger,
  OnModuleInit,
  Optional,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { In, Repository } from "typeorm";
import { AttractionRideProfile } from "../entities/attraction-ride-profile.entity";
import { RIDE_PROFILE_SEED } from "../data/ride-profile-seed";
import { RideProfileSeedEntry } from "../data/ride-profile-seed.types";

export interface RideProfileSeedResult {
  written: number;
  skipped: number;
}

/** One ride in the reverse (glossary → rides) lookup. */
export interface AttractionWithTerm {
  attractionId: string;
  attractionName: string;
  attractionSlug: string;
  parkId: string;
  parkName: string;
  parkSlug: string;
  citySlug: string;
  countrySlug: string;
  continentSlug: string;
  /** Where the term appears: as a track figure or as a ride type. */
  kind: "element" | "type" | "manufacturer";
  openedYear: number | null;
}

/** Injection token so tests can supply their own seed. */
export const RIDE_PROFILE_SEED_TOKEN = "RIDE_PROFILE_SEED";

/**
 * Owns the curated ride profiles: applies the seed, and serves both
 * directions of the ride ↔ glossary link.
 *
 * Pure database work — the seed is a checked-in file, there is no upstream to
 * call. Applying it is idempotent, so re-running after an edit is safe and
 * cheap (a few hundred rows).
 */
@Injectable()
export class RideProfileService implements OnModuleInit {
  private readonly logger = new Logger(RideProfileService.name);

  constructor(
    @InjectRepository(AttractionRideProfile)
    private readonly repo: Repository<AttractionRideProfile>,
    @Optional()
    @Inject(RIDE_PROFILE_SEED_TOKEN)
    private readonly seed: RideProfileSeedEntry[] = RIDE_PROFILE_SEED,
  ) {}

  onModuleInit(): void {
    // Fire and forget, same as the search service's trigram indexes: these are
    // CREATE…IF NOT EXISTS statements and the reverse lookup still works (just
    // with a seq scan over a few hundred rows) until they land.
    void this.ensureContainmentIndexes();
  }

  /**
   * GIN indexes for `elements @> '["zero-g-roll"]'` and the same on `types`.
   * `jsonb_path_ops` is the right operator class here: we only ever ask
   * "contains this value", never "has this key".
   */
  private async ensureContainmentIndexes(): Promise<void> {
    try {
      await this.repo.query(
        `CREATE INDEX IF NOT EXISTS idx_ride_profile_elements_gin
           ON attraction_ride_profiles USING gin (elements jsonb_path_ops);`,
      );
      await this.repo.query(
        `CREATE INDEX IF NOT EXISTS idx_ride_profile_types_gin
           ON attraction_ride_profiles USING gin (types jsonb_path_ops);`,
      );
    } catch (err) {
      this.logger.debug(`Ride-profile GIN indexes not created: ${err}`);
    }
  }

  /**
   * Write the curated seed to the database.
   *
   * A seed entry whose slugs match no attraction is skipped silently — park
   * and ride slugs drift as things are renamed, and a stale line in the seed
   * must not fail the whole run.
   */
  async apply(): Promise<RideProfileSeedResult> {
    const entries = this.seed ?? RIDE_PROFILE_SEED;
    const result: RideProfileSeedResult = { written: 0, skipped: 0 };
    const seededAt = new Date();

    for (const entry of entries) {
      const attraction = await this.repo.manager
        .createQueryBuilder()
        .select(["attraction.id AS id", "attraction.parkId AS parkid"])
        .from("attractions", "attraction")
        .innerJoin("parks", "park", "park.id = attraction.parkId")
        .where("park.citySlug = :citySlug", { citySlug: entry.citySlug })
        .andWhere("park.slug = :parkSlug", { parkSlug: entry.parkSlug })
        .andWhere("attraction.slug = :attractionSlug", {
          attractionSlug: entry.attractionSlug,
        })
        .getRawOne<{ id: string; parkid: string }>();

      if (!attraction) {
        result.skipped++;
        continue;
      }

      await this.repo.save({
        attractionId: attraction.id,
        parkId: attraction.parkid,
        elements: entry.elements ?? [],
        types: entry.types ?? [],
        manufacturerName: entry.manufacturer ?? null,
        manufacturerTermId: entry.manufacturerTermId ?? null,
        model: entry.model ?? null,
        openedYear: entry.openedYear ?? null,
        inversions: entry.inversions ?? null,
        seededAt,
      });
      result.written++;
    }

    this.logger.log(
      `🎢 Ride profiles applied: ${result.written} written, ${result.skipped} skipped (no matching attraction)`,
    );
    return result;
  }

  /** The profile for one attraction, or null when it has not been curated. */
  async findByAttraction(
    attractionId: string,
  ): Promise<AttractionRideProfile | null> {
    return this.repo.findOne({ where: { attractionId } });
  }

  /** Profiles for many attractions at once, keyed by attraction id. */
  async findByAttractions(
    attractionIds: string[],
  ): Promise<Map<string, AttractionRideProfile>> {
    if (attractionIds.length === 0) return new Map();
    const rows = await this.repo.find({
      where: { attractionId: In(attractionIds) },
    });
    return new Map(rows.map((row) => [row.attractionId, row]));
  }

  /**
   * The reverse link: every ride that features a glossary term.
   *
   * Matches the term in all three places it can appear (track figure, ride
   * type, manufacturer) in one query, so a glossary page for "Mack Rides" and
   * one for "Zero-G Roll" both work through the same call. Ordered by park so
   * the caller can group without a second sort.
   */
  async findAttractionsByTerm(
    termId: string,
    limit = 200,
  ): Promise<AttractionWithTerm[]> {
    return this.repo
      .createQueryBuilder("profile")
      .innerJoin(
        "attractions",
        "attraction",
        "attraction.id = profile.attractionId",
      )
      .innerJoin("parks", "park", "park.id = profile.parkId")
      .select([
        "profile.attractionId AS attractionid",
        "attraction.name AS attractionname",
        "attraction.slug AS attractionslug",
        "profile.parkId AS parkid",
        "park.name AS parkname",
        "park.slug AS parkslug",
        'park."citySlug" AS cityslug',
        'park."countrySlug" AS countryslug',
        'park."continentSlug" AS continentslug',
        "profile.openedYear AS openedyear",
        `CASE
           WHEN profile.elements @> :containment::jsonb THEN 'element'
           WHEN profile.types @> :containment::jsonb THEN 'type'
           ELSE 'manufacturer'
         END AS kind`,
      ])
      .where(
        `(profile.elements @> :containment::jsonb
          OR profile.types @> :containment::jsonb
          OR profile.manufacturer_term_id = :termId)`,
        { containment: JSON.stringify([termId]), termId },
      )
      .orderBy("park.name", "ASC")
      .addOrderBy("attraction.name", "ASC")
      .limit(limit)
      .getRawMany<{
        attractionid: string;
        attractionname: string;
        attractionslug: string;
        parkid: string;
        parkname: string;
        parkslug: string;
        cityslug: string;
        countryslug: string;
        continentslug: string;
        openedyear: number | null;
        kind: "element" | "type" | "manufacturer";
      }>()
      .then((rows) =>
        rows.map((row) => ({
          attractionId: row.attractionid,
          attractionName: row.attractionname,
          attractionSlug: row.attractionslug,
          parkId: row.parkid,
          parkName: row.parkname,
          parkSlug: row.parkslug,
          citySlug: row.cityslug,
          countrySlug: row.countryslug,
          continentSlug: row.continentslug,
          kind: row.kind,
          openedYear: row.openedyear,
        })),
      );
  }

  /**
   * How many rides carry each glossary term, for the whole curated set.
   *
   * The glossary overview uses this to show a count next to a term without
   * running one query per term.
   */
  async countByTerm(): Promise<Record<string, number>> {
    const rows = await this.repo.query<Array<{ term: string; count: string }>>(
      `SELECT term, COUNT(DISTINCT "attractionId")::text AS count
         FROM (
           SELECT "attractionId", jsonb_array_elements_text(elements) AS term
             FROM attraction_ride_profiles
           UNION ALL
           SELECT "attractionId", jsonb_array_elements_text(types) AS term
             FROM attraction_ride_profiles
           UNION ALL
           SELECT "attractionId", manufacturer_term_id AS term
             FROM attraction_ride_profiles
            WHERE manufacturer_term_id IS NOT NULL
         ) AS terms
        GROUP BY term`,
    );
    return Object.fromEntries(rows.map((row) => [row.term, Number(row.count)]));
  }
}
