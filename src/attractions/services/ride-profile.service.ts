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
  /**
   * The `citySlug/parkSlug/attractionSlug` of every entry that matched no
   * attraction. Skipping is deliberate — ride slugs drift and one stale line
   * must not fail the run — but a bare count makes that drift invisible: a
   * curated ride silently stops being served and nothing says which one.
   */
  skippedKeys: string[];
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
    const result: RideProfileSeedResult = {
      written: 0,
      skipped: 0,
      skippedKeys: [],
    };
    const seededAt = new Date();

    // Resolve every slug triple in ONE query. Doing it per entry was ~500
    // round-trips for a job whose whole point is to finish in seconds.
    const rows = await this.repo.manager
      .createQueryBuilder()
      .select([
        "attraction.id AS id",
        'attraction."parkId" AS parkid',
        'park."citySlug" AS cityslug',
        "park.slug AS parkslug",
        "attraction.slug AS attractionslug",
      ])
      .from("attractions", "attraction")
      .innerJoin("parks", "park", 'park.id = attraction."parkId"')
      .getRawMany<{
        id: string;
        parkid: string;
        cityslug: string;
        parkslug: string;
        attractionslug: string;
      }>();

    const byKey = new Map(
      rows.map((row) => [
        `${row.cityslug}/${row.parkslug}/${row.attractionslug}`,
        row,
      ]),
    );

    const profiles: Partial<AttractionRideProfile>[] = [];
    for (const entry of entries) {
      const key = `${entry.citySlug}/${entry.parkSlug}/${entry.attractionSlug}`;
      const match = byKey.get(key);
      if (!match) {
        result.skipped++;
        result.skippedKeys.push(key);
        continue;
      }

      profiles.push({
        attractionId: match.id,
        parkId: match.parkid,
        elements: entry.elements ?? [],
        types: entry.types ?? [],
        manufacturerName: entry.manufacturer ?? null,
        manufacturerTermId: entry.manufacturerTermId ?? null,
        model: entry.model ?? null,
        openedYear: entry.openedYear ?? null,
        inversions: entry.inversions ?? null,
        seededAt,
      });
    }

    // Chunked upsert on the primary key: re-running after a seed edit updates
    // in place instead of erroring, and never leaves a half-written table.
    const CHUNK = 200;
    for (let i = 0; i < profiles.length; i += CHUNK) {
      await this.repo.upsert(profiles.slice(i, i + CHUNK), ["attractionId"]);
    }
    result.written = profiles.length;

    this.logger.log(
      `🎢 Ride profiles applied: ${result.written} written, ${result.skipped} skipped (no matching attraction)`,
    );
    if (result.skippedKeys.length > 0) {
      // Naming them is the whole point: a skipped entry is curated data that
      // silently stops being served, and the only way to notice is to be told.
      this.logger.warn(
        `🎢 Ride profiles with no matching attraction: ${result.skippedKeys.join(", ")}`,
      );
    }
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
    // `@>` with a single-element array is the containment form the GIN
    // jsonb_path_ops index can serve.
    const containment = JSON.stringify([termId]);

    const rows = await this.repo
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
        // Returned so `kind` can be derived in TypeScript. A bound parameter
        // inside a SELECT expression is not something the query builder
        // guarantees to substitute, and these arrays are a handful of strings.
        "profile.elements AS elements",
        "profile.types AS types",
      ])
      .where(
        `(profile.elements @> :containment::jsonb
          OR profile.types @> :containment::jsonb
          OR profile.manufacturerTermId = :termId)`,
        { containment, termId },
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
        elements: string[] | null;
        types: string[] | null;
      }>();

    return rows.map((row) => ({
      attractionId: row.attractionid,
      attractionName: row.attractionname,
      attractionSlug: row.attractionslug,
      parkId: row.parkid,
      parkName: row.parkname,
      parkSlug: row.parkslug,
      citySlug: row.cityslug,
      countrySlug: row.countryslug,
      continentSlug: row.continentslug,
      kind: row.elements?.includes(termId)
        ? "element"
        : row.types?.includes(termId)
          ? "type"
          : "manufacturer",
      openedYear: row.openedyear,
    }));
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
