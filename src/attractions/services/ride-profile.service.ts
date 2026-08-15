import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { In, MoreThanOrEqual, Repository } from "typeorm";
import { AttractionRideProfile } from "../entities/attraction-ride-profile.entity";

/** A park whose cached response a write invalidated. */
export interface TouchedPark {
  parkId: string;
  attractionIds: string[];
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
  /**
   * Typical peak wait in whole minutes — the P90 over 548 days from
   * `attraction_p90_baselines`, not a live reading. Null when the ride has no
   * baseline yet, which is normal for recently added or rarely open rides.
   */
  typicalPeakWait: number | null;
  /** Whether the baseline job classified this ride as one of its park's headliners. */
  isHeadliner: boolean;
}

/** How the reverse lookup orders its rides. */
export type TermAttractionSort = "park" | "popularity";

/**
 * Reads the `sort` query parameter.
 *
 * Unknown values fall back to `park` rather than raising: this endpoint is
 * public and already in use, and a mistyped query string should still answer
 * with a usable list.
 */
export function parseTermAttractionSort(
  raw: string | undefined,
): TermAttractionSort {
  return raw === "popularity" ? "popularity" : "park";
}

/**
 * Serves both directions of the ride ↔ glossary link.
 *
 * Read-only. The curated profiles in `attraction_ride_profiles` are edited
 * directly in the database — there is no seed file and no apply job to run,
 * so nothing here writes the curation. `RideStatsService` still writes the
 * `stats` column from Wikidata, which is a different writer for a different
 * column.
 */
@Injectable()
export class RideProfileService implements OnModuleInit {
  private readonly logger = new Logger(RideProfileService.name);

  constructor(
    @InjectRepository(AttractionRideProfile)
    private readonly repo: Repository<AttractionRideProfile>,
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
   * Every distinct glossary term id the curation actually stores, across all
   * three places one can appear.
   *
   * `jsonb_array_elements_text` over a few hundred rows — no index needed, and
   * this runs on demand rather than per request.
   */
  async findDistinctTermIds(): Promise<string[]> {
    const rows = await this.repo.query<{ term_id: string }[]>(`
      SELECT DISTINCT term_id FROM (
        SELECT jsonb_array_elements_text(elements) AS term_id
          FROM attraction_ride_profiles
        UNION ALL
        SELECT jsonb_array_elements_text(types) FROM attraction_ride_profiles
        UNION ALL
        SELECT manufacturer_term_id FROM attraction_ride_profiles
         WHERE manufacturer_term_id IS NOT NULL
      ) t
      ORDER BY term_id
    `);
    return rows.map((r) => r.term_id);
  }

  /**
   * Which rides use a given set of term ids — so an audit can name the pages a
   * broken id actually damages instead of only the id.
   */
  async findRidesUsingTermIds(
    termIds: string[],
  ): Promise<{ termId: string; parkSlug: string; attractionSlug: string }[]> {
    if (termIds.length === 0) return [];
    return this.repo.query(
      `
      SELECT t.term_id AS "termId", p.slug AS "parkSlug", a.slug AS "attractionSlug"
        FROM attraction_ride_profiles rp
        JOIN attractions a ON a.id = rp."attractionId"
        JOIN parks p ON p.id = rp."parkId"
        CROSS JOIN LATERAL (
          SELECT jsonb_array_elements_text(rp.elements) AS term_id
          UNION ALL SELECT jsonb_array_elements_text(rp.types)
          UNION ALL SELECT rp.manufacturer_term_id
        ) t
       WHERE t.term_id = ANY($1)
       ORDER BY t.term_id, p.slug, a.slug
      `,
      [termIds],
    );
  }

  /**
   * Parks whose curation was written since `since`, with their attractions —
   * shaped for {@link TouchedPark} so a publish run can evict exactly those.
   *
   * `seeded_at` is what a hand-written UPDATE is expected to set (the guide
   * says so), which makes it the only marker of "this row was just curated".
   */
  async findCuratedSince(since: Date): Promise<TouchedPark[]> {
    const rows = await this.repo.find({
      where: { seededAt: MoreThanOrEqual(since) },
      select: ["attractionId", "parkId"],
    });

    const byPark = new Map<string, string[]>();
    for (const row of rows) {
      const ids = byPark.get(row.parkId) ?? [];
      ids.push(row.attractionId);
      byPark.set(row.parkId, ids);
    }
    return [...byPark].map(([parkId, attractionIds]) => ({
      parkId,
      attractionIds,
    }));
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
    sort: TermAttractionSort = "park",
  ): Promise<AttractionWithTerm[]> {
    // `@>` with a single-element array is the containment form the GIN
    // jsonb_path_ops index can serve.
    const containment = JSON.stringify([termId]);

    const query = this.repo
      .createQueryBuilder("profile")
      .innerJoin(
        "attractions",
        "attraction",
        "attraction.id = profile.attractionId",
      )
      .innerJoin("parks", "park", "park.id = profile.parkId")
      // LEFT, not INNER: a ride with no baseline must still appear in the
      // list. The count endpoint and this list are read side by side on the
      // glossary overview, and an inner join would make them disagree.
      .leftJoin(
        "attraction_p90_baselines",
        "baseline",
        'baseline."attractionId" = profile."attractionId"',
      )
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
        'baseline."p90Baseline" AS p90baseline',
        'baseline."isHeadliner" AS isheadliner',
        "baseline.confidence AS confidence",
      ])
      .where(
        `(profile.elements @> :containment::jsonb
          OR profile.types @> :containment::jsonb
          OR profile.manufacturerTermId = :termId)`,
        { containment, termId },
      );

    if (sort === "popularity") {
      // Confidence first: `low` means a handful of samples, and those readings
      // swing wildly. Sorting purely by P90 would put them on top.
      query
        .orderBy(
          `CASE baseline.confidence
             WHEN 'high' THEN 0
             WHEN 'medium' THEN 1
             ELSE 2
           END`,
          "ASC",
        )
        .addOrderBy('baseline."p90Baseline"', "DESC", "NULLS LAST");
    } else {
      query.orderBy("park.name", "ASC");
    }

    // Always last, in both modes: without a total order, two rides with the
    // same baseline can swap places between identical requests.
    query.addOrderBy("park.name", "ASC").addOrderBy("attraction.name", "ASC");

    const rows = await query.limit(limit).getRawMany<{
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
      p90baseline: string | null;
      isheadliner: boolean | null;
      confidence: "high" | "medium" | "low" | null;
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
      // `decimal` comes back from pg as a string ("75.00"), so this needs an
      // explicit conversion — `row.p90baseline > 60` would compare strings.
      typicalPeakWait:
        row.p90baseline === null ? null : Math.round(Number(row.p90baseline)),
      isHeadliner: row.isheadliner ?? false,
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
