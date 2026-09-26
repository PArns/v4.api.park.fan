import { Injectable, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import Redis from "ioredis";
import { Attraction } from "../attractions/entities/attraction.entity";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { CacheKeys } from "../common/cache/cache-keys";
import { buildAttractionUrl } from "../common/utils/url.util";
import { safeJsonParse } from "../common/utils/json.util";
import {
  chooseNameDuplicateWinner,
  nameDuplicateKey,
} from "../common/utils/name-duplicate.util";
import { resolveAttractionName } from "../attractions/utils/curated-attraction-facts.util";

export interface AttractionSitemapItem {
  url: string;
  slug: string;
}

@Injectable()
export class SitemapService {
  private readonly CACHE_KEY = CacheKeys.sitemapAttractions();
  private readonly CACHE_TTL = 24 * 60 * 60; // 24h — attractions change rarely

  constructor(
    @InjectRepository(Attraction)
    private readonly attractionRepository: Repository<Attraction>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  async getAttractionsSitemap(): Promise<AttractionSitemapItem[]> {
    const cached = safeJsonParse<AttractionSitemapItem[]>(
      await this.redis.get(this.CACHE_KEY),
    );
    if (cached) {
      return cached;
    }

    // Only select fields needed for URL building and for the name grouping
    // below — no analytics, no status
    const attractions = await this.attractionRepository
      .createQueryBuilder("a")
      .select([
        "a.slug",
        "a.name",
        "a.curatedName",
        "p.id",
        "p.slug",
        "p.continentSlug",
        "p.countrySlug",
        "p.citySlug",
      ])
      .innerJoin("a.park", "p")
      .where("p.continentSlug IS NOT NULL")
      .andWhere("p.countrySlug IS NOT NULL")
      .andWhere("p.citySlug IS NOT NULL")
      // Same rule as `ParksService.loadParkRelations`: a retired ride leaves every list that
      // describes the park as it is today. The frontend resolves a ride page from the park
      // payload, which already drops it, so listing it here advertised a 404 in six locales.
      // Measured 2026-09-24: 70 of the 7,374 rows were retired (36 of them in September).
      .andWhere("a.retiredAt IS NULL")
      .getMany();

    const items: AttractionSitemapItem[] = this.oneRowPerName(attractions)
      .map((a) => {
        const url = buildAttractionUrl(a.park, a);
        return url ? { url, slug: a.slug } : null;
      })
      .filter((item): item is AttractionSitemapItem => item !== null);

    await this.redis.setex(
      this.CACHE_KEY,
      this.CACHE_TTL,
      JSON.stringify(items),
    );
    return items;
  }

  /**
   * Drops the rows the park payload does not serve.
   *
   * `ParkIntegrationService.deduplicateEntities` keeps one row per attraction
   * name inside a park, so listing every row here advertised a URL that
   * resolves to nothing. Measured 2026-09-26 over all 201 parks: 48 of the
   * 7,304 entries had no row in the park payload, and they were exactly the
   * losers of the 45 duplicate name groups — no other reason for a gap existed
   * in either direction.
   *
   * Grouping is per park, on the resolved name trimmed by `nameDuplicateKey`
   * and on nothing else, because
   * that is the payload's key (`docs/architecture/attraction-status-and-seasonality.md`
   * §4a explains why it may not be the slug or the `externalId`). The winner is
   * `chooseNameDuplicateWinner`, shared with the payload so there is one rule
   * and not two, and status-free so the surviving URL does not move with the
   * feed (PAR-498).
   */
  private oneRowPerName(attractions: Attraction[]): Attraction[] {
    // Nested rather than a joined key: a separator is one more thing that can
    // collide with a ride name, and nothing here needs a flat key.
    const byPark = new Map<string, Map<string, Attraction[]>>();

    for (const attraction of attractions) {
      let byName = byPark.get(attraction.park.id);
      if (!byName) {
        byName = new Map<string, Attraction[]>();
        byPark.set(attraction.park.id, byName);
      }

      const name = nameDuplicateKey(resolveAttractionName(attraction));
      // A row the payload refuses to serve must not be advertised either.
      if (name === null) continue;

      const group = byName.get(name);
      if (group) group.push(attraction);
      else byName.set(name, [attraction]);
    }

    const kept: Attraction[] = [];
    for (const group of [...byPark.values()].flatMap((byName) => [
      ...byName.values(),
    ])) {
      if (group.length === 1) {
        kept.push(group[0]);
        continue;
      }

      const winner = chooseNameDuplicateWinner(
        group.map((a) => ({
          slug: a.slug,
          name: resolveAttractionName(a),
          attraction: a,
        })),
      );
      if (winner) kept.push(winner.attraction);
    }

    return kept;
  }
}
