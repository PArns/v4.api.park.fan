import { Injectable, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import Redis from "ioredis";
import { Attraction } from "../attractions/entities/attraction.entity";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { buildAttractionUrl } from "../common/utils/url.util";

export interface AttractionSitemapItem {
  url: string;
  slug: string;
}

@Injectable()
export class SitemapService {
  private readonly CACHE_KEY = "sitemap:attractions:v1";
  private readonly CACHE_TTL = 24 * 60 * 60; // 24h — attractions change rarely

  constructor(
    @InjectRepository(Attraction)
    private readonly attractionRepository: Repository<Attraction>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  async getAttractionsSitemap(): Promise<AttractionSitemapItem[]> {
    const cached = await this.redis.get(this.CACHE_KEY);
    if (cached) {
      return JSON.parse(cached);
    }

    // Only select fields needed for URL building — no analytics, no status
    const attractions = await this.attractionRepository
      .createQueryBuilder("a")
      .select([
        "a.slug",
        "p.slug",
        "p.continentSlug",
        "p.countrySlug",
        "p.citySlug",
      ])
      .innerJoin("a.park", "p")
      .where("p.continentSlug IS NOT NULL")
      .andWhere("p.countrySlug IS NOT NULL")
      .andWhere("p.citySlug IS NOT NULL")
      .getMany();

    const items: AttractionSitemapItem[] = attractions
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
}
