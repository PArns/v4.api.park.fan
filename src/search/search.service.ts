import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Show } from "../shows/entities/show.entity";
import { Restaurant } from "../restaurants/entities/restaurant.entity";
import { SearchQueryDto } from "./dto/search-query.dto";
import { SearchResultDto, SearchResultItemDto } from "./dto/search-result.dto";

@Injectable()
export class SearchService {
  constructor(
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
    @InjectRepository(Attraction)
    private readonly attractionRepository: Repository<Attraction>,
    @InjectRepository(Show)
    private readonly showRepository: Repository<Show>,
    @InjectRepository(Restaurant)
    private readonly restaurantRepository: Repository<Restaurant>,
  ) {}

  async search(query: SearchQueryDto): Promise<SearchResultDto> {
    const { q, type, limit = 20, offset = 0 } = query;

    // Determine which entity types to search
    const searchTypes =
      type && type.length > 0
        ? type
        : ["park", "attraction", "show", "restaurant"];

    const results: SearchResultItemDto[] = [];
    let totalCount = 0;

    // Search parks
    if (searchTypes.includes("park")) {
      const parks = await this.searchParks(q, limit, offset);
      results.push(
        ...parks.map((p) => ({
          type: "park" as const,
          id: p.id,
          name: p.name,
          slug: p.slug,
        })),
      );
      totalCount += parks.length;
    }

    // Search attractions
    if (searchTypes.includes("attraction")) {
      const attractions = await this.searchAttractions(q, limit, offset);
      results.push(
        ...attractions.map((a) => ({
          type: "attraction" as const,
          id: a.id,
          name: a.name,
          slug: a.slug,
        })),
      );
      totalCount += attractions.length;
    }

    // Search shows
    if (searchTypes.includes("show")) {
      const shows = await this.searchShows(q, limit, offset);
      results.push(
        ...shows.map((s) => ({
          type: "show" as const,
          id: s.id,
          name: s.name,
          slug: s.slug,
        })),
      );
      totalCount += shows.length;
    }

    // Search restaurants
    if (searchTypes.includes("restaurant")) {
      const restaurants = await this.searchRestaurants(q, limit, offset);
      results.push(
        ...restaurants.map((r) => ({
          type: "restaurant" as const,
          id: r.id,
          name: r.name,
          slug: r.slug,
        })),
      );
      totalCount += restaurants.length;
    }

    // Sort all results by relevance (similarity score)
    // For now, keep insertion order (can add sorting later if needed)

    return {
      results: results.slice(0, limit),
      total: totalCount,
      query: q,
      searchTypes,
    };
  }

  private async searchParks(
    query: string,
    limit: number,
    offset: number,
  ): Promise<Pick<Park, "id" | "slug" | "name">[]> {
    return this.parkRepository
      .createQueryBuilder("park")
      .select(["park.id", "park.slug", "park.name"])
      .where("park.name ILIKE :query", { query: `%${query}%` })
      .orderBy("similarity(park.name, :exactQuery)", "DESC")
      .setParameter("exactQuery", query)
      .limit(limit)
      .offset(offset)
      .getMany();
  }

  private async searchAttractions(
    query: string,
    limit: number,
    offset: number,
  ): Promise<Pick<Attraction, "id" | "slug" | "name">[]> {
    return this.attractionRepository
      .createQueryBuilder("attraction")
      .select(["attraction.id", "attraction.slug", "attraction.name"])
      .where("attraction.name ILIKE :query", { query: `%${query}%` })
      .orderBy("similarity(attraction.name, :exactQuery)", "DESC")
      .setParameter("exactQuery", query)
      .limit(limit)
      .offset(offset)
      .getMany();
  }

  private async searchShows(
    query: string,
    limit: number,
    offset: number,
  ): Promise<Pick<Show, "id" | "slug" | "name">[]> {
    return this.showRepository
      .createQueryBuilder("show")
      .select(["show.id", "show.slug", "show.name"])
      .where("show.name ILIKE :query", { query: `%${query}%` })
      .orderBy("similarity(show.name, :exactQuery)", "DESC")
      .setParameter("exactQuery", query)
      .limit(limit)
      .offset(offset)
      .getMany();
  }

  private async searchRestaurants(
    query: string,
    limit: number,
    offset: number,
  ): Promise<Pick<Restaurant, "id" | "slug" | "name">[]> {
    return this.restaurantRepository
      .createQueryBuilder("restaurant")
      .select(["restaurant.id", "restaurant.slug", "restaurant.name"])
      .where("restaurant.name ILIKE :query", { query: `%${query}%` })
      .orderBy("similarity(restaurant.name, :exactQuery)", "DESC")
      .setParameter("exactQuery", query)
      .limit(limit)
      .offset(offset)
      .getMany();
  }
}
