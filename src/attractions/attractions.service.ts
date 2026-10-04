import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { In, IsNull, MoreThan, Repository } from "typeorm";
import { Attraction } from "./entities/attraction.entity";
import { AttractionSlugAlias } from "./entities/attraction-slug-alias.entity";
import { NegativeCache } from "../common/utils/negative-cache.util";
import { normalizeSortDirection, paginate } from "../common/utils/query.util";
import {
  closedRideParkPageCutoff,
  isOnParkPage,
} from "./services/attraction-retirement.service";

@Injectable()
export class AttractionsService {
  /** Negative cache for geographic path lookups that returned null (404).
   *  Key: "continent:country:city:park:attraction". */
  private readonly notFoundCache = new NegativeCache();

  constructor(
    @InjectRepository(Attraction)
    private attractionRepository: Repository<Attraction>,
  ) {}

  /**
   * Get the repository instance (for advanced queries by other services)
   */
  getRepository(): Repository<Attraction> {
    return this.attractionRepository;
  }

  /**
   * Resolve attraction display names for a set of IDs (lightweight id → name map).
   *
   * Used by the calendar to label per-day headliner forecasts without loading
   * full attraction entities/relations. Missing IDs are simply absent from the map.
   */
  async getNamesByIds(ids: string[]): Promise<Map<string, string>> {
    const result = new Map<string, string>();
    if (ids.length === 0) return result;
    const rows = await this.attractionRepository.find({
      where: { id: In(ids) },
      select: ["id", "name"],
    });
    for (const row of rows) {
      result.set(row.id, row.name);
    }
    return result;
  }

  /**
   * Finds all attractions
   */
  async findAll(): Promise<Attraction[]> {
    return this.attractionRepository.find({
      relations: ["park"],
      order: { name: "ASC" },
    });
  }

  /**
   * Finds all attractions with filtering and sorting.
   *
   * Retired attractions are excluded, the same rule `loadParkRelations` applies
   * to the park payload: a retired attraction leaves the lists that describe a
   * park as it is today and keeps its own detail route, so its history stays
   * readable. Without it this list served 34 retired rows across 11 parks on
   * 2026-09-15 (17 of them at Universal Studios Singapore, retired via PAR-159
   * after ThemeParks.wiki reclassified them as shows) while the park payload
   * served none.
   *
   * The `retiredAt` docstring on the response DTO claims the same of search
   * and of the favorites list, and PAR-233 made both true: `searchAttractions`
   * and `loadAttractionIndexFromDb` filter, and so does
   * `FavoritesService.fetchAttractions`. The counts are only partly there —
   * the three in `AnalyticsService` still count retired rows, listed on the
   * `retiredAt` column in `attraction.entity.ts` and tracked as PAR-286.
   */
  async findAllWithFilters(filters: {
    park?: string;
    continentSlug?: string;
    countrySlug?: string;
    citySlug?: string;
    status?: string;
    queueType?: string;
    waitTimeMin?: number;
    waitTimeMax?: number;
    sort?: string;
    page?: number;
    limit?: number;
  }): Promise<{ data: Attraction[]; total: number }> {
    const queryBuilder = this.attractionRepository
      .createQueryBuilder("attraction")
      .leftJoinAndSelect("attraction.park", "park")
      .andWhere("attraction.retiredAt IS NULL");

    // Filter by park slug
    if (filters.park) {
      queryBuilder.andWhere("park.slug = :parkSlug", {
        parkSlug: filters.park,
      });
    }

    // Filter by geographic parameters (from geo routes)
    if (filters.continentSlug) {
      queryBuilder.andWhere("park.continentSlug = :continentSlug", {
        continentSlug: filters.continentSlug,
      });
    }

    if (filters.countrySlug) {
      queryBuilder.andWhere("park.countrySlug = :countrySlug", {
        countrySlug: filters.countrySlug,
      });
    }

    if (filters.citySlug) {
      queryBuilder.andWhere("park.citySlug = :citySlug", {
        citySlug: filters.citySlug,
      });
    }

    // For status, queueType, and waitTime filters, we need to join queue_data
    if (
      filters.status ||
      filters.queueType ||
      filters.waitTimeMin !== undefined ||
      filters.waitTimeMax !== undefined ||
      (filters.sort && filters.sort.startsWith("waitTime"))
    ) {
      // Join with latest queue data using DISTINCT ON subquery
      // This replaces the O(n²) correlated subquery with a single efficient query
      // Uses the composite index on (attractionId, queueType, timestamp) for optimal performance
      // Prioritizes STANDBY queue type, falls back to others if STANDBY not available
      //
      // Time-bound the subquery so TimescaleDB can exclude old chunks. Without it,
      // DISTINCT ON over the whole compressed hypertable decompresses every chunk
      // (~6s isolated, ~14s under load). A current-status view only cares about
      // recent readings; a 2-day window keeps even overnight/poll-gap parks (which
      // still emit fresh CLOSED rows each poll) while scanning ~2 of ~160 chunks.
      const latestSince = new Date(Date.now() - 2 * 24 * 60 * 60 * 1000);
      queryBuilder.leftJoin(
        (subQuery) => {
          return subQuery
            .where("qd.timestamp >= :latestSince")
            .select("qd.attractionId")
            .addSelect("qd.queueType")
            .addSelect("qd.timestamp")
            .addSelect("qd.status")
            .addSelect("qd.waitTime")
            .addSelect("qd.state")
            .addSelect("qd.returnStart")
            .addSelect("qd.returnEnd")
            .addSelect("qd.price")
            .addSelect("qd.allocationStatus")
            .addSelect("qd.currentGroupStart")
            .addSelect("qd.currentGroupEnd")
            .addSelect("qd.estimatedWait")
            .addSelect("qd.lastUpdated")
            .addSelect("qd.dataSource")
            .addSelect("qd.id")
            .from("queue_data", "qd")
            .distinctOn(["qd.attractionId"])
            .orderBy("qd.attractionId", "ASC")
            .addOrderBy(
              "CASE WHEN qd.queueType = 'STANDBY' THEN 0 ELSE 1 END",
              "ASC",
            ) // Prioritize STANDBY
            .addOrderBy("qd.timestamp", "DESC"); // Latest first within each group
        },
        "qd",
        "qd.attractionId = attraction.id",
      );
      queryBuilder.setParameter("latestSince", latestSince);

      // Filter by status
      if (filters.status) {
        queryBuilder.andWhere("qd.status = :status", {
          status: filters.status,
        });
      }

      // Filter by queue type
      if (filters.queueType) {
        queryBuilder.andWhere('qd."queueType" = :queueType', {
          queueType: filters.queueType,
        });
      }

      // Filter by wait time range
      if (filters.waitTimeMin !== undefined) {
        queryBuilder.andWhere('qd."waitTime" >= :waitTimeMin', {
          waitTimeMin: filters.waitTimeMin,
        });
      }

      if (filters.waitTimeMax !== undefined) {
        queryBuilder.andWhere('qd."waitTime" <= :waitTimeMax', {
          waitTimeMax: filters.waitTimeMax,
        });
      }
    }

    // Apply sorting
    if (filters.sort) {
      const [field, direction = "asc"] = filters.sort.split(":");
      const sortDirection = normalizeSortDirection(direction);

      if (field === "name") {
        queryBuilder.orderBy("attraction.name", sortDirection);
      } else if (field === "waitTime") {
        queryBuilder.orderBy('qd."waitTime"', sortDirection);
      } else if (field === "status") {
        queryBuilder.orderBy("qd.status", sortDirection);
      }
    } else {
      // Default sort by name
      queryBuilder.orderBy("attraction.name", "ASC");
    }

    return paginate(queryBuilder, filters.page, filters.limit);
  }

  /**
   * Finds attraction by slug
   */
  async findBySlug(slug: string): Promise<Attraction | null> {
    return this.attractionRepository.findOne({
      where: { slug },
      relations: ["park", "park.destination"],
    });
  }

  /**
   * Finds all attractions in a specific park
   *
   * Used for hierarchical routes: /parks/:parkSlug/attractions
   *
   * @param parkId - Park ID (UUID)
   * @returns Array of attractions in this park
   */
  async findByParkId(
    parkId: string,
    page: number = 1,
    limit: number = 10,
  ): Promise<{ data: Attraction[]; total: number }> {
    const [data, total] = await this.attractionRepository.findAndCount({
      // Retired attractions leave the park's list. The row and its history
      // stay; only its own detail endpoint still answers for it.
      where: { parkId, retiredAt: IsNull() },
      relations: ["park", "park.destination"],
      order: { name: "ASC" },
      take: limit,
      skip: (page - 1) * limit,
    });

    return { data, total };
  }

  /**
   * The rides of a park that closed for good and still show on its page:
   * retired as `closed` (a reclassification closed nothing), not hidden by an
   * editor, and closed within `CLOSED_RIDE_PARK_PAGE_DAYS` — after a year a
   * ride answers only on its own URL. The park page lists them apart from its
   * live rides; see `buildClosedAttractions`.
   */
  async findClosedForParkPage(
    parkId: string,
    now: Date = new Date(),
  ): Promise<Attraction[]> {
    const rows = await this.attractionRepository.find({
      where: {
        parkId,
        retiredAt: MoreThan(closedRideParkPageCutoff(now)),
        retiredHidden: false,
      },
    });
    return rows.filter((row) => isOnParkPage(row, now));
  }

  /**
   * Finds attraction by slug within a specific park
   *
   * Used for hierarchical routes: /parks/:parkSlug/attractions/:attractionSlug
   *
   * @param parkId - Park ID (UUID)
   * @param attractionSlug - Attraction slug
   * @returns Attraction if found in this park, null otherwise
   */
  async findBySlugInPark(
    parkId: string,
    attractionSlug: string,
  ): Promise<Attraction | null> {
    return this.attractionRepository.findOne({
      where: {
        parkId,
        slug: attractionSlug,
      },
      relations: ["park", "park.destination"],
    });
  }

  /**
   * Where an attraction slug that no longer answers has gone, if it was ever
   * recorded (PAR-687).
   *
   * The park is read through the alias's attraction, so a ride a park merge
   * moved keeps its old slugs without a write of its own. Returns null when
   * nothing is recorded, or when the alias would point at the path it came
   * from — a redirect to itself is a loop, not an answer.
   */
  async resolveSlugAlias(
    continentSlug: string,
    countrySlug: string,
    citySlug: string,
    parkSlug: string,
    attractionSlug: string,
  ): Promise<{
    continentSlug: string;
    countrySlug: string;
    citySlug: string;
    parkSlug: string;
    attractionSlug: string;
  } | null> {
    const row = await this.attractionRepository
      .createQueryBuilder("attraction")
      .innerJoin(
        AttractionSlugAlias,
        "alias",
        "alias.attractionId = attraction.id AND alias.slug = :attractionSlug",
        { attractionSlug },
      )
      .innerJoin("attraction.park", "park")
      .where("park.continentSlug = :continentSlug", { continentSlug })
      .andWhere("park.countrySlug = :countrySlug", { countrySlug })
      .andWhere("park.citySlug = :citySlug", { citySlug })
      .andWhere("park.slug = :parkSlug", { parkSlug })
      .orderBy("alias.createdAt", "DESC")
      .select("attraction.slug", "slug")
      .getRawOne<{ slug: string }>();

    if (!row?.slug || row.slug === attractionSlug) return null;
    return {
      continentSlug,
      countrySlug,
      citySlug,
      parkSlug,
      attractionSlug: row.slug,
    };
  }

  /**
   * Finds attraction by geographic path (continent/country/city/park/attraction)
   *
   * Used for full geo routes: /parks/:continent/:country/:city/:parkSlug/attractions/:attractionSlug
   *
   * @param continentSlug - Continent slug
   * @param countrySlug - Country slug
   * @param citySlug - City slug
   * @param parkSlug - Park slug
   * @param attractionSlug - Attraction slug
   * @returns Attraction if found, null otherwise
   */
  async findByGeographicPath(
    continentSlug: string,
    countrySlug: string,
    citySlug: string,
    parkSlug: string,
    attractionSlug: string,
  ): Promise<Attraction | null> {
    const cacheKey = `${continentSlug}:${countrySlug}:${citySlug}:${parkSlug}:${attractionSlug}`;
    if (this.notFoundCache.has(cacheKey)) {
      return null; // known 404 — skip DB query
    }

    const attraction = await this.attractionRepository
      .createQueryBuilder("attraction")
      .leftJoinAndSelect("attraction.park", "park")
      .leftJoinAndSelect("park.destination", "destination")
      .where("attraction.slug = :attractionSlug", { attractionSlug })
      .andWhere("park.continentSlug = :continentSlug", { continentSlug })
      .andWhere("park.countrySlug = :countrySlug", { countrySlug })
      .andWhere("park.citySlug = :citySlug", { citySlug })
      .andWhere("park.slug = :parkSlug", { parkSlug })
      .getOne();

    if (!attraction) {
      this.notFoundCache.add(cacheKey);
    }

    return attraction;
  }

  /**
   * Update land information for an attraction
   *
   * Used by wait-times processor to assign land/area names from Queue-Times data
   *
   * @param attractionId - Attraction ID (UUID)
   * @param landName - Land/area name (e.g., "Tomorrowland")
   * @param landExternalId - Queue-Times land ID
   */
  async updateLandInfo(
    attractionId: string,
    landName: string,
    landExternalId: string | null,
  ): Promise<boolean> {
    const attraction = await this.attractionRepository.findOne({
      where: { id: attractionId },
      select: ["landName", "landExternalId"],
    });

    if (
      attraction &&
      attraction.landName === landName &&
      attraction.landExternalId === landExternalId
    ) {
      return false; // No change
    }

    await this.attractionRepository.update(attractionId, {
      landName,
      landExternalId,
    });

    return true; // Updated
  }
}
