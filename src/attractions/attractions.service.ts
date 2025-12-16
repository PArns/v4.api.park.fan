import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Attraction } from "./entities/attraction.entity";
import { ThemeParksClient } from "../external-apis/themeparks/themeparks.client";
import { ThemeParksMapper } from "../external-apis/themeparks/themeparks.mapper";
import { ParksService } from "../parks/parks.service";
import { generateSlug, generateUniqueSlug } from "../common/utils/slug.util";
import { normalizeSortDirection } from "../common/utils/query.util";

@Injectable()
export class AttractionsService {
  private readonly logger = new Logger(AttractionsService.name);

  constructor(
    @InjectRepository(Attraction)
    private attractionRepository: Repository<Attraction>,
    private themeParksClient: ThemeParksClient,
    private themeParksMapper: ThemeParksMapper,
    private parksService: ParksService,
  ) {}

  /**
   * Get the repository instance (for advanced queries by other services)
   */
  getRepository(): Repository<Attraction> {
    return this.attractionRepository;
  }

  /**
   * Syncs all attractions from ThemeParks.wiki
   *
   * Strategy:
   * 1. Ensure parks are synced first
   * 2. For each park, fetch children (attractions)
   * 3. Map and save to DB
   */
  async syncAttractions(): Promise<number> {
    this.logger.log("Syncing attractions from ThemeParks.wiki...");

    // Ensure parks are synced first
    let parks = await this.parksService.findAll();

    if (parks.length === 0) {
      this.logger.warn("No parks found. Syncing parks first...");
      await this.parksService.syncParks();
      // Re-fetch parks after syncing
      parks = await this.parksService.findAll();
    }

    let syncedCount = 0;

    for (const park of parks) {
      // Fetch children (attractions, shows, restaurants, etc.)
      const childrenResponse = await this.themeParksClient.getEntityChildren(
        park.externalId,
      );

      // Filter only attractions
      const attractions = childrenResponse.children.filter(
        (child) => child.entityType === "ATTRACTION",
      );

      for (const attractionEntity of attractions) {
        const mappedData = this.themeParksMapper.mapAttraction(
          attractionEntity,
          park.id,
        );

        // Check if attraction exists (by externalId)
        const existing = await this.attractionRepository.findOne({
          where: { externalId: mappedData.externalId },
        });

        if (existing) {
          // Update existing attraction (keep existing slug)
          await this.attractionRepository.update(existing.id, {
            name: mappedData.name,
            latitude: mappedData.latitude,
            longitude: mappedData.longitude,
          });
        } else {
          // Generate unique slug for this park
          const baseSlug = mappedData.slug || generateSlug(mappedData.name!);

          // Get all existing slugs for this park
          const existingAttractions = await this.attractionRepository.find({
            where: { parkId: park.id },
            select: ["slug"],
          });
          const existingSlugs = existingAttractions.map((a) => a.slug);

          // Generate unique slug
          const uniqueSlug = generateUniqueSlug(baseSlug, existingSlugs);
          mappedData.slug = uniqueSlug;

          // Insert new attraction
          await this.attractionRepository.save(mappedData);
        }

        syncedCount++;
      }
    }

    this.logger.log(`✅ Synced ${syncedCount} attractions`);
    return syncedCount;
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
   * Finds all attractions with filtering and sorting
   */
  async findAllWithFilters(filters: {
    park?: string;
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
      .leftJoinAndSelect("attraction.park", "park");

    // Filter by park slug
    if (filters.park) {
      queryBuilder.andWhere("park.slug = :parkSlug", {
        parkSlug: filters.park,
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
      // Join with latest queue data
      queryBuilder.leftJoin(
        "queue_data",
        "qd",
        'qd."attractionId" = attraction.id AND qd.timestamp = (SELECT MAX(qd2.timestamp) FROM queue_data qd2 WHERE qd2."attractionId" = attraction.id)',
      );

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

    // Apply pagination
    const page = filters.page || 1;
    const limit = filters.limit || 10;
    queryBuilder.skip((page - 1) * limit).take(limit);

    const [data, total] = await queryBuilder.getManyAndCount();
    return { data, total };
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
  async findByParkId(parkId: string): Promise<Attraction[]> {
    return this.attractionRepository.find({
      where: { parkId },
      relations: ["park", "park.destination"],
      order: { name: "ASC" },
    });
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
