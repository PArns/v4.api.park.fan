import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Between, LessThanOrEqual, MoreThanOrEqual } from "typeorm";
import { Restaurant } from "./entities/restaurant.entity";
import { RestaurantLiveData } from "./entities/restaurant-live-data.entity";
import { ThemeParksClient } from "../external-apis/themeparks/themeparks.client";
import { ThemeParksMapper } from "../external-apis/themeparks/themeparks.mapper";
import { ParksService } from "../parks/parks.service";
import { EntityLiveResponse } from "../external-apis/themeparks/themeparks.types";
import { generateSlug, generateUniqueSlug } from "../common/utils/slug.util";
import { normalizeSortDirection } from "../common/utils/query.util";
import { formatInParkTimezone } from "../common/utils/date.util";

@Injectable()
export class RestaurantsService {
  private readonly logger = new Logger(RestaurantsService.name);

  constructor(
    @InjectRepository(Restaurant)
    private restaurantRepository: Repository<Restaurant>,
    @InjectRepository(RestaurantLiveData)
    private restaurantLiveDataRepository: Repository<RestaurantLiveData>,
    private themeParksClient: ThemeParksClient,
    private themeParksMapper: ThemeParksMapper,
    private parksService: ParksService,
  ) { }

  /**
   * Get the repository instance (for advanced queries by other services)
   */
  getRepository(): Repository<Restaurant> {
    return this.restaurantRepository;
  }

  /**
   * Syncs all restaurants from ThemeParks.wiki
   *
   * Strategy:
   * 1. Ensure parks are synced first
   * 2. For each park, fetch children (restaurants)
   * 3. Map and save to DB
   * @param options.deep - If true, fetches detailed data for each restaurant (slower, hits rate limits)
   */
  async syncRestaurants(options: { deep?: boolean } = {}): Promise<number> {
    this.logger.log(
      `Syncing restaurants from ThemeParks.wiki... (Deep Sync: ${options.deep ? "ON" : "OFF"})`,
    );

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
      // Skip parks that are not from ThemeParks.wiki (e.g. Queue-Times or Wartezeiten)
      if (
        !park.externalId ||
        park.externalId.startsWith("qt-") ||
        park.externalId.startsWith("wz-")
      ) {
        continue;
      }

      // Fetch children (attractions, shows, restaurants, etc.)
      const childrenResponse = await this.themeParksClient.getEntityChildren(
        park.externalId,
      );

      // Filter only restaurants
      const restaurants = childrenResponse.children.filter(
        (child) => child.entityType === "RESTAURANT",
      );

      for (const restaurantEntity of restaurants) {
        let entityData = restaurantEntity;

        // Deep Sync: Fetch detailed data if requested
        if (options.deep) {
          try {
            this.logger.debug(`Deep syncing ${restaurantEntity.name}...`);
            entityData = await this.themeParksClient.getEntity(
              restaurantEntity.id,
            );
          } catch (error) {
            this.logger.warn(
              `Failed to deep sync ${restaurantEntity.name}, using summary data: ${error}`,
            );
          }
        }

        const mappedData = this.themeParksMapper.mapRestaurant(
          entityData,
          park.id,
        );

        // Check if restaurant exists (by externalId)
        const existing = await this.restaurantRepository.findOne({
          where: { externalId: mappedData.externalId },
        });

        if (existing) {
          // Update existing restaurant (keep existing slug)
          await this.restaurantRepository.update(existing.id, {
            name: mappedData.name,
            latitude: mappedData.latitude,
            longitude: mappedData.longitude,
            cuisineType: mappedData.cuisineType,
            requiresReservation: mappedData.requiresReservation,
          });
        } else {
          // Generate unique slug for this park
          const baseSlug = mappedData.slug || generateSlug(mappedData.name!);

          // Get all existing slugs for this park
          const existingRestaurants = await this.restaurantRepository.find({
            where: { parkId: park.id },
            select: ["slug"],
          });
          const existingSlugs = existingRestaurants.map((r) => r.slug);

          // Generate unique slug
          const uniqueSlug = generateUniqueSlug(baseSlug, existingSlugs);
          mappedData.slug = uniqueSlug;

          // Insert new restaurant
          await this.restaurantRepository.save(mappedData);
        }

        syncedCount++;
      }
    }

    this.logger.log(`✅ Synced ${syncedCount} restaurants`);
    return syncedCount;
  }

  /**
   * Finds all restaurants
   */
  async findAll(): Promise<Restaurant[]> {
    return this.restaurantRepository.find({
      relations: ["park"],
      order: { name: "ASC" },
    });
  }

  /**
   * Finds all restaurants with filtering and sorting
   */
  async findAllWithFilters(filters: {
    park?: string;
    cuisineType?: string;
    requiresReservation?: boolean;
    sort?: string;
    page?: number;
    limit?: number;
  }): Promise<{ data: Restaurant[]; total: number }> {
    const queryBuilder = this.restaurantRepository
      .createQueryBuilder("restaurant")
      .leftJoinAndSelect("restaurant.park", "park");

    // Filter by park slug
    if (filters.park) {
      queryBuilder.andWhere("park.slug = :parkSlug", {
        parkSlug: filters.park,
      });
    }

    // Filter by cuisine type (case-insensitive partial match)
    if (filters.cuisineType) {
      queryBuilder.andWhere(
        "LOWER(restaurant.cuisineType) LIKE LOWER(:cuisineType)",
        {
          cuisineType: `%${filters.cuisineType}%`,
        },
      );
    }

    // Filter by reservation requirement
    if (filters.requiresReservation !== undefined) {
      queryBuilder.andWhere(
        "restaurant.requiresReservation = :requiresReservation",
        {
          requiresReservation: filters.requiresReservation,
        },
      );
    }

    // Apply sorting
    if (filters.sort) {
      const [field, direction = "asc"] = filters.sort.split(":");
      const sortDirection = normalizeSortDirection(direction);

      if (field === "name") {
        queryBuilder.orderBy("restaurant.name", sortDirection);
      }
    } else {
      // Default sort by name
      queryBuilder.orderBy("restaurant.name", "ASC");
    }

    // Apply pagination
    const page = filters.page || 1;
    const limit = filters.limit || 10;
    queryBuilder.skip((page - 1) * limit).take(limit);

    const [data, total] = await queryBuilder.getManyAndCount();
    return { data, total };
  }

  /**
   * Finds restaurant by slug
   */
  async findBySlug(slug: string): Promise<Restaurant | null> {
    return this.restaurantRepository.findOne({
      where: { slug },
      relations: ["park", "park.destination"],
    });
  }

  /**
   * Finds all restaurants in a specific park
   *
   * Used for hierarchical routes: /parks/:parkSlug/restaurants
   *
   * @param parkId - Park ID (UUID)
   * @returns Array of restaurants in this park
   */
  async findByParkId(parkId: string): Promise<Restaurant[]> {
    return this.restaurantRepository.find({
      where: { parkId },
      relations: ["park", "park.destination"],
      order: { name: "ASC" },
    });
  }

  /**
   * Finds restaurant by slug within a specific park
   *
   * Used for hierarchical routes: /parks/:parkSlug/restaurants/:restaurantSlug
   *
   * @param parkId - Park ID (UUID)
   * @param restaurantSlug - Restaurant slug
   * @returns Restaurant if found in this park, null otherwise
   */
  async findBySlugInPark(
    parkId: string,
    restaurantSlug: string,
  ): Promise<Restaurant | null> {
    return this.restaurantRepository.findOne({
      where: {
        parkId,
        slug: restaurantSlug,
      },
      relations: ["park", "park.destination"],
    });
  }

  /**
   * Saves dining availability data with delta strategy
   *
   * Strategy:
   * - Always save status changes (OPERATING → CLOSED, etc.)
   * - Save when waitTime changes by > 5 minutes (same as attraction queues)
   * - Save when partySize changes
   * - Skip if no significant changes detected
   *
   * @param restaurantId - Internal restaurant ID (UUID)
   * @param liveData - Live data from ThemeParks.wiki API
   * @returns Number of entries saved (0 or 1)
   */
  async saveDiningAvailability(
    restaurantId: string,
    liveData: EntityLiveResponse,
  ): Promise<number> {
    // Check if we should save (delta strategy)
    const shouldSave = await this.shouldSaveDiningAvailability(
      restaurantId,
      liveData,
    );

    if (!shouldSave) {
      return 0;
    }

    try {
      // Map API data to entity
      const restaurantLiveData: Partial<RestaurantLiveData> = {
        restaurantId,
        status: liveData.status,
        partySize: liveData.diningAvailability?.partySize ?? null,
        waitTime: liveData.diningAvailability?.waitTime ?? null,
        lastUpdated: liveData.lastUpdated
          ? new Date(liveData.lastUpdated)
          : null,
        operatingHours: liveData.operatingHours || null,
      };

      // Use create() to trigger @BeforeInsert hooks
      const entry =
        this.restaurantLiveDataRepository.create(restaurantLiveData);
      await this.restaurantLiveDataRepository.save(entry);

      // this.logger.verbose(`✅ Saved dining availability for ${liveData.name}`);
      return 1;
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `❌ Failed to save dining availability: ${errorMessage}`,
      );
      throw error;
    }
  }

  /**
   * Delta strategy: Only save if data has changed significantly
   *
   * Save when:
   * - No previous data exists
   * - Status changed (OPERATING → CLOSED, etc.)
   * - Wait time changed by > 5 minutes (same threshold as attractions)
   * - Party size changed
   */
  private async shouldSaveDiningAvailability(
    restaurantId: string,
    newData: EntityLiveResponse,
  ): Promise<boolean> {
    // Get latest entry for this restaurant
    const latest = await this.restaurantLiveDataRepository.findOne({
      where: { restaurantId },
      order: { timestamp: "DESC" },
    });

    // No previous data → save
    if (!latest) {
      return true;
    }

    // Status changed → save
    if (latest.status !== newData.status) {
      return true;
    }

    // Wait time changed → save
    const newWaitTime = newData.diningAvailability?.waitTime;
    if (newWaitTime !== undefined && latest.waitTime !== null) {
      if (Number(newWaitTime) !== Number(latest.waitTime)) {
        return true;
      }
    }

    // Party size changed → save
    const newPartySize = newData.diningAvailability?.partySize;
    if (newPartySize !== undefined && latest.partySize !== null) {
      if (newPartySize !== latest.partySize) {
        return true;
      }
    }

    // Operating hours changed → save
    if (
      this.hasOperatingHoursChanged(
        latest.operatingHours,
        newData.operatingHours,
      )
    ) {
      return true;
    }

    // No significant change
    return false;
  }

  /**
   * Compare two operating hours arrays for changes
   */
  private hasOperatingHoursChanged(
    oldHours:
      | Array<{ type: string; startTime: string; endTime: string }>
      | null
      | undefined,
    newHours:
      | Array<{ type: string; startTime: string; endTime: string }>
      | undefined,
  ): boolean {
    if (!oldHours && !newHours) return false;
    if (!oldHours || !newHours) return true;
    if (oldHours.length !== newHours.length) return true;

    const oldSorted = [...oldHours].sort((a, b) =>
      a.startTime.localeCompare(b.startTime),
    );
    const newSorted = [...newHours].sort((a, b) =>
      a.startTime.localeCompare(b.startTime),
    );

    for (let i = 0; i < oldSorted.length; i++) {
      if (
        oldSorted[i].type !== newSorted[i].type ||
        oldSorted[i].startTime !== newSorted[i].startTime ||
        oldSorted[i].endTime !== newSorted[i].endTime
      ) {
        return true;
      }
    }
    return false;
  }

  /**
   * Find current status for a restaurant (most recent live data)
   */
  async findCurrentStatusByRestaurant(
    restaurantId: string,
  ): Promise<RestaurantLiveData | null> {
    return this.restaurantLiveDataRepository.findOne({
      where: { restaurantId },
      relations: ["restaurant", "restaurant.park"],
      order: { timestamp: "DESC" },
    });
  }

  /**
   * Find live data for a restaurant with date range filtering
   */
  async findLiveDataByRestaurant(
    restaurantId: string,
    options: {
      from?: Date;
      to?: Date;
      page?: number;
      limit?: number;
    } = {},
  ): Promise<{ data: RestaurantLiveData[]; total: number }> {
    const { from, to, page = 1, limit = 50 } = options;

    const whereClause: Record<string, unknown> = { restaurantId };

    // Add date range filter
    if (from && to) {
      whereClause.timestamp = Between(from, to);
    } else if (from) {
      whereClause.timestamp = MoreThanOrEqual(from);
    } else if (to) {
      whereClause.timestamp = LessThanOrEqual(to);
    }

    const [data, total] = await this.restaurantLiveDataRepository.findAndCount({
      where: whereClause,
      relations: ["restaurant", "restaurant.park"],
      order: { timestamp: "DESC" },
      skip: (page - 1) * limit,
      take: limit,
    });

    return { data, total };
  }
  /**
   * Find current status for all restaurants in a park (bulk query optimization)
   */
  async findCurrentStatusByPark(
    parkId: string,
  ): Promise<Map<string, RestaurantLiveData>> {
    const data = await this.restaurantLiveDataRepository
      .createQueryBuilder("rld")
      .innerJoin("rld.restaurant", "restaurant")
      .where("restaurant.parkId = :parkId", { parkId })
      .andWhere(
        `rld.timestamp = (
          SELECT MAX(rld2.timestamp)
          FROM restaurant_live_data rld2
          WHERE rld2."restaurantId" = rld."restaurantId"
        )`,
      )
      .getMany();

    const result = new Map<string, RestaurantLiveData>();
    for (const item of data) {
      result.set(item.restaurantId, item);
    }

    return result;
  }
  /**
   * Find today's operating data for all restaurants in a park
   *
   * Used when park is CLOSED to recover the day's schedule.
   * Filters by the park's timezone to ensure we only get "today's" data.
   */
  async findTodayOperatingDataByPark(
    parkId: string,
    timezone: string,
  ): Promise<Map<string, RestaurantLiveData>> {
    // 1. Calculate Start of Day in Park's Timezone
    const now = new Date();
    const parkDate = formatInParkTimezone(now, timezone);

    // Simplified: Fetch all Operating data for the last 24h and filter in memory
    const lookbackHours = 26; // 24h + buffer
    const lookbackDate = new Date(
      now.getTime() - lookbackHours * 60 * 60 * 1000,
    );

    const data = await this.restaurantLiveDataRepository
      .createQueryBuilder("rld")
      .innerJoin("rld.restaurant", "restaurant")
      .where("restaurant.parkId = :parkId", { parkId })
      .andWhere("rld.status = 'OPERATING'")
      .andWhere("rld.timestamp > :lookbackDate", { lookbackDate })
      .orderBy("rld.timestamp", "DESC")
      .getMany();

    const result = new Map<string, RestaurantLiveData>();

    // Filter for "Today" in Park Time
    for (const item of data) {
      if (!result.has(item.restaurantId)) {
        // Check if this data point belongs to "today" in the park's timezone
        const entryDate = formatInParkTimezone(item.timestamp, timezone);

        if (entryDate === parkDate) {
          result.set(item.restaurantId, item);
        }
      }
    }

    return result;
  }
}
