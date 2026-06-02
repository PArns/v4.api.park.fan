import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Attraction } from "./entities/attraction.entity";
import { Park } from "../parks/entities/park.entity";
import { ThemeParksClient } from "../external-apis/themeparks/themeparks.client";
import { QueueTimesClient } from "../external-apis/queue-times/queue-times.client";
import { WartezeitenClient } from "../external-apis/wartezeiten/wartezeiten.client";
import { ThemeParksMapper } from "../external-apis/themeparks/themeparks.mapper";
import { ParksService } from "../parks/parks.service";
import { generateSlug, generateUniqueSlug } from "../common/utils/slug.util";
import { normalizeSortDirection } from "../common/utils/query.util";

@Injectable()
export class AttractionsService {
  private readonly logger = new Logger(AttractionsService.name);

  /** In-memory cache for geographic path lookups that returned null (404).
   *  Key: "continent:country:city:park:attraction" → expiry timestamp (ms). */
  private readonly notFoundCache = new Map<string, number>();
  private readonly NOT_FOUND_TTL_MS = 60 * 60 * 1000; // 1 hour

  constructor(
    @InjectRepository(Attraction)
    private attractionRepository: Repository<Attraction>,
    private themeParksClient: ThemeParksClient,
    private queueTimesClient: QueueTimesClient,
    private wartezeitenClient: WartezeitenClient,
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

    const parks = await this.parksService.ensureParksLoaded();

    let syncedCount = 0;

    for (const park of parks) {
      // 1. Queue-Times Sync
      if (park.externalId && park.externalId.startsWith("qt-")) {
        const qtId = parseInt(
          park.externalId.replace("qt-", "qt-park-").replace("qt-park-", ""),
          10,
        );
        if (!isNaN(qtId)) {
          await this.syncFromQueueTimes(park, qtId);
          syncedCount++; // Count park as synced (simplification)
        }
        continue;
      }

      // 2. Wartezeiten Sync
      if (park.externalId && park.externalId.startsWith("wz-")) {
        const wzId = park.externalId.replace("wz-", "");
        await this.syncFromWartezeiten(park, wzId);
        syncedCount++;
        continue;
      }

      // 3. ThemeParks.wiki Sync (Default)
      // Fetch children (attractions, shows, restaurants, etc.)
      const childrenResponse = await this.themeParksClient.getEntityChildren(
        park.externalId,
      );

      // Filter only attractions
      const attractions = childrenResponse.children.filter(
        (child) => child.entityType === "ATTRACTION",
      );

      // Pre-fetch every existing attraction for this park ONCE upfront.
      // The old loop did 1 findOne(externalId) per attraction PLUS a
      // separate find-all-attractions-for-park per *new* attraction to
      // compute slug uniqueness — quadratic on first-time syncs. Now we
      // do one SELECT and diff in memory.
      const existingForPark = await this.attractionRepository.find({
        where: { parkId: park.id },
        select: ["id", "externalId", "slug"],
      });
      const byExternalId = new Map(
        existingForPark.map((a) => [a.externalId, a]),
      );
      const usedSlugs = new Set(
        existingForPark.map((a) => a.slug).filter((s): s is string => !!s),
      );

      for (const attractionEntity of attractions) {
        const mappedData = this.themeParksMapper.mapAttraction(
          attractionEntity,
          park.id,
        );

        // mappedData.externalId is optional in the type; skip rows
        // without one — the old findOne() would have produced null too.
        const externalId = mappedData.externalId;
        if (!externalId) continue;
        const existing = byExternalId.get(externalId);
        if (existing) {
          await this.attractionRepository.update(existing.id, {
            name: mappedData.name,
            latitude: mappedData.latitude,
            longitude: mappedData.longitude,
          });
        } else {
          const baseSlug = mappedData.slug || generateSlug(mappedData.name!);
          const uniqueSlug = generateUniqueSlug(
            baseSlug,
            Array.from(usedSlugs),
          );
          mappedData.slug = uniqueSlug;
          usedSlugs.add(uniqueSlug);
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
      .leftJoinAndSelect("attraction.park", "park");

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
  async findByParkId(
    parkId: string,
    page: number = 1,
    limit: number = 10,
  ): Promise<{ data: Attraction[]; total: number }> {
    const [data, total] = await this.attractionRepository.findAndCount({
      where: { parkId },
      relations: ["park", "park.destination"],
      order: { name: "ASC" },
      take: limit,
      skip: (page - 1) * limit,
    });

    return { data, total };
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
    const expiry = this.notFoundCache.get(cacheKey);
    if (expiry !== undefined) {
      if (Date.now() < expiry) {
        return null; // known 404 — skip DB query
      }
      this.notFoundCache.delete(cacheKey);
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
      this.notFoundCache.set(cacheKey, Date.now() + this.NOT_FOUND_TTL_MS);
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

  /**
   * Sync attractions from Queue-Times
   */
  private async syncFromQueueTimes(park: Park, qtId: number) {
    try {
      const data = await this.queueTimesClient.getParkQueueTimes(qtId);
      const allRides = [...data.rides, ...data.lands.flatMap((l) => l.rides)];

      // Pre-fetch every existing attraction for this park ONCE. The old
      // path did a findOne(externalId) per ride inside upsertAttraction;
      // for a 50-ride park that's 50 extra round-trips per sync.
      const existingAttractions = await this.attractionRepository.find({
        where: { parkId: park.id },
        select: ["id", "externalId", "slug", "queueTimesEntityId"],
      });
      const existingSlugs = existingAttractions
        .map((a) => a.slug)
        .filter((s): s is string => !!s);
      const existingByExternalId = new Map(
        existingAttractions
          .filter((a): a is typeof a & { externalId: string } => !!a.externalId)
          .map((a) => [a.externalId, a]),
      );

      for (const ride of allRides) {
        const externalId = `qt-ride-${ride.id}`;
        await this.upsertAttraction(
          park,
          {
            externalId,
            name: ride.name,
            attractionType: "ROW",
            queueTimesEntityId: ride.id.toString(),
          },
          existingSlugs,
          existingByExternalId,
        );
      }
    } catch (error) {
      this.logger.error(
        `Failed to sync QT attractions for ${park.name}: ${error}`,
      );
    }
  }

  /**
   * Sync attractions from Wartezeiten.app
   */
  private async syncFromWartezeiten(park: Park, wzId: string) {
    try {
      const waitTimes = await this.wartezeitenClient.getWaitTimes(wzId);

      // Pre-fetch every existing attraction once — same fix as
      // syncFromQueueTimes above. Removes N findOne(externalId) calls
      // inside upsertAttraction.
      const existingAttractions = await this.attractionRepository.find({
        where: { parkId: park.id },
        select: ["id", "externalId", "slug"],
      });
      const existingSlugs = existingAttractions
        .map((a) => a.slug)
        .filter((s): s is string => !!s);
      const existingByExternalId = new Map(
        existingAttractions
          .filter((a): a is typeof a & { externalId: string } => !!a.externalId)
          .map((a) => [a.externalId, a]),
      );

      for (const item of waitTimes) {
        const externalId = item.uuid;
        await this.upsertAttraction(
          park,
          {
            externalId,
            name: item.name,
            attractionType: "ROW",
          },
          existingSlugs,
          existingByExternalId,
        );
      }
    } catch (error) {
      this.logger.error(
        `Failed to sync WZ attractions for ${park.name}: ${error}`,
      );
    }
  }

  /**
   * Unified Upsert Logic
   *
   * @param park - Park entity
   * @param data - Attraction data to upsert
   * @param existingSlugs - Optional pre-fetched slugs array to avoid N+1 queries
   *                       If not provided, will fetch slugs (for backward compatibility)
   */
  private async upsertAttraction(
    park: Park,
    data: {
      externalId: string;
      name: string;
      attractionType?: string;
      latitude?: number;
      longitude?: number;
      queueTimesEntityId?: string;
    },
    existingSlugs?: string[],
    existingByExternalId?: Map<string, Attraction>,
  ) {
    // Existence check: prefer the caller-supplied map (one batch read
    // per park) over a per-attraction findOne(). The findOne is kept as
    // a fallback for callers that haven't migrated to the batch form.
    const existing = existingByExternalId
      ? existingByExternalId.get(data.externalId)
      : await this.attractionRepository.findOne({
          where: { externalId: data.externalId },
        });

    if (existing) {
      // Update
      const updateData: Partial<Attraction> = {
        name: data.name,
        // Only update lat/lon if provided (QT/WZ usually don't have it)
        ...(data.latitude && { latitude: data.latitude }),
        ...(data.longitude && { longitude: data.longitude }),
      };

      // Update queueTimesEntityId if provided and missing
      if (data.queueTimesEntityId && !existing.queueTimesEntityId) {
        updateData.queueTimesEntityId = data.queueTimesEntityId;
      }

      await this.attractionRepository.update(existing.id, updateData);
    } else {
      // Create
      const baseSlug = generateSlug(data.name);

      // Use pre-fetched slugs if provided, otherwise fetch (backward compatibility)
      let slugs = existingSlugs;
      if (!slugs) {
        const existingAttractions = await this.attractionRepository.find({
          where: { parkId: park.id },
          select: ["slug"],
        });
        slugs = existingAttractions.map((a) => a.slug);
      }

      const uniqueSlug = generateUniqueSlug(baseSlug, slugs);

      await this.attractionRepository.save({
        parkId: park.id,
        externalId: data.externalId,
        name: data.name,
        slug: uniqueSlug,
        attractionType: data.attractionType || "UNKNOWN",
        latitude: data.latitude,
        longitude: data.longitude,
        queueTimesEntityId: data.queueTimesEntityId || null,
      });
    }
  }
}
