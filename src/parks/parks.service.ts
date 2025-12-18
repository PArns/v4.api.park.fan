import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Park } from "./entities/park.entity";
import { ScheduleEntry, ScheduleType } from "./entities/schedule-entry.entity";
import { ThemeParksClient } from "../external-apis/themeparks/themeparks.client";
import { ThemeParksMapper } from "../external-apis/themeparks/themeparks.mapper";
import { DestinationsService } from "../destinations/destinations.service";
import { generateSlug, generateUniqueSlug } from "../common/utils/slug.util";
import { normalizeSortDirection } from "../common/utils/query.util";

import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";

@Injectable()
export class ParksService {
  private readonly logger = new Logger(ParksService.name);
  private readonly TTL_SCHEDULE = 60 * 60; // 1 hour - park schedules
  private readonly CACHE_TTL_SECONDS = 60 * 60; // 1 hour - legacy

  constructor(
    @InjectRepository(Park)
    private parkRepository: Repository<Park>,
    @InjectRepository(ScheduleEntry)
    private scheduleRepository: Repository<ScheduleEntry>,
    private themeParksClient: ThemeParksClient,
    private themeParksMapper: ThemeParksMapper,
    private destinationsService: DestinationsService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) { }

  /**
   * Syncs all parks from ThemeParks.wiki
   *
   * Strategy:
   * 1. Ensure destinations are synced first
   * 2. Fetch full entity data for each park
   * 3. Map and save to DB
   */
  async syncParks(): Promise<number> {
    this.logger.log("Syncing parks from ThemeParks.wiki...");

    // Ensure destinations are synced first
    const { data: destinations } = await this.destinationsService.findAll(
      1,
      1000,
    );

    if (destinations.length === 0) {
      this.logger.warn("No destinations found. Syncing destinations first...");
      await this.destinationsService.syncDestinations();
    }

    // Fetch full park data
    const apiResponse = await this.themeParksClient.getDestinations();
    let syncedCount = 0;

    for (const apiDestination of apiResponse.destinations) {
      // Find our DB destination
      const destination = await this.destinationsService.findByExternalId(
        apiDestination.id,
      );

      if (!destination) {
        this.logger.warn(
          `Destination ${apiDestination.id} not found in DB, skipping parks`,
        );
        continue;
      }

      for (const parkSummary of apiDestination.parks) {
        // Fetch full park entity data
        const parkEntity = await this.themeParksClient.getEntity(
          parkSummary.id,
        );

        const mappedData = this.themeParksMapper.mapPark(
          parkEntity,
          destination.id,
        );

        // Check if park exists (by externalId)
        const existing = await this.parkRepository.findOne({
          where: { externalId: mappedData.externalId },
        });

        if (existing) {
          // Update existing park (keep existing slug)
          await this.parkRepository.update(existing.id, {
            name: mappedData.name,
            latitude: mappedData.latitude,
            longitude: mappedData.longitude,
            timezone: mappedData.timezone,
          });
        } else {
          // Generate unique slug for this destination
          const baseSlug = mappedData.slug || generateSlug(mappedData.name!);

          // Get all existing slugs for this destination
          const existingParks = await this.parkRepository.find({
            where: { destinationId: destination.id },
            select: ["slug"],
          });
          const existingSlugs = existingParks.map((p) => p.slug);

          // Generate unique slug
          const uniqueSlug = generateUniqueSlug(baseSlug, existingSlugs);
          mappedData.slug = uniqueSlug;

          // Insert new park
          try {
            await this.parkRepository.save(mappedData);
          } catch (error: unknown) {
            // Handle race condition where another sync created the park
            if (
              error instanceof Error &&
              error.message.includes("duplicate key")
            ) {
              this.logger.warn(
                `Park ${mappedData.name} already exists (race condition), updating instead`,
              );
              // Refetch and update
              const refetched = await this.parkRepository.findOne({
                where: { externalId: mappedData.externalId },
              });
              if (refetched) {
                await this.parkRepository.update(refetched.id, {
                  name: mappedData.name,
                  latitude: mappedData.latitude,
                  longitude: mappedData.longitude,
                  timezone: mappedData.timezone,
                });
              }
            } else {
              throw error;
            }
          }
        }

        syncedCount++;
      }
    }

    this.logger.log(`✅ Synced ${syncedCount} parks`);
    return syncedCount;
  }

  /**
   * Finds park by externalId
   */
  async findByExternalId(externalId: string): Promise<Park | null> {
    return this.parkRepository.findOne({
      where: { externalId },
      relations: ["destination", "attractions"],
    });
  }

  /**
   * Finds park by internal ID
   */
  async findById(id: string): Promise<Park | null> {
    return this.parkRepository.findOne({
      where: { id },
      relations: ["destination"],
    });
  }

  /**
   * Finds all parks
   */
  async findAll(): Promise<Park[]> {
    return this.parkRepository.find({
      relations: ["destination"],
      order: { name: "ASC" },
    });
  }

  /**
   * Finds all parks with filtering and sorting
   */
  async findAllWithFilters(filters: {
    continent?: string;
    country?: string;
    city?: string;
    sort?: string;
    page?: number;
    limit?: number;
  }): Promise<{ data: Park[]; total: number }> {
    const queryBuilder = this.parkRepository
      .createQueryBuilder("park")
      .leftJoinAndSelect("park.destination", "destination");

    // Apply filters
    if (filters.continent) {
      queryBuilder.andWhere("LOWER(park.continent) = LOWER(:continent)", {
        continent: filters.continent,
      });
    }

    if (filters.country) {
      queryBuilder.andWhere("LOWER(park.country) = LOWER(:country)", {
        country: filters.country,
      });
    }

    if (filters.city) {
      queryBuilder.andWhere("LOWER(park.city) = LOWER(:city)", {
        city: filters.city,
      });
    }

    // Apply sorting
    if (filters.sort) {
      const [field, direction = "asc"] = filters.sort.split(":");
      const sortDirection = normalizeSortDirection(direction);

      if (field === "name") {
        queryBuilder.orderBy("park.name", sortDirection);
      } else if (field === "openStatus") {
        // Sort by open status (OPERATING first, then CLOSED)
        // This is a placeholder - actual status comes from schedule/queue data
        queryBuilder.orderBy("park.name", sortDirection);
      }
    } else {
      // Default sort by name
      queryBuilder.orderBy("park.name", "ASC");
    }

    // Apply pagination
    const page = filters.page || 1;
    const limit = filters.limit || 10;
    queryBuilder.skip((page - 1) * limit).take(limit);

    const [data, total] = await queryBuilder.getManyAndCount();
    return { data, total };
  }

  /**
   * Finds park by slug
   */
  async findBySlug(slug: string): Promise<Park | null> {
    return this.parkRepository.findOne({
      where: { slug },
      relations: ["destination", "attractions", "shows", "restaurants"],
    });
  }

  /**
   * Saves schedule data for a park from ThemeParks.wiki API.
   *
   * Strategy:
   * - Upsert based on (parkId, date, scheduleType)
   * - Update if changed, otherwise skip
   * - Keep historical schedule entries for analysis
   *
   * @param parkId - Our internal park ID (UUID)
   * @param scheduleData - Schedule data from ThemeParks.wiki API
   */
  async saveScheduleData(parkId: string, scheduleData: any[]): Promise<number> {
    if (!scheduleData || scheduleData.length === 0) {
      return 0;
    }

    let savedCount = 0;

    for (const entry of scheduleData) {
      const scheduleEntry: Partial<ScheduleEntry> = {
        parkId,
        date: new Date(entry.date),
        scheduleType: entry.type as ScheduleType,
        openingTime: entry.openingTime ? new Date(entry.openingTime) : null,
        closingTime: entry.closingTime ? new Date(entry.closingTime) : null,
        description: entry.description || null,
        purchases: entry.purchases || null,
      };

      // Check if entry exists for this park, date, and type
      const existing = await this.scheduleRepository.findOne({
        where: {
          parkId,
          date: scheduleEntry.date,
          scheduleType: scheduleEntry.scheduleType,
        },
      });

      if (!existing) {
        await this.scheduleRepository.save(scheduleEntry);
        savedCount++;
      } else {
        // Update if times or description changed
        const hasChanges =
          existing.openingTime?.getTime() !==
          scheduleEntry.openingTime?.getTime() ||
          existing.closingTime?.getTime() !==
          scheduleEntry.closingTime?.getTime() ||
          existing.description !== scheduleEntry.description;

        if (hasChanges) {
          await this.scheduleRepository.update(existing.id, scheduleEntry);
          savedCount++;
        }
      }
    }

    return savedCount;
  }

  /**
   * Finds all parks that have latitude/longitude but missing geographic data
   *
   * Used for geocoding enrichment. Only returns parks that need geocoding.
   * Excludes parks that have already been attempted (to avoid wasting API quota).
   * Also filters out parks with imprecise coordinates (e.g., rounded to whole numbers).
   *
   * @returns Parks that need geocoding (have lat/lng but missing continent/country/city)
   */
  async findParksWithoutGeodata(): Promise<Park[]> {
    const parks = await this.parkRepository
      .createQueryBuilder("park")
      .where("park.latitude IS NOT NULL")
      .andWhere("park.longitude IS NOT NULL")
      .andWhere(
        "(park.continent IS NULL OR park.country IS NULL OR park.city IS NULL)",
      )
      .andWhere("park.geocodingAttemptedAt IS NULL") // Skip already attempted parks
      .getMany();

    // Filter out parks with imprecise coordinates (rounded to whole numbers)
    // e.g., lat=28.0, lng=-81.0 is too generic
    return parks.filter((park) => {
      const lat = park.latitude;
      const lng = park.longitude;

      // Check if coordinates are too rounded (exactly .0 or only 1 decimal place)
      const latDecimals = (lat.toString().split(".")[1] || "").length;
      const lngDecimals = (lng.toString().split(".")[1] || "").length;

      // Skip if both coordinates have 0 or only 1 decimal place
      if (latDecimals <= 1 && lngDecimals <= 1) {
        this.logger.warn(
          `Skipping ${park.name} - coordinates too imprecise (${lat}, ${lng})`,
        );
        return false;
      }

      return true;
    });
  }

  /**
   * Updates geographic data for a park
   *
   * Used after geocoding to populate continent, country, and city fields.
   * Also marks the park as attempted.
   *
   * IMPORTANT: Only updates fields that are currently NULL.
   * This allows manual data entry without risk of being overwritten.
   *
   * @param parkId - Park ID (UUID)
   * @param geodata - Geographic data (continent, country, city)
   */
  async updateGeodata(
    parkId: string,
    geodata: { continent: string; country: string; city: string },
  ): Promise<void> {
    // Load current park data to check what's already set
    const park = await this.parkRepository.findOne({ where: { id: parkId } });

    if (!park) {
      this.logger.warn(`Park ${parkId} not found, skipping geodata update`);
      return;
    }

    // Only update fields that are currently NULL (preserve manually set data)
    let hasChanges = false;

    if (!park.continent) {
      park.continent = geodata.continent;
      park.continentSlug = generateSlug(geodata.continent);
      hasChanges = true;
    }
    if (!park.country) {
      park.country = geodata.country;
      park.countrySlug = generateSlug(geodata.country);
      hasChanges = true;
    }
    if (!park.city) {
      park.city = geodata.city;
      park.citySlug = generateSlug(geodata.city);
      hasChanges = true;
    }

    // Always mark as attempted
    park.geocodingAttemptedAt = new Date();

    // Use save() instead of update() to ensure entity is properly persisted
    // (though we're generating slugs manually, this is more explicit)
    if (hasChanges) {
      await this.parkRepository.save(park);
    } else {
      // Just update the attempt timestamp
      await this.parkRepository.update(parkId, {
        geocodingAttemptedAt: new Date(),
      });
    }
  }

  /**
   * Marks a park as having had a geocoding attempt (even if failed)
   *
   * Used to prevent repeated API calls for parks that cannot be geocoded.
   *
   * @param parkId - Park ID (UUID)
   */
  async markGeocodingAttempted(parkId: string): Promise<void> {
    await this.parkRepository.update(parkId, {
      geocodingAttemptedAt: new Date(),
    });
  }

  /**
   * Gets unique country codes from all parks
   *
   * Used for holiday data sync - we only fetch holidays for countries
   * where we have parks.
   *
   * @returns Array of unique ISO 3166-1 alpha-2 country codes
   */
  async getUniqueCountries(): Promise<string[]> {
    const result = await this.parkRepository
      .createQueryBuilder("park")
      .select("DISTINCT park.country", "country")
      .where("park.country IS NOT NULL")
      .getRawMany();

    return result.map((r) => r.country).filter((c) => c);
  }

  /**
   * Gets schedule data for a park within a date range
   *
   * @param parkId - Park ID (UUID)
   * @param startDate - Start date (inclusive)
   * @param endDate - End date (inclusive)
   * @returns Schedule entries ordered by date
   */
  async getSchedule(
    parkId: string,
    startDate: Date,
    endDate: Date,
  ): Promise<ScheduleEntry[]> {
    return this.scheduleRepository
      .createQueryBuilder("schedule")
      .where("schedule.parkId = :parkId", { parkId })
      .andWhere("schedule.date >= :startDate", { startDate })
      .andWhere("schedule.date <= :endDate", { endDate })
      .orderBy("schedule.date", "ASC")
      .addOrderBy("schedule.scheduleType", "ASC")
      .getMany();
  }

  /**
   * Get today's schedule for a park
   *
   * Convenience method for integrated park endpoint.
   * Returns all schedule entries for today only.
   *
   * @param parkId - Park ID (UUID)
   * @returns Today's schedule entries
   */
  async getTodaySchedule(parkId: string): Promise<ScheduleEntry[]> {
    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const cacheKey = `schedule:today:${parkId}:${today.toISOString().split("T")[0]}`;
    const cached = await this.redis.get(cacheKey);

    if (cached) {
      const parsed = JSON.parse(cached) as any[]; // parsed from Redis string
      return parsed.map((entry) => ({
        ...entry,
        date: new Date(entry.date),
        openingTime: entry.openingTime ? new Date(entry.openingTime) : null,
        closingTime: entry.closingTime ? new Date(entry.closingTime) : null,
      })) as ScheduleEntry[];
    }

    const endOfToday = new Date(today);
    endOfToday.setHours(23, 59, 59, 999);

    const schedule = await this.getSchedule(parkId, today, endOfToday);

    // Cache result
    await this.redis.set(
      cacheKey,
      JSON.stringify(schedule),
      "EX",
      this.TTL_SCHEDULE,
    );

    return schedule;
  }

  /**
   * Get upcoming schedule for a park (today + next N days)
   *
   * Returns schedule entries from today through the next N days.
   * Used for trip planning - shows users when park will be open.
   *
   * @param parkId - Park ID (UUID)
   * @param days - Number of days to fetch (default: 7)
   * @returns Schedule entries for upcoming days
   */
  async getUpcomingSchedule(
    parkId: string,
    days: number = 7,
  ): Promise<ScheduleEntry[]> {
    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const endDate = new Date(today);
    endDate.setDate(endDate.getDate() + days);
    endDate.setHours(23, 59, 59, 999);

    const cacheKey = `schedule:upcoming:${parkId}:${today.toISOString().split("T")[0]}:${days}`;
    const cached = await this.redis.get(cacheKey);

    if (cached) {
      const parsed = JSON.parse(cached) as any[];
      return parsed.map((entry) => ({
        ...entry,
        date: new Date(entry.date),
        openingTime: entry.openingTime ? new Date(entry.openingTime) : null,
        closingTime: entry.closingTime ? new Date(entry.closingTime) : null,
      })) as ScheduleEntry[];
    }

    const schedule = await this.getSchedule(parkId, today, endDate);

    // Cache result (1 hour TTL)
    await this.redis.set(
      cacheKey,
      JSON.stringify(schedule),
      "EX",
      this.TTL_SCHEDULE,
    );

    return schedule;
  }

  /**
   * Finds parks by continent slug
   *
   * @param continentSlug - Continent slug (e.g., "north-america", "europe")
   * @returns Parks in the continent
   */
  async findByContinent(continentSlug: string): Promise<Park[]> {
    return this.parkRepository.find({
      where: { continentSlug },
      relations: ["destination", "attractions", "shows", "restaurants"],
      order: { name: "ASC" },
    });
  }

  /**
   * Finds parks by continent and country slugs
   *
   * @param continentSlug - Continent slug
   * @param countrySlug - Country slug (e.g., "united-states", "germany")
   * @returns Parks in the country
   */
  async findByCountry(
    continentSlug: string,
    countrySlug: string,
  ): Promise<Park[]> {
    return this.parkRepository.find({
      where: { continentSlug, countrySlug },
      relations: ["destination", "attractions", "shows", "restaurants"],
      order: { name: "ASC" },
    });
  }

  /**
   * Finds parks by continent, country, and city slugs
   *
   * @param continentSlug - Continent slug
   * @param countrySlug - Country slug
   * @param citySlug - City slug (e.g., "orlando", "rust")
   * @returns Parks in the city
   */
  async findByCity(
    continentSlug: string,
    countrySlug: string,
    citySlug: string,
  ): Promise<Park[]> {
    return this.parkRepository.find({
      where: { continentSlug, countrySlug, citySlug },
      relations: ["destination", "attractions", "shows", "restaurants"],
      order: { name: "ASC" },
    });
  }

  /**
   * Finds a park by geographic path (continent/country/city/park)
   *
   * @param continentSlug - Continent slug
   * @param countrySlug - Country slug
   * @param citySlug - City slug
   * @param parkSlug - Park slug
   * @returns Park if found, null otherwise
   */
  async findByGeographicPath(
    continentSlug: string,
    countrySlug: string,
    citySlug: string,
    parkSlug: string,
  ): Promise<Park | null> {
    return this.parkRepository.findOne({
      where: { continentSlug, countrySlug, citySlug, slug: parkSlug },
      relations: ["destination", "attractions", "shows", "restaurants"],
    });
  }

  /**
   * Checks if a park is currently open based on schedule and timezone
   *
   * Strategy:
   * 1. Get park's timezone
   * 2. Get current date/time in park's timezone
   * 3. Check today's schedule for OPERATING entries
   * 4. Verify current time is within operating hours
   *
   * @param parkId - Park ID (UUID)
   * @returns true if park is currently open, false otherwise
   */
  async isParkCurrentlyOpen(parkId: string): Promise<boolean> {
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "timezone"],
    });

    if (!park || !park.timezone) {
      // No timezone info = assume closed (safe default)
      return false;
    }

    // Get current time (UTC)
    const now = new Date();

    // Get current date in park's timezone (YYYY-MM-DD)
    const parkTimeStr = now.toLocaleString("en-US", {
      timeZone: park.timezone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    });
    const [month, day, year] = parkTimeStr.split("/");
    const parkDateStr = `${year}-${month}-${day}`;

    // Query schedule for today in park's timezone
    const todaySchedule = await this.scheduleRepository.findOne({
      where: {
        parkId,
        date: parkDateStr as any, // TypeORM will handle the date comparison
        scheduleType: "OPERATING" as ScheduleType,
      },
    });

    if (!todaySchedule) {
      // No schedule = assume closed
      return false;
    }

    // Check if current time is within operating hours
    // openingTime and closingTime are stored as UTC timestamps
    if (!todaySchedule.openingTime || !todaySchedule.closingTime) {
      return false;
    }

    const openingTime = new Date(todaySchedule.openingTime);
    const closingTime = new Date(todaySchedule.closingTime);

    // Compare UTC timestamps
    return now >= openingTime && now < closingTime;
  }

  /**
   * Checks if a park is scheduled to operate today (even if currently closed)
   *
   * Used by WaitTimesProcessor to decide whether to fetch live data.
   * fetch data if park operates TODAY, even if closed right now (e.g. before opening/after closing).
   */
  async isParkOperatingToday(parkId: string): Promise<boolean> {
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "timezone"],
    });

    if (!park || !park.timezone) {
      return false;
    }

    // Get current time (UTC)
    const now = new Date();

    // Get current date in park's timezone (YYYY-MM-DD)
    const parkTimeStr = now.toLocaleString("en-US", {
      timeZone: park.timezone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    });
    const [month, day, year] = parkTimeStr.split("/");
    const parkDateStr = `${year}-${month}-${day}`;

    // Query schedule for today in park's timezone
    const todaySchedule = await this.scheduleRepository.findOne({
      where: {
        parkId,
        date: parkDateStr as any,
      },
    });

    // If we have a schedule entry, trust it:
    if (todaySchedule) {
      return todaySchedule.scheduleType === 'OPERATING';
    }

    // If NO schedule exists (e.g. Toverland), default to TRUE to ensure we check live sources.
    // This allows us to "discover" open parks even if we lack schedule data.
    return true;
  }

  /**
   * Checks operating status for multiple parks efficiently
   *
   * @param parkIds - Array of Park IDs
   * @returns Map of Park ID -> Status ("OPERATING" or "CLOSED")
   */
  async getBatchParkStatus(
    parkIds: string[],
  ): Promise<Map<string, "OPERATING" | "CLOSED">> {
    if (parkIds.length === 0) {
      return new Map();
    }

    const now = new Date();

    // Find any OPERATING schedule that works for right now
    // We rely on openingTime/closingTime being absolute UTC timestamps
    const activeSchedules = await this.scheduleRepository
      .createQueryBuilder("schedule")
      .select("schedule.parkId", "parkId")
      .where("schedule.parkId IN (:...parkIds)", { parkIds })
      .andWhere("schedule.scheduleType = 'OPERATING'")
      .andWhere("schedule.openingTime <= :now", { now })
      .andWhere("schedule.closingTime > :now", { now })
      .getRawMany();

    const statusMap = new Map<string, "OPERATING" | "CLOSED">();

    // Default all to CLOSED
    for (const id of parkIds) {
      statusMap.set(id, "CLOSED");
    }

    // Mark active ones as OPERATING based on schedule
    for (const schedule of activeSchedules) {
      statusMap.set(schedule.parkId, "OPERATING");
    }

    // Heuristic Fallback: If no schedule (CLOSED), check if distinct rides are operating
    // If >0 attractions are marked OPERATING in the last hour, consider park open
    // We check for "CLOSED" parks only to save DB load
    const closedParkIds = parkIds.filter(
      (id) => statusMap.get(id) === "CLOSED",
    );

    if (closedParkIds.length > 0) {
      // Logic: If >= 50% of attractions in a park are OPERATING, mark park as OPERATING
      // We need to fetch latest status for attractions in these parks
      // Efficient query using LATERAL JOIN (Postgres)
      const stats = await this.parkRepository.manager.query(
        `
        SELECT
          p.id as "parkId",
          COUNT(a.id) as "total",
          SUM(CASE WHEN q.status = 'OPERATING' THEN 1 ELSE 0 END) as "operating"
        FROM parks p
        JOIN attractions a ON a."parkId" = p.id
        JOIN LATERAL (
          SELECT status
          FROM queue_data qd
          WHERE qd."attractionId" = a.id
            AND qd.timestamp > NOW() - INTERVAL '20 minutes'
          ORDER BY timestamp DESC
          LIMIT 1
        ) q ON true
        WHERE p.id = ANY($1)
        GROUP BY p.id
      `,
        [closedParkIds],
      );

      for (const stat of stats) {
        const total = parseInt(stat.total, 10);
        const operating = parseInt(stat.operating, 10);

        if (total > 0 && operating / total >= 0.5) {
          statusMap.set(stat.parkId, "OPERATING");
        }
      }
    }

    return statusMap;
  }
}
