import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Between, LessThanOrEqual, MoreThanOrEqual } from "typeorm";
import { Show } from "./entities/show.entity";
import { ShowLiveData } from "./entities/show-live-data.entity";
import { ThemeParksClient } from "../external-apis/themeparks/themeparks.client";
import { ThemeParksMapper } from "../external-apis/themeparks/themeparks.mapper";
import { ParksService } from "../parks/parks.service";
import {
  EntityLiveResponse,
  ShowtimeData,
} from "../external-apis/themeparks/themeparks.types";
import { generateSlug, generateUniqueSlug } from "../common/utils/slug.util";
import { normalizeSortDirection } from "../common/utils/query.util";
import { formatInParkTimezone } from "../common/utils/date.util";

@Injectable()
export class ShowsService {
  private readonly logger = new Logger(ShowsService.name);

  constructor(
    @InjectRepository(Show)
    private showRepository: Repository<Show>,
    @InjectRepository(ShowLiveData)
    private showLiveDataRepository: Repository<ShowLiveData>,
    private themeParksClient: ThemeParksClient,
    private themeParksMapper: ThemeParksMapper,
    private parksService: ParksService,
  ) {}

  /**
   * Get the repository instance (for advanced queries by other services)
   */
  getRepository(): Repository<Show> {
    return this.showRepository;
  }

  /**
   * Syncs all shows from ThemeParks.wiki
   *
   * Strategy:
   * 1. Ensure parks are synced first
   * 2. For each park, fetch children (shows)
   * 3. Map and save to DB
   */
  async syncShows(): Promise<number> {
    this.logger.log("Syncing shows from ThemeParks.wiki...");

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

      // Filter only shows
      const shows = childrenResponse.children.filter(
        (child) => child.entityType === "SHOW",
      );

      for (const showEntity of shows) {
        const mappedData = this.themeParksMapper.mapShow(showEntity, park.id);

        // Check if show exists (by externalId)
        const existing = await this.showRepository.findOne({
          where: { externalId: mappedData.externalId },
        });

        if (existing) {
          // Update existing show (keep existing slug)
          await this.showRepository.update(existing.id, {
            name: mappedData.name,
            latitude: mappedData.latitude,
            longitude: mappedData.longitude,
          });
        } else {
          // Generate unique slug for this park
          const baseSlug = mappedData.slug || generateSlug(mappedData.name!);

          // Get all existing slugs for this park
          const existingShows = await this.showRepository.find({
            where: { parkId: park.id },
            select: ["slug"],
          });
          const existingSlugs = existingShows.map((s) => s.slug);

          // Generate unique slug
          const uniqueSlug = generateUniqueSlug(baseSlug, existingSlugs);
          mappedData.slug = uniqueSlug;

          // Insert new show
          await this.showRepository.save(mappedData);
        }

        syncedCount++;
      }
    }

    this.logger.log(`✅ Synced ${syncedCount} shows`);
    return syncedCount;
  }

  /**
   * Finds all shows
   */
  async findAll(): Promise<Show[]> {
    return this.showRepository.find({
      relations: ["park"],
      order: { name: "ASC" },
    });
  }

  /**
   * Finds all shows with filtering and sorting
   */
  async findAllWithFilters(filters: {
    park?: string;
    durationMin?: number;
    durationMax?: number;
    sort?: string;
    page?: number;
    limit?: number;
  }): Promise<{ data: Show[]; total: number }> {
    const queryBuilder = this.showRepository
      .createQueryBuilder("show")
      .leftJoinAndSelect("show.park", "park");

    // Filter by park slug
    if (filters.park) {
      queryBuilder.andWhere("park.slug = :parkSlug", {
        parkSlug: filters.park,
      });
    }

    // Filter by duration range
    if (filters.durationMin !== undefined) {
      queryBuilder.andWhere("show.durationMinutes >= :durationMin", {
        durationMin: filters.durationMin,
      });
    }

    if (filters.durationMax !== undefined) {
      queryBuilder.andWhere("show.durationMinutes <= :durationMax", {
        durationMax: filters.durationMax,
      });
    }

    // Apply sorting
    if (filters.sort) {
      const [field, direction = "asc"] = filters.sort.split(":");
      const sortDirection = normalizeSortDirection(direction);

      if (field === "name") {
        queryBuilder.orderBy("show.name", sortDirection);
      } else if (field === "duration") {
        queryBuilder.orderBy("show.durationMinutes", sortDirection);
      }
    } else {
      // Default sort by name
      queryBuilder.orderBy("show.name", "ASC");
    }

    // Apply pagination
    const page = filters.page || 1;
    const limit = filters.limit || 10;
    queryBuilder.skip((page - 1) * limit).take(limit);

    const [data, total] = await queryBuilder.getManyAndCount();
    return { data, total };
  }

  /**
   * Finds show by slug
   */
  async findBySlug(slug: string): Promise<Show | null> {
    return this.showRepository.findOne({
      where: { slug },
      relations: ["park", "park.destination"],
    });
  }

  /**
   * Finds all shows in a specific park
   *
   * Used for hierarchical routes: /parks/:parkSlug/shows
   *
   * @param parkId - Park ID (UUID)
   * @returns Array of shows in this park
   */
  async findByParkId(parkId: string): Promise<Show[]> {
    return this.showRepository.find({
      where: { parkId },
      relations: ["park", "park.destination"],
      order: { name: "ASC" },
    });
  }

  /**
   * Finds show by slug within a specific park
   *
   * Used for hierarchical routes: /parks/:parkSlug/shows/:showSlug
   *
   * @param parkId - Park ID (UUID)
   * @param showSlug - Show slug
   * @returns Show if found in this park, null otherwise
   */
  async findBySlugInPark(
    parkId: string,
    showSlug: string,
  ): Promise<Show | null> {
    return this.showRepository.findOne({
      where: {
        parkId,
        slug: showSlug,
      },
      relations: ["park", "park.destination"],
    });
  }

  /**
   * Saves show live data with delta strategy
   *
   * Strategy:
   * - Always save status changes (OPERATING → CLOSED, etc.)
   * - Save when new showtimes appear or existing ones change
   * - Skip if no significant changes detected
   *
   * @param showId - Internal show ID (UUID)
   * @param liveData - Live data from ThemeParks.wiki API
   * @returns Number of entries saved (0 or 1)
   */
  async saveShowLiveData(
    showId: string,
    liveData: EntityLiveResponse,
  ): Promise<number> {
    // Check if we should save (delta strategy)
    const shouldSave = await this.shouldSaveShowLiveData(showId, liveData);

    if (!shouldSave) {
      return 0;
    }

    try {
      // Check if show exists, create if missing (orphan data from external API)
      // This handles race condition where live data arrives before metadata sync
      const showExists = await this.showRepository.findOne({
        where: { id: showId },
        select: ["id"], // Minimal select for performance
      });

      if (!showExists) {
        this.logger.warn(
          `Show ${showId} not found in database, creating placeholder entry for live data`,
        );

        // Create minimal show entry to satisfy foreign key constraint
        // This will be enriched later by the shows metadata sync
        const placeholderShow = this.showRepository.create({
          id: showId,
          name: `Show ${showId.substring(0, 8)}`, // Temporary name
          slug: `show-${showId.substring(0, 8)}`,
          externalId: `placeholder-${showId}`, // Temporary externalId
          // parkId will be set by metadata sync later
        });
        await this.showRepository.save(placeholderShow);

        this.logger.log(`✅ Created placeholder show entry for ${showId}`);
      }

      // Map API data to entity
      const showLiveData: Partial<ShowLiveData> = {
        showId,
        status: liveData.status,
        showtimes: liveData.showtimes || null,
        lastUpdated: liveData.lastUpdated
          ? new Date(liveData.lastUpdated)
          : null,
        operatingHours: liveData.operatingHours || null,
      };

      // Fix: Serialize/Normalize Showtimes
      // If we receive stale dates (e.g. from last month) but status is OPERATING,
      // project them to "Today" to ensure they show up in the schedule.
      if (
        showLiveData.status === "OPERATING" &&
        showLiveData.showtimes &&
        showLiveData.showtimes.length > 0
      ) {
        // Fetch show to get park timezone
        const show = await this.showRepository.findOne({
          where: { id: showId },
          relations: ["park"],
        });

        if (show && show.park && show.park.timezone) {
          const timezone = show.park.timezone;
          const now = new Date();
          const todayDateString = formatInParkTimezone(now, timezone); // YYYY-MM-DD

          showLiveData.showtimes = showLiveData.showtimes.map((st) => {
            if (!st.startTime) return st;

            const stDate = new Date(st.startTime);
            const stDateString = formatInParkTimezone(stDate, timezone);

            // If date is NOT today (e.g. old data), project to today
            if (stDateString !== todayDateString) {
              // Extract time part from original ISO string or Date object
              // Robust way: Use the time component from the Date object corresponding to the parsed startTime
              // Note: st.startTime is an ISO string. modifying it directly is risky if offsets differ.
              // Best approach: Construct new ISO string using Today's Date + Original Time part.

              // Parse original time parts
              const iso = st.startTime;

              // Construct new timestamp: Today's YYYY-MM-DD + T + Original Time Part
              // We rely on the fact that "todayDateString" is YYYY-MM-DD compatible with ISO
              // However, we must be careful about Timezones.
              // If we just swap the date part of the ISO string, it preserves the offset.

              // Example: 2025-11-13T11:30:00+01:00 -> 2025-12-29T11:30:00+01:00
              // This works if strictly replacing YYYY-MM-DD.
              const newIso = todayDateString + iso.substring(10);

              return {
                ...st,
                startTime: newIso,
                // adjust endTime similarly if present
                endTime: st.endTime
                  ? todayDateString + st.endTime.substring(10)
                  : st.endTime,
              };
            }
            return st;
          });
        }
      }

      // Use create() to trigger @BeforeInsert hooks
      const entry = this.showLiveDataRepository.create(showLiveData);
      await this.showLiveDataRepository.save(entry);

      return 1;
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`❌ Failed to save show live data: ${errorMessage}`);
      throw error;
    }
  }

  async saveLiveData(
    showId: string,
    status: string,
    showtimes: ShowtimeData[],
    lastUpdated: Date,
    operatingHours?: { openingTime: string; closingTime: string },
  ): Promise<void> {
    try {
      // Check if show exists, create if missing (orphan data from external API)
      // This handles race condition where live data arrives before metadata sync
      const showExists = await this.showRepository.findOne({
        where: { id: showId },
        select: ["id"], // Minimal select for performance
      });

      if (!showExists) {
        this.logger.warn(
          `Show ${showId} not found in database, creating placeholder entry for live data`,
        );

        // Create minimal show entry to satisfy foreign key constraint
        // This will be enriched later by the shows metadata sync
        const placeholderShow = this.showRepository.create({
          id: showId,
          name: `Show ${showId.substring(0, 8)}`, // Temporary name
          slug: `show-${showId.substring(0, 8)}`,
          // parkId will be set by metadata sync later
        });
        await this.showRepository.save(placeholderShow);

        this.logger.log(`✅ Created placeholder show entry for ${showId}`);
      }

      // Save live data (BeforeInsert hook will generate id and timestamp)
      // TypeScript workaround: cast to any because TypeORM doesn't recognize showId FK field
      await this.showLiveDataRepository.save({
        showId,
        status,
        showtimes,
        lastUpdated,
        operatingHours,
      } as any);
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(`❌ Failed to save show live data: ${errorMessage}`);
      throw error;
    }
  }

  /**
   * Delta strategy: Only save if data has changed significantly
   *
   * Save when:
   * - No previous data exists
   * - Status changed (OPERATING → CLOSED, etc.)
   * - Showtimes array changed (new times, removed times, or modified times)
   */
  private async shouldSaveShowLiveData(
    showId: string,
    newData: EntityLiveResponse,
  ): Promise<boolean> {
    // Get latest entry for this show
    const latest = await this.showLiveDataRepository.findOne({
      where: { showId },
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

    // Showtimes changed → save
    if (this.hasShowtimesChanged(latest.showtimes, newData.showtimes)) {
      return true;
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

    // Date changed → save (ensure at least one data point per day)
    // This fixes the issue where "Closed" status persists from yesterday and we ignore today's "Closed" update
    if (latest.timestamp) {
      const latestDate = new Date(latest.timestamp);
      const currentDate = new Date(); // UTC or server time (ensure consistency)
      if (
        latestDate.getDate() !== currentDate.getDate() ||
        latestDate.getMonth() !== currentDate.getMonth() ||
        latestDate.getFullYear() !== currentDate.getFullYear()
      ) {
        return true;
      }
    }

    // No significant change
    return false;
  }

  /**
   * Compare two showtimes arrays for changes
   *
   * @returns true if showtimes are different
   */
  private hasShowtimesChanged(
    oldShowtimes: ShowtimeData[] | null,
    newShowtimes: ShowtimeData[] | undefined,
  ): boolean {
    // Both null/undefined → no change
    if (!oldShowtimes && !newShowtimes) {
      return false;
    }

    // One is null, other isn't → changed
    if (!oldShowtimes || !newShowtimes) {
      return true;
    }

    // Different lengths → changed
    if (oldShowtimes.length !== newShowtimes.length) {
      return true;
    }

    // Check for legacy data (strings instead of objects)
    if (
      (oldShowtimes.length > 0 && typeof oldShowtimes[0] === "string") ||
      (newShowtimes.length > 0 && typeof newShowtimes[0] === "string")
    ) {
      return true; // Force update to fix data structure
    }

    // Compare each showtime (deep comparison)
    // Sort by startTime first for consistent comparison
    const oldSorted = [...oldShowtimes].sort((a, b) =>
      (a?.startTime || "").localeCompare(b?.startTime || ""),
    );
    const newSorted = [...newShowtimes].sort((a, b) =>
      (a?.startTime || "").localeCompare(b?.startTime || ""),
    );

    for (let i = 0; i < oldSorted.length; i++) {
      const old = oldSorted[i];
      const newSt = newSorted[i];

      if (
        old.type !== newSt.type ||
        old.startTime !== newSt.startTime ||
        old.endTime !== newSt.endTime
      ) {
        return true;
      }
    }

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
   * Find current status for a show (most recent live data)
   */
  async findCurrentStatusByShow(showId: string): Promise<ShowLiveData | null> {
    return this.showLiveDataRepository.findOne({
      where: { showId },
      relations: ["show", "show.park"],
      order: { timestamp: "DESC" },
    });
  }

  /**
   * Find live data for a show with date range filtering
   */
  async findLiveDataByShow(
    showId: string,
    options: {
      from?: Date;
      to?: Date;
      page?: number;
      limit?: number;
    } = {},
  ): Promise<{ data: ShowLiveData[]; total: number }> {
    const { from, to, page = 1, limit = 50 } = options;

    const whereClause: Record<string, unknown> = { showId };

    // Add date range filter
    if (from && to) {
      whereClause.timestamp = Between(from, to);
    } else if (from) {
      whereClause.timestamp = MoreThanOrEqual(from);
    } else if (to) {
      whereClause.timestamp = LessThanOrEqual(to);
    }

    const [data, total] = await this.showLiveDataRepository.findAndCount({
      where: whereClause,
      relations: ["show", "show.park"],
      order: { timestamp: "DESC" },
      skip: (page - 1) * limit,
      take: limit,
    });

    return { data, total };
  }
  /**
   * Find current status for all shows in a park (bulk query optimization)
   */
  async findCurrentStatusByPark(
    parkId: string,
  ): Promise<Map<string, ShowLiveData>> {
    const showData = await this.showLiveDataRepository
      .createQueryBuilder("sld")
      .innerJoinAndSelect("sld.show", "linked_show")
      .leftJoinAndSelect("linked_show.park", "linked_park")
      .where("linked_show.parkId = :parkId", { parkId })
      .andWhere(
        `sld.timestamp = (
          SELECT MAX(sld2.timestamp)
          FROM show_live_data sld2
          WHERE sld2."showId" = sld."showId"
        )`,
      )
      .getMany();

    const result = new Map<string, ShowLiveData>();
    const now = new Date(); // Use server time for "Today"

    for (const data of showData) {
      // Fix: Project Stale Showtimes to Today on Read
      // This ensures that even if the DB has old dates (e.g. from yesterday or last month),
      // we show them as "Today" if the show is OPERATING.
      if (
        data.status === "OPERATING" &&
        data.showtimes &&
        data.showtimes.length > 0 &&
        data.show &&
        data.show.park &&
        data.show.park.timezone
      ) {
        const timezone = data.show.park.timezone;
        const todayDateString = formatInParkTimezone(now, timezone);

        data.showtimes = data.showtimes.map((st) => {
          if (!st.startTime) return st;

          // Check if we already fixed it (optimization) or if it's stale
          // We can just blindly project to today's date + original time because
          // we only do this if status is OPERATING (implying the show is running today).

          // Robust reconstruction:
          const iso = st.startTime; // 2025-11-13T11:30:00+01:00

          // Reconstruct: Today's YYYY-MM-DD + Original Time Part (T...)
          // We use the original ISO string's time part to preserve offset and time
          // Assumption: park timezone offset hasn't changed significantly or we accept the slight error
          // closer match: todayDateString is YYYY-MM-DD.
          // We need to keep the T... part.

          // Safer: Check if the date string part needs updating
          const currentDatePart = iso.substring(0, 10);
          if (currentDatePart !== todayDateString) {
            const newIso = todayDateString + iso.substring(10);
            return {
              ...st,
              startTime: newIso,
              endTime: st.endTime
                ? todayDateString + st.endTime.substring(10)
                : st.endTime,
            };
          }
          return st;
        });
      }

      result.set(data.showId, data);
    }

    return result;
  }
  /**
  /**
   * Find today's operating data for all shows in a park
   *
   * Used when park is CLOSED to recover the day's schedule.
   * Filters by the park's timezone to ensure we only get "today's" data.
   */
  async findTodayOperatingDataByPark(
    parkId: string,
    timezone: string,
  ): Promise<Map<string, ShowLiveData>> {
    // 1. Calculate Start of Day in Park's Timezone
    // We want 00:00:00 in the park's timezone, converted to UTC
    const now = new Date();
    const parkDate = formatInParkTimezone(now, timezone);
    // Format: YYYY-MM-DD

    // Create Date object (node assumes local time if no Z, but we need to trick it or use a library)
    // Better strategy: Use the timestamp from DB directly with a string comparison or raw query if needed.
    // However, TypeORM abstracts this. Let's rely on the fact that we've stored UTC timestamps.
    // We strictly need the UTC timestamp corresponding to 00:00:00 Park Time.

    // Using simple offset calculation (approximate but robust enough for "today")
    // Or just use the string comparison logic on the application side if the volume is low.
    // Best: Use a library like date-fns-tz if available, or Intl.DateTimeFormat (which we used).

    // Let's get the UTC equivalent of "00:00 Park Time"
    // We can do this by creating a date at 00:00 UTC and adjusting.
    // But since we already have the YYYY-MM-DD for the park, let's just use that as a broad filter.
    // Ideally we'd use: timestamp >= StartOfTodayInUTC

    // Simplified: Fetch all Operating data for the last 24h and filter in memory for "today park time"
    const lookbackHours = 26; // 24h + buffer
    const lookbackDate = new Date(
      now.getTime() - lookbackHours * 60 * 60 * 1000,
    );

    const showData = await this.showLiveDataRepository
      .createQueryBuilder("sld")
      .innerJoin("sld.show", "show")
      .where("show.parkId = :parkId", { parkId })
      .andWhere("sld.status = 'OPERATING'")
      .andWhere("sld.timestamp > :lookbackDate", { lookbackDate })
      .orderBy("sld.timestamp", "DESC")
      .getMany();

    const result = new Map<string, ShowLiveData>();

    // Filter for "Today" in Park Time
    for (const data of showData) {
      if (!result.has(data.showId)) {
        // Check if this data point belongs to "today" in the park's timezone
        const entryDate = formatInParkTimezone(data.timestamp, timezone);

        if (entryDate === parkDate) {
          result.set(data.showId, data);
        }
      }
    }

    return result;
  }
}
