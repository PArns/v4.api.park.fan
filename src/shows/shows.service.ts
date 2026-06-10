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
import {
  formatInParkTimezone,
  getCurrentDateInTimezone,
} from "../common/utils/date.util";
import {
  hasDateChangedInTimezone,
  hasOperatingHoursChanged,
  liveDataCutoff,
} from "../common/utils/live-data.util";
import {
  applyLatestPerEntity,
  latestTodayPerEntity,
  todayLookbackDate,
} from "../common/utils/live-data-query.util";
import { formatInTimeZone, fromZonedTime } from "date-fns-tz";

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

    const parks = await this.parksService.ensureParksLoaded();

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

      const parkShows = await this.showRepository.find({
        where: { parkId: park.id },
        select: ["id", "externalId", "slug"],
      });
      const showsByExternalId = new Map(
        parkShows.map((s) => [s.externalId, s]),
      );
      const existingSlugs = new Set(parkShows.map((s) => s.slug));

      const toUpdate: {
        id: string;
        name?: string;
        latitude?: number;
        longitude?: number;
      }[] = [];
      const toInsert: Partial<Show>[] = [];

      for (const showEntity of shows) {
        const mappedData = this.themeParksMapper.mapShow(showEntity, park.id);
        const existing = showsByExternalId.get(mappedData.externalId!);

        if (existing) {
          toUpdate.push({
            id: existing.id,
            name: mappedData.name,
            ...(mappedData.latitude != null && {
              latitude: mappedData.latitude,
            }),
            ...(mappedData.longitude != null && {
              longitude: mappedData.longitude,
            }),
          });
        } else {
          const baseSlug = mappedData.slug || generateSlug(mappedData.name!);
          const uniqueSlug = generateUniqueSlug(baseSlug, [...existingSlugs]);
          existingSlugs.add(uniqueSlug);
          mappedData.slug = uniqueSlug;
          toInsert.push(mappedData);
        }

        syncedCount++;
      }

      if (toUpdate.length > 0) {
        await Promise.all(
          toUpdate.map(({ id, ...fields }) =>
            this.showRepository.update(id, fields),
          ),
        );
      }

      if (toInsert.length > 0) {
        await this.showRepository.save(toInsert);
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
        // Cannot create placeholder without parkId (NOT NULL constraint)
        // Skip placeholder creation - show will be created properly when metadata sync runs
        this.logger.warn(
          `Show ${showId} not found in database. Cannot create placeholder without parkId. ` +
            `Skipping live data save. Show will be created by metadata sync.`,
        );
        return 0;
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
          const todayDateString = getCurrentDateInTimezone(timezone); // YYYY-MM-DD

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
    // Get latest entry for this show, including park to get timezone.
    // 7-day cutoff enables TimescaleDB chunk exclusion (show_live_data is a
    // compressed hypertable; an unbounded latest-lookup decompresses every
    // chunk per show → ~14s under load). A show with no data in 7 days is
    // correctly treated as "no previous data" → save the fresh reading.
    const cutoff = liveDataCutoff();
    const latest = await this.showLiveDataRepository.findOne({
      where: { showId, timestamp: MoreThanOrEqual(cutoff) },
      order: { timestamp: "DESC" },
      relations: ["show", "show.park"],
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
      hasOperatingHoursChanged(latest.operatingHours, newData.operatingHours)
    ) {
      return true;
    }

    // Date changed → save (ensure at least one data point per day)
    if (
      latest.timestamp &&
      hasDateChangedInTimezone(
        latest.timestamp,
        latest.show?.park?.timezone || "UTC",
      )
    ) {
      return true;
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
   * Find current status for a show (most recent live data)
   */
  async findCurrentStatusByShow(showId: string): Promise<ShowLiveData | null> {
    // 7-day cutoff for chunk exclusion (see shouldSaveShowLiveData). Live data
    // is only useful for "now", so a show stale >7d correctly returns null.
    const cutoff = liveDataCutoff();
    return this.showLiveDataRepository.findOne({
      where: { showId, timestamp: MoreThanOrEqual(cutoff) },
      relations: ["show", "show.park"],
      order: { timestamp: "DESC" },
    });
  }

  /**
   * Find current status for multiple shows in batch
   * Uses DISTINCT ON to efficiently fetch latest live data for all shows
   *
   * @param showIds - Array of show IDs
   * @returns Map of showId -> ShowLiveData (or null if no data)
   */
  async findBatchCurrentStatusByShows(
    showIds: string[],
  ): Promise<Map<string, ShowLiveData | null>> {
    if (showIds.length === 0) {
      return new Map<string, ShowLiveData | null>();
    }

    const resultMap = new Map<string, ShowLiveData | null>(
      showIds.map((id) => [id, null]),
    );

    // Latest record per showId (DISTINCT ON, cutoff for chunk exclusion)
    const showData = await applyLatestPerEntity(
      this.showLiveDataRepository
        .createQueryBuilder("sld")
        .innerJoinAndSelect("sld.show", "linked_show")
        .leftJoinAndSelect("linked_show.park", "linked_park")
        .where("sld.showId IN (:...showIds)", { showIds }),
      "sld",
      "showId",
    ).getMany();

    const now = new Date();
    const maxShowAgeMs = 48 * 60 * 60 * 1000; // 48 hours

    // Process and project stale showtimes to today if needed
    for (const data of showData) {
      // Skip stale data: if the source API lastUpdated is >48h old, showtimes are from a past day
      if (
        data.status === "OPERATING" &&
        (!data.lastUpdated ||
          now.getTime() - data.lastUpdated.getTime() > maxShowAgeMs)
      ) {
        resultMap.set(data.showId, null);
        continue;
      }

      this.projectShowtimesToToday(data, now);
      resultMap.set(data.showId, data);
    }

    return resultMap;
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
    // Latest record per showId (DISTINCT ON, cutoff for chunk exclusion)
    const showData = await applyLatestPerEntity(
      this.showLiveDataRepository
        .createQueryBuilder("sld")
        .innerJoinAndSelect("sld.show", "linked_show")
        .leftJoinAndSelect("linked_show.park", "linked_park")
        .where("linked_show.parkId = :parkId", { parkId }),
      "sld",
      "showId",
    ).getMany();

    const result = new Map<string, ShowLiveData>();
    const now = new Date(); // Use server time for "Today"
    const maxShowAgeMs = 48 * 60 * 60 * 1000; // 48 hours

    for (const data of showData) {
      // Skip stale data: if the source API lastUpdated is >48h old, showtimes are from a past day
      if (
        data.status === "OPERATING" &&
        (!data.lastUpdated ||
          now.getTime() - data.lastUpdated.getTime() > maxShowAgeMs)
      ) {
        continue; // Don't include in result map → show will be treated as no live data
      }

      this.projectShowtimesToToday(data, now);
      result.set(data.showId, data);
    }

    return result;
  }

  /**
   * Projects all showtime start/end times to today's date in the park's timezone.
   * Only runs when status is OPERATING and showtimes + timezone are present.
   * Mutates data.showtimes in place.
   */
  private projectShowtimesToToday(data: ShowLiveData, now: Date): void {
    if (
      data.status !== "OPERATING" ||
      !data.showtimes ||
      data.showtimes.length === 0 ||
      !data.show?.park?.timezone
    )
      return;

    const timezone = data.show.park.timezone;
    const todayDateString = formatInParkTimezone(now, timezone);

    data.showtimes = data.showtimes.map((st) => {
      if (!st.startTime) return st;
      if (st.startTime.substring(0, 10) === todayDateString) return st;

      const originalDate = new Date(st.startTime);
      const originalTimeStr = formatInTimeZone(
        originalDate,
        timezone,
        "HH:mm:ss",
      );
      const projectedDate = fromZonedTime(
        `${todayDateString}T${originalTimeStr}`,
        timezone,
      );

      return {
        ...st,
        startTime: projectedDate.toISOString(),
        endTime: st.endTime
          ? (() => {
              const endDate = new Date(st.endTime);
              const endTimeStr = formatInTimeZone(
                endDate,
                timezone,
                "HH:mm:ss",
              );
              return fromZonedTime(
                `${todayDateString}T${endTimeStr}`,
                timezone,
              ).toISOString();
            })()
          : undefined,
      };
    });
  }

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
    const showData = await this.showLiveDataRepository
      .createQueryBuilder("sld")
      .innerJoin("sld.show", "show")
      .where("show.parkId = :parkId", { parkId })
      .andWhere("sld.status = 'OPERATING'")
      .andWhere("sld.timestamp > :lookbackDate", {
        lookbackDate: todayLookbackDate(),
      })
      .orderBy("sld.timestamp", "DESC")
      .getMany();

    return latestTodayPerEntity(showData, (data) => data.showId, timezone);
  }
}
