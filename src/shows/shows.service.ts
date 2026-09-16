import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import {
  Repository,
  Between,
  IsNull,
  LessThanOrEqual,
  MoreThanOrEqual,
} from "typeorm";
import { Show } from "./entities/show.entity";
import { ShowLiveData } from "./entities/show-live-data.entity";
import { ShowSchedulePattern } from "./entities/show-schedule-pattern.entity";
import { ThemeParksClient } from "../external-apis/themeparks/themeparks.client";
import { ThemeParksMapper } from "../external-apis/themeparks/themeparks.mapper";
import { ParksService } from "../parks/parks.service";
import {
  EntityLiveResponse,
  ShowtimeData,
} from "../external-apis/themeparks/themeparks.types";
import { generateSlug, generateUniqueSlug } from "../common/utils/slug.util";
import { isThemeParksWikiId } from "../common/utils/external-id.util";
import { normalizeSortDirection, paginate } from "../common/utils/query.util";
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
import { normalizedClosingSql } from "../common/utils/park-open-window.sql";

@Injectable()
export class ShowsService {
  private readonly logger = new Logger(ShowsService.name);

  constructor(
    @InjectRepository(Show)
    private showRepository: Repository<Show>,
    @InjectRepository(ShowLiveData)
    private showLiveDataRepository: Repository<ShowLiveData>,
    @InjectRepository(ShowSchedulePattern)
    private showSchedulePatternRepository: Repository<ShowSchedulePattern>,
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
      if (!isThemeParksWikiId(park.externalId)) {
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
      where: { retiredAt: IsNull() },
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
      .leftJoinAndSelect("show.park", "park")
      .where("show.retiredAt IS NULL");

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

    return paginate(queryBuilder, filters.page, filters.limit);
  }

  /**
   * Finds show by slug
   */
  async findBySlug(slug: string): Promise<Show | null> {
    // NOT filtered, matching `AttractionsService.findBySlug`: a retired row
    // leaves the lists that describe the park as it is today, while a lookup
    // of one named row still finds it, so its history stays readable. No
    // controller reaches this today — shows are served through the park
    // payload — so the rule is set here by parity, before a route exists that
    // would have to decide it in a hurry.
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
      where: { parkId, retiredAt: IsNull() },
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
    // See `findBySlug`: a lookup by name still finds a retired row.
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
        .where("sld.showId IN (:...showIds)", { showIds })
        // All three callers put this status in front of a visitor — the
        // favorites list, the search result enrichment and the followed-show
        // push. A retired show has left each of those surfaces already, so it
        // must not come back through its live data.
        .andWhere("linked_show.retiredAt IS NULL"),
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

  // ── Showtime patterns ───────────────────────────────────────────────────────

  /** How far back a pattern may look. Eight weeks covers a season change. */
  private static readonly PATTERN_WINDOW_DAYS = 56;

  /**
   * The `entries` CTE both day readers below open with: every showtime a park
   * published around one date, tagged with the operating day it belongs to.
   *
   * Extracted for the same reason as {@link ShowsService.operatingDaySql} — two
   * callers answering "which showtimes are on this day" must not be able to
   * drift apart. `getShowtimesOnDate` and `getShowtimeInstantsOnDate` serve the
   * same day to different consumers (the planner's display times, the push
   * job's instants), and a park whose late show appears in one and not the
   * other is worse than either answer alone.
   *
   * Binds `$1` park id, `$2` timezone, `$3` date.
   */
  private static showtimeEntriesCte(): string {
    const st = "(e->>'startTime')::timestamptz";
    return `entries AS (
           SELECT l."showId" AS show_id,
                  ${st} AS st,
                  ${ShowsService.operatingDaySql(st, 's."parkId"', "$2")} AS op_day
             FROM show_live_data l
             JOIN shows s ON s.id = l."showId"
            CROSS JOIN LATERAL jsonb_array_elements(COALESCE(l.showtimes, '[]'::jsonb)) e
            WHERE s."parkId" = $1::uuid
              AND l.status = 'OPERATING'
              -- The snapshot window is anchored on the DATE asked about, not on
              -- "now": that answers a past day from the snapshots taken on it,
              -- today from the day's own, and a future day not at all — which is
              -- the honest answer there, and what the projection is for.
              AND l.timestamp >= ($3::date - INTERVAL '1 day')
              AND l.timestamp <  ($3::date + INTERVAL '2 days')
              -- Cheap prefilter on the calendar date so the operating-day
              -- subquery only runs for rows that can still qualify. A showtime
              -- belongs to $3 either on its own date or on the morning after,
              -- never further out. This is also what keeps the feed's uncleared
              -- junk out: ThemeParks.wiki still serves showtimes from 2022, and
              -- they simply fall on another date.
              AND (${st} AT TIME ZONE $2)::date
                  BETWEEN $3::date AND $3::date + 1
         )`;
  }

  /**
   * The operating day a showtime belongs to, as a SQL scalar expression.
   *
   * §5 of `docs/frontend/plan-day-endpoint.md` unfolds a day that crosses
   * midnight: La Ronde's `10 → 0` is one ascending run of hours, and the hour
   * after 23 is 24 rather than 0 of the next date. Showtimes were never
   * unfolded that way — both readers below grouped them by the showtime's own
   * park-local calendar date — so the last performance of such a day landed on
   * the morning after, where `PlanDayService.buildShows` served it as that
   * day's `scheduled` programme and suppressed the projection behind it.
   *
   * The rule here is the same one, expressed against the schedule rather than
   * against an hour: a showtime belongs to the previous date when that date's
   * published operating window still covers it. Both day readers reach it
   * through {@link ShowsService.showtimeEntriesCte}; `rebuildSchedulePatterns`
   * calls it directly. Everything else keeps its calendar date, which is why
   * this is a narrowing and not a shift — a park with no wrap day is untouched,
   * and so is every showtime after the window closes.
   *
   * Measured against production on 2026-09-15 (PAR-51): **20 showtimes** move
   * — 19 at Disneyland Park (Anaheim) and one at Magic Kingdom Park, all of
   * them at exactly 00:00, the last performance of a day that closes at
   * midnight.
   *
   * How many parks publish a wrap day at all is a moving number and is quoted
   * as one: 34 that evening, 36 twelve hours earlier. Only 22 of the 34 sit on
   * a date already past; the rest are future dates the schedule sync rewrites
   * whenever an operator revises its hours, so a park leaves the set the day
   * its October closing changes from `00:00` to `23:00`. The 20 do not move
   * with it — they are performances in snapshots already taken.
   *
   * Two limits are deliberate, because both would be a different decision:
   *
   * `OPERATING` only, the same schedule type every other reader in this
   * codebase treats as opening hours. Universal's Halloween Horror Nights runs
   * past midnight as `TICKETED_EVENT`, and its 00:30 shows therefore stay on
   * the following date — production holds exactly two such rows, both still in
   * the future. Widening the type here would change what "the park is open"
   * means for shows alone.
   *
   * And a *published* window, never a guessed one. A fixed "before 06:00
   * belongs to yesterday" cutoff would sweep up feeds whose problem is a
   * different one: Universal Studios Japan serves `Ollivanders™` at 16:00 UTC,
   * which is 01:00 the next morning in Tokyo for a daytime walkthrough, on 148
   * days. Those parks publish no wrap day, so this expression leaves them
   * exactly where they are.
   *
   * The window is read through {@link normalizedClosingSql}, never from the raw
   * column, and that is load-bearing in both directions. A closing time stamped
   * with the day's OWN date — ThemeParks.wiki does that for `open 12:00 /
   * close 00:00`, Parque Warner Madrid every day — is not a wrap day to a raw
   * comparison, so the rule would miss exactly the rows §5 was written for. And
   * an overshot window (`operating-window.util.ts` names a 34-hour one at
   * SeaWorld San Diego and a three-year one at Busch Gardens Williamsburg)
   * would swallow the whole following day, dragging ordinary afternoon
   * performances onto the date before. Normalising re-anchors both to the
   * opening's park-local date, through the timezone rather than by adding 24
   * hours, so a DST night keeps its local closing time.
   *
   * It is a correlated subquery in a job that walks millions of rows, so the
   * cost was measured rather than assumed: against production on 2026-09-15
   * the pattern window `rebuildSchedulePatterns` reads holds **3,788,203**
   * showtime entries, and resolving the operating day for every one of them
   * takes **8.1 s** against the 5.7 s that aggregation costs without it.
   * Roughly two and a half seconds, once a night.
   *
   * That is the nightly job, which is the expensive caller. The day readers run
   * the same subquery on a request path, but against a bounded set rather than
   * the pattern window: one park, a three-day snapshot window, and a calendar
   * prefilter that leaves only the two dates a showtime could still qualify on
   * (see `showtimeEntriesCte`). The subquery never runs for the rest.
   *
   * **Why this is not built on {@link parkOpenWindowCtes}**, which is the
   * canonical operating-day definition and was made reusable for callers that
   * number their placeholders differently (PAR-129). The load-bearing reason
   * is shape: that helper produces CTEs, while both readers here need the day
   * resolved per row inside a SELECT list, so adopting it means restructuring
   * both statements around a join rather than calling a function. Whether that
   * join would be cheaper, dearer or the same has **not** been measured — the
   * 8.1 s above compares this subquery against resolving no operating day at
   * all, which is a different question, and `windows_raw` pre-selects on
   * `se."openingTime"`, which `idx_schedule_operating_times` does cover.
   *
   * Two differences would have to survive that move, and both are deliberate
   * here:
   *
   * The closing bound is **inclusive** (`st <= closes`), where the rest of this
   * repository is half-open (`closure-gap.sql.ts`, `isParkOpen`). It has to be:
   * every one of the 20 showtimes the rule moves in production sits exactly on
   * its day's closing instant, so a half-open bound moves none of them.
   * `test/e2e/show-operating-day.e2e-spec.ts` pins it, and PAR-260 carries it
   * as an acceptance criterion so a unification cannot drop it by accident.
   *
   * The day is anchored on `se.date`, a **feed value** (`parks.service.ts`
   * writes `date: new Date(entry.date)`), not on the opening's park-local date
   * the way `win` anchors it. That the two agree is a property of the sources,
   * not an invariant — measured on 2026-09-15, 0 of 36,543 park-wide
   * `OPERATING` rows disagree. The two directions of a disagreement differ: a
   * row dated *later* than its opening simply fails the pre-selection and the
   * showtime keeps its calendar date, but a row dated *earlier* passes every
   * test here and would anchor the showtime on the earlier date — a genuine
   * mis-assignment, not a miss. Unifying the two definitions is PAR-260.
   *
   * The timezone and the showtime expression are interpolated by the caller,
   * and the two callers pass different things: the day readers hand in the
   * bound parameter `$2`, while `rebuildSchedulePatterns` hands in the column
   * `tz` it already carries. The subquery itself binds nothing.
   */
  private static operatingDaySql(startTs: string, parkId: string, tz: string) {
    const closes = normalizedClosingSql(
      'se."openingTime"',
      'se."closingTime"',
      tz,
    );
    return `COALESCE(
              (SELECT se.date
                 FROM schedule_entries se
                WHERE se."parkId" = ${parkId}
                  AND se."attractionId" IS NULL
                  AND se."scheduleType" = 'OPERATING'
                  AND se."openingTime" IS NOT NULL
                  AND se."closingTime" IS NOT NULL
                  AND se.date = (${startTs} AT TIME ZONE ${tz})::date - 1
                  -- A wrap day, in the same terms as §5: the window ends on a
                  -- later park-local date than it starts on.
                  --
                  -- This test and the coverage test below overlap on an
                  -- ORDINARY day: either one alone already rejects it, so a
                  -- mutation that drops only this one leaves
                  -- \`show-operating-day.e2e-spec.ts\` green (measured
                  -- 2026-09-15, 13/13). That is not licence to delete it — it
                  -- is the reason a green run says nothing here.
                  --
                  -- The two are NOT interchangeable. Dropping the coverage
                  -- test instead fails two cases and breaks the rule outright:
                  -- every afternoon time of the following day would move onto
                  -- the previous date. Dropping both fails four. They say
                  -- different things — this one that the day reaches into the
                  -- night at all, the one below how far it reaches — and only
                  -- the second one carries the rule on a wrap day.
                  AND (${closes} AT TIME ZONE ${tz})::date
                    > (se."openingTime" AT TIME ZONE ${tz})::date
                  AND ${startTs} >  se."openingTime"
                  AND ${startTs} <= ${closes}
                LIMIT 1),
              (${startTs} AT TIME ZONE ${tz})::date
            )`;
  }

  /**
   * Rebuild the per-weekday showtime patterns for every show.
   *
   * Runs nightly, because the question it answers cannot be answered per
   * request: one park's window is 47,000 snapshots and 350,000 showtime entries,
   * and the aggregation below takes 5.7 s across all parks. It is written as one
   * statement rather than a loop for the same reason — 213 parks of round trips
   * would cost more than the scan does.
   *
   * Two filters carry the whole correctness of this, and neither is obvious:
   *
   * The window is applied to the **showtime**, not only to the snapshot.
   * ThemeParks.wiki keeps returning showtimes it never cleared — Europa-Park's
   * live response today still carries entries from 2022 and 2023 — so a snapshot
   * written this morning can contain a start time from four years ago. Filtering
   * on `timestamp` alone would fold "Surpr'Ice with the Celtic Shadows" from
   * November 2022 into a September pattern.
   *
   * And the times come from the **most recent matching day**, not from a union
   * over the window. A union merges a summer programme with an autumn one into a
   * day that never happened; the latest matching day is a day that did.
   *
   * Both the day and the weekday are the **operating** ones — see
   * {@link ShowsService.operatingDaySql}. A park that closes at midnight has
   * its last performance counted on the day it belongs to, and on that day's
   * weekday, rather than seeding a pattern for the morning after.
   */
  async rebuildSchedulePatterns(): Promise<{
    patterns: number;
    shows: number;
  }> {
    const windowDays = ShowsService.PATTERN_WINDOW_DAYS;

    const rows: Array<{
      show_id: string;
      weekday: number;
      times: string[];
      observed_days: number;
      last_observed_on: string;
    }> = await this.showLiveDataRepository.manager.query(
      `WITH times AS (
         SELECT l."showId"        AS show_id,
                s."parkId"        AS park_id,
                p.timezone        AS tz,
                (e->>'startTime')::timestamptz AS st
           FROM show_live_data l
           JOIN shows s ON s.id = l."showId"
           JOIN parks p ON p.id = s."parkId"
          CROSS JOIN LATERAL jsonb_array_elements(COALESCE(l.showtimes, '[]'::jsonb)) e
          WHERE l.timestamp > now() - ($1 || ' days')::interval
            AND l.status = 'OPERATING'
       ), local AS (
         SELECT show_id,
                ${ShowsService.operatingDaySql("st", "park_id", "tz")} AS day,
                to_char(st AT TIME ZONE tz, 'HH24:MI')           AS hhmm,
                st,
                tz
           FROM times
          -- The showtime itself has to be recent: the feed serves years-old ones.
          WHERE st > now() - ($1 || ' days')::interval
            AND st < now() + INTERVAL '2 days'
       ), keyed AS (
         -- The weekday follows the OPERATING DAY, not the wall clock: a
         -- performance at 00:00 on a day that opened the previous morning runs
         -- on that morning's weekday, and a pattern keyed the other way would
         -- promise Saturday's late show on Sunday.
         SELECT show_id, day,
                EXTRACT(DOW FROM day)::int AS weekday,
                hhmm,
                -- Ordering is unfolded the way §5 unfolds hours: a time that
                -- falls on the date AFTER its operating day belongs at the end
                -- of that day, not at its start. Without this a 00:00 show
                -- sorts in front of the 22:00 one it follows, and both
                -- \`times[0]\` and the plan's "earliest first" become wrong.
                (st AT TIME ZONE tz)::date > day AS after_midnight
           FROM local
       ), per_time AS (
         -- One row per distinct wall-clock time, which is what the old
         -- \`array_agg(DISTINCT hhmm)\` guaranteed and a DISTINCT over the wider
         -- (hhmm, after_midnight) tuple would not: a time that appears on both
         -- sides of a midnight would otherwise be listed twice, out of order.
         -- \`bool_and\` resolves that to the earlier slot, which is where a
         -- reader expects an ambiguous time to sit.
         --
         -- The case is reachable exactly once a year, and only there. Two
         -- instants sharing a wall clock are normally 24 hours apart while
         -- \`normalizedClosingSql\` caps an operating day at 24 — but on a
         -- spring-forward night they are 23 apart, so both fit. Toronto,
         -- 2026-03-08: a day opening 03-07 21:00 EST and closing 03-08 22:00
         -- EDT spans 24 hours and holds 21:30 on both dates.
         -- \`show-operating-day.e2e-spec.ts\` pins it against the day reader's
         -- copy of this fold; widening that key there returns
         -- \`["21:30", "21:30"]\` (measured by mutation, 2026-09-15).
         --
         -- Removing the deduplication altogether fails a second case, because
         -- every poll republishes the day's whole programme.
         SELECT show_id, weekday, day, hhmm,
                bool_and(after_midnight) AS after_midnight
           FROM keyed
          GROUP BY 1, 2, 3, 4
       ), per_day AS (
         SELECT show_id, weekday, day,
                array_agg(hhmm ORDER BY after_midnight, hhmm) AS times
           FROM per_time
          GROUP BY 1, 2, 3
       ), ranked AS (
         SELECT *,
                row_number() OVER (PARTITION BY show_id, weekday ORDER BY day DESC) AS rn,
                count(*)     OVER (PARTITION BY show_id, weekday)                   AS observed_days
           FROM per_day
       )
       SELECT show_id, weekday, times, observed_days, day AS last_observed_on
         FROM ranked
        WHERE rn = 1`,
      [windowDays],
    );

    const computedAt = new Date();
    const entities = rows.map((r) => ({
      showId: r.show_id,
      weekday: r.weekday,
      times: r.times,
      observedDays: Number(r.observed_days),
      lastObservedOn: r.last_observed_on,
      computedAt,
    }));

    // Replace wholesale: a pattern that stopped being observed has to disappear,
    // and an upsert would leave last season's Saturday sitting there forever.
    await this.showSchedulePatternRepository.manager.transaction(async (tx) => {
      await tx.clear(ShowSchedulePattern);
      for (let i = 0; i < entities.length; i += 500) {
        await tx.insert(ShowSchedulePattern, entities.slice(i, i + 500));
      }
    });

    const shows = new Set(rows.map((r) => r.show_id)).size;
    this.logger.log(
      `🎭 Show patterns rebuilt: ${entities.length} pattern(s) across ${shows} show(s)`,
    );
    return { patterns: entities.length, shows };
  }

  /**
   * The showtimes a park's shows actually have on one park-local date.
   *
   * Unions EVERY snapshot taken around that date, rather than reading the
   * freshest one. Showtimes are delta-written and the feed drops performances
   * as they pass, so by the evening the newest snapshot holds what is left of
   * the day — measured live at 19:20 in Rust, that was 10 of Europa-Park's 35
   * shows, and the other 25 fell through to the projection on a day we had 186
   * real times for. The day's programme is the union of what the day reported.
   *
   * In practice this answers for today and nothing else — no source publishes
   * further ahead — but it is written against the date rather than against
   * "today" so it keeps working the day one does.
   *
   * `date` is the **operating** day, not the calendar one — see
   * {@link ShowsService.operatingDaySql}. On a day that runs past midnight the
   * answer therefore includes that day's late performances, and the morning
   * after does not inherit them.
   */
  async getShowtimesOnDate(
    parkId: string,
    timezone: string,
    date: string,
  ): Promise<Map<string, string[]>> {
    const rows: Array<{ show_id: string; times: string[] }> =
      await this.showLiveDataRepository.manager.query(
        `WITH ${ShowsService.showtimeEntriesCte()}
         SELECT show_id,
                array_agg(hhmm ORDER BY after_midnight, hhmm) AS times
           FROM (SELECT show_id,
                        to_char(st AT TIME ZONE $2, 'HH24:MI') AS hhmm,
                        -- Same unfolding as the patterns: the 00:00 performance
                        -- of a wrap day is the day's LAST one, so it sorts after
                        -- 22:00 rather than in front of it. Grouped rather than
                        -- DISTINCTed so a time cannot be listed twice by
                        -- appearing on both sides of the midnight.
                        bool_and((st AT TIME ZONE $2)::date > op_day) AS after_midnight
                   FROM entries
                  WHERE op_day = $3::date
                  GROUP BY show_id, to_char(st AT TIME ZONE $2, 'HH24:MI')) d
          GROUP BY show_id`,
        [parkId, timezone, date],
      );

    return new Map(rows.map((r) => [r.show_id, r.times]));
  }

  /**
   * The same day's showtimes as {@link getShowtimesOnDate}, but as instants.
   *
   * A caller that needs to know WHEN a performance starts cannot rebuild it
   * from the `HH:mm` strings above, and since showtimes follow the operating
   * day it is no longer even close: the 00:00 performance of a day that runs
   * past midnight is returned for that day, and pinning it to that day's date
   * puts it 24 hours early. Measured on 2026-09-15, that is 20 performances —
   * every one of Disneyland Park's and Magic Kingdom Park's wrap-day finales.
   *
   * It also removes a reconstruction that could only ever lose: the caller used
   * to run the wall clock back through `fromZonedTime` and then check whether
   * it round-tripped, because a local time inside a spring-forward gap has no
   * instant at all. The instant was in the row the whole time.
   */
  async getShowtimeInstantsOnDate(
    parkId: string,
    timezone: string,
    date: string,
  ): Promise<Map<string, string[]>> {
    const rows: Array<{ show_id: string; starts: Date[] }> =
      await this.showLiveDataRepository.manager.query(
        `WITH ${ShowsService.showtimeEntriesCte()}
         SELECT show_id, array_agg(DISTINCT st ORDER BY st) AS starts
           FROM entries
          WHERE op_day = $3::date
          GROUP BY show_id`,
        [parkId, timezone, date],
      );

    return new Map(
      rows.map((r) => [r.show_id, r.starts.map((s) => s.toISOString())]),
    );
  }

  /**
   * The per-weekday patterns for one park's shows, keyed by show id.
   *
   * `weekday` is the Postgres convention (0 = Sunday), which is also
   * JavaScript's `getUTCDay()` — see the column's own note on why that is worth
   * stating out loud in this codebase.
   */
  async getSchedulePatterns(
    parkId: string,
    weekday: number,
  ): Promise<Map<string, ShowSchedulePattern>> {
    const rows = await this.showSchedulePatternRepository
      .createQueryBuilder("p")
      .innerJoin(Show, "s", "s.id = p.showId")
      .where("s.parkId = :parkId", { parkId })
      .andWhere("p.weekday = :weekday", { weekday })
      .getMany();

    return new Map(rows.map((r) => [r.showId, r]));
  }
}
