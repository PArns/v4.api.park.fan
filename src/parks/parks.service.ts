import { Inject, Injectable, Logger, forwardRef } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, In, IsNull } from "typeorm";
import { Park } from "./entities/park.entity";
import { ScheduleEntry, ScheduleType } from "./entities/schedule-entry.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Show } from "../shows/entities/show.entity";
import { Restaurant } from "../restaurants/entities/restaurant.entity";
import { ThemeParksClient } from "../external-apis/themeparks/themeparks.client";
import { ThemeParksMapper } from "../external-apis/themeparks/themeparks.mapper";
import { DestinationsService } from "../destinations/destinations.service";
import { HolidaysService } from "../holidays/holidays.service";
import { generateSlug, generateUniqueSlug } from "../common/utils/slug.util";
import { normalizeSortDirection } from "../common/utils/query.util";
import {
  formatInParkTimezone,
  getCurrentDateInTimezone,
  getStartOfDayInTimezone,
  getTomorrowDateInTimezone,
} from "../common/utils/date.util";
import { addDays, subDays } from "date-fns";
import { normalizeRegionCode } from "../common/utils/region.util";
import {
  calculateHolidayInfo,
  HolidayEntry,
} from "../common/utils/holiday.utils";
import {
  calculateParkPriority,
  findDuplicatePark,
  hasScheduleData,
  hasRecentQueueData,
} from "./utils/park-merge.util";

import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";

@Injectable()
export class ParksService {
  private readonly logger = new Logger(ParksService.name);
  private readonly TTL_SCHEDULE = 60 * 60; // 1 hour - park schedules
  private readonly CACHE_TTL_SECONDS = 60 * 60; // 1 hour - legacy

  /** In-memory cache for geographic path lookups that returned null (404).
   *  Key: "continent:country:city:slug" → expiry timestamp (ms). */
  private readonly notFoundCache = new Map<string, number>();
  private readonly NOT_FOUND_TTL_MS = 60 * 60 * 1000; // 1 hour

  constructor(
    @InjectRepository(Park)
    private parkRepository: Repository<Park>,
    @InjectRepository(ScheduleEntry)
    private scheduleRepository: Repository<ScheduleEntry>,
    private themeParksClient: ThemeParksClient,
    private themeParksMapper: ThemeParksMapper,
    private destinationsService: DestinationsService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    @Inject(forwardRef(() => HolidaysService))
    private holidaysService: HolidaysService,
  ) {}

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

      const destinationParks = await this.parkRepository.find({
        where: { destinationId: destination.id },
      });
      const parksByExternalId = new Map(
        destinationParks.map((p) => [p.externalId, p]),
      );

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
        let existing = mappedData.externalId
          ? (parksByExternalId.get(mappedData.externalId) ??
            (await this.parkRepository.findOne({
              where: { externalId: mappedData.externalId },
            })))
          : null;

        // If not found by externalId, check for potential duplicates by name
        if (!existing) {
          const allParks = destinationParks;

          const duplicate = findDuplicatePark(
            mappedData.name!,
            allParks,
            0.9, // 90% similarity threshold
          );

          if (duplicate) {
            // Silent duplicate detection - log only if merging

            // Check which park has schedule data AND queue data
            const duplicateHasSchedule = await hasScheduleData(
              duplicate.id,
              this.scheduleRepository,
            );

            // Check for recent queue data (strongest signal of active park)
            const duplicateHasQueueData = await hasRecentQueueData(
              duplicate.id,
              this.parkRepository.manager,
            );

            const duplicatePriority = calculateParkPriority(
              duplicate,
              duplicateHasSchedule,
              duplicateHasQueueData,
            );

            // Calculate priority for new park (assume no schedule/queue data yet)
            const newParkPriority = calculateParkPriority(
              { ...duplicate, ...mappedData } as Park,
              false,
              false,
            );

            // TRUE MERGE: Consolidate external IDs from both parks
            // Winner keeps its data + inherits missing IDs from loser
            // This ensures we get both schedule data AND queue-times capabilities
            if (duplicatePriority >= newParkPriority) {
              // Keep existing park, merge new data into it (silent)

              // Consolidate ALL external IDs from both sources
              const updateData: Partial<Park> = {};

              // Add Wiki ID if missing
              if (mappedData.wikiEntityId && !duplicate.wikiEntityId) {
                updateData.wikiEntityId = mappedData.wikiEntityId;
              }

              // Add Queue-Times ID if missing (enables queue data)
              if (
                mappedData.queueTimesEntityId &&
                !duplicate.queueTimesEntityId
              ) {
                updateData.queueTimesEntityId = mappedData.queueTimesEntityId;
              }

              // Update other fields to latest data
              updateData.name = mappedData.name;
              updateData.latitude = mappedData.latitude;
              updateData.longitude = mappedData.longitude;
              updateData.timezone = mappedData.timezone;

              if (Object.keys(updateData).length > 0) {
                await this.parkRepository.update(duplicate.id, updateData);
              }

              // TRUE MERGE: Migrate child entities (shows, restaurants)
              // losingPark would have been found at the externalId check above; null here is safe
              const losingPark =
                (mappedData.externalId &&
                  parksByExternalId.get(mappedData.externalId)) ||
                null;

              if (losingPark && losingPark.id !== duplicate.id) {
                // Silent migration - log only summary

                let totalMigrated = 0;

                // Migrate Shows
                const showCount = await this.parkRepository.manager.query(
                  `UPDATE shows SET "parkId" = $1 WHERE "parkId" = $2::uuid`,
                  [duplicate.id, losingPark.id],
                );
                totalMigrated += showCount[1] || 0;

                // Migrate Restaurants
                const restaurantCount = await this.parkRepository.manager.query(
                  `UPDATE restaurants SET "parkId" = $1 WHERE "parkId" = $2::uuid`,
                  [duplicate.id, losingPark.id],
                );
                totalMigrated += restaurantCount[1] || 0;

                // Migrate Attractions
                const attractionCount = await this.parkRepository.manager.query(
                  `UPDATE attractions SET "parkId" = $1 WHERE "parkId" = $2::uuid`,
                  [duplicate.id, losingPark.id],
                );
                totalMigrated += attractionCount[1] || 0;

                // Check if losing park is now empty
                const remaining = await this.parkRepository.manager.query(
                  `SELECT 
                    (SELECT COUNT(*) FROM shows WHERE "parkId" = $1::uuid) as shows,
                    (SELECT COUNT(*) FROM restaurants WHERE "parkId" = $1::uuid) as restaurants,
                    (SELECT COUNT(*) FROM attractions WHERE "parkId" = $1::uuid) as attractions
                  `,
                  [losingPark.id],
                );

                const isEmpty =
                  remaining[0].shows === "0" &&
                  remaining[0].restaurants === "0" &&
                  remaining[0].attractions === "0";

                if (isEmpty) {
                  // Safe to delete losing park - all entities have been migrated
                  await this.parkRepository.delete(losingPark.id);
                }

                if (totalMigrated > 0) {
                  this.logger.log(
                    `🔀 Migrated ${totalMigrated} entities from "${losingPark.name}" to "${duplicate.name}"`,
                  );
                }
              }

              existing = duplicate;
            } else {
              // New park has higher priority -  silent merge

              // Inherit IDs from duplicate if new park doesn't have them
              if (duplicate.wikiEntityId && !mappedData.wikiEntityId) {
                mappedData.wikiEntityId = duplicate.wikiEntityId;
              }
              if (
                duplicate.queueTimesEntityId &&
                !mappedData.queueTimesEntityId
              ) {
                mappedData.queueTimesEntityId = duplicate.queueTimesEntityId;
              }

              // In this case, we'd need to migrate FROM duplicate TO the new park
              // But we're setting existing = duplicate, so the new park won't be created
              // This is the correct behavior: Keep duplicate, skip creating new park
              existing = duplicate;
            }
          }
        }

        if (existing) {
          // Update existing park (keep existing slug)
          await this.parkRepository.update(existing.id, {
            name: mappedData.name,
            latitude: mappedData.latitude,
            longitude: mappedData.longitude,
            timezone: mappedData.timezone,
          });

          // Check for "Ghost Parks" (duplicates by Queue-Times ID)
          // This fixes the "Split Brain" issue where we have one park from Wiki and another from Queue-Times
          const qtId =
            mappedData.queueTimesEntityId || existing.queueTimesEntityId;

          if (qtId) {
            // Look for any OTHER park that has this Queue-Times ID
            // or has an externalId matching 'qt-{id}'
            const ghostPark = await this.parkRepository
              .createQueryBuilder("park")
              .where("park.id != :currentId", { currentId: existing.id })
              .andWhere(
                "(park.queue_times_entity_id = :qtId OR park.externalId = :qtExternalId)",
                {
                  qtId: qtId,
                  qtExternalId: `qt-${qtId}`,
                },
              )
              .getOne();

            if (ghostPark) {
              this.logger.log(
                `👻 Found Ghost Park "${ghostPark.name}" (ID: ${ghostPark.id}) matching Queue-Times ID ${qtId}`,
              );
              this.logger.log(
                `🔀 Merging Ghost Park "${ghostPark.name}" into "${existing.name}"`,
              );

              // Migrate child entities with collision handling
              await this.parkRepository.manager.transaction(
                async (transactionalEntityManager) => {
                  // 1. Handle Attraction Collisions
                  // Fetch attractions from both parks to compare
                  const existingAttractions =
                    await transactionalEntityManager.query(
                      `SELECT id, slug, "queue_times_entity_id", "land_name", "land_external_id" FROM attractions WHERE "parkId" = $1::uuid`,
                      [existing.id],
                    );
                  const ghostAttractions =
                    await transactionalEntityManager.query(
                      `SELECT id, slug, "queue_times_entity_id", "land_name", "land_external_id" FROM attractions WHERE "parkId" = $1::uuid`,
                      [ghostPark.id],
                    );

                  for (const ghostAttr of ghostAttractions) {
                    const match = existingAttractions.find(
                      (a: any) => a.slug === ghostAttr.slug,
                    );

                    if (match) {
                      // COLLISION: Merge data into existing attraction, then delete ghost attraction
                      // We specifically want the Land Info and Queue-Times ID from the ghost
                      await transactionalEntityManager.query(
                        `UPDATE attractions 
                         SET "land_name" = COALESCE($1, "land_name"),
                             "land_external_id" = COALESCE($2, "land_external_id"),
                             "queue_times_entity_id" = COALESCE($3, "queue_times_entity_id")
                         WHERE id = $4`,
                        [
                          ghostAttr.land_name,
                          ghostAttr.land_external_id,
                          ghostAttr.queue_times_entity_id,
                          match.id,
                        ],
                      );
                      // Delete the ghost attraction since we merged its useful data
                      await transactionalEntityManager.query(
                        `DELETE FROM attractions WHERE id = $1`,
                        [ghostAttr.id],
                      );
                      this.logger.log(
                        `    Merges attraction data: ${ghostAttr.slug}`,
                      );
                    } else {
                      // NO COLLISION: Move attraction to new park
                      await transactionalEntityManager.query(
                        `UPDATE attractions SET "parkId" = $1 WHERE id = $2`,
                        [existing.id, ghostAttr.id],
                      );
                    }
                  }

                  // 2. Migrate Shows (Blind update OK if slugs distinctive, else duplicate logic needed? mostly safe for now)
                  await transactionalEntityManager.query(
                    `UPDATE shows SET "parkId" = $1 WHERE "parkId" = $2::uuid`,
                    [existing.id, ghostPark.id],
                  );

                  // 3. Migrate Restaurants
                  await transactionalEntityManager.query(
                    `UPDATE restaurants SET "parkId" = $1 WHERE "parkId" = $2::uuid`,
                    [existing.id, ghostPark.id],
                  );

                  // 4. Delete the ghost park
                  await transactionalEntityManager.delete(Park, ghostPark.id);
                },
              );
              this.logger.log(`✅ Ghost Park merged and deleted successfully.`);
            }
          }
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

    // Run self-repair to clean up any duplicates (e.g. Wiki vs Queue-Times split)
    await this.repairDuplicates();

    return syncedCount;
  }

  /**
   * Scans for and merges duplicate parks based on shared Queue-Times IDs.
   * This fixes "Split Brain" issues where a park exists separately from Wiki and Queue-Times sources.
   */
  async repairDuplicates(): Promise<void> {
    this.logger.debug("🔧 Running Duplicate Park Repair...");

    // Find all Queue-Times IDs that are used by more than one park
    const duplicates = await this.parkRepository.query(`
      SELECT "queue_times_entity_id"
      FROM parks
      WHERE "queue_times_entity_id" IS NOT NULL
      GROUP BY "queue_times_entity_id"
      HAVING COUNT(*) > 1
    `);

    if (duplicates.length > 0) {
      this.logger.warn(
        `Found ${duplicates.length} duplicate sets into repair.`,
      );
    } else {
      this.logger.debug("Found 0 duplicate sets to repair.");
    }

    for (const dup of duplicates) {
      const qtId = dup.queue_times_entity_id;

      // Get all parks with this ID
      const parks = await this.parkRepository.find({
        where: { queueTimesEntityId: qtId },
        order: { createdAt: "ASC" }, // Oldest first usually, but we prefer Wiki-sourced
      });

      if (parks.length < 2) continue;

      // Identify Primary (Winner) and Ghost (Loser)
      // Preference: Park with Wiki ID > Oldest Park
      let primary = parks.find((p) => p.wikiEntityId !== null);
      if (!primary) primary = parks[0]; // Fallback to first one

      const ghosts = parks.filter((p) => p.id !== primary!.id);

      for (const ghostPark of ghosts) {
        this.logger.log(
          `🔀 Merging Ghost Park "${ghostPark.name}" into "${primary!.name}" (Shared QT ID: ${qtId})`,
        );

        await this.parkRepository.manager.transaction(
          async (transactionalEntityManager) => {
            // 1. Handle Attraction Collisions
            const existingAttractions = await transactionalEntityManager.query(
              `SELECT id, slug, "land_name", "land_external_id" FROM attractions WHERE "parkId" = $1::uuid`,
              [primary!.id],
            );
            const ghostAttractions = await transactionalEntityManager.query(
              `SELECT id, slug, "land_name", "land_external_id" FROM attractions WHERE "parkId" = $1::uuid`,
              [ghostPark.id],
            );

            for (const ghostAttr of ghostAttractions) {
              const match = existingAttractions.find(
                (a: any) => a.slug === ghostAttr.slug,
              );

              if (match) {
                // COLLISION: Merge data, move mappings, delete ghost attraction

                // 1. Copy Land Data if missing in primary
                await transactionalEntityManager.query(
                  `UPDATE attractions 
                   SET "land_name" = COALESCE("land_name", $1),
                       "land_external_id" = COALESCE("land_external_id", $2)
                   WHERE id = $3`,
                  [ghostAttr.land_name, ghostAttr.land_external_id, match.id],
                );

                // 2. Move External Mappings (Queue-Times ID mappings) from Ghost to Primary
                await transactionalEntityManager.query(
                  `UPDATE external_entity_mapping 
                   SET "internal_entity_id" = $1 
                   WHERE "internal_entity_id" = $2`,
                  [match.id, ghostAttr.id],
                );

                // 3. Move Queue Data (Wait Times History)
                await transactionalEntityManager.query(
                  `UPDATE queue_data 
                   SET "attractionId" = $1 
                   WHERE "attractionId" = $2::uuid`,
                  [match.id, ghostAttr.id],
                );

                // 4. Move Wait Time Predictions
                await transactionalEntityManager.query(
                  `UPDATE wait_time_predictions 
                   SET "attractionId" = $1 
                   WHERE "attractionId" = $2::uuid`,
                  [match.id, ghostAttr.id],
                );

                // 5. Move Prediction Accuracy Records
                await transactionalEntityManager.query(
                  `UPDATE prediction_accuracy 
                   SET "attractionId" = $1 
                   WHERE "attractionId" = $2::uuid`,
                  [match.id, ghostAttr.id],
                );

                // 6. Delete the ghost attraction
                await transactionalEntityManager.query(
                  `DELETE FROM attractions WHERE id = $1`,
                  [ghostAttr.id],
                );
                this.logger.log(
                  `    Merged attraction data & mappings: ${ghostAttr.slug}`,
                );
              } else {
                // NO COLLISION: Move
                await transactionalEntityManager.query(
                  `UPDATE attractions SET "parkId" = $1 WHERE id = $2`,
                  [primary!.id, ghostAttr.id],
                );
              }
            }

            // 2. Migrate Shows
            await transactionalEntityManager.query(
              `UPDATE shows SET "parkId" = $1 WHERE "parkId" = $2::uuid`,
              [primary!.id, ghostPark.id],
            );

            // 3. Migrate Restaurants
            await transactionalEntityManager.query(
              `UPDATE restaurants SET "parkId" = $1 WHERE "parkId" = $2::uuid`,
              [primary!.id, ghostPark.id],
            );

            // 4. Delete the ghost park
            await transactionalEntityManager.delete(Park, ghostPark.id);
          },
        );
        this.logger.log(
          `✅ Ghost Park "${ghostPark.name}" merged and deleted.`,
        );
      }
    }
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
    continentSlug?: string;
    countrySlug?: string;
    citySlug?: string;
    sort?: string;
    page?: number;
    limit?: number;
  }): Promise<{ data: Park[]; total: number }> {
    const queryBuilder = this.parkRepository
      .createQueryBuilder("park")
      .leftJoinAndSelect("park.destination", "destination");

    // Apply filters - prefer slugs over names (from geo routes)
    if (filters.continentSlug) {
      queryBuilder.andWhere("park.continentSlug = :continentSlug", {
        continentSlug: filters.continentSlug,
      });
    } else if (filters.continent) {
      queryBuilder.andWhere("LOWER(park.continent) = LOWER(:continent)", {
        continent: filters.continent,
      });
    }

    if (filters.countrySlug) {
      queryBuilder.andWhere("park.countrySlug = :countrySlug", {
        countrySlug: filters.countrySlug,
      });
    } else if (filters.country) {
      queryBuilder.andWhere("LOWER(park.country) = LOWER(:country)", {
        country: filters.country,
      });
    }

    if (filters.citySlug) {
      queryBuilder.andWhere("park.citySlug = :citySlug", {
        citySlug: filters.citySlug,
      });
    } else if (filters.city) {
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
    const park = await this.parkRepository.findOne({
      where: { slug },
      relations: ["destination"],
    });
    if (!park) return null;
    return this.loadParkRelations(park);
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

    // 1. Fetch Park geo data for holiday checking
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "countryCode", "regionCode", "timezone"],
    });

    // 2. Pre-fetch holidays for the date range (Extended by +/ 1 day for bridge day checks)
    const holidayMap = new Map<string, string | HolidayEntry>(); // Date -> Name or HolidayEntry
    if (park?.countryCode) {
      try {
        // Use noon-UTC timestamps so formatInParkTimezone() stays on the same
        // calendar day for parks in every timezone (west-of-UTC parks shift
        // midnight-UTC to the previous day, which would narrow the holiday range).
        const dates = scheduleData.map((e) => {
          const raw =
            typeof e.date === "string"
              ? e.date
              : e.date instanceof Date
                ? e.date.toISOString().split("T")[0]
                : String(e.date);
          const isDateOnly = /^\d{4}-\d{2}-\d{2}$/.test(raw);
          const dStr = isDateOnly
            ? raw
            : formatInParkTimezone(new Date(raw), park!.timezone);
          return new Date(`${dStr}T12:00:00Z`).getTime();
        });
        const minDate = new Date(Math.min(...dates));
        const maxDate = new Date(Math.max(...dates));

        // Extend range by 1 day for bridge day detection
        minDate.setDate(minDate.getDate() - 1);
        maxDate.setDate(maxDate.getDate() + 1);

        const holidays = await this.holidaysService.getHolidays(
          park.countryCode,
          formatInParkTimezone(minDate, park.timezone),
          formatInParkTimezone(maxDate, park.timezone),
        );

        // Normalize region codes for consistent comparison
        const normalizedParkRegion = normalizeRegionCode(park.regionCode);

        for (const h of holidays) {
          // Match: Nationwide holidays OR region matches (using normalized codes)
          const normalizedHolidayRegion = normalizeRegionCode(h.region);
          if (
            h.isNationwide ||
            (normalizedParkRegion &&
              normalizedHolidayRegion === normalizedParkRegion)
          ) {
            // Use park's timezone to determine the holiday date string
            // Normalize to noon UTC to prevent timezone shifts (YYYY-MM-DD from DB)
            const normalizedDate = new Date(h.date);
            normalizedDate.setUTCHours(12, 0, 0, 0);
            const dateStr = formatInParkTimezone(normalizedDate, park.timezone);

            // If multiple holidays on same day, prefer public holidays over school holidays
            const existing = holidayMap.get(dateStr);
            const hType = h.holidayType;

            if (!existing) {
              // Store as HolidayEntry to preserve type information for bridge day logic
              holidayMap.set(dateStr, {
                name: h.localName || h.name || "",
                type: hType,
                allTypes: [hType],
              });
            } else {
              if (typeof existing !== "string") {
                // Aggregate types
                if (!existing.allTypes) existing.allTypes = [existing.type];
                if (!existing.allTypes.includes(hType)) {
                  existing.allTypes.push(hType);
                }

                // Prioritize public holidays for the main entry
                if (hType === "public" || hType === "bank") {
                  existing.name = h.localName || h.name || "";
                  existing.type = hType;
                }
              }
            }
          }
        }
      } catch (error) {
        this.logger.warn(
          `Failed to fetch holidays for schedule sync (Park ${parkId}): ${error}`,
        );
      }
    }

    let savedCount = 0;

    for (const entry of scheduleData) {
      // Derive the park-local calendar date (YYYY-MM-DD) safely.
      // The API returns date-only strings ("YYYY-MM-DD") that represent the park's local
      // calendar date. new Date("YYYY-MM-DD") produces midnight UTC, so applying
      // formatInParkTimezone to it would shift the date back by one day for parks west
      // of UTC (e.g. "2026-03-02" → "2026-03-01" in America/New_York). Instead we detect
      // date-only strings and use them directly; full datetime strings are still converted.
      const rawDateStr: string =
        typeof entry.date === "string"
          ? entry.date
          : entry.date instanceof Date
            ? entry.date.toISOString().split("T")[0]
            : String(entry.date);
      const isDateOnly = /^\d{4}-\d{2}-\d{2}$/.test(rawDateStr);
      const dateStr: string = isDateOnly
        ? rawDateStr
        : formatInParkTimezone(new Date(rawDateStr), park!.timezone);
      // Use noon UTC so timezone conversions inside calculateHolidayInfo won't shift the date
      const dateObj = new Date(`${dateStr}T12:00:00Z`);

      const holidayInfo = calculateHolidayInfo(
        dateObj,
        holidayMap,
        park!.timezone,
      );

      // Normalize API type: "Closed"/"CLOSED" → CLOSED so off-season (e.g. Phantasialand Feb) is persisted
      // Also normalize "PARK_OPEN" to OPERATING
      const rawType = entry.type?.toString().toUpperCase();
      let scheduleType: ScheduleType;

      if (rawType === "CLOSED") {
        scheduleType = ScheduleType.CLOSED;
      } else if (rawType === "PARK_OPEN") {
        scheduleType = ScheduleType.OPERATING;
      } else {
        scheduleType = entry.type as ScheduleType;
      }

      // dateStr is already the park-local calendar date (YYYY-MM-DD)
      const normalizedDate = new Date(`${dateStr}T12:00:00Z`);

      const scheduleEntry: Partial<ScheduleEntry> = {
        parkId,
        date: normalizedDate,
        scheduleType,
        openingTime: entry.openingTime ? new Date(entry.openingTime) : null,
        closingTime: entry.closingTime ? new Date(entry.closingTime) : null,
        description: entry.description || null,
        purchases: entry.purchases || null,
        isHoliday: holidayInfo.isHoliday,
        holidayName: holidayInfo.holidayName,
        isBridgeDay: holidayInfo.isBridgeDay,
      };

      // Check if entry exists for this park, date, and type
      // Use date string for reliable comparison (avoids TZ issues with Date objects)
      const existing = await this.scheduleRepository
        .createQueryBuilder("schedule")
        .where("schedule.parkId = :parkId", { parkId })
        .andWhere("schedule.date = :date", { date: dateStr })
        .andWhere("schedule.scheduleType = :type", {
          type: scheduleEntry.scheduleType,
        })
        .getOne();

      if (!existing) {
        await this.scheduleRepository.save(scheduleEntry);
        savedCount++;
        await this.invalidateScheduleCache(parkId);
      } else {
        // Update if times, description, or holiday/bridge status changed
        const hasChanges =
          existing.openingTime?.getTime() !==
            scheduleEntry.openingTime?.getTime() ||
          existing.closingTime?.getTime() !==
            scheduleEntry.closingTime?.getTime() ||
          existing.description !== scheduleEntry.description ||
          existing.purchases !== scheduleEntry.purchases ||
          existing.isHoliday !== scheduleEntry.isHoliday ||
          existing.holidayName !== scheduleEntry.holidayName ||
          existing.isBridgeDay !== scheduleEntry.isBridgeDay;

        if (hasChanges) {
          await this.scheduleRepository.update(existing.id, scheduleEntry);
          savedCount++;
          await this.invalidateScheduleCache(parkId);
        }
      }

      // Note: Deletions are batched and executed after the loop for performance
      // (reduces N DELETE queries to 3 batch queries, 99% reduction)
    }

    // Batch DELETE operations: Cleanup placeholders when we have real data from the API.
    // Use date strings for reliable deletion (avoids TZ-dependent off-by-one with Date objects).

    // Normalize entries once (avoid 3x redundant normalization).
    // Use the same date-only detection as the main loop to avoid off-by-one TZ shifts.
    const normalizedEntries = scheduleData.map((e) => {
      const rawType = e.type?.toString().toUpperCase();
      const raw: string =
        typeof e.date === "string"
          ? e.date
          : e.date instanceof Date
            ? e.date.toISOString().split("T")[0]
            : String(e.date);
      const date = /^\d{4}-\d{2}-\d{2}$/.test(raw)
        ? raw
        : formatInParkTimezone(new Date(raw), park!.timezone);

      let scheduleType: ScheduleType;
      if (rawType === "CLOSED") {
        scheduleType = ScheduleType.CLOSED;
      } else if (rawType === "PARK_OPEN") {
        scheduleType = ScheduleType.OPERATING;
      } else {
        scheduleType = e.type as ScheduleType;
      }

      return {
        date,
        scheduleType,
      };
    });

    // Filter normalized entries for deletion
    const deleteUnknownDates = normalizedEntries
      .filter((e) => e.scheduleType !== ScheduleType.UNKNOWN)
      .map((e) => e.date);

    const deleteClosedDates = normalizedEntries
      .filter((e) => e.scheduleType === ScheduleType.OPERATING)
      .map((e) => e.date);

    const deleteOperatingDates = normalizedEntries
      .filter((e) => e.scheduleType === ScheduleType.CLOSED)
      .map((e) => e.date);

    if (deleteUnknownDates.length > 0) {
      await this.scheduleRepository
        .createQueryBuilder()
        .delete()
        .from(ScheduleEntry)
        .where('"parkId" = :parkId', { parkId })
        .andWhere("date IN (:...dates)", { dates: deleteUnknownDates })
        .andWhere('"scheduleType" = :type', {
          type: ScheduleType.UNKNOWN,
        })
        .execute();
    }

    if (deleteClosedDates.length > 0) {
      await this.scheduleRepository
        .createQueryBuilder()
        .delete()
        .from(ScheduleEntry)
        .where('"parkId" = :parkId', { parkId })
        .andWhere("date IN (:...dates)", { dates: deleteClosedDates })
        .andWhere('"scheduleType" = :type', {
          type: ScheduleType.CLOSED,
        })
        .execute();
    }

    if (deleteOperatingDates.length > 0) {
      await this.scheduleRepository
        .createQueryBuilder()
        .delete()
        .from(ScheduleEntry)
        .where('"parkId" = :parkId', { parkId })
        .andWhere("date IN (:...dates)", { dates: deleteOperatingDates })
        .andWhere('"scheduleType" = :type', {
          type: ScheduleType.OPERATING,
        })
        .execute();
    }

    return savedCount;
  }

  /**
   * True if park has at least one OPERATING schedule entry (any date).
   * Used to decide whether we trust schedule as source of truth for UNKNOWN→OPERATING inference.
   */
  async hasOperatingSchedule(parkId: string): Promise<boolean> {
    const count = await this.scheduleRepository.count({
      where: {
        parkId,
        scheduleType: ScheduleType.OPERATING,
        attractionId: IsNull(),
      },
    });
    return count > 0;
  }

  /**
   * Returns the min/max OPERATING dates for a park (YYYY-MM-DD strings in park timezone).
   * Used by the calendar to infer CLOSED for dates between operating ranges that have no schedule entry.
   */
  async getOperatingDateRange(
    parkId: string,
    timezone: string,
  ): Promise<{ minDate: string | null; maxDate: string | null }> {
    const result = await this.scheduleRepository
      .createQueryBuilder("schedule")
      .select("MIN(schedule.date)", "minDate")
      .addSelect("MAX(schedule.date)", "maxDate")
      .where("schedule.parkId = :parkId", { parkId })
      .andWhere("schedule.scheduleType = :type", {
        type: ScheduleType.OPERATING,
      })
      .andWhere("schedule.attractionId IS NULL") // Only park-level schedules
      .getRawOne<{
        minDate: string | Date | null;
        maxDate: string | Date | null;
      }>();

    const fmt = (v: string | Date | null | undefined): string | null => {
      if (!v) return null;
      if (typeof v === "string") return v; // already YYYY-MM-DD from PG date column
      return formatInParkTimezone(v, timezone);
    };

    if (!result) {
      return { minDate: null, maxDate: null };
    }

    return {
      minDate: fmt(result.minDate),
      maxDate: fmt(result.maxDate),
    };
  }

  /**
   * Batch version of hasOperatingSchedule check.
   */
  async getBatchHasOperatingSchedule(
    parkIds: string[],
  ): Promise<Map<string, boolean>> {
    if (parkIds.length === 0) return new Map();

    const results = await this.scheduleRepository.manager.query(
      `
      SELECT "parkId", COUNT(*) > 0 as "hasSchedule"
      FROM schedule_entries
      WHERE "parkId" = ANY($1)
        AND "scheduleType" = 'OPERATING'
        AND "attractionId" IS NULL
      GROUP BY "parkId"
    `,
      [parkIds],
    );

    const map = new Map<string, boolean>();
    parkIds.forEach((id) => map.set(id, false)); // Default to false
    results.forEach((r: any) => map.set(r.parkId, r.hasSchedule));
    return map;
  }

  /**
   * Checks if a park has a history of seasonal closures (gaps > 21 days between OPERATING entries).
   * Optimized using window functions for significantly better performance (from ~70ms to ~0.5ms).
   */
  async isParkSeasonal(parkId: string): Promise<boolean> {
    const cacheKey = `park:isSeasonal:${parkId}`;
    const cached = await this.redis.get(cacheKey);
    if (cached !== null) return cached === "1";

    const result = await this.scheduleRepository.manager.query(
      `
      SELECT EXISTS (
        SELECT 1 
        FROM (
          SELECT date, LEAD(date) OVER (ORDER BY date) as next_date
          FROM schedule_entries
          WHERE "parkId" = $1 
            AND "scheduleType" = 'OPERATING'
            AND "attractionId" IS NULL
        ) t
        WHERE next_date - date > 21
      ) as "is_seasonal"
    `,
      [parkId],
    );

    const isSeasonal = result[0]?.is_seasonal === true;
    await this.redis.set(cacheKey, isSeasonal ? "1" : "0", "EX", 86400); // 24h
    return isSeasonal;
  }

  /**
   * Derives historical opening hours from ride activity.
   * Logic:
   * - Opening: First 15min window where >= 10% of attractions (min 2, max 10) show activity, rounded down.
   * - Closing: Last 15min window with activity, rounded up.
   */
  async getDerivedHistoricalHours(
    parkId: string,
    fromDate: string,
    toDate: string,
    timezone: string,
  ): Promise<Map<string, { openingTime: string; closingTime: string }>> {
    const results = await this.scheduleRepository.manager.query(
      `
      WITH park_rides AS (
        SELECT id 
        FROM attractions 
        WHERE "parkId" = $1
          AND name NOT ILIKE '%bar%'
          AND name NOT ILIKE '%snack%'
          AND name NOT ILIKE '%corner%'
          AND name NOT ILIKE '%restaurant%'
          AND name NOT ILIKE '%shop%'
          AND name NOT ILIKE '%cafe%'
          AND name NOT ILIKE '%hire%'
      ),
      park_info AS (
        SELECT COUNT(*) as total_attr FROM park_rides
      ),
      windowed_activity AS (
        SELECT 
          (q.timestamp AT TIME ZONE $4)::date as "date",
          date_trunc('hour', q.timestamp AT TIME ZONE $4) + (date_part('minute', q.timestamp AT TIME ZONE $4)::int / 15 * interval '15 min') as "window",
          COUNT(DISTINCT q."attractionId") FILTER (WHERE q."waitTime" >= 5) as "active_count"
        FROM queue_data q
        WHERE q."attractionId" IN (SELECT id FROM park_rides)
          AND q.timestamp >= ($2::date AT TIME ZONE $4)
          AND q.timestamp <= ($3::date AT TIME ZONE $4 + INTERVAL '1 day')
        GROUP BY 1, 2
      ),
      daily_bounds AS (
        SELECT 
          "date",
          MIN("window") FILTER (WHERE "active_count" >= LEAST(10, GREATEST(2, (SELECT total_attr FROM park_info) * 0.10))) as "first_window",
          MAX("window") FILTER (WHERE "active_count" >= LEAST(10, GREATEST(2, (SELECT total_attr FROM park_info) * 0.10))) as "last_window"
        FROM windowed_activity
        GROUP BY 1
      )
      SELECT 
        "date",
        date_trunc('hour', "first_window") as "derived_open",
        date_trunc('hour', "last_window" + INTERVAL '59 minutes') as "derived_close"
      FROM daily_bounds
      WHERE "first_window" IS NOT NULL AND "last_window" IS NOT NULL
    `,
      [parkId, fromDate, toDate, timezone],
    );

    const map = new Map<string, { openingTime: string; closingTime: string }>();
    results.forEach((r: any) => {
      const dStr = r.date.toISOString().split("T")[0];
      map.set(dStr, {
        openingTime: r.derived_open.toISOString(),
        closingTime: r.derived_close.toISOString(),
      });
    });
    return map;
  }
  /**
   * Fills missing schedule entries with CLOSED or UNKNOWN and holiday/bridge metadata.
   *
   * Gap classification (CLOSED vs UNKNOWN):
   * - CLOSED: Day has no schedule but there is at least one OPERATING day before AND after
   *   (in our stored schedule). So we know the park was closed that day (e.g. mid-week closure).
   * - UNKNOWN: Day has no schedule and we have no OPERATING at or after it, or no OPERATING
   *   before it. So either "before season / before we have data" or "after last known schedule".
   *   Also: if the park has no OPERATING entries at all, all gaps stay UNKNOWN.
   *
   * Demotion: Gap-fill CLOSED (no opening/closing times) that is now after the last OPERATING
   * date gets demoted to UNKNOWN — e.g. when the API removes future OPERATING entries.
   *
   * This allows the calendar to show "Closed" vs "Opening hours not yet available" correctly.
   */
  async fillScheduleGaps(
    parkId: string,
    lookAheadDays = 182,
    lookBackDays = 182,
  ): Promise<number> {
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "countryCode", "regionCode", "timezone"],
    });

    if (!park?.countryCode) return 0;

    // Clean up duplicates before gap-filling to prevent conflicts
    // (e.g. when parallel schedule syncs create duplicate entries)
    await this.cleanupDuplicateScheduleEntriesForPark(parkId);

    // Range: (today - lookBackDays) through (today + lookAheadDays) in PARK timezone.
    // Use noon-UTC dates for arithmetic to avoid DST boundary issues.
    const todayStr = getCurrentDateInTimezone(park.timezone);
    const todayNoon = new Date(`${todayStr}T12:00:00Z`);
    const startStr = subDays(todayNoon, lookBackDays)
      .toISOString()
      .slice(0, 10);
    const endStr = addDays(todayNoon, lookAheadDays).toISOString().slice(0, 10);

    // 0. Min/max OPERATING dates for this park (any time) to classify gaps as CLOSED vs UNKNOWN
    const operatingRange = await this.getOperatingDateRange(
      parkId,
      park.timezone,
    );
    const minOpStr = operatingRange.minDate;
    const maxOpStr = operatingRange.maxDate;

    const isGapClosed = (dateStr: string): boolean => {
      if (!minOpStr || !maxOpStr) return false; // no OPERATING at all → UNKNOWN
      return dateStr > minOpStr && dateStr < maxOpStr; // strictly between
    };

    // 1. Fetch existing entries (use date strings for range to avoid TZ issues in query)
    const existingEntries = await this.scheduleRepository
      .createQueryBuilder("schedule")
      .where("schedule.parkId = :parkId", { parkId })
      .andWhere("schedule.date >= :startDate", { startDate: startStr })
      .andWhere("schedule.date <= :endDate", { endDate: endStr })
      .getMany();

    const existingDates = new Set(
      existingEntries.map((e) =>
        formatInParkTimezone(
          e.date instanceof Date ? e.date : new Date(e.date),
          park.timezone,
        ),
      ),
    );

    // 2. Fetch Holidays
    // Extend range by 1 day for bridge day detection
    const holidayStartStr = subDays(new Date(`${startStr}T12:00:00Z`), 1)
      .toISOString()
      .slice(0, 10);
    const holidayEndStr = addDays(new Date(`${endStr}T12:00:00Z`), 1)
      .toISOString()
      .slice(0, 10);

    const holidays = await this.holidaysService.getHolidays(
      park.countryCode,
      holidayStartStr,
      holidayEndStr,
    );

    // Map Holidays by Date (with type information for bridge day logic)
    const holidayMap = new Map<string, string | HolidayEntry>();
    // Normalize region codes for consistent comparison
    const normalizedParkRegion = normalizeRegionCode(park.regionCode);

    for (const h of holidays) {
      // Match: Nationwide holidays OR region matches (using normalized codes)
      const normalizedHolidayRegion = normalizeRegionCode(h.region);
      if (
        h.isNationwide ||
        (normalizedParkRegion &&
          normalizedHolidayRegion === normalizedParkRegion)
      ) {
        // Use park's timezone to determine the holiday date string
        // Normalize to noon UTC to prevent timezone shifts (YYYY-MM-DD from DB)
        const normalizedDate = new Date(h.date);
        normalizedDate.setUTCHours(12, 0, 0, 0);
        const d = formatInParkTimezone(normalizedDate, park.timezone);

        // If multiple holidays on same day, prefer public holidays over school holidays
        const existing = holidayMap.get(d);
        const hType = h.holidayType;

        if (!existing) {
          // Store as HolidayEntry to preserve type information for bridge day logic
          holidayMap.set(d, {
            name: h.localName || h.name || "",
            type: hType,
            allTypes: [hType],
          });
        } else {
          if (typeof existing !== "string") {
            // Aggregate types
            if (!existing.allTypes) existing.allTypes = [existing.type];
            if (!existing.allTypes.includes(hType)) {
              existing.allTypes.push(hType);
            }

            // Prioritize public holidays for the main entry
            if (hType === "public" || hType === "bank") {
              existing.name = h.localName || h.name || "";
              existing.type = hType;
            }
          }
        }
      }
    }

    let filledCount = 0;

    // 3. Batch collectors for INSERT/UPDATE operations (performance optimization)
    const entriesToInsert: Partial<ScheduleEntry>[] = [];
    const holidayUpdates: Array<{ id: string; fields: any }> = [];
    const statusPromotions: string[] = []; // IDs to promote UNKNOWN → CLOSED
    const statusDemotions: string[] = []; // IDs to demote gap-filled CLOSED → UNKNOWN

    // 4. Iterate all days in range using date strings (avoids DST/timezone issues entirely)
    let dateStr = startStr;
    while (dateStr <= endStr) {
      // Noon-UTC date for this dateStr (DST-safe: noon never hits DST boundary)
      const noonUtc = new Date(`${dateStr}T12:00:00Z`);

      const holidayInfo = calculateHolidayInfo(
        noonUtc,
        holidayMap,
        park.timezone,
      );

      // If no entry exists for this date, collect it for batch insert
      if (!existingDates.has(dateStr)) {
        const scheduleType = isGapClosed(dateStr)
          ? ScheduleType.CLOSED
          : ScheduleType.UNKNOWN;

        entriesToInsert.push({
          parkId,
          date: noonUtc, // Noon UTC → correct date regardless of system/PG timezone
          scheduleType,
          description: "Gap-filled", // Distinguishes from API-provided entries (prevents wrong demotion)
          isHoliday: holidayInfo.isHoliday,
          holidayName: holidayInfo.holidayName,
          isBridgeDay: holidayInfo.isBridgeDay,
          openingTime: null,
          closingTime: null,
        });

        existingDates.add(dateStr); // Prevent duplicate within same run
        filledCount++;
      } else {
        // Entry exists: collect updates for batch processing
        const existing = existingEntries.find((e) => {
          const eDateStr = formatInParkTimezone(
            e.date instanceof Date ? e.date : new Date(e.date),
            park.timezone,
          );
          return eDateStr === dateStr;
        });

        if (!existing) {
          // Advance to next day
          dateStr = addDays(noonUtc, 1).toISOString().slice(0, 10);
          continue;
        }

        const holidayChanged =
          existing.isHoliday !== holidayInfo.isHoliday ||
          existing.holidayName !== holidayInfo.holidayName ||
          existing.isBridgeDay !== holidayInfo.isBridgeDay;
        const shouldBeClosed =
          existing.scheduleType === ScheduleType.UNKNOWN &&
          isGapClosed(dateStr);
        const shouldBeUnknown =
          existing.scheduleType === ScheduleType.CLOSED &&
          existing.description === "Gap-filled" && // Only demote gap-fill, never API-provided CLOSED
          maxOpStr !== null &&
          dateStr > maxOpStr;

        if (holidayChanged || shouldBeClosed || shouldBeUnknown) {
          // Collect updates instead of executing immediately
          if (shouldBeClosed) {
            statusPromotions.push(existing.id);
          } else if (shouldBeUnknown) {
            statusDemotions.push(existing.id);
          } else if (holidayChanged) {
            holidayUpdates.push({
              id: existing.id,
              fields: {
                isHoliday: holidayInfo.isHoliday,
                holidayName: holidayInfo.holidayName,
                isBridgeDay: holidayInfo.isBridgeDay,
              },
            });
          }
          filledCount++;
        }
      }

      // Advance to next day (noon UTC + 1 day → always correct, no DST issues)
      dateStr = addDays(noonUtc, 1).toISOString().slice(0, 10);
    }

    // 5. Execute batch operations (reduces ~364 queries to ~5 queries, 98.6% reduction)

    // Batch INSERT for new gap-filled entries
    if (entriesToInsert.length > 0) {
      await this.scheduleRepository
        .createQueryBuilder()
        .insert()
        .into(ScheduleEntry)
        .values(entriesToInsert)
        .execute();
    }

    // Batch UPDATE for UNKNOWN → CLOSED promotions
    if (statusPromotions.length > 0) {
      await this.scheduleRepository
        .createQueryBuilder()
        .update(ScheduleEntry)
        .set({
          scheduleType: ScheduleType.CLOSED,
          description: "Gap-filled", // Mark so we can safely demote later if maxOp shrinks
        })
        .whereInIds(statusPromotions)
        .execute();
    }

    // Batch UPDATE for gap-filled CLOSED → UNKNOWN demotions
    if (statusDemotions.length > 0) {
      await this.scheduleRepository
        .createQueryBuilder()
        .update(ScheduleEntry)
        .set({ scheduleType: ScheduleType.UNKNOWN })
        .whereInIds(statusDemotions)
        .execute();
    }

    // Individual UPDATEs for holiday changes (fields may differ per entry)
    for (const update of holidayUpdates) {
      await this.scheduleRepository.update(update.id, update.fields);
    }

    if (filledCount > 0) {
      await this.invalidateScheduleCache(parkId);
      this.logger.debug(
        `Filled or updated ${filledCount} schedule entries for Park ${parkId}`,
      );
    }
    return filledCount;
  }

  /**
   * Refreshes holiday and bridge day metadata for ALL parks
   */
  async fillAllParksGaps(): Promise<number> {
    this.logger.log("🔄 Starting gap filling for ALL parks...");

    // Clean up duplicate schedule entries before filling gaps
    await this.cleanupDuplicateScheduleEntries();

    const parks = await this.parkRepository.find();
    let totalUpdated = 0;

    for (const park of parks) {
      try {
        const count = await this.fillScheduleGaps(park.id);
        totalUpdated += count;
      } catch (error) {
        this.logger.error(`Failed gap filling for ${park.name}: ${error}`);
      }
    }

    this.logger.log(
      `✅ Completed gap filling. Total entries updated: ${totalUpdated}`,
    );
    return totalUpdated;
  }

  /**
   * Full schedule deduplication: handles ALL schedule entries, not just gap-filled ones.
   *
   * Phase 1 — Same-type duplicates:
   *   Multiple entries with identical (parkId, date, scheduleType).
   *   Keeps the most recent (by updatedAt), deletes the rest.
   *
   * Phase 2 — Cross-type conflicts:
   *   Multiple entries for the same (parkId, date) with different scheduleTypes.
   *   Priority: OPERATING > API-provided CLOSED > Gap-filled CLOSED > UNKNOWN.
   *   When a higher-priority entry exists, lower-priority entries are removed.
   */
  async cleanupDuplicateScheduleEntries(): Promise<number> {
    let deletedCount = 0;

    // ── Phase 1: same-type duplicates ──────────────────────────────────
    // Optimized: Single SQL query with window function (instead of N+1 queries)
    const deletedSameType = await this.scheduleRepository.query(`
      DELETE FROM schedule_entries
      WHERE id IN (
        SELECT id FROM (
          SELECT id,
                 ROW_NUMBER() OVER (
                   PARTITION BY "parkId", date, "scheduleType"
                   ORDER BY "updatedAt" DESC
                 ) as rn
          FROM schedule_entries
        ) sub
        WHERE rn > 1
      )
    `);
    deletedCount += deletedSameType[1] || 0;

    // ── Phase 2: cross-type conflicts ──────────────────────────────────
    // Optimized: Single SQL query with CTE + priority logic (instead of N+1 queries)
    const deletedCrossType = await this.scheduleRepository.query(`
      WITH ranked AS (
        SELECT id,
               ROW_NUMBER() OVER (
                 PARTITION BY "parkId", date
                 ORDER BY
                   CASE
                     WHEN "scheduleType" = 'OPERATING' THEN 0
                     WHEN "scheduleType" = 'CLOSED' AND description != 'Gap-filled' THEN 1
                     WHEN "scheduleType" = 'CLOSED' THEN 2
                     WHEN "scheduleType" = 'UNKNOWN' THEN 3
                     ELSE 4
                   END,
                   "updatedAt" DESC
               ) as rn
        FROM schedule_entries
        WHERE ("parkId", date) IN (
          SELECT "parkId", date
          FROM schedule_entries
          GROUP BY "parkId", date
          HAVING COUNT(DISTINCT "scheduleType") > 1
        )
      )
      DELETE FROM schedule_entries
      WHERE id IN (SELECT id FROM ranked WHERE rn > 1)
    `);
    deletedCount += deletedCrossType[1] || 0;

    if (deletedCount > 0) {
      this.logger.log(
        `🧹 Cleaned up ${deletedCount} duplicate schedule entries (optimized SQL).`,
      );
    } else {
      this.logger.debug("No duplicate schedule entries found.");
    }
    return deletedCount;
  }

  /**
   * Cleanup duplicate schedule entries for a single park (optimized for per-park operations).
   *
   * This is called by fillScheduleGaps to prevent duplicates from accumulating between
   * daily global cleanups. Uses the same priority logic as cleanupDuplicateScheduleEntries
   * but scoped to a single park for better performance.
   *
   * Phase 1: Remove same-type duplicates (keeps most recent by updatedAt)
   * Phase 2: Remove cross-type conflicts (priority: OPERATING > API-CLOSED > Gap-CLOSED > UNKNOWN)
   */
  private async cleanupDuplicateScheduleEntriesForPark(
    parkId: string,
  ): Promise<number> {
    let deletedCount = 0;

    // Phase 1: Same-type duplicates for this park
    const deletedSameType = await this.scheduleRepository.query(
      `
      DELETE FROM schedule_entries
      WHERE id IN (
        SELECT id FROM (
          SELECT id,
                 ROW_NUMBER() OVER (
                   PARTITION BY "parkId", date, "scheduleType"
                   ORDER BY "updatedAt" DESC
                 ) as rn
          FROM schedule_entries
          WHERE "parkId" = $1::uuid
        ) sub
        WHERE rn > 1
      )
    `,
      [parkId],
    );
    deletedCount += deletedSameType[1] || 0;

    // Phase 2: Cross-type conflicts for this park
    const deletedCrossType = await this.scheduleRepository.query(
      `
      WITH ranked AS (
        SELECT id,
               ROW_NUMBER() OVER (
                 PARTITION BY "parkId", date
                 ORDER BY
                   CASE
                     WHEN "scheduleType" = 'OPERATING' THEN 0
                     WHEN "scheduleType" = 'CLOSED' AND description != 'Gap-filled' THEN 1
                     WHEN "scheduleType" = 'CLOSED' THEN 2
                     WHEN "scheduleType" = 'UNKNOWN' THEN 3
                     ELSE 4
                   END,
                   "updatedAt" DESC
               ) as rn
        FROM schedule_entries
        WHERE "parkId" = $1::uuid
          AND ("parkId", date) IN (
            SELECT "parkId", date
            FROM schedule_entries
            WHERE "parkId" = $1::uuid
            GROUP BY "parkId", date
            HAVING COUNT(DISTINCT "scheduleType") > 1
          )
      )
      DELETE FROM schedule_entries
      WHERE id IN (SELECT id FROM ranked WHERE rn > 1)
    `,
      [parkId],
    );
    deletedCount += deletedCrossType[1] || 0;

    if (deletedCount > 0) {
      this.logger.debug(
        `🧹 Cleaned up ${deletedCount} duplicate schedule entries for park ${parkId}`,
      );
    }

    return deletedCount;
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
      // Remove strict missing data check to allow re-geocoding/verification of parks that have data (e.g. from Wiki)
      // but haven't been processed by our geocoder yet (geocodingAttemptedAt IS NULL).
      // This is crucial for applying Metro Mappings (e.g. fixing Bay Lake -> Orlando).
      .andWhere(
        "(park.geocodingAttemptedAt IS NULL OR (park.metadataRetryCount < 3 AND (park.countryCode IS NULL OR park.regionCode IS NULL OR park.city IS NULL OR park.country IS NULL)))",
      )
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
   * Also generates corresponding slugs for continent, country, and city.
   * Also marks the park as attempted.
   *
   * IMPORTANT: Only updates fields that are currently NULL.
   * This allows manual data entry without risk of being overwritten.
   *
   * @param parkId - Park ID (UUID)
   * @param geodata - Geographic data (continent, country, city)
   */
  async updateGeodata(parkId: string, geodata: Partial<Park>): Promise<void> {
    const updates: Partial<Park> = {
      ...geodata,
      geocodingAttemptedAt: new Date(),
    };

    // Generate geographic slugs from their respective fields
    if (geodata.continent) {
      updates.continentSlug = generateSlug(geodata.continent);
    }
    if (geodata.country) {
      updates.countrySlug = generateSlug(geodata.country);
    }
    if (geodata.city) {
      updates.citySlug = generateSlug(geodata.city);
    }

    await this.parkRepository.update(parkId, updates);
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
   * Get schedule for a single calendar date (YYYY-MM-DD).
   * Use this for "today" / "tomorrow" to avoid timezone ambiguity:
   * schedule.date is a DATE column; comparing with a string is timezone-safe.
   */
  async getScheduleForDate(
    parkId: string,
    dateStr: string,
  ): Promise<ScheduleEntry[]> {
    return this.scheduleRepository
      .createQueryBuilder("schedule")
      .where("schedule.parkId = :parkId", { parkId })
      .andWhere("schedule.date = :dateStr", { dateStr })
      .orderBy("schedule.scheduleType", "ASC")
      .getMany();
  }

  /**
   * Get today's schedule for a park.
   * "Today" is the current calendar day in the park's timezone (00:00–23:59 park time).
   * Uses date-string equality so results are independent of DB session timezone.
   *
   * @param parkId - Park ID (UUID)
   * @returns Today's schedule entries
   */
  async getTodaySchedule(
    parkId: string,
    timezone?: string,
  ): Promise<ScheduleEntry[]> {
    let tz = timezone;
    if (!tz) {
      const park = await this.parkRepository.findOne({
        where: { id: parkId },
        select: ["id", "timezone"],
      });
      if (!park) return [];
      tz = park.timezone ?? "UTC";
    }

    const todayStr = getCurrentDateInTimezone(tz);
    const cacheKey = `schedule:today:${parkId}:${todayStr}`;
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

    const schedule = await this.getScheduleForDate(parkId, todayStr);

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
   * Get next scheduled opening for a park
   *
   * Finds the next day when the park will be operating.
   * Useful for parks in off-season to show when they will next open.
   *
   * @param parkId - Park ID (UUID)
   * @returns Next operating schedule entry or null if none found
   */
  /**
   * Get next scheduled opening for a park.
   * "Tomorrow" is the next calendar day in the park's timezone; query uses date string for consistency.
   */
  async getNextSchedule(parkId: string): Promise<ScheduleEntry | null> {
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "timezone"],
    });

    if (!park) return null;

    const tomorrowStr = getTomorrowDateInTimezone(park.timezone || "UTC");
    const cacheKey = `schedule:next:${parkId}:${tomorrowStr}`;
    const cached = await this.redis.get(cacheKey);

    if (cached) {
      const parsed = JSON.parse(cached);
      if (!parsed) return null;
      return {
        ...parsed,
        date: new Date(parsed.date),
        openingTime: parsed.openingTime ? new Date(parsed.openingTime) : null,
        closingTime: parsed.closingTime ? new Date(parsed.closingTime) : null,
      } as ScheduleEntry;
    }

    // Look ahead: from park's "tomorrow" up to 365 days (use date string for lower bound)
    const lookAheadDate = new Date(tomorrowStr + "T12:00:00.000Z");
    lookAheadDate.setDate(lookAheadDate.getDate() + 365);

    const nextSchedule = await this.scheduleRepository
      .createQueryBuilder("schedule")
      .where("schedule.parkId = :parkId", { parkId })
      .andWhere("schedule.date >= :tomorrowStr", { tomorrowStr })
      .andWhere("schedule.date <= :lookAheadDate", { lookAheadDate })
      .andWhere("schedule.scheduleType = :type", {
        type: ScheduleType.OPERATING,
      })
      .andWhere("schedule.openingTime IS NOT NULL")
      .andWhere("schedule.closingTime IS NOT NULL")
      .orderBy("schedule.date", "ASC")
      .limit(1)
      .getOne();

    // Cache result (1 hour TTL)
    await this.redis.set(
      cacheKey,
      JSON.stringify(nextSchedule),
      "EX",
      this.TTL_SCHEDULE,
    );

    return nextSchedule;
  }

  /**
   * Batch fetch schedules for multiple parks
   * Returns both today's schedule and next schedule for all parks
   * Optimized to avoid N+1 queries by batching database queries
   *
   * @param parkIds - Array of park IDs
   * @returns Object with today and next schedule maps
   */
  async getBatchSchedules(parkIds: string[]): Promise<{
    today: Map<string, ScheduleEntry[]>;
    next: Map<string, ScheduleEntry | null>;
  }> {
    const todayMap = new Map<string, ScheduleEntry[]>();
    const nextMap = new Map<string, ScheduleEntry | null>();

    if (parkIds.length === 0) {
      return { today: todayMap, next: nextMap };
    }

    // Fetch parks with timezones
    const parks = await this.parkRepository.find({
      where: { id: In(parkIds) },
      select: ["id", "timezone"],
    });

    const timezoneMap = new Map<string, string>();
    for (const park of parks) {
      timezoneMap.set(park.id, park.timezone || "UTC");
    }

    // Try to get from cache first (today/tomorrow in each park's timezone)
    const cacheKeysToday = parks.map(
      (p) =>
        `schedule:today:${p.id}:${getCurrentDateInTimezone(p.timezone || "UTC")}`,
    );
    const cacheKeysNext = parks.map(
      (p) =>
        `schedule:next:${p.id}:${getTomorrowDateInTimezone(p.timezone || "UTC")}`,
    );

    const cachedToday = await this.redis.mget(...cacheKeysToday);
    const cachedNext = await this.redis.mget(...cacheKeysNext);

    // Process cached results and identify which parks need DB queries
    const parksNeedingTodayQuery: string[] = [];
    const parksNeedingNextQuery: string[] = [];

    for (let i = 0; i < parks.length; i++) {
      const park = parks[i];
      const parkId = park.id;

      // Process today's schedule
      const cachedTodayValue = cachedToday[i];
      if (cachedTodayValue) {
        try {
          const parsed = JSON.parse(cachedTodayValue) as any[];
          todayMap.set(
            parkId,
            parsed.map((entry) => ({
              ...entry,
              date: new Date(entry.date),
              openingTime: entry.openingTime
                ? new Date(entry.openingTime)
                : null,
              closingTime: entry.closingTime
                ? new Date(entry.closingTime)
                : null,
            })) as ScheduleEntry[],
          );
        } catch {
          parksNeedingTodayQuery.push(parkId);
        }
      } else {
        parksNeedingTodayQuery.push(parkId);
      }

      // Process next schedule
      const cachedNextValue = cachedNext[i];
      if (cachedNextValue) {
        try {
          const parsed = JSON.parse(cachedNextValue);
          if (parsed) {
            nextMap.set(parkId, {
              ...parsed,
              date: new Date(parsed.date),
              openingTime: parsed.openingTime
                ? new Date(parsed.openingTime)
                : null,
              closingTime: parsed.closingTime
                ? new Date(parsed.closingTime)
                : null,
            } as ScheduleEntry);
          } else {
            nextMap.set(parkId, null);
          }
        } catch {
          parksNeedingNextQuery.push(parkId);
        }
      } else {
        parksNeedingNextQuery.push(parkId);
      }
    }

    // Batch fetch today's schedules for parks not in cache
    if (parksNeedingTodayQuery.length > 0) {
      const parksForToday = parks.filter((p) =>
        parksNeedingTodayQuery.includes(p.id),
      );

      // Group by timezone for efficient querying
      const timezoneGroups = new Map<string, string[]>();
      for (const park of parksForToday) {
        const tz = park.timezone || "UTC";
        if (!timezoneGroups.has(tz)) {
          timezoneGroups.set(tz, []);
        }
        timezoneGroups.get(tz)!.push(park.id);
      }

      const todayPromises = Array.from(timezoneGroups.entries()).map(
        async ([timezone, ids]) => {
          const todayStr = getCurrentDateInTimezone(timezone);

          // Query by date string (schedule.date is DATE); avoids session-timezone ambiguity
          const schedules = await this.scheduleRepository
            .createQueryBuilder("schedule")
            .where("schedule.parkId IN (:...parkIds)", { parkIds: ids })
            .andWhere("schedule.date = :todayStr", { todayStr })
            .orderBy("schedule.parkId", "ASC")
            .addOrderBy("schedule.date", "ASC")
            .addOrderBy("schedule.scheduleType", "ASC")
            .getMany();

          // Group by parkId
          const scheduleMap = new Map<string, ScheduleEntry[]>();
          for (const schedule of schedules) {
            if (!scheduleMap.has(schedule.parkId)) {
              scheduleMap.set(schedule.parkId, []);
            }
            scheduleMap.get(schedule.parkId)!.push(schedule);
          }

          // Cache results
          for (const park of parksForToday.filter((p) => ids.includes(p.id))) {
            const parkSchedules = scheduleMap.get(park.id) || [];
            const cacheKey = `schedule:today:${park.id}:${todayStr}`;
            await this.redis.set(
              cacheKey,
              JSON.stringify(parkSchedules),
              "EX",
              this.TTL_SCHEDULE,
            );
            todayMap.set(park.id, parkSchedules);
          }

          return scheduleMap;
        },
      );

      await Promise.all(todayPromises);
    }

    // Batch fetch next schedules for parks not in cache
    if (parksNeedingNextQuery.length > 0) {
      const parksForNext = parks.filter((p) =>
        parksNeedingNextQuery.includes(p.id),
      );

      // Earliest "tomorrow" among parks (for query start); lookAhead = +365 days
      const tomorrowStrs = parksForNext.map((p) =>
        getTomorrowDateInTimezone(p.timezone || "UTC"),
      );
      const minTomorrowStr = tomorrowStrs.sort()[0];
      const minTomorrowDate = new Date(minTomorrowStr + "T12:00:00Z");
      const lookAheadDate = new Date(minTomorrowDate);
      lookAheadDate.setDate(lookAheadDate.getDate() + 365);

      // Fetch all next schedules in one query
      const nextSchedules = await this.scheduleRepository
        .createQueryBuilder("schedule")
        .where("schedule.parkId IN (:...parkIds)", {
          parkIds: parksNeedingNextQuery,
        })
        .andWhere("schedule.date >= :tomorrow", {
          tomorrow: minTomorrowStr,
        })
        .andWhere("schedule.date <= :lookAheadDate", { lookAheadDate })
        .andWhere("schedule.scheduleType = :type", {
          type: ScheduleType.OPERATING,
        })
        .andWhere("schedule.openingTime IS NOT NULL")
        .andWhere("schedule.closingTime IS NOT NULL")
        .orderBy("schedule.parkId", "ASC")
        .addOrderBy("schedule.date", "ASC")
        .getMany();

      // Group by parkId and take first (earliest) for each park
      const nextScheduleMap = new Map<string, ScheduleEntry>();
      for (const schedule of nextSchedules) {
        if (!nextScheduleMap.has(schedule.parkId)) {
          nextScheduleMap.set(schedule.parkId, schedule);
        }
      }

      // Cache and set results
      for (const park of parksForNext) {
        const schedule = nextScheduleMap.get(park.id) || null;
        const timezone = park.timezone || "UTC";
        const cacheKey = `schedule:next:${park.id}:${getTomorrowDateInTimezone(timezone)}`;
        await this.redis.set(
          cacheKey,
          JSON.stringify(schedule),
          "EX",
          this.TTL_SCHEDULE,
        );
        nextMap.set(park.id, schedule);
      }
    }

    return { today: todayMap, next: nextMap };
  }

  async getUpcomingSchedule(
    parkId: string,
    days: number = 7,
  ): Promise<ScheduleEntry[]> {
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "timezone"],
    });

    if (!park) return [];

    const tz = park.timezone || "UTC";
    // Range in PARK timezone: start 2 days ago (for late-night hours, cross-timezone), end today + days
    const startDate = getStartOfDayInTimezone(tz);
    const twoDaysAgo = addDays(startDate, -2);
    const endDate = addDays(startDate, days + 1);

    const cacheKey = `schedule:upcoming:${parkId}:${getCurrentDateInTimezone(tz)}:${days}`;
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

    const schedule = await this.getSchedule(parkId, twoDaysAgo, endDate);

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
      relations: ["destination"],
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
      relations: ["destination"],
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
      relations: ["destination"],
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
    const cacheKey = `${continentSlug}:${countrySlug}:${citySlug}:${parkSlug}`;
    const expiry = this.notFoundCache.get(cacheKey);
    if (expiry !== undefined) {
      if (Date.now() < expiry) {
        return null; // known 404 — skip DB query
      }
      this.notFoundCache.delete(cacheKey);
    }

    const park = await this.parkRepository.findOne({
      where: { continentSlug, countrySlug, citySlug, slug: parkSlug },
      relations: ["destination"],
    });

    if (!park) {
      this.notFoundCache.set(cacheKey, Date.now() + this.NOT_FOUND_TTL_MS);
    }

    return park;
  }

  /**
   * Loads attractions, shows, and restaurants onto an already-fetched park entity
   * using 3 parallel queries instead of a single JOIN.
   *
   * TypeORM's `relations` option produces a Cartesian product JOIN — for a park
   * with 200 attractions × 20 shows × 10 restaurants that means 40 000 intermediate
   * rows. This helper avoids that by issuing separate queries in parallel.
   */
  async loadParkRelations(park: Park): Promise<Park> {
    const em = this.parkRepository.manager;
    const [attractions, shows, restaurants] = await Promise.all([
      em.find(Attraction, { where: { parkId: park.id } }),
      em.find(Show, { where: { parkId: park.id } }),
      em.find(Restaurant, { where: { parkId: park.id } }),
    ]);
    park.attractions = attractions;
    park.shows = shows;
    park.restaurants = restaurants;
    return park;
  }

  /**
   * Finds a park by geographic path with all relations (attractions, shows, restaurants).
   * Use only for the main park endpoint that needs full relation data.
   */
  async findByGeographicPathWithRelations(
    continentSlug: string,
    countrySlug: string,
    citySlug: string,
    parkSlug: string,
  ): Promise<Park | null> {
    const park = await this.findByGeographicPath(
      continentSlug,
      countrySlug,
      citySlug,
      parkSlug,
    );
    if (!park) return null;
    return this.loadParkRelations(park);
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

    const now = new Date();
    const parkDateStr = getCurrentDateInTimezone(park.timezone);

    // Query schedule for today in park's timezone
    const todaySchedule = await this.scheduleRepository.findOne({
      where: {
        parkId,
        date: parkDateStr as any,
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

    const parkDateStr = getCurrentDateInTimezone(park.timezone);

    // Query schedule for today in park's timezone
    const todaySchedule = await this.scheduleRepository.findOne({
      where: {
        parkId,
        date: parkDateStr as any,
      },
    });

    // If we have a schedule entry, trust OPERATING/CLOSED explicitly.
    // UNKNOWN means the source has no data for today — treat like "no schedule":
    // default to true so we still generate predictions for potentially-open parks.
    if (todaySchedule) {
      if (todaySchedule.scheduleType === "OPERATING") return true;
      if (todaySchedule.scheduleType === "CLOSED") return false;
      // UNKNOWN → fall through to default below
    }

    // No schedule or UNKNOWN: check today's ride data to decide.
    // Uses park-local midnight as the start (not a rolling 24h) to avoid
    // yesterday's operating data leaking into today's closed determination.
    // ≥3 rides with data AND ≥25% with waitTime ≥ 10 → likely open.
    // ≥3 rides with data AND 0% with waitTime ≥ 10 → likely closed.
    // Too little data → default true (conservative, we don't know).
    const todayStart = getStartOfDayInTimezone(park.timezone);
    const rideStats: {
      withData: string;
      operating5min: string;
      operating10min: string;
    }[] = await this.parkRepository.manager.query(
      `
        SELECT
          COUNT(*) as "withData",
          SUM(CASE WHEN q.status = 'OPERATING' AND q."waitTime" >= 5 THEN 1 ELSE 0 END) as "operating5min",
          SUM(CASE WHEN q.status = 'OPERATING' AND q."waitTime" >= 10 THEN 1 ELSE 0 END) as "operating10min"
        FROM attractions a
        JOIN LATERAL (
          SELECT status, "waitTime"
          FROM queue_data qd
          WHERE qd."attractionId" = a.id
            AND qd.timestamp >= $2
          ORDER BY timestamp DESC
          LIMIT 1
        ) q ON true
        WHERE a."parkId" = $1::uuid
      `,
      [parkId, todayStart],
    );

    if (rideStats.length > 0) {
      const withData = parseInt(rideStats[0].withData, 10);
      const operating5min = parseInt(rideStats[0].operating5min, 10);
      const operating10min = parseInt(rideStats[0].operating10min, 10);
      if (withData >= 3) {
        return (
          operating10min / withData >= 0.25 || operating5min / withData >= 0.5
        );
      }
    }
    return true;
  }

  /**
   * Checks if any rides in the park have been active in the last 2 hours.
   * Used as a safety net to ensure active parks get predictions even if
   * the schedule says CLOSED.
   */
  async hasRecentRideActivity(parkId: string): Promise<boolean> {
    const twoHoursAgo = new Date(Date.now() - 2 * 60 * 60 * 1000);

    const result = await this.parkRepository.manager.query(
      `
      SELECT COUNT(*) as "activeRides"
      FROM queue_data qd
      INNER JOIN attractions a ON a.id = qd."attractionId"
      WHERE a."parkId" = $1::uuid
        AND qd.timestamp >= $2
        AND qd.status = 'OPERATING'
        AND qd."waitTime" >= 10
    `,
      [parkId, twoHoursAgo],
    );

    const activeRides = parseInt(result[0]?.activeRides || "0", 10);
    // If at least 2 rides are active, we consider the park active
    return activeRides >= 2;
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

    // Heuristic Fallback: For parks not currently OPERATING per schedule, check ride data.
    // Applies ONLY to parks with UNKNOWN or NO schedule data for today.
    // Explicitly CLOSED parks for today trust the schedule entirely.
    const candidateParkIds = parkIds.filter(
      (id) => statusMap.get(id) === "CLOSED",
    );

    if (candidateParkIds.length > 0) {
      // Exclude any park that has an explicit CLOSED schedule entry for today.
      // UNKNOWN rows (common for gap-fills) or missing rows should still allow the ride-based fallback.
      const parksWithClosedScheduleRows: { parkId: string }[] =
        await this.parkRepository.manager.query(
          `SELECT DISTINCT se."parkId"
           FROM schedule_entries se
           WHERE se."parkId" = ANY($1)
             AND se."scheduleType" = 'CLOSED'
             AND se.date = CURRENT_DATE`,
          [candidateParkIds],
        );

      const parksWithClosedSchedule = new Set(
        parksWithClosedScheduleRows.map((r) => r.parkId),
      );
      const parksNeedingFallback = candidateParkIds.filter(
        (id) => !parksWithClosedSchedule.has(id),
      );

      if (parksNeedingFallback.length > 0) {
        const stats: {
          parkId: string;
          withData: string;
          operating5min: string;
          operating10min: string;
        }[] = await this.parkRepository.manager.query(
          `
          SELECT
            p.id as "parkId",
            COUNT(*) as "withData",
            SUM(CASE WHEN q.status = 'OPERATING' AND q."waitTime" >= 5 THEN 1 ELSE 0 END) as "operating5min",
            SUM(CASE WHEN q.status = 'OPERATING' AND q."waitTime" >= 10 THEN 1 ELSE 0 END) as "operating10min"
          FROM parks p
          JOIN attractions a ON a."parkId" = p.id
          JOIN LATERAL (
            SELECT status, "waitTime"
            FROM queue_data qd
            WHERE qd."attractionId" = a.id
              AND qd.timestamp > NOW() - INTERVAL '2 hours'
            ORDER BY timestamp DESC
            LIMIT 1
          ) q ON true
          WHERE p.id = ANY($1)
          GROUP BY p.id
        `,
          [parksNeedingFallback],
        );

        for (const stat of stats) {
          const withData = parseInt(stat.withData, 10);
          const operating5min = parseInt(stat.operating5min, 10);
          const operating10min = parseInt(stat.operating10min, 10);

          // Relaxed heuristic:
          // 1. Classic: ≥3 rides with data AND ≥25% reporting ≥10 min wait
          // 2. Heartbeat: ≥3 rides with data AND ≥50% reporting ANY wait (≥5 min)
          //    (handles "heartbeat" parks that only report 5 mins)
          const isClassicOperating =
            withData >= 3 && operating10min / withData >= 0.25;
          const isHeartbeatOperating =
            withData >= 3 && operating5min / withData >= 0.5;

          if (isClassicOperating || isHeartbeatOperating) {
            statusMap.set(stat.parkId, "OPERATING");
          }
        }
      }
    }

    // ... logic ...
    return statusMap;
  }

  /**
   * Gets all unique country codes relevant for holiday sync
   * Includes countries with parks AND all unique influencing countries
   */
  async getSyncCountryCodes(): Promise<string[]> {
    const parks = await this.parkRepository.find({
      select: ["countryCode", "influencingRegions"],
    });

    const codes = new Set<string>();

    for (const park of parks) {
      if (park.countryCode) {
        codes.add(park.countryCode);
      }

      if (park.influencingRegions) {
        for (const reg of park.influencingRegions) {
          if (reg.countryCode) {
            codes.add(reg.countryCode);
          }
        }
      }
    }

    return [...codes];
  }

  /**
   * Invalidates schedule cache for a park
   */
  async invalidateScheduleCache(parkId: string): Promise<void> {
    const keys = await this.redis.keys(`schedule:*:${parkId}:*`);
    if (keys.length > 0) {
      await this.redis.del(...keys);
    }
  }

  /**
   * Invalidates calendar month cache for a park.
   * Called after schedule sync so stale UNKNOWN months are not served from cache
   * after ThemeParks Wiki publishes new opening hours.
   */
  async invalidateCalendarMonthCache(parkId: string): Promise<void> {
    const keys = await this.redis.keys(`calendar:month:${parkId}:*`);
    if (keys.length > 0) {
      await this.redis.del(...keys);
      this.logger.debug(
        `Cleared ${keys.length} calendar month cache keys for park ${parkId}`,
      );
    }
  }
}
