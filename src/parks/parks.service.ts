import { Inject, Injectable, Logger, forwardRef } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Park } from "./entities/park.entity";
import { ScheduleEntry, ScheduleType } from "./entities/schedule-entry.entity";
import { ThemeParksClient } from "../external-apis/themeparks/themeparks.client";
import { ThemeParksMapper } from "../external-apis/themeparks/themeparks.mapper";
import { DestinationsService } from "../destinations/destinations.service";
import { HolidaysService } from "../holidays/holidays.service";
import { generateSlug, generateUniqueSlug } from "../common/utils/slug.util";
import { normalizeSortDirection } from "../common/utils/query.util";
import { formatInParkTimezone } from "../common/utils/date.util";
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
        let existing = await this.parkRepository.findOne({
          where: { externalId: mappedData.externalId },
        });

        // If not found by externalId, check for potential duplicates by name
        if (!existing) {
          const allParks = await this.parkRepository.find({
            where: { destinationId: destination.id },
          });

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
              // Check if there's a "losing" park by externalId
              const losingPark = await this.parkRepository.findOne({
                where: { externalId: mappedData.externalId },
              });

              if (losingPark && losingPark.id !== duplicate.id) {
                // Silent migration - log only summary

                let totalMigrated = 0;

                // Migrate Shows
                const showCount = await this.parkRepository.manager.query(
                  `UPDATE shows SET "parkId" = $1 WHERE "parkId" = $2`,
                  [duplicate.id, losingPark.id],
                );
                totalMigrated += showCount[1] || 0;

                // Migrate Restaurants
                const restaurantCount = await this.parkRepository.manager.query(
                  `UPDATE restaurants SET "parkId" = $1 WHERE "parkId" = $2`,
                  [duplicate.id, losingPark.id],
                );
                totalMigrated += restaurantCount[1] || 0;

                // Migrate Attractions
                const attractionCount = await this.parkRepository.manager.query(
                  `UPDATE attractions SET "parkId" = $1 WHERE "parkId" = $2`,
                  [duplicate.id, losingPark.id],
                );
                totalMigrated += attractionCount[1] || 0;

                // Check if losing park is now empty
                const remaining = await this.parkRepository.manager.query(
                  `SELECT 
                    (SELECT COUNT(*) FROM shows WHERE "parkId" = $1) as shows,
                    (SELECT COUNT(*) FROM restaurants WHERE "parkId" = $1) as restaurants,
                    (SELECT COUNT(*) FROM attractions WHERE "parkId" = $1) as attractions
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
                      `SELECT id, slug, "queue_times_entity_id", "land_name", "land_external_id" FROM attractions WHERE "parkId" = $1`,
                      [existing.id],
                    );
                  const ghostAttractions =
                    await transactionalEntityManager.query(
                      `SELECT id, slug, "queue_times_entity_id", "land_name", "land_external_id" FROM attractions WHERE "parkId" = $1`,
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
                    `UPDATE shows SET "parkId" = $1 WHERE "parkId" = $2`,
                    [existing.id, ghostPark.id],
                  );

                  // 3. Migrate Restaurants
                  await transactionalEntityManager.query(
                    `UPDATE restaurants SET "parkId" = $1 WHERE "parkId" = $2`,
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
    this.logger.log("🔧 Running Duplicate Park Repair...");

    // Find all Queue-Times IDs that are used by more than one park
    const duplicates = await this.parkRepository.query(`
      SELECT "queue_times_entity_id"
      FROM parks
      WHERE "queue_times_entity_id" IS NOT NULL
      GROUP BY "queue_times_entity_id"
      HAVING COUNT(*) > 1
    `);

    this.logger.log(`Found ${duplicates.length} duplicate sets to repair.`);

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
              `SELECT id, slug, "land_name", "land_external_id" FROM attractions WHERE "parkId" = $1`,
              [primary!.id],
            );
            const ghostAttractions = await transactionalEntityManager.query(
              `SELECT id, slug, "land_name", "land_external_id" FROM attractions WHERE "parkId" = $1`,
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
                   WHERE "attractionId" = $2`,
                  [match.id, ghostAttr.id],
                );

                // 4. Move Wait Time Predictions
                await transactionalEntityManager.query(
                  `UPDATE wait_time_predictions 
                   SET "attractionId" = $1 
                   WHERE "attractionId" = $2`,
                  [match.id, ghostAttr.id],
                );

                // 5. Move Prediction Accuracy Records
                await transactionalEntityManager.query(
                  `UPDATE prediction_accuracy 
                   SET "attractionId" = $1 
                   WHERE "attractionId" = $2`,
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
              `UPDATE shows SET "parkId" = $1 WHERE "parkId" = $2`,
              [primary!.id, ghostPark.id],
            );

            // 3. Migrate Restaurants
            await transactionalEntityManager.query(
              `UPDATE restaurants SET "parkId" = $1 WHERE "parkId" = $2`,
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

    // 1. Fetch Park geo data for holiday checking
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "countryCode", "regionCode", "timezone"],
    });

    // 2. Pre-fetch holidays for the date range (Extended by +/ 1 day for bridge day checks)
    const holidayMap = new Map<string, string>(); // Date -> Name
    if (park?.countryCode) {
      try {
        const dates = scheduleData.map((e) => new Date(e.date).getTime());
        const minDate = new Date(Math.min(...dates));
        const maxDate = new Date(Math.max(...dates));

        // Extend range by 1 day for bridge day detection
        minDate.setDate(minDate.getDate() - 1);
        maxDate.setDate(maxDate.getDate() + 1);

        const holidays = await this.holidaysService.getHolidays(
          park.countryCode,
          minDate,
          maxDate,
        );

        const fullRegion = park.regionCode
          ? `${park.countryCode}-${park.regionCode}`
          : "";

        for (const h of holidays) {
          // Logic mirrors isHoliday: Nationwide OR region matches
          if (h.isNationwide || (park.regionCode && h.region === fullRegion)) {
            // Use park's timezone to determine the holiday date string
            const dateStr = formatInParkTimezone(h.date, park.timezone);
            // If multiple holidays on same day, just picking first or specific one?
            if (!holidayMap.has(dateStr)) {
              holidayMap.set(dateStr, h.localName || h.name || "");
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
      const dateObj = new Date(entry.date);
      const dateStr = formatInParkTimezone(dateObj, park!.timezone);
      const holidayName = holidayMap.get(dateStr) || null;
      const isHoliday = !!holidayName;

      // Check Bridge Day Logic
      // Friday (5) after Thursday Holiday OR Monday (1) before Tuesday Holiday
      let isBridgeDay = false;
      const dayOfWeek = dateObj.getDay();

      if (dayOfWeek === 5) {
        // Friday
        const prevDate = new Date(dateObj);
        prevDate.setDate(dateObj.getDate() - 1);
        const prevDateStr = formatInParkTimezone(prevDate, park!.timezone);
        if (holidayMap.has(prevDateStr)) isBridgeDay = true;
      } else if (dayOfWeek === 1) {
        // Monday
        const nextDate = new Date(dateObj);
        nextDate.setDate(dateObj.getDate() + 1);
        const nextDateStr = formatInParkTimezone(nextDate, park!.timezone);
        if (holidayMap.has(nextDateStr)) isBridgeDay = true;
      }

      const scheduleEntry: Partial<ScheduleEntry> = {
        parkId,
        date: new Date(entry.date),
        scheduleType: entry.type as ScheduleType,
        openingTime: entry.openingTime ? new Date(entry.openingTime) : null,
        closingTime: entry.closingTime ? new Date(entry.closingTime) : null,
        description: entry.description || null,
        purchases: entry.purchases || null,
        isHoliday: isHoliday,
        holidayName: holidayName,
        isBridgeDay: isHoliday ? false : isBridgeDay,
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
        // Update if times, description, or holiday/bridge status changed
        const hasChanges =
          existing.openingTime?.getTime() !==
            scheduleEntry.openingTime?.getTime() ||
          existing.closingTime?.getTime() !==
            scheduleEntry.closingTime?.getTime() ||
          existing.description !== scheduleEntry.description ||
          existing.isHoliday !== scheduleEntry.isHoliday ||
          existing.isBridgeDay !== scheduleEntry.isBridgeDay;

        if (hasChanges) {
          await this.scheduleRepository.update(existing.id, scheduleEntry);
          savedCount++;
          await this.invalidateScheduleCache(parkId);
        }
      }
    }

    return savedCount;
  }

  /**
   * Fills missing schedule entries for Holidays and Bridge Days
   * Ensures that even if the park has no operating hours listed,
   * we still expose the Holiday/Bridge Day status.
   */
  async fillScheduleGaps(parkId: string, lookAheadDays = 90): Promise<number> {
    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "countryCode", "regionCode", "timezone"],
    });

    if (!park?.countryCode) return 0;

    const startDate = new Date();
    startDate.setHours(0, 0, 0, 0);
    const endDate = new Date(startDate);
    endDate.setDate(startDate.getDate() + lookAheadDays);

    // 1. Fetch existing entries
    const existingEntries = await this.scheduleRepository
      .createQueryBuilder("schedule")
      .where("schedule.parkId = :parkId", { parkId })
      .andWhere("schedule.date >= :startDate", { startDate })
      .andWhere("schedule.date <= :endDate", { endDate })
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
    const minDate = new Date(startDate);
    minDate.setDate(minDate.getDate() - 1);
    const maxDate = new Date(endDate);
    maxDate.setDate(maxDate.getDate() + 1);

    const holidays = await this.holidaysService.getHolidays(
      park.countryCode,
      minDate,
      maxDate,
    );

    // Map Holidays by Date
    const holidayMap = new Map<string, string>();
    const fullRegion = park.regionCode
      ? `${park.countryCode}-${park.regionCode}`
      : "";

    // Also store dates to check bridge logic efficiently
    const holidayDatesSet = new Set<string>();

    for (const h of holidays) {
      if (h.isNationwide || (park.regionCode && h.region === fullRegion)) {
        const d = formatInParkTimezone(h.date, park.timezone);
        if (!holidayMap.has(d)) {
          holidayMap.set(d, h.localName || h.name || "");
        }
        holidayDatesSet.add(d);
      }
    }

    let filledCount = 0;

    // 3. Iterate all days in range
    const currentDate = new Date(startDate);
    while (currentDate <= endDate) {
      const dateStr = formatInParkTimezone(currentDate, park.timezone);

      let holidayName: string | null = null;
      let isHoliday = false;
      let isBridgeDay = false;

      // Check Holiday
      if (holidayMap.has(dateStr)) {
        isHoliday = true;
        holidayName = holidayMap.get(dateStr)!;
      }

      // Check Bridge Day
      const dayOfWeek = currentDate.getDay();
      if (dayOfWeek === 5) {
        // Friday -> Check Thursday
        const prev = new Date(currentDate);
        prev.setDate(currentDate.getDate() - 1);
        const prevStr = formatInParkTimezone(prev, park.timezone);
        if (holidayDatesSet.has(prevStr)) {
          isBridgeDay = true;
        }
      } else if (dayOfWeek === 1) {
        // Monday -> Check Tuesday
        const next = new Date(currentDate);
        next.setDate(currentDate.getDate() + 1);
        const nextStr = formatInParkTimezone(next, park.timezone);
        if (holidayDatesSet.has(nextStr)) {
          isBridgeDay = true;
        }
      }

      const finalIsBridgeDay = isHoliday ? false : isBridgeDay;

      // If no entry exists for this date, create it
      if (!existingDates.has(dateStr)) {
        await this.scheduleRepository.save({
          parkId,
          date: new Date(currentDate),
          scheduleType: ScheduleType.UNKNOWN,
          isHoliday,
          holidayName,
          isBridgeDay: finalIsBridgeDay,
          openingTime: null,
          closingTime: null,
        });
        filledCount++;
      } else {
        // Entry exists, check if holiday info needs updating
        const existing = existingEntries.find((e) => {
          const eDateStr = formatInParkTimezone(
            e.date instanceof Date ? e.date : new Date(e.date),
            park.timezone,
          );
          return eDateStr === dateStr;
        });

        if (
          existing &&
          (existing.isHoliday !== isHoliday ||
            existing.holidayName !== holidayName ||
            existing.isBridgeDay !== finalIsBridgeDay)
        ) {
          await this.scheduleRepository.update(existing.id, {
            isHoliday,
            holidayName,
            isBridgeDay: finalIsBridgeDay,
          });
          filledCount++; // Count updates too
        }
      }

      currentDate.setDate(currentDate.getDate() + 1);
    }

    if (filledCount > 0) {
      await this.invalidateScheduleCache(parkId);
    }
    this.logger.log(
      `Filled or updated ${filledCount} schedule entries for Park ${parkId}`,
    );
    return filledCount;
  }

  /**
   * Refreshes holiday and bridge day metadata for ALL parks
   */
  async fillAllParksGaps(): Promise<number> {
    this.logger.log("🔄 Starting gap filling for ALL parks...");
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
        "(park.continent IS NULL OR park.country IS NULL OR park.countryCode IS NULL OR park.regionCode IS NULL OR park.city IS NULL)",
      )
      .andWhere(
        "(park.geocodingAttemptedAt IS NULL OR (park.metadataRetryCount < 3 AND (park.countryCode IS NULL OR park.regionCode IS NULL OR park.city IS NULL)))",
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
   * Also marks the park as attempted.
   *
   * IMPORTANT: Only updates fields that are currently NULL.
   * This allows manual data entry without risk of being overwritten.
   *
   * @param parkId - Park ID (UUID)
   * @param geodata - Geographic data (continent, country, city)
   */
  async updateGeodata(parkId: string, geodata: Partial<Park>): Promise<void> {
    await this.parkRepository.update(parkId, {
      ...geodata,
      geocodingAttemptedAt: new Date(),
    });
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

    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "timezone"],
    });

    if (!park) return [];

    const cacheKey = `schedule:today:${parkId}:${formatInParkTimezone(today, park.timezone)}`;
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

  async getUpcomingSchedule(
    parkId: string,
    days: number = 7,
  ): Promise<ScheduleEntry[]> {
    const today = new Date();
    // Start from yesterday to ensure we capture schedules for parks in earlier timezones (e.g. US West Coast from Europe)
    // and correctly handle late-night operating hours that cross midnight
    today.setDate(today.getDate() - 1);
    today.setHours(0, 0, 0, 0);

    const endDate = new Date(today);
    // Adjust end date calculation since we started 1 day earlier
    endDate.setDate(endDate.getDate() + days + 1);
    endDate.setHours(23, 59, 59, 999);

    const park = await this.parkRepository.findOne({
      where: { id: parkId },
      select: ["id", "timezone"],
    });

    if (!park) return [];

    const cacheKey = `schedule:upcoming:${parkId}:${formatInParkTimezone(today, park.timezone)}:${days}`;
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
      return todaySchedule.scheduleType === "OPERATING";
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

    // ... logic ...
    return statusMap;
  }

  /**
   * Invalidates schedule cache for a park
   */
  async invalidateScheduleCache(parkId: string): Promise<void> {
    const keys = await this.redis.keys(`schedule:*:${parkId}:*`);
    if (keys.length > 0) {
      await this.redis.del(...keys);
      this.logger.debug(
        `Cleared ${keys.length} schedule cache keys for park ${parkId}`,
      );
    }
  }
}
