import { Injectable, Inject, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Not, IsNull } from "typeorm";
import { Redis } from "ioredis";
import { Park } from "../parks/entities/park.entity";
import { ParkDailyStats } from "../stats/entities/park-daily-stats.entity";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import {
  CountrySummaryDto,
  TopParkSummaryDto,
} from "./dto/country-summary.dto";
import { roundToNearest5Minutes } from "../common/utils/wait-time.utils";
import { CrowdLevel } from "../common/types/crowd-level.type";
import {
  GeoStructureDto,
  ContinentDto,
  CountryDto,
  CityDto,
  ParkReferenceDto,
} from "./dto/geo-structure.dto";
import { ParksService } from "../parks/parks.service";

/**
 * Discovery Service
 *
 * Builds hierarchical geographic structures for route generation.
 * Heavily cached (24h TTL) since structure changes infrequently.
 */
@Injectable()
export class DiscoveryService {
  private readonly logger = new Logger(DiscoveryService.name);
  private readonly CACHE_KEY = "discovery:geo:structure:v4"; // v4: added timezone to ParkReference
  private readonly CACHE_TTL = 24 * 60 * 60; // 24 hours
  private readonly LIVE_STATS_CACHE_KEY = "discovery:live_stats:v1";
  private readonly LIVE_STATS_TTL = 5 * 60; // 5 minutes

  constructor(
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
    @InjectRepository(ParkDailyStats)
    private readonly dailyStatsRepo: Repository<ParkDailyStats>,
    @Inject(REDIS_CLIENT)
    private readonly redis: Redis,
    private readonly parksService: ParksService,
  ) {}

  /**
   * Get complete geographic structure
   *
   * Returns hierarchical structure: continents → countries → cities → parks
   * Cached for 24 hours to improve performance.
   *
   * @returns Complete geo structure
   */
  async getGeoStructure(): Promise<GeoStructureDto> {
    // Try cache first
    const cached = await this.redis.get(this.CACHE_KEY);
    if (cached) {
      this.logger.debug("Returning cached geo structure");
      return this.hydrateStructure(JSON.parse(cached));
    }

    this.logger.log("Building geo structure from database");

    // Fetch all parks with complete geographic data and attractions
    const parks = await this.parkRepository.find({
      where: {
        continent: Not(IsNull()),
        country: Not(IsNull()),
        city: Not(IsNull()),
        continentSlug: Not(IsNull()),
        countrySlug: Not(IsNull()),
        citySlug: Not(IsNull()),
      },
      select: [
        "id",
        "name",
        "slug",
        "timezone",
        "continent",
        "continentSlug",
        "country",
        "countrySlug",
        "countryCode",
        "city",
        "citySlug",
      ],
      order: {
        continent: "ASC",
        country: "ASC",
        city: "ASC",
        name: "ASC",
      },
    });

    const parkIds = parks.map((p) => p.id);
    const scheduleFlags =
      await this.parksService.getBatchHasOperatingSchedule(parkIds);

    // Build hierarchical structure
    const continentMap = new Map<string, ContinentDto>();

    for (const park of parks) {
      const continentKey = park.continentSlug;

      // Get or create continent
      let continent = continentMap.get(continentKey);
      if (!continent) {
        continent = {
          name: park.continent,
          slug: park.continentSlug,
          countries: [],
          countryCount: 0,
          parkCount: 0,
        };
        continentMap.set(continentKey, continent);
      }

      // Get or create country
      // Match by countryCode (primary) or Name (fallback)
      let country = continent.countries.find(
        (c) =>
          (park.countryCode && c.code === park.countryCode) ||
          c.name === park.country,
      );

      if (country) {
        // If we found a match, check if we should upgrade to better metadata
        // We prefer longer slugs (e.g. 'france' > 'fr')
        if (
          park.countrySlug &&
          country.slug &&
          park.countrySlug.length > country.slug.length
        ) {
          country.name = park.country;
          country.slug = park.countrySlug;
        }
        // Ensure code is populated if missing
        if (!country.code && park.countryCode) {
          country.code = park.countryCode;
        }
      } else {
        country = {
          name: park.country,
          slug: park.countrySlug,
          code: park.countryCode || "",
          cities: [],
          cityCount: 0,
          parkCount: 0,
        };
        continent.countries.push(country);
      }

      // Get or create city
      let city = country.cities.find((c) => c.slug === park.citySlug);
      if (!city) {
        city = {
          name: park.city,
          slug: park.citySlug,
          parks: [],
          parkCount: 0,
        };
        country.cities.push(city);
      }

      // Add park reference
      const parkBaseUrl = `/v1/parks/${park.continentSlug}/${park.countrySlug}/${park.citySlug}/${park.slug}`;

      const parkRef: ParkReferenceDto = {
        id: park.id,
        name: park.name,
        slug: park.slug,
        url: parkBaseUrl,
        timezone: park.timezone,
        hasOperatingSchedule: scheduleFlags.get(park.id) ?? false,
        attractionCount: 0, // Will be hydrated with live stats
        status: "CLOSED", // Default, will be hydrated
      };
      city.parks.push(parkRef);
      city.parkCount++;
      country.parkCount++;
      continent.parkCount++;
    }

    // Update counts
    const continents = Array.from(continentMap.values());
    for (const continent of continents) {
      continent.countryCount = continent.countries.length;
      for (const country of continent.countries) {
        country.cityCount = country.cities.length;
      }
    }

    const structure: GeoStructureDto = {
      continents,
      continentCount: continents.length,
      countryCount: continents.reduce((sum, c) => sum + c.countries.length, 0),
      cityCount: continents.reduce(
        (sum, c) =>
          sum + c.countries.reduce((s, co) => s + co.cities.length, 0),
        0,
      ),
      parkCount: parks.length,
      attractionCount: 0, // Hydrated after live stats are applied
      generatedAt: new Date().toISOString(),
    };

    // Cache the structure (skeleton) for 24h
    await this.redis.setex(
      this.CACHE_KEY,
      this.CACHE_TTL,
      JSON.stringify(structure),
    );

    this.logger.log(
      `Built geo structure: ${structure.continentCount} continents, ${structure.countryCount} countries, ${structure.cityCount} cities, ${structure.parkCount} parks, ${structure.attractionCount} attractions`,
    );

    // Apply Live Stats (Fresh)
    return this.hydrateStructure(structure);
  }

  /**
   * Hydrates the geographic structure with live data
   */
  private async hydrateStructure(
    structure: GeoStructureDto,
  ): Promise<GeoStructureDto> {
    const allParkIds: string[] = [];
    for (const continent of structure.continents) {
      for (const country of continent.countries) {
        for (const city of country.cities) {
          for (const park of city.parks) {
            allParkIds.push(park.id);
          }
        }
      }
    }

    const [liveStats, schedules] = await Promise.all([
      this.getLiveStats(),
      allParkIds.length > 0
        ? this.parksService.getBatchSchedules(allParkIds)
        : Promise.resolve({ today: new Map(), next: new Map() }),
    ]);

    for (const continent of structure.continents) {
      let continentOpenCount = 0;
      let continentTotalWait = 0;
      let continentWaitCount = 0;

      for (const country of continent.countries) {
        let countryOpenCount = 0;
        let countryTotalWait = 0;
        let countryWaitCount = 0;

        for (const city of country.cities) {
          let cityOpenCount = 0;
          let cityTotalWait = 0;
          let cityWaitCount = 0;

          for (const park of city.parks) {
            const stats = liveStats.get(park.id);
            if (stats) {
              // Hydrate Park Status
              park.status = stats.isOpen ? "OPERATING" : "CLOSED";

              // Hydrate Analytics
              park.attractionCount = stats.totalAttractions;
              park.analytics = {
                statistics: {
                  avgWaitTime: stats.avgWait,
                  // Optimistic Calculation: Total - Explicitly Closed (if status is OPERATING)
                  operatingAttractions:
                    stats.isOpen && stats.explicitlyClosedCount !== undefined
                      ? Math.max(
                          0,
                          stats.totalAttractions - stats.explicitlyClosedCount,
                        )
                      : 0,
                  closedAttractions:
                    stats.explicitlyClosedCount ?? stats.totalAttractions,
                  totalAttractions: stats.totalAttractions,
                  crowdLevel:
                    stats.crowdLevel !== null
                      ? this.occupancyToCrowdLevel(stats.crowdLevel)
                      : undefined,
                },
              };

              // City Aggregations
              if (stats.isOpen) {
                cityOpenCount++;
                if (stats.avgWait > 0) {
                  cityTotalWait += stats.avgWait;
                  cityWaitCount++;
                }
              }
            } else {
              // Default offline status
              park.status = "CLOSED";
            }

            // Hydrate Schedules
            const todaySchedule = schedules.today.get(park.id);
            const nextSchedule = schedules.next.get(park.id);

            park.todaySchedule =
              todaySchedule && todaySchedule.length > 0
                ? {
                    openingTime:
                      todaySchedule[0].openingTime?.toISOString() || "",
                    closingTime:
                      todaySchedule[0].closingTime?.toISOString() || "",
                    scheduleType: todaySchedule[0].scheduleType,
                  }
                : undefined;

            park.nextSchedule = nextSchedule
              ? {
                  openingTime: nextSchedule.openingTime?.toISOString() || "",
                  closingTime: nextSchedule.closingTime?.toISOString() || "",
                  scheduleType: nextSchedule.scheduleType,
                }
              : undefined;
          }

          city.openParkCount = cityOpenCount;
          city.averageWaitTime =
            cityWaitCount > 0
              ? roundToNearest5Minutes(cityTotalWait / cityWaitCount)
              : undefined;

          countryOpenCount += cityOpenCount;
          countryTotalWait += cityTotalWait;
          countryWaitCount += cityWaitCount;
        }

        country.openParkCount = countryOpenCount;
        country.averageWaitTime =
          countryWaitCount > 0
            ? roundToNearest5Minutes(countryTotalWait / countryWaitCount)
            : undefined;

        continentOpenCount += countryOpenCount;
        continentTotalWait += countryTotalWait;
        continentWaitCount += countryWaitCount;
      }

      continent.openParkCount = continentOpenCount;
      continent.averageWaitTime =
        continentWaitCount > 0
          ? roundToNearest5Minutes(continentTotalWait / continentWaitCount)
          : undefined;
    }

    return structure;
  }

  /**
   * Get all continents
   *
   * @returns List of continents with basic info
   */
  async getContinents(): Promise<ContinentDto[]> {
    const structure = await this.getGeoStructure();
    return structure.continents;
  }

  /**
   * Get countries in a specific continent
   *
   * @param continentSlug - Continent slug
   * @returns List of countries or null if continent not found
   */
  async getCountriesInContinent(
    continentSlug: string,
  ): Promise<CountryDto[] | null> {
    const structure = await this.getGeoStructure();
    const continent = structure.continents.find(
      (c) => c.slug === continentSlug,
    );
    return continent ? continent.countries : null;
  }

  /**
   * Get cities in a specific country
   *
   * @param continentSlug - Continent slug
   * @param countrySlug - Country slug
   * @returns List of cities or null if not found
   */
  async getCitiesInCountry(
    continentSlug: string,
    countrySlug: string,
  ): Promise<CityDto[] | null> {
    const structure = await this.getGeoStructure();
    const continent = structure.continents.find(
      (c) => c.slug === continentSlug,
    );
    if (!continent) return null;

    const country = continent.countries.find((c) => c.slug === countrySlug);
    return country ? country.cities : null;
  }

  /**
   * Get live park statistics (open counts + wait times + crowd levels)
   * Cached for 5 minutes, performant single query
   *
   * @returns Map of Park ID -> Stats
   */
  private async getLiveStats(): Promise<
    Map<
      string,
      {
        isOpen: boolean;
        avgWait: number;
        operatingAttractions: number;
        explicitlyClosedCount: number;
        totalAttractions: number;
        crowdLevel: number | null;
      }
    >
  > {
    const cached = await this.redis.get(this.LIVE_STATS_CACHE_KEY);
    if (cached) {
      this.logger.debug("Returning cached live stats");
      return new Map(JSON.parse(cached));
    }

    this.logger.log("Fetching live park statistics");

    // ONE performant query for all parks
    // Implements same hybrid logic as central utility:
    // - Primary: Schedule-based (if schedule exists)
    // - Fallback: Ride-based (for parks without schedules, using recent data)
    const result = await this.parkRepository.query(`
      WITH park_schedules AS (
        SELECT DISTINCT s."parkId"
        FROM schedule_entries s
        WHERE s."scheduleType" = 'OPERATING'
          AND s."openingTime" <= NOW()
          AND s."closingTime" > NOW()
      ),
      parks_with_schedule AS (
        SELECT DISTINCT s."parkId"
        FROM schedule_entries s
        WHERE s."scheduleType" = 'OPERATING'
      ),
      latest_attraction_data AS (
        SELECT 
          a.id as "attractionId",
          a."parkId",
          qd."waitTime",
          qd."status"
        FROM attractions a
        JOIN LATERAL (
          SELECT "waitTime", "status"
          FROM queue_data qd
          WHERE qd."attractionId" = a.id
            AND qd.timestamp > NOW() - INTERVAL '30 minutes'
          ORDER BY 
            CASE WHEN qd."queueType" = 'STANDBY' THEN 0 ELSE 1 END,
            qd.timestamp DESC
          LIMIT 1
        ) qd ON true
      ),
      park_stats AS (
        SELECT 
          lad."parkId",
          COUNT(*) FILTER (
            WHERE lad.status = 'OPERATING' 
              AND lad."waitTime" > 0
          ) as active_rides,
          AVG(lad."waitTime") as avg_wait,
          -- Only count attractions with status OPERATING (includes those with waitTime=0)
          -- This matches the actual operational state from recent data
          COUNT(CASE WHEN lad.status = 'OPERATING' THEN 1 END) as operating_count,
          COUNT(CASE WHEN lad.status != 'OPERATING' THEN 1 END) as explicitly_closed_count
        FROM latest_attraction_data lad
        GROUP BY lad."parkId"
      )
      SELECT
        p.id,
        p."current_crowd_level",
        CASE
          -- If park has schedule: Use schedule-based logic
          WHEN pws."parkId" IS NOT NULL THEN
            CASE WHEN ps."parkId" IS NOT NULL THEN true ELSE false END
          -- If park has NO schedule: Use ride-based fallback
          ELSE
            CASE WHEN COALESCE(stats.active_rides, 0) > 0 THEN true ELSE false END
        END as is_open,
        COALESCE(stats.avg_wait, 0) as avg_wait,
        COALESCE(stats.operating_count, 0) as operating_conf_count,
        COALESCE(stats.explicitly_closed_count, 0) as explicitly_closed_count,
        (SELECT COUNT(*)::int FROM attractions a WHERE a."parkId" = p.id) as total_attractions
      FROM parks p
      LEFT JOIN park_schedules ps ON ps."parkId" = p.id
      LEFT JOIN parks_with_schedule pws ON pws."parkId" = p.id
      LEFT JOIN park_stats stats ON stats."parkId" = p.id
    `);

    const stats = new Map<
      string,
      {
        isOpen: boolean;
        avgWait: number;
        operatingAttractions: number;
        explicitlyClosedCount: number;
        totalAttractions: number;
        crowdLevel: number | null;
      }
    >();
    for (const row of result) {
      stats.set(row.id, {
        isOpen: row.is_open,
        avgWait: roundToNearest5Minutes(parseFloat(row.avg_wait || 0)),
        operatingAttractions: parseInt(row.operating_conf_count || "0", 10),
        explicitlyClosedCount: parseInt(row.explicitly_closed_count || "0", 10),
        totalAttractions: parseInt(row.total_attractions || "0", 10),
        crowdLevel: row.current_crowd_level
          ? parseFloat(row.current_crowd_level)
          : null,
      });
    }

    // Cache for 5 minutes
    await this.redis.setex(
      this.LIVE_STATS_CACHE_KEY,
      this.LIVE_STATS_TTL,
      JSON.stringify(Array.from(stats.entries())),
    );

    this.logger.log(
      `Fetched live stats for ${stats.size} parks (${Array.from(stats.values()).filter((s) => s.isOpen).length} open)`,
    );

    return stats;
  }

  /**
   * Invalidate cache (for manual refresh or on data changes)
   */
  async invalidateCache(): Promise<void> {
    await this.redis.del(this.CACHE_KEY);
    this.logger.log("Geo structure cache invalidated");
  }

  private occupancyToCrowdLevel(occupancy: number): CrowdLevel {
    if (occupancy <= 60) return "very_low";
    if (occupancy <= 89) return "low";
    if (occupancy <= 110) return "moderate";
    if (occupancy <= 150) return "high";
    if (occupancy <= 200) return "very_high";
    return "extreme";
  }

  /**
   * GET /v1/discovery/continents/:continent/:country/summary
   *
   * Returns enriched country metadata for landing pages.
   * Aggregates ParkDailyStats (last 12 months) to produce avgCrowdScore per park,
   * top-5 parks, and peak/quiet month arrays.
   * Cached for 24 hours.
   */
  async getCountrySummary(
    continentSlug: string,
    countrySlug: string,
  ): Promise<CountrySummaryDto | null> {
    const cacheKey = `discovery:country-summary:v1:${continentSlug}:${countrySlug}`;
    const cached = await this.redis.get(cacheKey);
    if (cached) return JSON.parse(cached) as CountrySummaryDto;

    const parks = await this.parkRepository.find({
      where: { continentSlug, countrySlug },
      select: [
        "id",
        "name",
        "slug",
        "city",
        "citySlug",
        "continentSlug",
        "countrySlug",
      ],
    });

    if (parks.length === 0) return null;

    const parkIds = parks.map((p) => p.id);
    const cityCount = new Set(parks.map((p) => p.citySlug)).size;

    // Aggregate monthly averages per park for last 12 months.
    // Use noon UTC as cutoff (see datetime-handling.md: T12:00:00Z for date-only conversions).
    // For a country-level summary spanning many parks with different timezones,
    // a single UTC-noon anchor is an acceptable approximation for the 1-year boundary.
    const cutoffDate = new Date();
    cutoffDate.setUTCFullYear(cutoffDate.getUTCFullYear() - 1);
    const cutoffStr = cutoffDate.toISOString().slice(0, 10);

    // Column names are camelCase (TypeORM default, no naming strategy) — must be quoted.
    const rows: Array<{
      parkId: string;
      month: string;
      avg_p50: string;
    }> = await this.dailyStatsRepo.manager.query(
      `SELECT
         "parkId",
         EXTRACT(MONTH FROM date::date)::int AS month,
         AVG("p50WaitTime")                  AS avg_p50
       FROM park_daily_stats
       WHERE "parkId" = ANY($1)
         AND date >= $2
         AND "p50WaitTime" IS NOT NULL
       GROUP BY "parkId", month`,
      [parkIds, cutoffStr],
    );

    // Build per-park annual avg crowd score
    const parkScoreMap = new Map<string, number[]>();
    const monthlyTotals = new Map<number, number[]>(); // month → avg_p50 values across parks

    for (const row of rows) {
      const p50 = Number(row.avg_p50);
      const month = Number(row.month);

      if (!parkScoreMap.has(row.parkId)) parkScoreMap.set(row.parkId, []);
      parkScoreMap.get(row.parkId)!.push(p50);

      if (!monthlyTotals.has(month)) monthlyTotals.set(month, []);
      monthlyTotals.get(month)!.push(p50);
    }

    const avg = (nums: number[]) =>
      nums.length ? nums.reduce((s, n) => s + n, 0) / nums.length : 0;

    const toCrowdScore = (p50: number) =>
      Math.round(Math.min(Math.max(p50 / 10, 1.0), 5.0) * 10) / 10;

    // Top-5 parks by annual avg crowd score
    const scoredParks: Array<TopParkSummaryDto> = parks.map((park) => {
      const vals = parkScoreMap.get(park.id) ?? [];
      const avgP50 = avg(vals);
      return {
        name: park.name,
        slug: park.slug,
        city: park.city,
        path: `/parks/${park.continentSlug}/${park.countrySlug}/${park.citySlug}/${park.slug}`,
        avgAnnualCrowdScore: toCrowdScore(avgP50),
      };
    });

    scoredParks.sort((a, b) => b.avgAnnualCrowdScore - a.avgAnnualCrowdScore);
    const topParks = scoredParks.slice(0, 5);

    // Peak / quiet months across all parks.
    // Guard against overlap: only emit peak/quiet arrays when enough distinct
    // months exist (≥6); otherwise return empty arrays rather than overlapping ones.
    const monthScores: Array<{ month: number; score: number }> = [];
    for (const [month, vals] of monthlyTotals.entries()) {
      monthScores.push({ month, score: avg(vals) });
    }
    monthScores.sort((a, b) => b.score - a.score);

    const avgPeakMonths =
      monthScores.length >= 6
        ? monthScores
            .slice(0, 3)
            .map((m) => m.month)
            .sort((a, b) => a - b)
        : [];
    const avgQuietMonths =
      monthScores.length >= 6
        ? monthScores
            .slice(-3)
            .map((m) => m.month)
            .sort((a, b) => a - b)
        : [];

    const result: CountrySummaryDto = {
      countrySlug,
      parkCount: parks.length,
      cityCount,
      topParks,
      avgPeakMonths,
      avgQuietMonths,
    };

    await this.redis.set(cacheKey, JSON.stringify(result), "EX", 24 * 60 * 60);
    return result;
  }
}
