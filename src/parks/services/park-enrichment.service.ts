import { Injectable, Logger } from "@nestjs/common";
import { Park } from "../entities/park.entity";
import { ParkResponseDto } from "../dto/park-response.dto";
import { ParksService } from "../parks.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { OccupancyDto, ParkStatisticsDto } from "../../analytics/dto";
import { HolidaysService } from "../../holidays/holidays.service";
import { ScheduleItemDto, InfluencingHoliday } from "../dto/schedule-item.dto";
import {
  calculateHolidayInfoFromString,
  HolidayEntry,
} from "../../common/utils/holiday.utils";
import { formatInParkTimezone } from "../../common/utils/date.util";
import { normalizeRegionCode } from "../../common/utils/region.util";
import { fromZonedTime } from "date-fns-tz";

/**
 * Park Enrichment Service
 *
 * Centralized service for enriching park entities with live data (status, analytics, statistics).
 * Eliminates code duplication across ParksController, DiscoveryController, and LocationService.
 *
 * Optimizations:
 * - Uses batch methods to avoid N+1 queries
 * - Single source of truth for park DTO mapping logic
 * - Consistent behavior across all endpoints
 */
@Injectable()
export class ParkEnrichmentService {
  private readonly logger = new Logger(ParkEnrichmentService.name);

  constructor(
    private readonly parksService: ParksService,
    private readonly analyticsService: AnalyticsService,
    private readonly holidaysService: HolidaysService,
  ) {}

  /**
   * Enrich parks with status, analytics, and statistics
   *
   * Optimized batch fetching:
   * - 1 query for all statuses
   * - 1 query for all occupancies
   * - 1 query for all statistics
   *
   * Replaces N+1 pattern where each park triggered separate statistics query.
   *
   * @param parks - Array of park entities to enrich
   * @returns Array of enriched ParkResponseDto
   */
  async enrichParksWithLiveData(parks: Park[]): Promise<ParkResponseDto[]> {
    if (parks.length === 0) {
      return [];
    }

    const parkIds = parks.map((p) => p.id);

    // Pre-calculate context (timezone + startTime) for batch park statistics
    const context = new Map<string, { timezone: string; startTime: Date }>();
    for (const park of parks) {
      const startTime = await this.analyticsService.getEffectiveStartTime(
        park.id,
        park.timezone,
      );
      context.set(park.id, { timezone: park.timezone, startTime });
    }

    // Batch fetch all data in parallel (4 queries total regardless of park count)
    const [statusMap, occupancyMap, statisticsMap, schoolHolidayMap] =
      await Promise.all([
        this.parksService.getBatchParkStatus(parkIds),
        this.analyticsService.getBatchParkOccupancy(parkIds),
        this.analyticsService.getBatchParkStatistics(parkIds, context),
        this.getBatchSchoolHolidayStatus(parks),
      ]);

    // Map each park using fetched data
    return parks.map((park) =>
      this.mapParkToDto(
        park,
        statusMap,
        occupancyMap,
        statisticsMap,
        schoolHolidayMap,
      ),
    );
  }

  /**
   * Helper to fetch school holiday status for multiple parks in parallel
   * @private
   */
  private async getBatchSchoolHolidayStatus(
    parks: Park[],
  ): Promise<Map<string, boolean>> {
    const results = await Promise.all(
      parks.map(async (park) => {
        if (!park.countryCode) return { id: park.id, isSchoolHoliday: false };
        try {
          const now = new Date();
          const isSchoolHoliday =
            await this.holidaysService.isEffectiveSchoolHoliday(
              now,
              park.countryCode,
              park.regionCode,
              park.timezone,
            );
          return { id: park.id, isSchoolHoliday };
        } catch (error) {
          this.logger.warn(
            `Failed to check school holiday for ${park.name}: ${error}`,
          );
          return { id: park.id, isSchoolHoliday: false };
        }
      }),
    );

    return new Map(results.map((r) => [r.id, r.isSchoolHoliday]));
  }

  /**
   * Map a single park entity to DTO with live data
   * @private
   */
  private mapParkToDto(
    park: Park,
    statusMap: Map<string, string>,
    occupancyMap: Map<string, OccupancyDto>,
    statisticsMap: Map<string, ParkStatisticsDto>,
    schoolHolidayMap: Map<string, boolean>,
  ): ParkResponseDto {
    const dto = ParkResponseDto.fromEntity(park);

    // Status
    dto.status = (statusMap.get(park.id) as "OPERATING" | "CLOSED") || "CLOSED";

    // School Holiday
    dto.isSchoolVacation = schoolHolidayMap.get(park.id) || false;

    // Occupancy & Analytics
    const occupancy = occupancyMap.get(park.id);
    const stats = statisticsMap.get(park.id);

    if (occupancy) {
      dto.analytics = {
        occupancy: {
          current: occupancy.current,
          trend: occupancy.trend,
          comparedToTypical: occupancy.comparedToTypical,
          comparisonStatus: occupancy.comparisonStatus,
          baseline90thPercentile: occupancy.baseline90thPercentile,
          updatedAt: occupancy.updatedAt,
        },
        statistics: {
          avgWaitTime: occupancy.breakdown?.currentAvgWait || 0,
          avgWaitToday: stats?.avgWaitToday || 0,
          peakWaitToday: stats?.peakWaitToday || 0,
          peakHour: stats?.peakHour || null,
          crowdLevel: this.analyticsService.determineCrowdLevel(
            occupancy.current,
          ),
          totalAttractions: stats?.totalAttractions || 0,
          operatingAttractions: stats?.operatingAttractions || 0,
          closedAttractions: stats?.closedAttractions || 0,
          timestamp: occupancy.updatedAt,
        },
      };
    }

    return dto;
  }

  /**
   * Enrich schedule items with holiday information
   *
   * Shared function used by both park and attraction endpoints.
   * Fetches holidays for the given date range and enriches schedule items
   * with holiday information (isHoliday, holidayName, holidayType, isBridgeDay, etc.)
   *
   * @param scheduleItems - Schedule items to enrich
   * @param park - Park entity (for country, region, timezone, influencingRegions)
   * @returns Enriched schedule items (mutates the input array)
   */
  async enrichScheduleWithHolidays(
    scheduleItems: ScheduleItemDto[],
    park: Park,
  ): Promise<void> {
    if (!scheduleItems || scheduleItems.length === 0) {
      return;
    }

    // Skip if park doesn't have countryCode (required for holiday lookup)
    if (!park.countryCode) {
      this.logger.debug(
        `Skipping holiday enrichment for ${park.slug}: missing countryCode`,
      );
      return;
    }

    try {
      // Find date range from schedule items
      const dates = scheduleItems.map((s) => s.date).sort();
      // Parse date strings in park timezone to create proper Date objects
      const minDate = fromZonedTime(`${dates[0]}T00:00:00`, park.timezone);
      const maxDate = fromZonedTime(
        `${dates[dates.length - 1]}T23:59:59`,
        park.timezone,
      );
      // Extend range by 1 day on each side for bridge day detection
      minDate.setDate(minDate.getDate() - 1);
      maxDate.setDate(maxDate.getDate() + 1);

      // Fetch all holidays for this range (Home Country + Influencing Regions)
      const relevantRegions = [
        { countryCode: park.countryCode, regionCode: park.regionCode },
        ...(park.influencingRegions || []),
      ];

      const countryCodes = [
        ...new Set(relevantRegions.map((r) => r.countryCode)),
      ];
      const allHolidays = await Promise.all(
        countryCodes.map((cc) =>
          this.holidaysService.getHolidays(
            cc,
            formatInParkTimezone(minDate, park.timezone),
            formatInParkTimezone(maxDate, park.timezone),
          ),
        ),
      );
      const holidays = allHolidays.flat();

      // Map for fast lookup: "YYYY-MM-DD" -> HolidayEntry (for bridge day logic)
      // We need separate tracking for school holidays since a day can have both
      // a public holiday and a school holiday
      const holidayMap = new Map<string, HolidayEntry>();
      const schoolHolidayMap = new Map<string, HolidayEntry>();
      const influencingMap = new Map<string, InfluencingHoliday[]>();

      for (const h of holidays) {
        // Normalize holiday date to park timezone for consistent matching
        const dateStr = formatInParkTimezone(h.date, park.timezone);

        // Check if this holiday matches the park's primary region
        const matchesPrimaryRegion = (() => {
          // Only check primary region (park's own region)
          const primaryRegion = {
            countryCode: park.countryCode,
            regionCode: park.regionCode,
          };

          if (primaryRegion.countryCode !== h.country) return false;

          // For school holidays: Must match region explicitly
          // Use fallback to handle cases where holidayType might be missing
          const holidayType = h.holidayType || "public";
          if (holidayType === "school") {
            if (!h.region) {
              // No region = truly nationwide school holiday (rare)
              return true;
            }
            // Check if region matches (normalize both sides)
            const holidayRegionCode = normalizeRegionCode(h.region);
            const parkRegionCode = normalizeRegionCode(
              primaryRegion.regionCode,
            );
            return holidayRegionCode === parkRegionCode;
          }

          // For public holidays: Nationwide always matches
          if (h.isNationwide || !h.region) return true;

          // Region matches if it's explicitly the park's region (normalize both sides)
          const holidayRegionCode = normalizeRegionCode(h.region);
          const parkRegionCode = normalizeRegionCode(primaryRegion.regionCode);
          return holidayRegionCode === parkRegionCode;
        })();

        // Check if this holiday matches any influencing region
        const matchesInfluencingRegion = relevantRegions.some((reg) => {
          if (reg.countryCode !== h.country) return false;
          // Skip primary region (already checked above)
          if (
            reg.countryCode === park.countryCode &&
            normalizeRegionCode(reg.regionCode) ===
              normalizeRegionCode(park.regionCode)
          ) {
            return false;
          }

          // For school holidays: Must match region explicitly
          // Use fallback to handle cases where holidayType might be missing
          const holidayType = h.holidayType || "public";
          if (holidayType === "school") {
            if (!h.region) {
              // No region = truly nationwide school holiday (rare)
              return true;
            }
            // Check if region matches (normalize both sides)
            const holidayRegionCode = normalizeRegionCode(h.region);
            const parkRegionCode = normalizeRegionCode(reg.regionCode);
            return holidayRegionCode === parkRegionCode;
          }

          // For public holidays: Nationwide always matches
          if (h.isNationwide || !h.region) return true;

          // Region matches if it's explicitly the influencing region (normalize both sides)
          const holidayRegionCode = normalizeRegionCode(h.region);
          const parkRegionCode = normalizeRegionCode(reg.regionCode);
          return holidayRegionCode === parkRegionCode;
        });

        if (matchesPrimaryRegion) {
          // Local holiday: determines isHoliday and holidayName
          // Ensure holidayType is set - if missing from DB, infer from name or default to "public"
          let holidayType = h.holidayType;
          if (!holidayType) {
            // Fallback: Try to infer type from name
            const name = (h.localName || h.name || "").toLowerCase();
            if (
              name.includes("holiday") ||
              name.includes("ferien") ||
              name.includes("vacation")
            ) {
              holidayType = "school";
            } else {
              holidayType = "public"; // Default to public for official holidays
            }
          }

          if (holidayType === "school") {
            // School holidays: Store in separate map (a day can have both public and school holidays)
            schoolHolidayMap.set(dateStr, {
              name: h.localName || h.name,
              type: holidayType,
            });
          } else {
            // Public holidays: Store in main holiday map
            const existing = holidayMap.get(dateStr);
            const isBetterType =
              holidayType === "public" || holidayType === "bank";

            if (!existing || (isBetterType && existing.type === "observance")) {
              holidayMap.set(dateStr, {
                name: h.localName || h.name,
                type: holidayType as
                  | "public"
                  | "school"
                  | "observance"
                  | "bank",
              });
            }
          }
        } else if (matchesInfluencingRegion) {
          // Influencing holiday: added to a list for context (even if same country)
          const currentInfluencing = influencingMap.get(dateStr) || [];
          currentInfluencing.push({
            name: h.name,
            source: {
              countryCode: h.country,
              regionCode: normalizeRegionCode(h.region),
            },
            holidayType: h.holidayType,
          });
          influencingMap.set(dateStr, currentInfluencing);
        }
      }

      // Apply to schedule items
      for (const item of scheduleItems) {
        const dateStr = item.date;
        const localInfluencing = influencingMap.get(dateStr) || [];

        // Use utility function to calculate holiday info (including weekend extensions and bridge days)
        const holidayInfo = calculateHolidayInfoFromString(
          dateStr,
          holidayMap,
          park.timezone,
        );

        // Check for school holidays separately (a day can have both public and school holidays)
        const schoolHoliday = schoolHolidayMap.get(dateStr);
        const isSchoolHoliday = !!schoolHoliday;

        // Determine holiday type from both maps
        // Check maps directly to ensure we get the type even if calculateHolidayInfoFromString doesn't return it
        const publicHoliday = holidayMap.get(dateStr);
        const publicHolidayType =
          publicHoliday && typeof publicHoliday !== "string"
            ? publicHoliday.type
            : null;
        const schoolHolidayType = schoolHoliday?.type ?? null;
        const finalHolidayType =
          publicHolidayType || schoolHolidayType || holidayInfo.holidayType;

        // Set all holiday information from utility function
        item.isHoliday = holidayInfo.isHoliday || isSchoolHoliday;
        // Prefer public holiday name, but use school holiday name if no public holiday
        item.holidayName =
          holidayInfo.holidayName || (schoolHoliday?.name ?? null);
        // If both exist, prefer public holiday type, otherwise use school holiday type
        item.holidayType = finalHolidayType;
        // Set flags based on type (explicitly check type, not just rely on utility function)
        item.isPublicHoliday =
          finalHolidayType === "public" || finalHolidayType === "bank";
        item.isSchoolHoliday = isSchoolHoliday || finalHolidayType === "school";
        item.isBridgeDay = holidayInfo.isBridgeDay;

        // Always attach influencing holidays if any exist
        item.influencingHolidays = localInfluencing;
      }
    } catch (error) {
      this.logger.warn(
        `Failed to enrich schedule with holidays for ${park.slug}: ${error}`,
      );
    }
  }
}
