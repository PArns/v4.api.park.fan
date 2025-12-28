import { Injectable, Logger } from "@nestjs/common";
import { Park } from "../entities/park.entity";
import { ParkResponseDto } from "../dto/park-response.dto";
import { ParksService } from "../parks.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { OccupancyDto, ParkStatisticsDto } from "../../analytics/dto";

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

    // Batch fetch all data in parallel (3 queries total regardless of park count)
    const [statusMap, occupancyMap, statisticsMap] = await Promise.all([
      this.parksService.getBatchParkStatus(parkIds),
      this.analyticsService.getBatchParkOccupancy(parkIds),
      this.analyticsService.getBatchParkStatistics(parkIds),
    ]);

    // Map each park using fetched data
    return parks.map((park) =>
      this.mapParkToDto(park, statusMap, occupancyMap, statisticsMap),
    );
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
  ): ParkResponseDto {
    const dto = ParkResponseDto.fromEntity(park);

    // Status
    dto.status = (statusMap.get(park.id) as "OPERATING" | "CLOSED") || "CLOSED";

    // Occupancy & Analytics
    const occupancy = occupancyMap.get(park.id);
    const stats = statisticsMap.get(park.id);

    if (occupancy) {
      dto.currentLoad = {
        crowdLevel: this.mapCrowdLevel(occupancy.current),
        baseline: occupancy.baseline90thPercentile,
        currentWaitTime: occupancy.breakdown?.currentAvgWait || 0,
      };

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
          peakHour: stats?.peakHour || null,
          crowdLevel: this.mapCrowdLevel(occupancy.current),
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
   * Map occupancy percentage to unified crowd level enum (5 levels)
   * Unified standard across all park DTOs
   * @private
   */
  private mapCrowdLevel(
    occupancy: number,
  ): "very_low" | "low" | "moderate" | "high" | "very_high" {
    if (occupancy < 20) return "very_low";
    if (occupancy < 40) return "low";
    if (occupancy < 70) return "moderate";
    if (occupancy < 95) return "high";
    return "very_high";
  }
}
