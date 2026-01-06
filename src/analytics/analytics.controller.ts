import { Controller, Get, Param, UseInterceptors } from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse, ApiParam } from "@nestjs/swagger";
import { AnalyticsService } from "./analytics.service";
import {
  GlobalStatsDto,
  ParkPercentilesDto,
  AttractionPercentilesDto,
  GeoLiveStatsDto,
} from "./dto";
import { HttpCacheInterceptor } from "../common/interceptors/cache.interceptor";

@ApiTags("stats")
@Controller("analytics")
export class AnalyticsController {
  constructor(private readonly analyticsService: AnalyticsService) {}

  @Get("realtime")
  @UseInterceptors(new HttpCacheInterceptor(120))
  @ApiOperation({
    summary: "Get global platform statistics",
    description:
      "Returns real-time global statistics including open/closed parks, most/least crowded parks, longest/shortest wait rides, and platform-wide counts. Cached for 2 minutes for optimal performance.",
  })
  @ApiResponse({
    status: 200,
    description: "Returns real-time global statistics about parks and rides",
    type: GlobalStatsDto,
  })
  @ApiResponse({
    status: 500,
    description: "Internal server error",
  })
  async getGlobalStats(): Promise<GlobalStatsDto> {
    return this.analyticsService.getGlobalRealtimeStats();
  }

  @Get("parks/:parkId/percentiles")
  @UseInterceptors(new HttpCacheInterceptor(12 * 60 * 60))
  @ApiOperation({
    summary: "Get complete percentile distribution for a park",
    description:
      "Returns percentile distribution (P50, P75, P90, P95) for today plus rolling windows (7d, 30d). Percentiles update daily, cached for 12 hours.",
  })
  @ApiParam({
    name: "parkId",
    description: "UUID of the park",
    example: "123e4567-e89b-12d3-a456-426614174000",
  })
  @ApiResponse({
    status: 200,
    description: "Returns today's percentiles + rolling windows (7d, 30d)",
    type: ParkPercentilesDto,
  })
  @ApiResponse({
    status: 404,
    description: "Park not found",
  })
  @ApiResponse({
    status: 500,
    description: "Internal server error",
  })
  async getParkPercentiles(
    @Param("parkId") parkId: string,
  ): Promise<ParkPercentilesDto> {
    return this.analyticsService.getParkPercentiles(
      parkId,
    ) as Promise<ParkPercentilesDto>;
  }

  @Get("attractions/:attractionId/percentiles")
  @UseInterceptors(new HttpCacheInterceptor(12 * 60 * 60))
  @ApiOperation({
    summary: "Get complete percentile distribution for an attraction",
    description:
      "Returns percentile distribution (P25, P50, P75, P90) for today plus hourly breakdown and rolling windows (7d, 30d). Percentiles update daily, cached for 12 hours.",
  })
  @ApiParam({
    name: "attractionId",
    description: "UUID of the attraction",
    example: "123e4567-e89b-12d3-a456-426614174000",
  })
  @ApiResponse({
    status: 200,
    description: "Returns today's percentiles + hourly array + rolling windows",
    type: AttractionPercentilesDto,
  })
  @ApiResponse({
    status: 404,
    description: "Attraction not found",
  })
  @ApiResponse({
    status: 500,
    description: "Internal server error",
  })
  async getAttractionPercentiles(
    @Param("attractionId") attractionId: string,
  ): Promise<AttractionPercentilesDto> {
    return this.analyticsService.getAttractionPercentiles(
      attractionId,
    ) as Promise<AttractionPercentilesDto>;
  }

  @Get("geo-live")
  @UseInterceptors(new HttpCacheInterceptor(120))
  @ApiOperation({
    summary: "Get live geographic statistics",
    description:
      "Returns real-time statistics (open park count and average wait time) " +
      "for all continents, countries, and cities. Useful for showing live data " +
      "on geographic navigation pages. Cached for 2 minutes.",
  })
  @ApiResponse({
    status: 200,
    description: "Geographic live statistics with hierarchical structure",
    type: GeoLiveStatsDto,
  })
  @ApiResponse({
    status: 500,
    description: "Internal server error",
  })
  async getGeoLiveStats(): Promise<GeoLiveStatsDto> {
    return this.analyticsService.getGeoLiveStats();
  }
}
