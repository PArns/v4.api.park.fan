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
  @UseInterceptors(new HttpCacheInterceptor(120)) // 2 minutes - live stats need freshness
  @ApiOperation({ summary: "Get global platform statistics" })
  @ApiResponse({
    status: 200,
    description: "Returns real-time global statistics about parks and rides",
    type: GlobalStatsDto,
  })
  async getGlobalStats(): Promise<GlobalStatsDto> {
    return this.analyticsService.getGlobalRealtimeStats();
  }

  @Get("parks/:parkId/percentiles")
  @UseInterceptors(new HttpCacheInterceptor(12 * 60 * 60)) // 12 hours (percentiles update daily)
  @ApiOperation({ summary: "Get complete percentile distribution for a park" })
  @ApiParam({ name: "parkId", description: "Park ID" })
  @ApiResponse({
    status: 200,
    description: "Returns today's percentiles + rolling windows (7d, 30d)",
    type: ParkPercentilesDto,
  })
  async getParkPercentiles(
    @Param("parkId") parkId: string,
  ): Promise<ParkPercentilesDto> {
    return this.analyticsService.getParkPercentiles(
      parkId,
    ) as Promise<ParkPercentilesDto>;
  }

  @Get("attractions/:attractionId/percentiles")
  @UseInterceptors(new HttpCacheInterceptor(12 * 60 * 60)) // 12 hours (percentiles update daily)
  @ApiOperation({
    summary: "Get complete percentile distribution for an attraction",
  })
  @ApiParam({ name: "attractionId", description: "Attraction ID" })
  @ApiResponse({
    status: 200,
    description: "Returns today's percentiles + hourly array + rolling windows",
    type: AttractionPercentilesDto,
  })
  async getAttractionPercentiles(
    @Param("attractionId") attractionId: string,
  ): Promise<AttractionPercentilesDto> {
    return this.analyticsService.getAttractionPercentiles(
      attractionId,
    ) as Promise<AttractionPercentilesDto>;
  }

  @Get("geo-live")
  @UseInterceptors(new HttpCacheInterceptor(120)) // 2 minutes - live stats
  @ApiOperation({
    summary: "Get live geographic statistics",
    description:
      "Returns real-time statistics (open park count and average wait time) " +
      "for all continents, countries, and cities. Useful for showing live data " +
      "on geographic navigation pages. Cached for 5 minutes.",
  })
  @ApiResponse({
    status: 200,
    description: "Geographic live statistics with hierarchical structure",
    type: GeoLiveStatsDto,
  })
  async getGeoLiveStats(): Promise<GeoLiveStatsDto> {
    return this.analyticsService.getGeoLiveStats();
  }
}
