import { Controller, Get, UseInterceptors } from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse } from "@nestjs/swagger";
import { AnalyticsService } from "./analytics.service";
import { GlobalStatsDto, GeoLiveStatsDto } from "./dto";
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
