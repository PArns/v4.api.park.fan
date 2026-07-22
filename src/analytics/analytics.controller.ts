import { Controller, Get, UseInterceptors } from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse } from "@nestjs/swagger";
import { AnalyticsService } from "./analytics.service";
import {
  GlobalStatsDto,
  GeoLiveStatsDto,
  TickerResponseDto,
  GlobalBestTimesDto,
} from "./dto";
import { HttpCacheInterceptor } from "../common/interceptors/cache.interceptor";

@ApiTags("stats")
@Controller("analytics")
export class AnalyticsController {
  constructor(private readonly analyticsService: AnalyticsService) {}

  @Get("realtime")
  @UseInterceptors(new HttpCacheInterceptor(300))
  @ApiOperation({
    summary: "Get global platform statistics",
    description:
      "Returns real-time global statistics including open/closed parks, most/least crowded parks, longest/shortest wait rides, and platform-wide counts. Cached for 5 minutes for optimal performance.",
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
  @UseInterceptors(new HttpCacheInterceptor(300))
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
  @ApiResponse({
    status: 500,
    description: "Internal server error",
  })
  async getGeoLiveStats(): Promise<GeoLiveStatsDto> {
    return this.analyticsService.getGeoLiveStats();
  }

  @Get("ticker")
  @UseInterceptors(new HttpCacheInterceptor(300))
  @ApiOperation({
    summary: "Live wait-time ticker",
    description:
      "Returns the top 40 attractions sorted by current wait time across all open parks. " +
      "Only includes OPERATING attractions with a wait time > 0. Cached for 5 minutes.",
  })
  @ApiResponse({
    status: 200,
    description:
      "Sorted list of highest current wait times for the homepage ticker",
    type: TickerResponseDto,
  })
  async getTicker(): Promise<TickerResponseDto> {
    return this.analyticsService.getTickerData() as Promise<TickerResponseDto>;
  }

  @Get("best-times")
  @UseInterceptors(new HttpCacheInterceptor(24 * 60 * 60))
  @ApiOperation({
    summary: "Global best time to visit theme parks",
    description:
      "Relative busyness across all tracked parks, aggregated by weekday and by month " +
      "over the last 24 months. Each park is normalised to its own average first, so " +
      "large and small parks count equally — the honest 'when is it quietest overall' " +
      "signal for the best-time-to-visit hub. Cached for 24 hours.",
  })
  @ApiResponse({
    status: 200,
    description: "Global relative busyness by weekday and month",
    type: GlobalBestTimesDto,
  })
  async getBestTimes(): Promise<GlobalBestTimesDto> {
    return this.analyticsService.getGlobalBestTimes();
  }
}
