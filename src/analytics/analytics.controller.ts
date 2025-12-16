import { Controller, Get, Param, UseInterceptors } from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse, ApiParam } from "@nestjs/swagger";
import { AnalyticsService } from "./analytics.service";
import {
  GlobalStatsDto,
  ParkPercentilesDto,
  AttractionPercentilesDto,
} from "./dto";
import { HttpCacheInterceptor } from "../common/interceptors/cache.interceptor";

@ApiTags("stats")
@Controller("analytics")
export class AnalyticsController {
  constructor(private readonly analyticsService: AnalyticsService) {}

  @Get("realtime")
  @UseInterceptors(new HttpCacheInterceptor(3 * 60)) // 3 minutes (queue data updates every 5min)
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
}
