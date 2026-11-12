import {
  Controller,
  Get,
  Param,
  Query,
  Res,
  UseInterceptors,
} from "@nestjs/common";
import {
  ApiExtraModels,
  ApiOperation,
  ApiParam,
  ApiResponse,
  ApiTags,
} from "@nestjs/swagger";
import type { Response } from "express";
import { RequiresApiKey } from "../api-keys/requires-api-key.decorator";
import { HttpCacheInterceptor } from "../common/interceptors/cache.interceptor";
import { TripRegionService } from "./trip-region.service";
import { TripPlanService } from "./trip-plan.service";
import {
  TripCalendarQueryDto,
  TripPlanQueryDto,
} from "./dto/trip-region-query.dto";
import {
  TripCalendarDto,
  TripParkDayDto,
  TripPlanDto,
  TripRegionDto,
} from "./dto/trip-region-response.dto";

/**
 * The policy a keyed answer asks for, as `/best-days` does: an hour, a day of
 * stale grace. `ForecastAccessGuard` turns it private unless the Cloudflare
 * Vary rule is on, and then caps the shared lifetime at five minutes.
 */
const KEYED_CACHE_CONTROL =
  "public, max-age=3600, s-maxage=3600, stale-while-revalidate=86400";

/**
 * Trip regions: a curated region of parks (Orlando), its days side by side and
 * the plan that puts each park on its quietest trip day. The region route is
 * static facts and stays open; the calendar and the plan carry forecasts and
 * need a data key. Contract: docs/frontend/trip-regions.md.
 */
@ApiTags("trip-regions")
@ApiExtraModels(TripParkDayDto)
@Controller("trip-regions")
export class TripRegionsController {
  constructor(
    private readonly regions: TripRegionService,
    private readonly plans: TripPlanService,
  ) {}

  @Get(":region")
  @UseInterceptors(new HttpCacheInterceptor(60 * 60, 24 * 60 * 60))
  @ApiOperation({
    summary: "A trip region: its parks, groups, hotel areas and travel times",
    description:
      "No forecast figures, so no key. Travel times are a drive with no traffic from " +
      "OpenStreetMap (OSRM), stored in code; `alternatives` are the resort's own transport. " +
      "Cache: an hour in the browser, a day at the edge.",
  })
  @ApiParam({ name: "region", example: "orlando" })
  @ApiResponse({ status: 200, type: TripRegionDto })
  @ApiResponse({ status: 404, description: "Unknown region" })
  describe(@Param("region") region: string): Promise<TripRegionDto> {
    return this.regions.describe(region);
  }

  @Get(":region/calendar")
  @RequiresApiKey()
  @ApiOperation({
    summary: "Every park of a region, day by day, with its events",
    description:
      "Per day and park: status, the day guests' hours, the early and evening slots, " +
      "crowd level, the headliners' mean expected wait and where that figure comes from " +
      "(`basis`). From today to a year ahead, at most 92 days.",
  })
  @ApiParam({ name: "region", example: "orlando" })
  @ApiResponse({ status: 200, type: TripCalendarDto })
  @ApiResponse({
    status: 400,
    description: "Bad date, range over 92 days or past a year",
  })
  @ApiResponse({ status: 404, description: "Unknown region" })
  async calendar(
    @Param("region") region: string,
    @Query() query: TripCalendarQueryDto,
    @Res({ passthrough: true }) res: Response,
  ): Promise<TripCalendarDto> {
    const body = await this.regions.calendar(region, query.from, query.to);
    if (!res.headersSent) res.setHeader("Cache-Control", KEYED_CACHE_CONTROL);
    return body;
  }

  @Get(":region/plan")
  @RequiresApiKey()
  @ApiOperation({
    summary: "The optimized itinerary for a trip",
    description:
      "Puts each chosen park on the trip day where its headliners queue least, under the " +
      "trip's rules (blocks, rest days, hop days, party nights, hotel perks, pinned days). " +
      "At most 21 days, 16 park visits, 3 visits a park. Cached an hour by the query.",
  })
  @ApiParam({ name: "region", example: "orlando" })
  @ApiResponse({ status: 200, type: TripPlanDto })
  @ApiResponse({ status: 400, description: "Invalid trip, parks or options" })
  @ApiResponse({ status: 404, description: "Unknown region" })
  async plan(
    @Param("region") region: string,
    @Query() query: TripPlanQueryDto,
    @Res({ passthrough: true }) res: Response,
  ): Promise<TripPlanDto> {
    const body = await this.plans.plan(region, query);
    if (!res.headersSent) res.setHeader("Cache-Control", KEYED_CACHE_CONTROL);
    return body;
  }
}
