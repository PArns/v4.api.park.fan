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
import { TripWhenService } from "./trip-when.service";
import { MeasureTripRequest } from "./metrics/trip-request-metrics.interceptor";
import type { TripWhen } from "./trip-when";
import type { TripSchoolBreaks } from "./trip-school-breaks";
import {
  TripSchoolBreaksDto,
  TripSchoolBreaksQueryDto,
  TripWhenDto,
  TripWhenQueryDto,
} from "./dto/trip-when.dto";
import {
  TripBestDaysQueryDto,
  TripCalendarQueryDto,
  TripPlanQueryDto,
} from "./dto/trip-region-query.dto";
import type { TripBestDays } from "./trip-best-days";
import {
  TripCalendarDto,
  TripParkDayDto,
  TripPlanDto,
  TripRegionDto,
  TripRegionListDto,
} from "./dto/trip-region-response.dto";
import {
  TripPlanTicketDto,
  TripTicketCatalogDto,
  TripTicketDaysRangeDto,
  TripTicketSuggestionDto,
} from "./dto/trip-tickets.dto";

/**
 * The policy a keyed answer asks for, as `/best-days` does: an hour, a day of
 * stale grace. `ForecastAccessGuard` turns it private unless the Cloudflare
 * Vary rule is on, and then caps the shared lifetime at five minutes.
 */
const KEYED_CACHE_CONTROL =
  "public, max-age=3600, s-maxage=3600, stale-while-revalidate=86400";

/**
 * Trip regions: curated regions of parks (Orlando, Southern California, Japan,
 * Paris), their days side by side and the plan that puts each park on its
 * quietest trip day. The list and the region route are static facts and stay
 * open; the calendar and the plan carry forecasts and need a data key.
 * The plan, `/when` and `/best-days` each leave a metric row
 * (docs/frontend/trip-regions.md#what-the-server-records).
 * Contract: docs/frontend/trip-regions.md.
 */
@ApiTags("trip-regions")
@ApiExtraModels(
  TripParkDayDto,
  TripTicketDaysRangeDto,
  TripPlanTicketDto,
  TripTicketSuggestionDto,
)
@Controller("trip-regions")
export class TripRegionsController {
  constructor(
    private readonly regions: TripRegionService,
    private readonly plans: TripPlanService,
    private readonly whens: TripWhenService,
  ) {}

  @Get()
  @UseInterceptors(new HttpCacheInterceptor(60 * 60, 24 * 60 * 60))
  @ApiOperation({
    summary: "Every trip region: slug, name, timezone, park count and groups",
    description:
      "For the site's navigation: one entry per region with a planner, in a fixed order. " +
      "`parkCount` is the region route's `parks`. No key. Cache: an hour in the browser, " +
      "a day at the edge.",
  })
  @ApiResponse({ status: 200, type: TripRegionListDto })
  list(): Promise<TripRegionListDto> {
    return this.regions.list();
  }

  @Get(":region")
  @UseInterceptors(new HttpCacheInterceptor(60 * 60, 24 * 60 * 60))
  @ApiOperation({
    summary: "A trip region: its parks, groups, hotel areas and travel times",
    description:
      "No forecast figures, so no key. Travel times are a drive with no traffic from " +
      "OpenStreetMap (OSRM) or curated from timetables (`travel.source`), stored in code; " +
      "`alternatives` are the resort's own transport. Cache: an hour in the browser, a day at the edge.",
  })
  @ApiParam({ name: "region", example: "orlando" })
  @ApiResponse({ status: 200, type: TripRegionDto })
  @ApiResponse({ status: 404, description: "Unknown region" })
  describe(@Param("region") region: string): Promise<TripRegionDto> {
    return this.regions.describe(region);
  }

  @Get(":region/tickets")
  // Five minutes at the edge and ten stale: an admin edit reaches every reader
  // within 15 minutes, and the second revalidation after CDN_SETTLE_MS lands
  // after the edge has let go.
  @UseInterceptors(new HttpCacheInterceptor(5 * 60))
  @ApiOperation({
    summary: "The region's ticket catalog, as the admin keeps it",
    description:
      "The active products in order, their rules and purchase links, and the catalog's version. " +
      "No key, no prices. Cache: five minutes in the browser and at the edge, ten more stale. " +
      "Clients read the catalog here, not off the region route.",
  })
  @ApiParam({ name: "region", example: "orlando" })
  @ApiResponse({ status: 200, type: TripTicketCatalogDto })
  @ApiResponse({ status: 404, description: "Unknown region" })
  tickets(@Param("region") region: string): Promise<TripTicketCatalogDto> {
    return this.regions.ticketCatalog(region);
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

  @Get(":region/best-days")
  @RequiresApiKey()
  @MeasureTripRequest("best_days")
  @ApiOperation({
    summary:
      "Per park its quietest and busiest days, per resort the quietest dates",
    description:
      "Read off the region calendar for the window (default today … +30, at most 92 days), " +
      "ranked on the unrounded headliner wait; a night the day guests lose to a party ranks " +
      "behind every full day. `last_year` days are included with their basis.",
  })
  @ApiParam({ name: "region", example: "orlando" })
  @ApiResponse({ status: 200, description: "The best days" })
  @ApiResponse({
    status: 400,
    description: "Bad date, range over 92 days or past a year",
  })
  @ApiResponse({ status: 404, description: "Unknown region" })
  async bestDays(
    @Param("region") region: string,
    @Query() query: TripBestDaysQueryDto,
    @Res({ passthrough: true }) res: Response,
  ): Promise<TripBestDays> {
    const body = await this.regions.bestDays(
      region,
      query.from,
      query.to,
      query.limit ?? 5,
    );
    if (!res.headersSent) res.setHeader("Cache-Control", KEYED_CACHE_CONTROL);
    return body;
  }

  @Get(":region/plan")
  @RequiresApiKey()
  @MeasureTripRequest("plan")
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
  @Get(":region/when")
  @RequiresApiKey()
  @MeasureTripRequest("when")
  @ApiOperation({
    summary: "The best dates for a trip of given parks and length",
    description:
      "Ranks every window of `days` days in the search range (default today … +365, at most " +
      "366 days) by the plan `/plan` would give it, and returns the best `limit`, at least " +
      "`gap` days apart, with the year strip by ISO week and calendar month. `within` keeps " +
      "the windows inside a school-holiday region's breaks (`/school-breaks`). Cached six hours.",
  })
  @ApiParam({ name: "region", example: "orlando" })
  @ApiResponse({ status: 200, type: TripWhenDto })
  @ApiResponse({
    status: 400,
    description:
      "Bad range (over 366 days, past a year, in the past), days outside 3–21, unknown within code, or invalid trip options",
  })
  @ApiResponse({ status: 404, description: "Unknown region" })
  async when(
    @Param("region") region: string,
    @Query() query: TripWhenQueryDto,
    @Res({ passthrough: true }) res: Response,
  ): Promise<TripWhen> {
    const body = await this.whens.when(region, query);
    if (!res.headersSent) res.setHeader("Cache-Control", KEYED_CACHE_CONTROL);
    return body;
  }

  @Get(":region/school-breaks")
  @UseInterceptors(new HttpCacheInterceptor(60 * 60, 24 * 60 * 60))
  @ApiOperation({
    summary:
      "School breaks of the visitors' home regions, by stored region code",
    description:
      "Per holiday region its breaks in the range (default today … +365), each widened over the " +
      "weekends at its edges, and `through`, the last day the stored data reaches. The codes are " +
      "the ones `/when?within=` accepts. No forecast, so no key. Cache: an hour in the browser, " +
      "a day at the edge.",
  })
  @ApiParam({ name: "region", example: "orlando" })
  @ApiResponse({ status: 200, type: TripSchoolBreaksDto })
  @ApiResponse({ status: 400, description: "Bad range or country list" })
  @ApiResponse({ status: 404, description: "Unknown region" })
  schoolBreaks(
    @Param("region") region: string,
    @Query() query: TripSchoolBreaksQueryDto,
  ): Promise<TripSchoolBreaks> {
    return this.whens.schoolBreaks(
      region,
      query.from,
      query.to,
      query.countries,
    );
  }
}
