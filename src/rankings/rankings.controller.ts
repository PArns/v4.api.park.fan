import {
  BadRequestException,
  Controller,
  Get,
  Param,
  UseInterceptors,
} from "@nestjs/common";
import { ApiOperation, ApiQuery, ApiResponse, ApiTags } from "@nestjs/swagger";
import { HttpCacheInterceptor } from "../common/interceptors/cache.interceptor";
import { ApiPermission } from "../api-keys/api-permission.decorator";
import { QueryInt } from "../common/pipes/query-int.pipe";
import { RankingsService } from "./rankings.service";
import { AttractionRankingDto } from "./dto/attraction-ranking.dto";

/** Most rides one request returns. */
const MAX_LIMIT = 200;

/** A ranking is always of one named year; there is no "current" default to drift on Jan 1. */
function requireYear(year: number | undefined): number {
  if (year === undefined) throw new BadRequestException("year is required");
  return year;
}

@ApiTags("rankings")
@Controller("rankings")
export class RankingsController {
  constructor(private readonly rankingsService: RankingsService) {}

  /**
   * GET /v1/rankings/attractions/:continent[/:country]?year=2026
   *
   * The rides with the longest waits of one calendar year in a country or a
   * continent. Cached 24 hours: the window grows by one day a day.
   */
  @Get("attractions/:continent")
  @ApiPermission("stats.history")
  @UseInterceptors(new HttpCacheInterceptor(24 * 60 * 60))
  @ApiOperation({
    summary: "Year ranking of rides by wait in a continent",
    description:
      "Every ride in the continent's parks with at least 60 measured days in the year, ordered by the average of its hourly P90 waits (the figure a park's `topAttractions` orders by). While the year runs, the window ends yesterday in each park's time zone. Cached for 24 hours.",
  })
  @ApiQuery({ name: "year", required: true, example: 2026 })
  @ApiQuery({ name: "limit", required: false, example: 50 })
  @ApiResponse({ status: 200, type: AttractionRankingDto })
  @ApiResponse({ status: 404, description: "No parks in this continent" })
  async getContinentRanking(
    @Param("continent") continent: string,
    @QueryInt({ name: "year", min: 2019, max: 2100 }) year: number | undefined,
    @QueryInt({ name: "limit", fallback: 50, min: 1, max: MAX_LIMIT })
    limit: number,
  ): Promise<AttractionRankingDto> {
    return this.rankingsService.getAttractionRanking(
      { continentSlug: continent, countrySlug: null },
      requireYear(year),
      limit,
    );
  }

  @Get("attractions/:continent/:country")
  @ApiPermission("stats.history")
  @UseInterceptors(new HttpCacheInterceptor(24 * 60 * 60))
  @ApiOperation({
    summary: "Year ranking of rides by wait in a country",
    description:
      "Every ride in the country's parks with at least 60 measured days in the year, ordered by the average of its hourly P90 waits (the figure a park's `topAttractions` orders by). While the year runs, the window ends yesterday in each park's time zone. Cached for 24 hours.",
  })
  @ApiQuery({ name: "year", required: true, example: 2026 })
  @ApiQuery({ name: "limit", required: false, example: 50 })
  @ApiResponse({ status: 200, type: AttractionRankingDto })
  @ApiResponse({ status: 404, description: "No parks in this country" })
  async getCountryRanking(
    @Param("continent") continent: string,
    @Param("country") country: string,
    @QueryInt({ name: "year", min: 2019, max: 2100 }) year: number | undefined,
    @QueryInt({ name: "limit", fallback: 50, min: 1, max: MAX_LIMIT })
    limit: number,
  ): Promise<AttractionRankingDto> {
    return this.rankingsService.getAttractionRanking(
      { continentSlug: continent, countrySlug: country },
      requireYear(year),
      limit,
    );
  }
}
