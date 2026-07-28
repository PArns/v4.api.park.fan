import { Controller, Get, Param, Query, UseInterceptors } from "@nestjs/common";
import {
  ApiOperation,
  ApiParam,
  ApiQuery,
  ApiResponse,
  ApiTags,
} from "@nestjs/swagger";
import { RideProfileService } from "./services/ride-profile.service";
import { TermAttractionDto, mapTermAttraction } from "./dto/ride-profile.dto";
import { HttpCacheInterceptor } from "../common/interceptors/cache.interceptor";

/**
 * The glossary → rides direction of the ride/glossary link.
 *
 * The ride page gets its terms embedded in the attraction response; this is
 * the other way round, so a glossary term page can list "rides with a zero-g
 * roll" or "rides built by Mack Rides".
 *
 * The data is hand-curated seed that only changes when someone edits the seed
 * file, so it is cached hard.
 */
@ApiTags("glossary")
@Controller("glossary/terms")
export class GlossaryRidesController {
  constructor(private readonly rideProfileService: RideProfileService) {}

  /** How many rides carry each term, for the whole curated set. */
  @Get("counts")
  @UseInterceptors(new HttpCacheInterceptor(3600))
  @ApiOperation({
    summary: "Ride counts per glossary term",
    description:
      "Map of glossary term id → number of curated rides that feature it. " +
      "Used by the glossary overview to badge terms that have rides.",
  })
  @ApiResponse({ status: 200, description: "Term id → ride count" })
  async counts(): Promise<Record<string, number>> {
    return this.rideProfileService.countByTerm();
  }

  @Get(":termId/attractions")
  @UseInterceptors(new HttpCacheInterceptor(3600))
  @ApiOperation({
    summary: "Rides that feature a glossary term",
    description:
      "Every curated ride whose track figures, ride type or manufacturer " +
      "matches the term. Ordered by park name, then ride name.",
  })
  @ApiParam({
    name: "termId",
    description:
      "Glossary term id, e.g. 'zero-g-roll', 'dive-coaster', 'b-and-m'",
    example: "zero-g-roll",
  })
  @ApiQuery({
    name: "limit",
    required: false,
    description: "Maximum rides to return (default 200, max 500)",
    example: 200,
  })
  @ApiResponse({ status: 200, type: [TermAttractionDto] })
  async attractionsForTerm(
    @Param("termId") termId: string,
    @Query("limit") limit?: string,
  ): Promise<{ termId: string; total: number; data: TermAttractionDto[] }> {
    const parsed = Number.parseInt(limit ?? "", 10);
    const capped = Number.isFinite(parsed)
      ? Math.min(Math.max(parsed, 1), 500)
      : 200;

    const rows = await this.rideProfileService.findAttractionsByTerm(
      termId,
      capped,
    );
    return {
      termId,
      total: rows.length,
      data: rows.map(mapTermAttraction),
    };
  }
}
