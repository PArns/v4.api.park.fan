import {
  Controller,
  Get,
  Param,
  Query,
  NotFoundException,
} from "@nestjs/common";
import { AttractionsService } from "./attractions.service";
import { AttractionIntegrationService } from "./services/attraction-integration.service";
import { AttractionResponseDto } from "./dto/attraction-response.dto";
import { AttractionQueryDto } from "./dto/attraction-query.dto";

// Removed EntityLiveResponse as it's not used in the provided code snippet
// Removed Entity as it's not used and replaced Controller which is used

/**
 * Attractions Controller
 *
 * Provides REST API endpoints for accessing attraction data.
 *
 * Endpoints:
 * - GET /attractions - List all attractions
 * - GET /attractions/:slug - Get specific attraction (integrated with live data)
 */
@Controller("attractions")
export class AttractionsController {
  constructor(
    private readonly attractionsService: AttractionsService,
    private readonly attractionIntegrationService: AttractionIntegrationService,
  ) {}

  /**
   * GET /v1/attractions
   *
   * Returns all attractions globally with optional filtering and sorting.
   *
   * @param query - Filter and sort options (park, status, queueType, waitTime range, sort)
   *
   * Note: This can return a large dataset (4000+ attractions).
   * Consider using pagination in future versions.
   */
  @Get()
  async findAll(
    @Query() query: AttractionQueryDto,
  ): Promise<AttractionResponseDto[]> {
    const attractions = await this.attractionsService.findAllWithFilters(query);
    return attractions.map((attraction) =>
      AttractionResponseDto.fromEntity(attraction),
    );
  }

  /**
   * GET /v1/attractions/:slug
   *
   * Returns a specific attraction with integrated live data:
   * - Current queue data (all queue types)
   * - Status (OPERATING, DOWN, CLOSED, REFURBISHMENT)
   * - Forecasts (next 24 hours predictions)
   *
   * @param slug - Attraction slug (e.g., "space-mountain")
   * @throws NotFoundException if attraction not found
   */
  @Get(":slug")
  async findOne(@Param("slug") slug: string): Promise<AttractionResponseDto> {
    const attraction = await this.attractionsService.findBySlug(slug);

    if (!attraction) {
      throw new NotFoundException(`Attraction with slug "${slug}" not found`);
    }

    return this.attractionIntegrationService.buildIntegratedResponse(
      attraction,
    );
  }
}
