import {
  Controller,
  Get,
  Param,
  Query,
  NotFoundException,
} from "@nestjs/common";
import { ApiTags } from "@nestjs/swagger";
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
@ApiTags("attractions")
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
   * Now supports pagination with default limit of 10 items.
   *
   * @param query - Filter and sort options (park, status, queueType, waitTime range, sort, page, limit)
   */
  @Get()
  async findAll(@Query() query: AttractionQueryDto): Promise<{
    data: AttractionResponseDto[];
    pagination: {
      page: number;
      limit: number;
      total: number;
      totalPages: number;
      hasNext: boolean;
      hasPrevious: boolean;
    };
  }> {
    const { data: attractions, total } =
      await this.attractionsService.findAllWithFilters(query);
    const mappedAttractions = attractions.map((attraction) =>
      AttractionResponseDto.fromEntity(attraction),
    );

    return {
      data: mappedAttractions,
      pagination: {
        page: query.page || 1,
        limit: query.limit || 10,
        total,
        totalPages: Math.ceil(total / (query.limit || 10)),
        hasNext: (query.page || 1) < Math.ceil(total / (query.limit || 10)),
        hasPrevious: (query.page || 1) > 1,
      },
    };
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
