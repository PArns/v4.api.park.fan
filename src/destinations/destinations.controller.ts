import {
  Controller,
  Get,
  Param,
  Query,
  NotFoundException,
} from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse } from "@nestjs/swagger";
import { DestinationsService } from "./destinations.service";
import { DestinationResponseDto } from "./dto/destination-response.dto";
import { DestinationWithParksDto } from "./dto/destination-with-parks.dto";
import { DestinationQueryDto } from "./dto/destination-query.dto";

/**
 * Destinations Controller
 *
 * Provides REST API endpoints for accessing destination (resort-level) data.
 *
 * Endpoints:
 * - GET /destinations - List all destinations
 * - GET /destinations/:slug - Get specific destination with parks
 */
@ApiTags("destinations")
@Controller("destinations")
export class DestinationsController {
  constructor(private readonly destinationsService: DestinationsService) {}

  /**
   * GET /v1/destinations
   *
   * Returns all destinations (resort-level entities).
   * Supports pagination.
   *
   * Example: Walt Disney World Resort, Disneyland Resort, etc.
   */
  @Get()
  @ApiOperation({
    summary: "List all destinations",
    description: "Returns a paginated list of all resort-level destinations.",
  })
  @ApiResponse({
    status: 200,
    description: "List of destinations retrieved successfully",
    type: DestinationResponseDto, // Swagger will not show pagination structure automatically without generic wrap detailed manual type, keeping simple for now
  })
  async findAll(@Query() query: DestinationQueryDto): Promise<{
    data: DestinationResponseDto[];
    pagination: {
      page: number;
      limit: number;
      total: number;
      totalPages: number;
      hasNext: boolean;
      hasPrevious: boolean;
    };
  }> {
    const { page = 1, limit = 10 } = query;
    const { data: destinations, total } =
      await this.destinationsService.findAll(page, limit);

    const mappedDestinations = destinations.map((dest) =>
      DestinationResponseDto.fromEntity(dest),
    );

    return {
      data: mappedDestinations,
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit),
        hasNext: page < Math.ceil(total / limit),
        hasPrevious: page > 1,
      },
    };
  }

  /**
   * GET /v1/destinations/:slug
   *
   * Returns a specific destination with all its parks.
   *
   * @param slug - Destination slug (e.g., "walt-disney-world-resort")
   * @throws NotFoundException if destination not found
   */
  @Get(":slug")
  @ApiOperation({
    summary: "Get a destination by slug",
    description:
      "Returns detailed information about a specific destination, including its parks.",
  })
  @ApiResponse({
    status: 200,
    description: "Destination details retrieved successfully",
    type: DestinationWithParksDto,
  })
  @ApiResponse({ status: 404, description: "Destination not found" })
  async findOne(@Param("slug") slug: string): Promise<DestinationWithParksDto> {
    const destination = await this.destinationsService.findBySlug(slug);

    if (!destination) {
      throw new NotFoundException(`Destination with slug "${slug}" not found`);
    }

    return DestinationWithParksDto.fromEntity(destination);
  }
}
