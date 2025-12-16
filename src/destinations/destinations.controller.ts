import { Controller, Get, Param, NotFoundException } from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse } from "@nestjs/swagger";
import { DestinationsService } from "./destinations.service";
import { DestinationResponseDto } from "./dto/destination-response.dto";
import { DestinationWithParksDto } from "./dto/destination-with-parks.dto";

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
   *
   * Example: Walt Disney World Resort, Disneyland Resort, etc.
   */
  @Get()
  @ApiOperation({
    summary: "List all destinations",
    description: "Returns a list of all resort-level destinations.",
  })
  @ApiResponse({
    status: 200,
    description: "List of destinations retrieved successfully",
    type: [DestinationResponseDto],
  })
  async findAll(): Promise<DestinationResponseDto[]> {
    const destinations = await this.destinationsService.findAll();
    return destinations.map((dest) => DestinationResponseDto.fromEntity(dest));
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
