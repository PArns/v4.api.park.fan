import {
  Controller,
  Get,
  Param,
  Query,
  NotFoundException,
} from "@nestjs/common";
import {
  ApiTags,
  ApiOperation,
  ApiResponse,
  ApiExtraModels,
  getSchemaPath,
} from "@nestjs/swagger";
import { ShowsService } from "./shows.service";
import { ShowResponseDto, ShowWithLiveDataDto } from "./dto/show-response.dto";
import { ShowQueryDto } from "./dto/show-query.dto";
import { PaginatedResponseDto } from "../common/dto/pagination.dto";

/**
 * Shows Controller
 *
 * Provides REST API endpoints for accessing show data.
 *
 * Endpoints:
 * - GET /shows - List all shows
 * - GET /shows/:slug - Get specific show
 */
@ApiTags("shows")
@Controller("shows")
export class ShowsController {
  constructor(private readonly showsService: ShowsService) {}

  /**
   * GET /v1/shows
   *
   * Returns all shows globally with optional filtering and sorting.
   * Now supports pagination with default limit of 10 items.
   *
   * @param query - Filter and sort options (park, duration range, sort, page, limit)
   */
  @Get()
  @ApiOperation({
    summary: "List shows",
    description: "Returns a paginated list of all shows globally.",
  })
  @ApiExtraModels(PaginatedResponseDto, ShowResponseDto)
  @ApiResponse({
    status: 200,
    description: "List of shows",
    schema: {
      allOf: [
        { $ref: getSchemaPath(PaginatedResponseDto) },
        {
          properties: {
            data: {
              type: "array",
              items: { $ref: getSchemaPath(ShowResponseDto) },
            },
          },
        },
      ],
    },
  })
  async findAll(@Query() query: ShowQueryDto): Promise<{
    data: ShowResponseDto[];
    pagination: {
      page: number;
      limit: number;
      total: number;
      totalPages: number;
      hasNext: boolean;
      hasPrevious: boolean;
    };
  }> {
    const { data: shows, total } =
      await this.showsService.findAllWithFilters(query);
    const mappedShows = shows.map((show) => ShowResponseDto.fromEntity(show));

    return {
      data: mappedShows,
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
   * GET /v1/shows/:slug
   *
   * Returns a specific show with park information.
   *
   * @param slug - Show slug (e.g., "festival-of-the-lion-king")
   * @throws NotFoundException if show not found
   */
  @Get(":slug")
  @ApiOperation({
    summary: "Get show details",
    description: "Returns details for a specific show.",
  })
  @ApiResponse({
    status: 200,
    description: "Show details",
    type: ShowWithLiveDataDto,
  })
  @ApiResponse({ status: 404, description: "Show not found" })
  async findOne(@Param("slug") slug: string): Promise<ShowWithLiveDataDto> {
    const show = await this.showsService.findBySlug(slug);

    if (!show) {
      throw new NotFoundException(`Show with slug "${slug}" not found`);
    }

    // Get live data
    const liveData = await this.showsService.findCurrentStatusByShow(show.id);

    // Build integrated response
    const dto = ShowResponseDto.fromEntity(show) as ShowWithLiveDataDto;

    if (liveData) {
      dto.status = liveData.status;
      dto.showtimes = liveData.showtimes;
      dto.operatingHours = liveData.operatingHours;
      dto.lastUpdated = (
        liveData.lastUpdated || liveData.timestamp
      ).toISOString();
    } else {
      dto.status = "CLOSED"; // Default fallback
      dto.showtimes = [];
      dto.operatingHours = [];
      dto.lastUpdated = new Date().toISOString();
    }

    return dto;
  }

  /**
   * GET /v1/shows/:slug/showtimes
   *
   * Returns upcoming showtimes for a specific show.
   *
   * @param slug - Show slug
   * @throws NotFoundException if show not found
   */
  @Get(":slug/showtimes")
  @ApiOperation({
    summary: "Get showtimes",
    description: "Returns upcoming showtimes for a specific show.",
  })
  @ApiResponse({ status: 200, description: "Showtimes data" })
  @ApiResponse({ status: 404, description: "Show not found" })
  async getShowtimes(@Param("slug") slug: string): Promise<any> {
    const show = await this.showsService.findBySlug(slug);

    if (!show) {
      throw new NotFoundException(`Show with slug "${slug}" not found`);
    }

    const liveData = await this.showsService.findCurrentStatusByShow(show.id);

    return {
      show: {
        id: show.id,
        name: show.name,
        slug: show.slug,
      },
      showtimes: liveData?.showtimes || [],
      lastUpdated: liveData
        ? (liveData.lastUpdated || liveData.timestamp).toISOString()
        : null,
    };
  }
}
