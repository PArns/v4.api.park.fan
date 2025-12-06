import {
  Controller,
  Get,
  Param,
  Query,
  NotFoundException,
} from "@nestjs/common";
import { ShowsService } from "./shows.service";
import { ShowResponseDto, ShowWithLiveDataDto } from "./dto/show-response.dto";
import { ShowQueryDto } from "./dto/show-query.dto";

/**
 * Shows Controller
 *
 * Provides REST API endpoints for accessing show data.
 *
 * Endpoints:
 * - GET /shows - List all shows
 * - GET /shows/:slug - Get specific show
 */
@Controller("shows")
export class ShowsController {
  constructor(private readonly showsService: ShowsService) {}

  /**
   * GET /v1/shows
   *
   * Returns all shows globally with optional filtering and sorting.
   *
   * @param query - Filter and sort options (park, duration range, sort)
   */
  @Get()
  async findAll(@Query() query: ShowQueryDto): Promise<ShowResponseDto[]> {
    const shows = await this.showsService.findAllWithFilters(query);
    return shows.map((show) => ShowResponseDto.fromEntity(show));
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
