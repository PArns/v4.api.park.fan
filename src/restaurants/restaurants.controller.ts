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
import { RestaurantsService } from "./restaurants.service";
import {
  RestaurantResponseDto,
  RestaurantWithLiveDataDto,
} from "./dto/restaurant-response.dto";
import { RestaurantQueryDto } from "./dto/restaurant-query.dto";
import { AvailabilityResponseDto } from "./dto/availability-response.dto";
import { PaginatedResponseDto } from "../common/dto/pagination.dto";

/**
 * Restaurants Controller
 *
 * Provides REST API endpoints for accessing restaurant data.
 *
 * Endpoints:
 * - GET /restaurants - List all restaurants
 * - GET /restaurants/:slug - Get specific restaurant
 */
@ApiTags("restaurants")
@Controller("restaurants")
export class RestaurantsController {
  constructor(private readonly restaurantsService: RestaurantsService) {}

  /**
   * GET /v1/restaurants
   *
   * Returns all restaurants globally with optional filtering and sorting.
   * Now supports pagination with default limit of 10 items.
   *
   * @param query - Filter and sort options (park, cuisineType, requiresReservation, sort, page, limit)
   */
  @Get()
  @ApiOperation({
    summary: "List restaurants",
    description: "Returns a paginated list of all restaurants globally.",
  })
  @ApiExtraModels(PaginatedResponseDto, RestaurantResponseDto)
  @ApiResponse({
    status: 200,
    description: "List of restaurants",
    schema: {
      allOf: [
        { $ref: getSchemaPath(PaginatedResponseDto) },
        {
          properties: {
            data: {
              type: "array",
              items: { $ref: getSchemaPath(RestaurantResponseDto) },
            },
          },
        },
      ],
    },
  })
  async findAll(@Query() query: RestaurantQueryDto): Promise<{
    data: RestaurantResponseDto[];
    pagination: {
      page: number;
      limit: number;
      total: number;
      totalPages: number;
      hasNext: boolean;
      hasPrevious: boolean;
    };
  }> {
    const { data: restaurants, total } =
      await this.restaurantsService.findAllWithFilters(query);
    const mappedRestaurants = restaurants.map((restaurant) =>
      RestaurantResponseDto.fromEntity(restaurant),
    );

    return {
      data: mappedRestaurants,
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
   * GET /v1/restaurants/:slug
   *
   * Returns a specific restaurant with park information.
   *
   * @param slug - Restaurant slug (e.g., "be-our-guest-restaurant")
   * @throws NotFoundException if restaurant not found
   */
  @Get(":slug")
  @ApiOperation({
    summary: "Get restaurant details",
    description: "Returns details for a specific restaurant.",
  })
  @ApiResponse({
    status: 200,
    description: "Restaurant details",
    type: RestaurantWithLiveDataDto,
  })
  @ApiResponse({ status: 404, description: "Restaurant not found" })
  async findOne(
    @Param("slug") slug: string,
  ): Promise<RestaurantWithLiveDataDto> {
    const restaurant = await this.restaurantsService.findBySlug(slug);

    if (!restaurant) {
      throw new NotFoundException(`Restaurant with slug "${slug}" not found`);
    }

    // Get live data
    const liveData =
      await this.restaurantsService.findCurrentStatusByRestaurant(
        restaurant.id,
      );

    // Build integrated response
    const dto = RestaurantResponseDto.fromEntity(
      restaurant,
    ) as RestaurantWithLiveDataDto;

    if (liveData) {
      dto.status = liveData.status;
      dto.waitTime = liveData.waitTime ?? null; // Ensure null if undefined
      dto.partySize = liveData.partySize ?? null;
      dto.operatingHours = liveData.operatingHours;
      dto.lastUpdated = (
        liveData.lastUpdated || liveData.timestamp
      ).toISOString();
    } else {
      dto.status = "CLOSED"; // Default fallback
      dto.waitTime = null;
      dto.partySize = null;
      dto.operatingHours = [];
      dto.lastUpdated = new Date().toISOString();
    }

    return dto;
  }

  /**
   * GET /v1/restaurants/:slug/availability
   *
   * Returns current dining availability (wait times, party size) for a specific restaurant.
   *
   * @param slug - Restaurant slug
   * @throws NotFoundException if restaurant not found
   */
  @Get(":slug/availability")
  @ApiOperation({
    summary: "Get availability",
    description: "Returns dining availability for a specific restaurant.",
  })
  @ApiResponse({
    status: 200,
    description: "Availability data",
    type: AvailabilityResponseDto,
  })
  @ApiResponse({ status: 404, description: "Restaurant not found" })
  async getAvailability(
    @Param("slug") slug: string,
  ): Promise<AvailabilityResponseDto> {
    const restaurant = await this.restaurantsService.findBySlug(slug);

    if (!restaurant) {
      throw new NotFoundException(`Restaurant with slug "${slug}" not found`);
    }

    const liveData =
      await this.restaurantsService.findCurrentStatusByRestaurant(
        restaurant.id,
      );

    return {
      restaurant: {
        id: restaurant.id,
        name: restaurant.name,
        slug: restaurant.slug,
      },
      status: liveData?.status || "CLOSED",
      waitTime: liveData?.waitTime ?? null,
      partySize: liveData?.partySize ?? null,
      lastUpdated: liveData
        ? (liveData.lastUpdated || liveData.timestamp).toISOString()
        : null,
    };
  }
}
