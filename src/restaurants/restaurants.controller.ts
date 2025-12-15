import {
  Controller,
  Get,
  Param,
  Query,
  NotFoundException,
} from "@nestjs/common";
import { ApiTags } from "@nestjs/swagger";
import { RestaurantsService } from "./restaurants.service";
import {
  RestaurantResponseDto,
  RestaurantWithLiveDataDto,
} from "./dto/restaurant-response.dto";
import { RestaurantQueryDto } from "./dto/restaurant-query.dto";

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
   *
   * @param query - Filter and sort options (park, cuisineType, requiresReservation, sort)
   */
  @Get()
  async findAll(
    @Query() query: RestaurantQueryDto,
  ): Promise<RestaurantResponseDto[]> {
    const restaurants = await this.restaurantsService.findAllWithFilters(query);
    return restaurants.map((restaurant) =>
      RestaurantResponseDto.fromEntity(restaurant),
    );
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
  async getAvailability(@Param("slug") slug: string): Promise<any> {
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
