import { Controller, Get, Query, UseInterceptors } from "@nestjs/common";
import {
  ApiTags,
  ApiOperation,
  ApiResponse,
  ApiQuery,
  ApiExtraModels,
} from "@nestjs/swagger";
import { FavoritesService } from "./favorites.service";
import { FavoritesQueryDto } from "./dto/favorites-request.dto";
import { FavoritesResponseDto } from "./dto/favorites-response.dto";
import {
  ParkWithDistanceDto,
  AttractionWithDistanceDto,
  ShowWithDistanceDto,
  RestaurantWithDistanceDto,
} from "./dto/favorites-response.dto";
import { HttpCacheInterceptor } from "../common/interceptors/cache.interceptor";

/**
 * Favorites Controller
 *
 * Provides endpoint to retrieve favorite entities (parks, attractions, shows, restaurants)
 * with full information including live data and optional distance calculations.
 */
@ApiTags("favorites")
@Controller("favorites")
export class FavoritesController {
  constructor(private readonly favoritesService: FavoritesService) {}

  /**
   * GET /v1/favorites
   *
   * Returns favorite entities with full information grouped by type.
   * Supports optional distance calculation if user location is provided.
   */
  @Get()
  @UseInterceptors(new HttpCacheInterceptor(120)) // 2 minutes - live data
  @ApiExtraModels(
    FavoritesResponseDto,
    ParkWithDistanceDto,
    AttractionWithDistanceDto,
    ShowWithDistanceDto,
    RestaurantWithDistanceDto,
  )
  @ApiOperation({
    summary: "Get favorites with full information",
    description:
      "Returns favorite parks, attractions, shows, and restaurants with complete information " +
      "including live data (status, wait times, showtimes, dining availability, etc.). " +
      "Data is grouped by entity type for easy consumption. " +
      "If latitude and longitude are provided, distances from user location are calculated in meters. " +
      "Cached for 2 minutes for optimal performance.",
  })
  @ApiQuery({
    name: "parkIds",
    description:
      "Comma-separated list of park IDs (UUIDs). Supports both comma-separated string and array format.",
    example: "abc-123,def-456",
    required: false,
    type: String,
  })
  @ApiQuery({
    name: "attractionIds",
    description:
      "Comma-separated list of attraction (ride) IDs (UUIDs). Supports both comma-separated string and array format.",
    example: "xyz-789,uvw-012",
    required: false,
    type: String,
  })
  @ApiQuery({
    name: "showIds",
    description:
      "Comma-separated list of show IDs (UUIDs). Supports both comma-separated string and array format.",
    example: "show-123,show-456",
    required: false,
    type: String,
  })
  @ApiQuery({
    name: "restaurantIds",
    description:
      "Comma-separated list of restaurant IDs (UUIDs). Supports both comma-separated string and array format.",
    example: "rest-123,rest-456",
    required: false,
    type: String,
  })
  @ApiQuery({
    name: "lat",
    description:
      "User latitude for distance calculation (WGS84). Must be between -90 and 90. " +
      "If provided with lng, distances will be calculated for all entities with coordinates.",
    example: 48.266,
    required: false,
    type: Number,
  })
  @ApiQuery({
    name: "lng",
    description:
      "User longitude for distance calculation (WGS84). Must be between -180 and 180. " +
      "If provided with lat, distances will be calculated for all entities with coordinates.",
    example: 7.722,
    required: false,
    type: Number,
  })
  @ApiResponse({
    status: 200,
    description: "Favorites retrieved successfully with live data",
    type: FavoritesResponseDto,
  })
  @ApiResponse({
    status: 400,
    description:
      "Invalid query parameters (e.g., invalid latitude/longitude range)",
  })
  async getFavorites(
    @Query() query: FavoritesQueryDto,
  ): Promise<FavoritesResponseDto> {
    // Build user location if provided
    const userLocation =
      query.lat !== undefined && query.lng !== undefined
        ? { latitude: query.lat, longitude: query.lng }
        : undefined;

    return this.favoritesService.getFavorites(
      query.parkIds || [],
      query.attractionIds || [],
      query.showIds || [],
      query.restaurantIds || [],
      userLocation,
    );
  }
}
