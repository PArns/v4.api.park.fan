import { Controller, Get, Query, Req, UseInterceptors } from "@nestjs/common";
import {
  ApiTags,
  ApiOperation,
  ApiResponse,
  ApiQuery,
  ApiExtraModels,
} from "@nestjs/swagger";
import { Request } from "express";
import { getClientIp, normalizeIp } from "../common/utils/request.util";
import { GeoipService } from "../geoip/geoip.service";
import { FavoritesService } from "./favorites.service";
import { FavoritesQueryDto } from "./dto/favorites-request.dto";
import { FavoritesResponseDto } from "./dto/favorites-response.dto";
import {
  AttractionWithDistanceDto,
  ShowWithDistanceDto,
  RestaurantWithDistanceDto,
} from "./dto/favorites-response.dto";
import { ParkWithDistanceDto } from "../common/dto/park-with-distance.dto";
import { NoCdnCacheInterceptor } from "../common/interceptors/no-cdn-cache.interceptor";

/**
 * Favorites Controller
 *
 * Provides endpoint to retrieve favorite entities (parks, attractions, shows, restaurants)
 * with full information including live data and optional distance calculations.
 */
@ApiTags("favorites")
@Controller("favorites")
export class FavoritesController {
  constructor(
    private readonly favoritesService: FavoritesService,
    private readonly geoipService: GeoipService,
  ) {}

  /**
   * GET /v1/favorites
   *
   * Returns favorite entities with full information grouped by type.
   * Supports optional distance calculation if user location is provided.
   */
  @Get()
  @UseInterceptors(new NoCdnCacheInterceptor()) // Response can depend on client IP (GeoIP) – must not be CDN-cached
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
      "If latitude and longitude are provided (or derived from IP via GeoIP when omitted), distances from user location are calculated in meters. " +
      "Cached in Redis and HTTP cache for 2 minutes for optimal performance. " +
      "Uses stale-while-revalidate pattern to refresh cache in background when TTL < 1 minute.",
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
      "User latitude for distance calculation (WGS84). If omitted, location may be derived from IP (GeoLite2-City) when available.",
    example: 48.266,
    required: false,
    type: Number,
  })
  @ApiQuery({
    name: "lng",
    description:
      "User longitude for distance calculation (WGS84). If omitted, location may be derived from IP (GeoLite2-City) when available.",
    example: 7.722,
    required: false,
    type: Number,
  })
  @ApiQuery({
    name: "ip",
    description:
      "IP address for GeoIP lookup (debug). If omitted, uses X-Forwarded-For or request IP.",
    required: false,
    type: String,
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
    @Query("ip") ipParam: string | undefined,
    @Req() req: Request | undefined,
  ): Promise<FavoritesResponseDto> {
    // Resolve user location: 1) lat/lng params, 2) ip param or request IP → GeoIP
    let userLocation: { latitude: number; longitude: number } | undefined =
      undefined;

    if (query.lat !== undefined && query.lng !== undefined) {
      const lat = Number(query.lat);
      const lng = Number(query.lng);
      if (
        !Number.isNaN(lat) &&
        !Number.isNaN(lng) &&
        lat >= -90 &&
        lat <= 90 &&
        lng >= -180 &&
        lng <= 180
      ) {
        userLocation = { latitude: lat, longitude: lng };
      }
    }

    if (!userLocation && this.geoipService.isAvailable()) {
      const rawIp = ipParam?.trim() || getClientIp(req ?? undefined) || "";
      const ip = rawIp ? normalizeIp(rawIp) : "";
      if (ip) {
        const coords = this.geoipService.lookupCoordinates(ip);
        if (coords) {
          userLocation = {
            latitude: coords.latitude,
            longitude: coords.longitude,
          };
        }
      }
    }

    return this.favoritesService.getFavorites(
      query.parkIds || [],
      query.attractionIds || [],
      query.showIds || [],
      query.restaurantIds || [],
      userLocation,
    );
  }
}
