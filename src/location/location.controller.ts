import {
  Controller,
  Get,
  Query,
  Req,
  BadRequestException,
  UseInterceptors,
} from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse, ApiQuery } from "@nestjs/swagger";
import { Request } from "express";
import { getClientIp, normalizeIp } from "../common/utils/request.util";
import { LocationService } from "./location.service";
import { GeoipService } from "../geoip/geoip.service";
import { NearbyResponseDto } from "./dto/nearby-response.dto";
import { HttpCacheInterceptor } from "../common/interceptors/cache.interceptor";

/**
 * Location Controller
 *
 * Provides location-based discovery endpoints.
 * Allows users to find nearby parks and attractions based on their geographic coordinates.
 */
@ApiTags("discovery")
@Controller("discovery")
export class LocationController {
  constructor(
    private readonly locationService: LocationService,
    private readonly geoipService: GeoipService,
  ) {}

  /**
   * GET /v1/discovery/nearby
   *
   * Find nearby parks or rides based on user location.
   * If user is within a park's radius (default 500m), returns rides in that park.
   * Otherwise, returns up to 5 nearest parks.
   */
  @Get("nearby")
  @UseInterceptors(new HttpCacheInterceptor(60)) // 1 minute - fresher park status
  @ApiOperation({
    summary: "Find nearby parks or rides",
    description:
      "Returns rides if user is within a park (default 1000m radius), " +
      "otherwise returns up to 6 nearest parks with live statistics. " +
      "Rides are sorted by distance from user position.",
  })
  @ApiQuery({
    name: "lat",
    description:
      "User latitude. If omitted, location is derived from IP (GeoLite2-City) when possible.",
    example: 48.266,
    required: false,
    type: Number,
  })
  @ApiQuery({
    name: "lng",
    description:
      "User longitude. If omitted, location is derived from IP (GeoLite2-City) when possible.",
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
  @ApiQuery({
    name: "radius",
    description: "Radius in meters to consider 'in park' (default: 1000)",
    example: 1000,
    required: false,
    type: Number,
  })
  @ApiQuery({
    name: "limit",
    description: "Maximum number of parks to return (default: 6)",
    example: 6,
    required: false,
    type: Number,
  })
  @ApiResponse({
    status: 200,
    description: "Successfully found nearby parks or rides",
    type: NearbyResponseDto,
    examples: {
      in_park: {
        summary: "User is in a park",
        value: {
          type: "in_park",
          userLocation: {
            latitude: 48.266,
            longitude: 7.722,
          },
          data: {
            park: {
              id: "abc-123",
              name: "Europa-Park",
              slug: "europa-park",
              distance: 150,
              status: "OPERATING",
              analytics: {
                avgWaitTime: 25,
                crowdLevel: "moderate",
                operatingAttractions: 28,
              },
            },
            rides: [
              {
                id: "xyz-789",
                name: "Blue Fire Megacoaster",
                slug: "blue-fire",
                distance: 250,
                waitTime: 35,
                status: "OPERATING",
                analytics: {
                  p50: 30,
                  p90: 45,
                },
                url: "/europe/germany/rust/europa-park/blue-fire",
              },
            ],
          },
        },
      },
      nearby_parks: {
        summary: "User is outside all parks",
        value: {
          type: "nearby_parks",
          userLocation: {
            latitude: 52.52,
            longitude: 13.405,
          },
          data: {
            parks: [
              {
                id: "def-456",
                name: "Europa-Park",
                slug: "europa-park",
                distance: 650000,
                city: "Rust",
                country: "Germany",
                status: "OPERATING",
                totalAttractions: 35,
                operatingAttractions: 28,
                analytics: {
                  avgWaitTime: 25,
                  crowdLevel: "moderate",
                  occupancy: 65,
                },
                url: "/europe/germany/rust/europa-park",
              },
            ],
            count: 5,
          },
        },
      },
    },
  })
  @ApiResponse({
    status: 400,
    description: "Invalid coordinates or parameters",
    example: {
      statusCode: 400,
      message: "Invalid latitude or longitude",
      error: "Bad Request",
    },
  })
  async getNearby(
    @Query("lat") lat: string | undefined,
    @Query("lng") lng: string | undefined,
    @Query("ip") ipParam: string | undefined,
    @Query("radius") radius?: string,
    @Query("limit") limit?: string,
    @Req() req?: Request,
  ): Promise<NearbyResponseDto> {
    // Resolve coordinates: 1) lat/lng params, 2) ip param (debug), 3) X-Forwarded-For, 4) request IP → GeoIP
    let latitude: number | null = null;
    let longitude: number | null = null;

    if (lat != null && lng != null) {
      const parsedLat = parseFloat(lat);
      const parsedLng = parseFloat(lng);
      if (
        !Number.isNaN(parsedLat) &&
        !Number.isNaN(parsedLng) &&
        parsedLat >= -90 &&
        parsedLat <= 90 &&
        parsedLng >= -180 &&
        parsedLng <= 180
      ) {
        latitude = parsedLat;
        longitude = parsedLng;
      }
    }

    if (latitude === null || longitude === null) {
      const rawIp = ipParam?.trim() || getClientIp(req ?? undefined) || "";
      const ip = rawIp ? normalizeIp(rawIp) : "";
      if (!ip || !this.geoipService.isAvailable()) {
        throw new BadRequestException(
          "Location required. Provide lat and lng, or ensure GeoIP is configured (GEOIP_* env) and the request carries a valid client IP (e.g. X-Forwarded-For).",
        );
      }
      const coords = this.geoipService.lookupCoordinates(ip);
      if (!coords) {
        throw new BadRequestException(
          "Could not resolve location from IP. Provide lat and lng explicitly.",
        );
      }
      latitude = coords.latitude;
      longitude = coords.longitude;
    }

    // Parse and validate radius
    let radiusInMeters = 1000; // Default: 1km to cover large parks
    if (radius) {
      radiusInMeters = parseInt(radius);
      if (
        isNaN(radiusInMeters) ||
        radiusInMeters < 0 ||
        radiusInMeters > 10000
      ) {
        throw new BadRequestException(
          "Invalid radius (must be between 0 and 10000 meters)",
        );
      }
    }

    // Parse and validate limit
    let limitCount = 6; // Default: 6 parks
    if (limit) {
      limitCount = parseInt(limit);
      if (isNaN(limitCount) || limitCount < 1 || limitCount > 50) {
        throw new BadRequestException(
          "Invalid limit (must be between 1 and 50)",
        );
      }
    }

    return this.locationService.findNearby(
      latitude,
      longitude,
      radiusInMeters,
      limitCount,
    );
  }
}
