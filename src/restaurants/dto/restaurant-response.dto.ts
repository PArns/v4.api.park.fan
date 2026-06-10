import { ApiProperty } from "@nestjs/swagger";
import {
  ParkSummaryDto,
  mapParkSummary,
} from "../../common/dto/park-summary.dto";
import { Restaurant } from "../entities/restaurant.entity";

/**
 * Restaurant Response DTO
 *
 * Used for API responses when returning restaurant data.
 */
export class RestaurantResponseDto {
  @ApiProperty({ description: "Unique identifier of the restaurant" })
  id: string;

  @ApiProperty({ description: "Name of the restaurant" })
  name: string;

  @ApiProperty({ description: "URL-friendly slug" })
  slug: string;

  @ApiProperty({ description: "Cuisine type", required: false, nullable: true })
  cuisineType: string | null;

  @ApiProperty({ description: "Reservation requirement" })
  requiresReservation: boolean;

  @ApiProperty({
    description: "Latitude coordinate",
    required: false,
    nullable: true,
  })
  latitude: number | null;

  @ApiProperty({
    description: "Longitude coordinate",
    required: false,
    nullable: true,
  })
  longitude: number | null;

  @ApiProperty({
    description: "Parent park details",
    required: false,
    nullable: true,
  })
  park: ParkSummaryDto | null;

  /**
   * Maps Restaurant entity to DTO
   */
  static fromEntity(restaurant: Restaurant): RestaurantResponseDto {
    const dto = new RestaurantResponseDto();

    dto.id = restaurant.id;
    dto.name = restaurant.name;
    dto.slug = restaurant.slug;

    dto.cuisineType = restaurant.cuisineType || null;
    dto.requiresReservation = restaurant.requiresReservation;

    dto.latitude = restaurant.latitude || null;
    dto.longitude = restaurant.longitude || null;

    dto.park = mapParkSummary(restaurant.park);

    return dto;
  }
}

export class RestaurantWithLiveDataDto extends RestaurantResponseDto {
  @ApiProperty({ description: "Current operating status" })
  status: string;

  @ApiProperty({
    description: "Current wait time",
    required: false,
    nullable: true,
  })
  waitTime: number | null;

  @ApiProperty({
    description: "Current party size",
    required: false,
    nullable: true,
  })
  partySize: number | null;

  @ApiProperty({
    description: "General operating hours",
    required: false,
    nullable: true,
  })
  operatingHours?: Array<{
    type: string;
    startTime: string;
    endTime: string;
  }> | null;

  @ApiProperty({ description: "Last updated timestamp" })
  lastUpdated: string;
}
