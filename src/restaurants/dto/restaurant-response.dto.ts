import { Restaurant } from "../entities/restaurant.entity";

/**
 * Restaurant Response DTO
 *
 * Used for API responses when returning restaurant data.
 */
export class RestaurantResponseDto {
  id: string;
  name: string;
  slug: string;

  cuisineType: string | null;
  requiresReservation: boolean;

  latitude: number | null;
  longitude: number | null;

  park: {
    id: string;
    name: string;
    slug: string;
    timezone: string;
    continent: string | null;
    country: string | null;
    city: string | null;
  } | null;

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

    // Map park relation if loaded
    if (restaurant.park) {
      dto.park = {
        id: restaurant.park.id,
        name: restaurant.park.name,
        slug: restaurant.park.slug,
        timezone: restaurant.park.timezone,
        continent: restaurant.park.continent || null,
        country: restaurant.park.country || null,
        city: restaurant.park.city || null,
      };
    } else {
      dto.park = null;
    }

    return dto;
  }
}

export class RestaurantWithLiveDataDto extends RestaurantResponseDto {
  status: string;
  waitTime: number | null;
  partySize: number | null;

  operatingHours?: Array<{
    type: string;
    startTime: string;
    endTime: string;
  }> | null;

  lastUpdated: string;
}
