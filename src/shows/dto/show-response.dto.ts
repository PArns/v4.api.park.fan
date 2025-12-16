import { Show } from "../entities/show.entity";

/**
 * Show Response DTO
 *
 * Used for API responses when returning show data.
 */
export class ShowResponseDto {
  id: string;
  name: string;
  slug: string;

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
   * Maps Show entity to DTO
   */
  static fromEntity(show: Show): ShowResponseDto {
    const dto = new ShowResponseDto();

    dto.id = show.id;
    dto.name = show.name;
    dto.slug = show.slug;

    dto.latitude = show.latitude || null;
    dto.longitude = show.longitude || null;

    // Map park relation if loaded
    if (show.park) {
      dto.park = {
        id: show.park.id,
        name: show.park.name,
        slug: show.park.slug,
        timezone: show.park.timezone,
        continent: show.park.continent || null,
        country: show.park.country || null,
        city: show.park.city || null,
      };
    } else {
      dto.park = null;
    }

    return dto;
  }
}

export class ShowWithLiveDataDto extends ShowResponseDto {
  status: string;

  showtimes: Array<{
    type: string;
    startTime: string;
    endTime?: string;
  }> | null;

  operatingHours?: Array<{
    type: string;
    startTime: string;
    endTime: string;
  }> | null;

  lastUpdated: string;
}
