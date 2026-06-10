import { ApiProperty } from "@nestjs/swagger";
import {
  ParkSummaryDto,
  mapParkSummary,
} from "../../common/dto/park-summary.dto";
import { Show } from "../entities/show.entity";

/**
 * Show Response DTO
 *
 * Used for API responses when returning show data.
 */
export class ShowResponseDto {
  @ApiProperty({ description: "Unique identifier of the show" })
  id: string;

  @ApiProperty({ description: "Name of the show" })
  name: string;

  @ApiProperty({ description: "URL-friendly slug" })
  slug: string;

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
   * Maps Show entity to DTO
   */
  static fromEntity(show: Show): ShowResponseDto {
    const dto = new ShowResponseDto();

    dto.id = show.id;
    dto.name = show.name;
    dto.slug = show.slug;

    dto.latitude = show.latitude || null;
    dto.longitude = show.longitude || null;

    dto.park = mapParkSummary(show.park);

    return dto;
  }
}

export class ShowWithLiveDataDto extends ShowResponseDto {
  @ApiProperty({ description: "Current operating status" })
  status: string;

  @ApiProperty({
    description: "Upcoming showtimes",
    required: false,
    nullable: true,
  })
  showtimes: Array<{
    type: string;
    startTime: string;
    endTime?: string;
  }> | null;

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
