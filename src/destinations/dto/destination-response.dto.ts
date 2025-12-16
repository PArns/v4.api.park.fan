import { ApiProperty } from "@nestjs/swagger";
import { Destination } from "../entities/destination.entity";

/**
 * Destination Response DTO
 *
 * Used for API responses when returning destination data.
 */
export class DestinationResponseDto {
  @ApiProperty({ description: "Unique identifier for the destination" })
  id: string;

  @ApiProperty({ description: "External identifier from source" })
  externalId: string;

  @ApiProperty({ description: "URL-friendly slug for the destination" })
  slug: string;

  static fromEntity(destination: Destination): DestinationResponseDto {
    return {
      id: destination.id,
      externalId: destination.externalId,
      slug: destination.slug,
    };
  }
}
