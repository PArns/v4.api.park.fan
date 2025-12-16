import { ApiProperty } from "@nestjs/swagger";
import { Destination } from "../entities/destination.entity";

/**
 * Destination with Parks Response DTO
 *
 * Used when returning destination data with its parks included.
 */
export class DestinationParkDto {
  @ApiProperty({ description: "Unique identifier for the park" })
  id: string;

  @ApiProperty({ description: "Name of the park" })
  name: string;

  @ApiProperty({ description: "URL-friendly slug for the park" })
  slug: string;

  @ApiProperty({ description: "External identifier from source" })
  externalId: string;

  @ApiProperty({ description: "Timezone of the park" })
  timezone: string;
}

export class DestinationWithParksDto {
  @ApiProperty({ description: "Unique identifier for the destination" })
  id: string;

  @ApiProperty({ description: "Name of the destination" })
  name: string;

  @ApiProperty({ description: "URL-friendly slug for the destination" })
  slug: string;

  @ApiProperty({ description: "External identifier from source" })
  externalId: string;

  @ApiProperty({
    description: "List of parks within this destination",
    type: [DestinationParkDto],
  })
  parks: DestinationParkDto[];

  static fromEntity(destination: Destination): DestinationWithParksDto {
    return {
      id: destination.id,
      name: destination.name,
      slug: destination.slug,
      externalId: destination.externalId,
      parks: destination.parks
        ? destination.parks.map((park) => ({
            id: park.id,
            name: park.name,
            slug: park.slug,
            externalId: park.externalId,
            timezone: park.timezone,
          }))
        : [],
    };
  }
}
