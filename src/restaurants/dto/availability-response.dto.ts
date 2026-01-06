import { ApiProperty } from "@nestjs/swagger";

/**
 * Response DTO for GET /v1/restaurants/:slug/availability
 * Returns dining availability for a specific restaurant
 */
export class AvailabilityResponseDto {
  @ApiProperty({
    description: "Restaurant details",
    example: {
      id: "uuid",
      name: "Be Our Guest Restaurant",
      slug: "be-our-guest-restaurant",
    },
  })
  restaurant: {
    id: string;
    name: string;
    slug: string;
  };

  @ApiProperty({
    description: "Current operating status",
    enum: ["OPERATING", "CLOSED", "DOWN", "REFURBISHMENT"],
    example: "OPERATING",
  })
  status: string;

  @ApiProperty({
    description: "Current wait time in minutes",
    example: 15,
    nullable: true,
  })
  waitTime: number | null;

  @ApiProperty({
    description: "Party size",
    example: 4,
    nullable: true,
  })
  partySize: number | null;

  @ApiProperty({
    description: "Last update timestamp",
    example: "2024-12-25T12:00:00.000Z",
    nullable: true,
  })
  lastUpdated: string | null;
}
