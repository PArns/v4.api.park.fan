import { ApiProperty } from "@nestjs/swagger";

/**
 * Response DTO for GET /v1/shows/:slug/showtimes
 * Returns upcoming showtimes for a specific show
 */
export class ShowtimesResponseDto {
  @ApiProperty({
    description: "Show details",
    example: {
      id: "uuid",
      name: "Festival of the Lion King",
      slug: "festival-of-the-lion-king",
    },
  })
  show: {
    id: string;
    name: string;
    slug: string;
  };

  @ApiProperty({
    description: "List of upcoming showtimes",
    type: [String],
    example: [
      "2024-12-25T14:00:00.000Z",
      "2024-12-25T16:30:00.000Z",
      "2024-12-25T19:00:00.000Z",
    ],
  })
  showtimes: string[];

  @ApiProperty({
    description: "Last update timestamp",
    example: "2024-12-25T12:00:00.000Z",
    nullable: true,
  })
  lastUpdated: string | null;
}
