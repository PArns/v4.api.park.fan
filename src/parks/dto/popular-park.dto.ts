import { ApiProperty } from "@nestjs/swagger";
import { Park } from "../entities/park.entity";
import { buildParkUrl } from "../../common/utils/url.util";

/**
 * Popular Park DTO
 *
 * One entry in the most-requested parks ranking. Combines a lightweight
 * park summary with the request count that drives the ordering.
 */
export class PopularParkDto {
  @ApiProperty({ description: "1-based rank in the popularity ranking" })
  rank: number;

  @ApiProperty({
    description: "Number of tracked requests in the current window",
  })
  requests: number;

  @ApiProperty({ description: "Unique identifier of the park" })
  id: string;

  @ApiProperty({ description: "Name of the park", example: "Phantasialand" })
  name: string;

  @ApiProperty({ description: "URL-friendly slug", example: "phantasialand" })
  slug: string;

  @ApiProperty({
    description: "Frontend URL",
    required: false,
    nullable: true,
  })
  url: string | null;

  @ApiProperty({ description: "Country name", required: false, nullable: true })
  country: string | null;

  @ApiProperty({ description: "City name", required: false, nullable: true })
  city: string | null;

  @ApiProperty({
    description: "Continent name",
    required: false,
    nullable: true,
  })
  continent: string | null;

  static fromEntity(
    park: Park,
    rank: number,
    requests: number,
  ): PopularParkDto {
    return {
      rank,
      requests,
      id: park.id,
      name: park.name,
      slug: park.slug,
      url: buildParkUrl(park),
      country: park.country || null,
      city: park.city || null,
      continent: park.continent || null,
    };
  }
}
