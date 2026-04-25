import { ApiProperty } from "@nestjs/swagger";
import { CrowdLevel } from "../../common/types/crowd-level.type";

export class TickerItemDto {
  @ApiProperty({ example: "Tokyo DisneySea" })
  parkName: string;

  @ApiProperty({ example: "tokyo-disneysea" })
  parkSlug: string;

  @ApiProperty({ example: "Soaring: Fantastic Flight" })
  attractionName: string;

  @ApiProperty({ example: "soaring-fantastic-flight" })
  attractionSlug: string;

  @ApiProperty({ example: 110, description: "Current wait time in minutes" })
  waitTime: number;

  @ApiProperty({
    enum: ["very_low", "low", "moderate", "high", "very_high", "extreme"],
    nullable: true,
    description: "Crowd level relative to P50 baseline",
  })
  crowdLevel: CrowdLevel | null;

  @ApiProperty({
    example:
      "/v1/parks/asia/japan/tokyo/tokyo-disneysea/attractions/soaring-fantastic-flight",
    nullable: true,
  })
  url: string | null;
}

export class TickerResponseDto {
  @ApiProperty({ type: [TickerItemDto] })
  items: TickerItemDto[];

  @ApiProperty({ example: "2026-04-25T14:00:00.000Z" })
  generatedAt: string;
}
