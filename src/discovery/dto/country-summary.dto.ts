import { ApiProperty } from "@nestjs/swagger";

export class TopParkSummaryDto {
  @ApiProperty({ example: "Europa-Park" })
  name: string;

  @ApiProperty({ example: "europa-park" })
  slug: string;

  @ApiProperty({ example: "Rust" })
  city: string;

  @ApiProperty({ example: "/parks/europe/germany/rust/europa-park" })
  path: string;

  @ApiProperty({
    example: 3.9,
    description: "Average annual crowd score (1.0–5.0)",
  })
  avgAnnualCrowdScore: number;
}

export class CountrySummaryDto {
  @ApiProperty({ example: "germany" })
  countrySlug: string;

  @ApiProperty({ example: 12 })
  parkCount: number;

  @ApiProperty({ example: 8 })
  cityCount: number;

  @ApiProperty({ type: [TopParkSummaryDto] })
  topParks: TopParkSummaryDto[];

  @ApiProperty({
    example: [7, 8, 10],
    description: "Months with highest average crowd scores (1=January)",
  })
  avgPeakMonths: number[];

  @ApiProperty({
    example: [3, 4, 5],
    description: "Months with lowest average crowd scores",
  })
  avgQuietMonths: number[];
}
