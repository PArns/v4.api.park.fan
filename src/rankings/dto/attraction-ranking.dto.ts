import { ApiProperty } from "@nestjs/swagger";

/** One ride's place in a country's or continent's year ranking. */
export class RankedAttractionDto {
  @ApiProperty({ example: 1, description: "Place, 1 = longest waits" })
  rank: number;

  @ApiProperty({ example: "taron" })
  attractionSlug: string;

  @ApiProperty({ example: "Taron" })
  attractionName: string;

  @ApiProperty({ example: "Mystery", nullable: true, type: String })
  land: string | null;

  @ApiProperty({ example: "phantasialand" })
  parkSlug: string;

  @ApiProperty({ example: "Phantasialand" })
  parkName: string;

  @ApiProperty({ example: "europe" })
  continentSlug: string;

  @ApiProperty({ example: "germany" })
  countrySlug: string;

  @ApiProperty({ example: "bruehl" })
  citySlug: string;

  @ApiProperty({
    example: 40,
    description:
      "Average of the year's hourly P50 waits in minutes, rounded to 5 like every wait this API emits",
  })
  avgWaitP50: number;

  @ApiProperty({
    example: 45,
    description:
      "Average of the year's hourly P90 waits in minutes, rounded to 5. The ranking sorts on the unrounded value, so two rides with the same figure can hold different places",
  })
  avgWaitP90: number;

  @ApiProperty({
    example: 260,
    description: "Park-local days with measured waits in the year",
  })
  sampleDays: number;
}

export class AttractionRankingMetaDto {
  @ApiProperty({ example: 2026 })
  year: number;

  @ApiProperty({ example: "europe" })
  continentSlug: string;

  @ApiProperty({ example: "germany", nullable: true, type: String })
  countrySlug: string | null;

  @ApiProperty({
    example: false,
    description:
      "True once the year is over in every ranked park; while it runs, the window ends yesterday",
  })
  yearComplete: boolean;

  @ApiProperty({
    example: "2026-11-25",
    nullable: true,
    type: String,
    description: "Last measured day in the window, park-local",
  })
  dataTo: string | null;

  @ApiProperty({
    example: 20,
    description: "Measured days a ride needs in the year to be ranked",
  })
  minAttractionDays: number;

  @ApiProperty({
    example: 9,
    description: "Parks with at least one ranked ride",
  })
  parkCount: number;

  @ApiProperty({ example: 212, description: "Rides ranked, before `limit`" })
  totalRanked: number;

  @ApiProperty({ example: "2026-11-26T11:00:00.000Z" })
  generatedAt: string;
}

export class AttractionRankingDto {
  @ApiProperty({ type: [RankedAttractionDto] })
  attractions: RankedAttractionDto[];

  @ApiProperty({ type: AttractionRankingMetaDto })
  meta: AttractionRankingMetaDto;
}
