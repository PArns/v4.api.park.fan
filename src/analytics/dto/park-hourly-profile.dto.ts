import { ApiProperty } from "@nestjs/swagger";

export class HourlyProfileAttractionDto {
  @ApiProperty({ example: "voletarium" })
  attractionSlug: string;

  @ApiProperty({ example: "Voletarium" })
  attractionName: string;

  @ApiProperty({
    required: false,
    nullable: true,
    example: "Iceland",
    description: "Land the ride stands in (curated value winning).",
  })
  land?: string | null;

  @ApiProperty({
    type: [Number],
    example: [20, 38, 32, 20, 25, 21, 20, 18, 14, 10],
    description:
      "Quiet-hour wait (P25) per hour, aligned with `hours`. Together with " +
      "`p90` it is the spread a queue moves in at that hour: P50 alone says " +
      "what a typical day looks like, this pair says how far a day can " +
      "reasonably fall either side of it. Drawn as the lower edge of the " +
      "band under the median curve.",
  })
  p25: Array<number | null>;

  @ApiProperty({
    type: [Number],
    example: [33, 57, 48, 30, 38, 31, 30, 28, 21, 16],
    description:
      "Median wait per hour, one entry per `hours` entry and in the same " +
      "order. `null` where the ride has no qualifying samples in that hour — " +
      "render a gap, never a zero.",
  })
  p50: Array<number | null>;

  @ApiProperty({
    type: [Number],
    example: [45, 70, 62, 44, 50, 43, 41, 39, 30, 22],
    description: "Busy-hour wait (P90) per hour, aligned with `hours`.",
  })
  p90: Array<number | null>;

  @ApiProperty({
    example: 10,
    description:
      "The hour of `hours` where this ride's P50 peaks, or null when the " +
      "ride has no readable hour at all.",
    nullable: true,
  })
  peakHour: number | null;

  @ApiProperty({
    example: 122,
    description: "Measured days behind this ride's row.",
  })
  sampleDays: number;
}

export class ParkHourlyProfileMetaDto {
  @ApiProperty({ example: "europa-park" })
  parkSlug: string;

  @ApiProperty({ example: "2025-08-24" })
  dataFrom: string;

  @ApiProperty({ example: "2026-08-23" })
  dataTo: string;

  @ApiProperty({ example: 1 })
  windowYears: number;

  @ApiProperty({
    example: 157,
    description: "Operating days with readable wait times in the window.",
  })
  totalSampleDays: number;

  @ApiProperty({
    example: true,
    description:
      "Render gate. False when the window holds too few measured days for " +
      "an hour-by-hour claim — hide the whole table rather than showing " +
      "the noisy numbers behind it.",
  })
  displayable: boolean;

  @ApiProperty({ example: "2026-08-24T03:14:00.000Z" })
  generatedAt: string;

  @ApiProperty({ example: 1 })
  schemaVersion: number;
}

/**
 * The park's day shape, ride by ride: what a queue typically looks like at
 * each hour of the operating day.
 *
 * A deliberately lean projection rather than a slice of the attraction detail
 * endpoint — that one answers ~53 KB per ride, 45 % of it a `schedule` nobody
 * renders, so eight rides cost 424 KB for a table that fits in 2 KB.
 */
export class ParkHourlyProfileDto {
  @ApiProperty({
    type: [Number],
    example: [9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
    description:
      "Hours of the operating day covered by the table, park-local and " +
      "ascending. Derived from the data, so a park that opens at 11 starts " +
      "at 11 — never assume a fixed 9–18 window.",
  })
  hours: number[];

  @ApiProperty({ type: [HourlyProfileAttractionDto] })
  attractions: HourlyProfileAttractionDto[];

  @ApiProperty({ type: ParkHourlyProfileMetaDto })
  meta: ParkHourlyProfileMetaDto;
}
