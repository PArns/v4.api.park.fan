import { ApiProperty } from "@nestjs/swagger";

export class ParkPercentilesDto {
  @ApiProperty({
    description: "Today's percentile stats",
    nullable: true,
    example: {
      p50: 35,
      p75: 50,
      p90: 65,
      p95: 75,
      timestamp: "2023-10-27T10:00:00Z",
    },
  })
  today: {
    p50: number;
    p75: number;
    p90: number;
    p95: number;
    timestamp: Date;
  } | null;

  @ApiProperty({
    description: "Rolling 7-day stats",
    nullable: true,
  })
  rolling7d: {
    p50: number;
    p90: number;
    iqr: number;
  } | null;

  @ApiProperty({
    description: "Rolling 30-day stats",
    nullable: true,
  })
  rolling30d: {
    p50: number;
    p90: number;
    iqr: number;
  } | null;
}

export class HourlyPercentileDto {
  @ApiProperty({ description: "Hour of the day" })
  hour: Date;

  @ApiProperty({ description: "Median wait time" })
  p50: number;

  @ApiProperty({ description: "90th percentile wait time" })
  p90: number;

  @ApiProperty({ description: "Interquartile range" })
  iqr: number;
}

export class AttractionPercentilesDto {
  @ApiProperty({
    description: "Today's specific stats",
    nullable: true,
  })
  today: {
    p25: number;
    p50: number;
    p75: number;
    p90: number;
    iqr: number;
    sampleCount: number;
    timestamp: Date;
  } | null;

  @ApiProperty({
    description: "Hourly percentile breakdown",
    type: [HourlyPercentileDto],
  })
  hourly: HourlyPercentileDto[];

  @ApiProperty({
    description: "Rolling window stats",
    example: { last7d: { p50: 20, p90: 45, iqr: 15 }, last30d: null },
  })
  rolling: {
    last7d: { p50: number; p90: number; iqr: number } | null;
    last30d: { p50: number; p90: number; iqr: number } | null;
  };
}
