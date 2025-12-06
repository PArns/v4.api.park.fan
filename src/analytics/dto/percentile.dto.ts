export class ParkPercentilesDto {
  today: {
    p50: number;
    p75: number;
    p90: number;
    p95: number;
    timestamp: Date;
  } | null;

  rolling7d: {
    p50: number;
    p90: number;
    iqr: number;
  } | null;

  rolling30d: {
    p50: number;
    p90: number;
    iqr: number;
  } | null;
}

export class HourlyPercentileDto {
  hour: Date;
  p50: number;
  p90: number;
  iqr: number;
}

export class AttractionPercentilesDto {
  today: {
    p25: number;
    p50: number;
    p75: number;
    p90: number;
    iqr: number;
    sampleCount: number;
    timestamp: Date;
  } | null;

  hourly: HourlyPercentileDto[];

  rolling: {
    last7d: { p50: number; p90: number; iqr: number } | null;
    last30d: { p50: number; p90: number; iqr: number } | null;
  };
}
