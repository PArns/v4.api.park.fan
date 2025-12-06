export class ParkStatisticsDto {
  avgWaitTime: number;
  avgWaitToday: number;
  peakHour: string | null;
  crowdLevel: "very_low" | "low" | "moderate" | "high" | "very_high";
  totalAttractions: number;
  operatingAttractions: number;
  closedAttractions: number;
  timestamp: Date;

  // Phase 4: Optional percentile data
  percentilesToday?: {
    p50: number;
    p75: number;
    p90: number;
    p95: number;
  };
}

export class AttractionStatisticsDto {
  avgWaitToday: number | null;
  peakWaitToday: number | null;
  minWaitToday: number | null;
  typicalWaitThisHour: number | null;
  percentile95ThisHour: number | null;
  currentVsTypical: number | null;
  dataPoints: number;
  timestamp: Date;

  // Phase 4: Optional percentile data
  distributionToday?: {
    p25: number;
    p50: number;
    p75: number;
    p90: number;
    iqr: number;
  };

  recentPatterns?: {
    p50Last7d: number;
    p90Last7d: number;
  };
}

export interface StatisticsResponseDto {
  park?: ParkStatisticsDto;
  attraction?: AttractionStatisticsDto;
}
