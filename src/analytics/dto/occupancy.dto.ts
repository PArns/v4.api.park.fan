export interface OccupancyDto {
  current: number; // 0-100+ (can exceed 100 on extreme days)
  trend: "up" | "stable" | "down"; // Park-wide trend (last hour)
  comparedToTypical: number; // Absolute difference from typical (P90 baseline)
  comparisonStatus: "lower" | "typical" | "higher" | "closed"; // Human-readable status
  baseline90thPercentile: number; // The calculated P90 value (NEW - Phase 4)
  updatedAt: string; // ISO 8601 timestamp - when occupancy was calculated
  breakdown?: {
    currentAvgWait: number;
    typicalAvgWait: number;
    activeAttractions: number;
  };
}

export interface ParkOccupancyResponseDto {
  occupancy: OccupancyDto;
  message?: string;
}
