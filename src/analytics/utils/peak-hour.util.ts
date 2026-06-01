import { PeakHourSource } from "../dto/statistics.dto";

/**
 * Confidence (0..1) for a peak-hour value, tiered by provenance:
 * an already-observed peak is the most reliable, a forecast with live data
 * less so, a pure historical fallback the least. Null source => 0.
 */
export function peakHourConfidence(source: PeakHourSource | null): number {
  switch (source) {
    case "observed_today":
      return 0.9;
    case "prediction":
      return 0.6;
    case "historical_fallback":
      return 0.4;
    default:
      return 0;
  }
}
