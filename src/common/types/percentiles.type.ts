/**
 * Percentile Types
 *
 * Used for statistical analysis of wait times
 */

export interface Percentiles {
  p25?: number;
  p50: number;
  p75: number;
  p90: number;
  p95?: number;
  p99?: number;
}

export interface PercentilesWithIqr extends Percentiles {
  iqr: number;
}
