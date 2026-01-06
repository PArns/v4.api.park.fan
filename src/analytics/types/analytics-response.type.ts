/**
 * Analytics Response Types
 *
 * Used for analytics service return types
 */

export interface AttractionCounts {
  total: number;
  operating: number;
  closed: number;
}

export interface WaitTimeTrend {
  trend: "increasing" | "stable" | "decreasing";
  changeRate: number; // Minutes per hour
  recentAverage: number | null; // Last hour average
  previousAverage: number | null; // 2-3 hours ago average
}

export interface WaitTimeHistoryItem {
  timestamp: string;
  waitTime: number;
}
