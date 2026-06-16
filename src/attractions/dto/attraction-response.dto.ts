import { ApiProperty } from "@nestjs/swagger";
import {
  ParkSummaryDto,
  mapParkSummary,
} from "../../common/dto/park-summary.dto";
import { Attraction } from "../entities/attraction.entity";
import { QueueDataItemDto } from "../../queue-data/dto/queue-data-item.dto";
import { ForecastItemDto } from "../../queue-data/dto/forecast-response.dto";
import { CrowdLevel } from "../../common/types/crowd-level.type";
import { HistoryDayDto } from "./history-day.dto";
import { ScheduleItemDto } from "../../parks/dto/schedule-item.dto";
import { cleanSlugSuffix } from "../../common/utils/slug.util";
import type { BestVisitSlot } from "../../common/utils/best-visit-times.util";
import type { RopeDropInfo } from "../../common/types/rope-drop.type";

/**
 * One weekday/weekend bucket of the typical-waits summary.
 *
 * Both values are derived from the distribution of *daily peak* waits (the
 * day's highest hourly P90) across operating days in the bucket:
 * - `typical` = P50 of those daily peaks → a normal day's peak wait
 * - `busy`    = P90 of those daily peaks → a busy day's peak wait
 */
export class TypicalWaitBucketDto {
  @ApiProperty({
    example: 35,
    nullable: true,
    description:
      "Typical day's peak wait in minutes (median of daily peaks). Null if no data.",
  })
  typical: number | null;

  @ApiProperty({
    example: 60,
    nullable: true,
    description:
      "Busy day's peak wait in minutes (90th percentile of daily peaks). Null if no data.",
  })
  busy: number | null;

  @ApiProperty({
    example: 142,
    description: "Number of operating days with data in this bucket",
  })
  sampleDays: number;
}

/**
 * Typical-vs-busy peak waits, split by weekday and weekend.
 * Weekend days are country-aware (e.g. Fri+Sat in the Gulf states).
 */
export class TypicalWaitsDto {
  @ApiProperty({
    type: TypicalWaitBucketDto,
    description: "Stats over weekday (non-weekend) operating days",
  })
  weekday: TypicalWaitBucketDto;

  @ApiProperty({
    type: TypicalWaitBucketDto,
    description: "Stats over weekend operating days (country-aware)",
  })
  weekend: TypicalWaitBucketDto;

  @ApiProperty({
    example: 365,
    description: "Size of the look-back window in days",
  })
  windowDays: number;

  @ApiProperty({ example: "2025-06-16", description: "Window start (park tz)" })
  dataFrom: string;

  @ApiProperty({ example: "2026-06-15", description: "Window end (park tz)" })
  dataTo: string;

  @ApiProperty({
    example: true,
    description:
      "True when the total sample is large enough to display. Gate rendering " +
      "on this instead of a client-side threshold.",
  })
  displayable: boolean;

  @ApiProperty({
    example: "2026-06-16T03:00:00.000Z",
    description: "When this aggregate was computed (ISO 8601 UTC)",
  })
  generatedAt: string;
}

/**
 * Attraction Response DTO
 *
 * Used for API responses when returning attraction data.
 * Now includes integrated live data: current queues, status, forecasts, ML predictions, statistics.
 */
export class AttractionResponseDto {
  @ApiProperty({ description: "Unique identifier of the attraction" })
  id: string;

  @ApiProperty({ description: "Name of the attraction" })
  name: string;

  @ApiProperty({ description: "URL-friendly slug" })
  slug: string;

  @ApiProperty({ description: "Current status", required: false })
  status?: string; // Overall status: OPERATING, DOWN, CLOSED, REFURBISHMENT

  @ApiProperty({
    description: "Effective status (considering park status)",
    required: false,
  })
  effectiveStatus?: string;

  @ApiProperty({
    description:
      "Themed land name (e.g. 'The Wizarding World of Harry Potter')",
    required: false,
    nullable: true,
  })
  land?: string | null;

  // Live Data
  @ApiProperty({
    description: "Current wait times (all queue types)",
    required: false,
    type: [QueueDataItemDto],
  })
  queues?: QueueDataItemDto[];

  @ApiProperty({
    description: "Hourly ML predictions (internal model)",
    required: false,
  })
  hourlyForecast?: {
    predictedTime: string;
    predictedWaitTime: number;
    confidence: number;
    trend: string;
  }[];

  @ApiProperty({
    description: "External forecasts (e.g. ThemeParks.wiki)",
    required: false,
    type: [ForecastItemDto],
  })
  forecasts?: ForecastItemDto[];

  @ApiProperty({
    description: "Latitude coordinate",
    required: false,
    nullable: true,
  })
  latitude: number | null;

  @ApiProperty({
    description: "Longitude coordinate",
    required: false,
    nullable: true,
  })
  longitude: number | null;

  @ApiProperty({
    description: "Parent park details",
    required: false,
    nullable: true,
  })
  park: ParkSummaryDto | null;

  @ApiProperty({
    description: "Whether this attraction only operates during certain seasons",
    required: false,
  })
  isSeasonal?: boolean;

  @ApiProperty({
    description:
      "Months (1–12) when this attraction typically operates. Null if not seasonal.",
    required: false,
    nullable: true,
    type: [Number],
  })
  seasonMonths?: number[] | null;

  @ApiProperty({
    description:
      "Whether the attraction is currently in its operating season. Null for non-seasonal attractions.",
    required: false,
    nullable: true,
  })
  isCurrentlyInSeason?: boolean | null;

  @ApiProperty({
    description: "Frontend URL to attraction",
    nullable: true,
    required: false,
  })
  url?: string | null;

  @ApiProperty({
    description: "Attraction statistics",
    required: false,
    nullable: true,
  })
  statistics?: {
    avgWaitToday: number | null;
    peakWaitToday: number | null;
    peakWaitTimestamp: string | null;
    minWaitToday: number | null;
    typicalWaitThisHour: number | null;
    percentile95ThisHour: number | null;
    currentVsTypical: number | null;
    dataPoints: number;
    history: {
      timestamp: string;
      waitTime: number;
    }[];
    timestamp: string;
  } | null;

  @ApiProperty({
    description: "Wait time trend direction (up/down/stable)",
    enum: ["up", "down", "stable"],
    required: false,
    nullable: true,
  })
  trend?: "up" | "down" | "stable" | null;

  @ApiProperty({
    description: "Current crowd level badge",
    enum: [
      "very_low",
      "low",
      "moderate",
      "high",
      "very_high",
      "extreme",
      "closed",
    ],
    required: false,
    nullable: true,
  })
  crowdLevel?: CrowdLevel | "closed" | null;

  @ApiProperty({
    description:
      "90th percentile baseline wait time for this hour/day (minutes)",
    required: false,
    nullable: true,
  })
  baseline?: number | null;

  @ApiProperty({
    description: "How current wait compares to baseline",
    enum: ["much_lower", "lower", "typical", "higher", "much_higher"],
    required: false,
    nullable: true,
  })
  comparison?:
    | "much_lower"
    | "lower"
    | "typical"
    | "higher"
    | "much_higher"
    | null;

  // Prediction Accuracy (Feedback Loop)
  @ApiProperty({
    description: "Prediction accuracy metrics",
    required: false,
    nullable: true,
  })
  predictionAccuracy?: {
    badge: "excellent" | "good" | "fair" | "poor" | "insufficient_data";
    last30Days: {
      mae: number;
      comparedPredictions: number;
      totalPredictions: number;
    };
    message?: string;
  } | null;

  // Historical Data
  @ApiProperty({
    description:
      "Historical daily data (utilization, hourly P90, down counts) for the requested period",
    required: false,
    type: [HistoryDayDto],
  })
  history?: HistoryDayDto[];

  @ApiProperty({
    description:
      "Park schedule (opening hours and holidays) for the last 30 days, aligned with history data",
    required: false,
    type: [ScheduleItemDto],
  })
  schedule?: ScheduleItemDto[];

  @ApiProperty({
    description:
      "Typical vs busy-day peak waits, split by weekday and weekend. " +
      "Derived from the distribution of daily peak waits over a sliding window. " +
      "Render only when `displayable` is true.",
    required: false,
    nullable: true,
    type: TypicalWaitsDto,
  })
  typicalWaits?: TypicalWaitsDto | null;

  @ApiProperty({
    description:
      "Recommended visit time slots for today based on 15-min ML predictions. " +
      "Sorted by time. 'optimal' = global minimum wait, 'good' = within 40% of minimum.",
    required: false,
    nullable: true,
  })
  bestVisitTimes?: BestVisitSlot[] | null;

  @ApiProperty({
    description:
      "Rope-drop recommendation for this headliner (worth arriving at park opening). " +
      "Only present for tier1/tier2 headliners in parks with a schedule. " +
      "`worth` flips seasonally. Times are UTC ISO 8601; offsets are minutes-after-open.",
    required: false,
    nullable: true,
  })
  ropeDrop?: RopeDropInfo | null;

  static fromEntity(attraction: Attraction): AttractionResponseDto {
    return {
      id: attraction.id,
      name: attraction.name,
      slug: cleanSlugSuffix(attraction.slug),

      status: "CLOSED", // Default

      latitude: attraction.latitude !== undefined ? attraction.latitude : null,
      longitude:
        attraction.longitude !== undefined ? attraction.longitude : null,

      park: mapParkSummary(attraction.park),

      land: attraction.landName || null,

      isSeasonal: attraction.isSeasonal || false,
      seasonMonths: attraction.seasonMonths || null,
      isCurrentlyInSeason: (() => {
        if (!attraction.isSeasonal) return null;
        if (!attraction.seasonMonths || attraction.seasonMonths.length === 0)
          return null;
        const currentMonth = new Date().getMonth() + 1;
        return attraction.seasonMonths.includes(currentMonth);
      })(),

      hourlyForecast: [],
      forecasts: [],
      statistics: null,
    };
  }
}
