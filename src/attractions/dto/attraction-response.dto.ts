import { ApiProperty } from "@nestjs/swagger";
import {
  ParkSummaryDto,
  mapParkSummary,
} from "../../common/dto/park-summary.dto";
import { Attraction } from "../entities/attraction.entity";
import { QueueDataItemDto } from "../../queue-data/dto/queue-data-item.dto";
import { ForecastItemDto } from "../../queue-data/dto/forecast-response.dto";
import {
  CROWD_LEVEL_WITH_CLOSED_VALUES,
  CrowdLevel,
} from "../../common/types/crowd-level.type";
import { HistoryDayDto } from "./history-day.dto";
import { ScheduleItemDto } from "../../parks/dto/schedule-item.dto";
import { cleanSlugSuffix } from "../../common/utils/slug.util";
import {
  isCurrentlyInSeason,
  resolveCuratedFacts,
} from "../utils/curated-attraction-facts.util";
import type { BestVisitSlot } from "../../common/utils/best-visit-times.util";
import type { RopeDropInfo } from "../../common/types/rope-drop.type";
import { RideProfileDto } from "./ride-profile.dto";
import { FastPassDto } from "./fast-pass.dto";
import { resolveFastPass } from "../utils/fast-pass.util";

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
 * One day-of-week bucket: the weekday/weekend bucket plus which day it is.
 */
export class DayOfWeekWaitDto extends TypicalWaitBucketDto {
  @ApiProperty({
    example: 6,
    description: "Day of week (0=Sunday, 1=Monday, …, 6=Saturday)",
  })
  dayOfWeek: number;

  @ApiProperty({
    example: true,
    description: "Whether this day counts as weekend in the park's country",
  })
  isWeekend: boolean;
}

/**
 * The record peak wait over the look-back window, with the date it occurred.
 */
export class PeakWaitDto {
  @ApiProperty({
    example: 120,
    description: "Highest daily peak wait in the window (minutes)",
  })
  value: number;

  @ApiProperty({
    example: "2025-08-09",
    description: "Date the record peak occurred (YYYY-MM-DD, park timezone)",
  })
  date: string;
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
    type: [DayOfWeekWaitDto],
    description:
      "Per day-of-week breakdown (only days with data), ordered 0=Sun…6=Sat",
  })
  byDayOfWeek: DayOfWeekWaitDto[];

  @ApiProperty({
    type: PeakWaitDto,
    nullable: true,
    description:
      "Record peak wait over the window with its date. Null if no data.",
  })
  peak: PeakWaitDto | null;

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
    /**
     * Width of the model's uncertainty band in minutes (top trained quantile
     * minus the served median), so a chart can draw the band rather than infer
     * one from `confidence`. Absent where the model reports no real spread —
     * which is not a zero-wide band, and must not be drawn as one.
     */
    uncertaintyMinutes?: number | null;
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
    description:
      "Parent park details. On this endpoint the block also carries the park's live `status`, " +
      "so a client rendering a single ride has everything it needs (the park's `timezone` was " +
      "already here) without also fetching the full park payload.",
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
    description: "Minimum rider height in cm. Null if unrestricted or unknown.",
    example: 140,
    required: false,
    nullable: true,
  })
  minimumHeight?: number | null;

  @ApiProperty({
    description:
      "Unit the operator publishes the height in. minimumHeight is always " +
      "centimetres; this says how to present it (US parks publish inches).",
    example: "in",
    enum: ["cm", "in"],
    nullable: true,
  })
  minimumHeightUnit?: "cm" | "in" | null;

  @ApiProperty({
    description:
      "Maximum rider height in cm (kiddie rides). Null if unrestricted or unknown.",
    example: 150,
    required: false,
    nullable: true,
  })
  maximumHeight?: number | null;

  @ApiProperty({
    description: "Whether riders may get wet. Null = unknown (not 'dry').",
    example: true,
    required: false,
    nullable: true,
  })
  mayGetWet?: boolean | null;

  @ApiProperty({
    description:
      "Whether the ride has a single-rider line at all. A static fact about " +
      "the queue layout — NOT whether it is open right now, which `queues` " +
      "answers. Null = unknown, not 'no'.",
    example: true,
    required: false,
    nullable: true,
  })
  hasSingleRider?: boolean | null;

  @ApiProperty({
    description:
      "The paid queue-jump product this ride sells, or absent. Absent means " +
      "either nobody has checked or the park sells none — the two are one " +
      "absence to a visitor, so never render it as 'no fast pass'.",
    required: false,
    nullable: true,
    type: FastPassDto,
  })
  fastPass?: FastPassDto | null;

  @ApiProperty({
    description:
      "RCDB (rcdb.com) database id for outbound links (https://rcdb.com/{id}.htm). Null for non-coasters or unmatched rides.",
    example: 12723,
    required: false,
    nullable: true,
  })
  rcdbId?: number | null;

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
    description:
      "Current crowd level badge. `unknown` when there is nothing to rate " +
      "against (thin park, or no P50 baseline for this ride); `closed` when " +
      "the ride is not operating.",
    enum: CROWD_LEVEL_WITH_CLOSED_VALUES,
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
    "much_lower" | "lower" | "typical" | "higher" | "much_higher" | null;

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

  @ApiProperty({
    description:
      "Curated ride profile: the track figures in ride order, the ride type, " +
      "manufacturer and opening year. Every id is a glossary term id. " +
      "Absent for rides that have not been curated yet.",
    required: false,
    nullable: true,
    type: RideProfileDto,
  })
  rideProfile?: RideProfileDto | null;

  @ApiProperty({
    description:
      "When this attraction stopped existing (ISO 8601), or null while it is " +
      "still around. A retired attraction is absent from park listings, " +
      "counts and search, but keeps answering here so its history stays " +
      'readable — render it as "operated until …", never as closed.',
    required: false,
    nullable: true,
  })
  retiredAt?: string | null;

  @ApiProperty({
    description:
      "Why it was retired, including the source it was established from.",
    required: false,
    nullable: true,
  })
  retiredReason?: string | null;

  static fromEntity(attraction: Attraction): AttractionResponseDto {
    const curated = resolveCuratedFacts(attraction);

    return {
      id: attraction.id,
      // Curated name, when a human wrote one: the sync rewrites `name` on
      // every run, so a correction lives in its own column. The slug is
      // deliberately untouched — see the entity.
      name: curated.name,
      slug: cleanSlugSuffix(attraction.slug),

      status: "CLOSED", // Default

      latitude: attraction.latitude !== undefined ? attraction.latitude : null,
      longitude:
        attraction.longitude !== undefined ? attraction.longitude : null,

      park: mapParkSummary(attraction.park),

      land: curated.landName,

      isSeasonal: curated.isSeasonal,
      seasonMonths: curated.seasonMonths,
      minimumHeight: curated.minimumHeight,
      minimumHeightUnit: curated.minimumHeightUnit,
      maximumHeight: curated.maximumHeight,
      mayGetWet: curated.mayGetWet,
      hasSingleRider: attraction.hasSingleRider ?? null,
      // The name lives on the park row, so this is the one resolver that needs
      // both entities. A projection that did not join the park still resolves —
      // it just falls back to the neutral name and withholds the price.
      fastPass: resolveFastPass(attraction, attraction.park),
      rcdbId: attraction.rcdbId ?? null,
      retiredAt: attraction.retiredAt
        ? attraction.retiredAt.toISOString()
        : null,
      retiredReason: attraction.retiredReason ?? null,
      isCurrentlyInSeason: isCurrentlyInSeason(curated),

      hourlyForecast: [],
      forecasts: [],
      statistics: null,
    };
  }
}
