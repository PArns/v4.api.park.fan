import { ApiProperty } from "@nestjs/swagger";
import { Park } from "../entities/park.entity";
import { WeatherItemDto } from "./weather-item.dto";
import { WeatherWarningDto } from "./weather-warning.dto";
import { ScheduleItemDto } from "./schedule-item.dto";
import { ScheduleCoverage } from "./integrated-calendar.dto";
import { QueueDataItemDto } from "../../queue-data/dto/queue-data-item.dto";
import { buildParkUrl, buildAttractionUrl } from "../../common/utils/url.util";
import {
  CROWD_LEVEL_VALUES,
  CROWD_LEVEL_WITH_CLOSED_VALUES,
  CrowdLevel,
} from "../../common/types/crowd-level.type";
import type { BestVisitSlot } from "../../common/utils/best-visit-times.util";
import type { RopeDropInfo } from "../../common/types/rope-drop.type";
import type { TypicalWaitsDto } from "../../attractions/dto/attraction-response.dto";
import {
  isCurrentlyInSeason,
  resolveCuratedFacts,
} from "../../attractions/utils/curated-attraction-facts.util";
import { RideProfileDto } from "../../attractions/dto/ride-profile.dto";
import { LiveWaitTimesDto, buildLiveWaitTimes } from "./live-wait-times.dto";
import {
  resolveCuratedPark,
  resolveParkInfo,
  type ParkInfo,
} from "../utils/curated-park-facts.util";

export class ParkAttractionDto {
  @ApiProperty({ description: "Unique identifier" })
  id: string;

  @ApiProperty({ description: "Name" })
  name: string;

  @ApiProperty({ description: "Slug" })
  slug: string;

  @ApiProperty({ description: "Latitude", required: false, nullable: true })
  latitude: number | null;

  @ApiProperty({ description: "Longitude", required: false, nullable: true })
  longitude: number | null;

  @ApiProperty({
    description: "Queues",
    type: [QueueDataItemDto],
    required: false,
  })
  queues?: QueueDataItemDto[];

  @ApiProperty({
    description: "Themed land name",
    required: false,
    nullable: true,
  })
  land?: string | null;

  @ApiProperty({ description: "Status", required: false })
  status?: string;

  @ApiProperty({
    description: "Effective status (considering park status)",
    required: false,
  })
  effectiveStatus?: string;

  @ApiProperty({
    description:
      "Crowd level badge. `unknown` when there is no baseline to rate this " +
      "ride against; `closed` when it is not operating.",
    enum: CROWD_LEVEL_WITH_CLOSED_VALUES,
    required: false,
  })
  crowdLevel?: CrowdLevel | "closed";

  @ApiProperty({
    description: "Wait time trend",
    enum: ["up", "stable", "down"],
    required: false,
    nullable: true,
  })
  trend?: "up" | "stable" | "down" | null;

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
    description: "Frontend URL to attraction (geo route)",
    nullable: true,
    required: false,
  })
  url?: string | null;

  @ApiProperty({
    description:
      "Whether this is a headliner (top) attraction for this park, based on historical wait-time data",
    required: false,
  })
  isHeadliner?: boolean;

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
      "RCDB (rcdb.com) database id for outbound links (https://rcdb.com/{id}.htm). Null for non-coasters or unmatched rides.",
    example: 12723,
    required: false,
    nullable: true,
  })
  rcdbId?: number | null;

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
      "Only present for tier1/tier2 headliners in parks with a schedule.",
    required: false,
    nullable: true,
  })
  ropeDrop?: RopeDropInfo | null;

  @ApiProperty({
    description:
      "Precomputed typical (P50) vs busy (P90) peak-wait stats. Present only for " +
      "displayable headliners — lets the SSR ride-page shell render them.",
    required: false,
    nullable: true,
  })
  typicalWaits?: TypicalWaitsDto | null;

  @ApiProperty({
    description:
      "Curated ride profile: track figures in ride order, ride type, " +
      "manufacturer and opening year. Every id is a glossary term id. " +
      "Absent for rides that have not been curated yet.",
    required: false,
    nullable: true,
    type: RideProfileDto,
  })
  rideProfile?: RideProfileDto | null;
}

export class ParkShowDto {
  @ApiProperty({ description: "Unique identifier" })
  id: string;

  @ApiProperty({ description: "Name" })
  name: string;

  @ApiProperty({ description: "Slug" })
  slug: string;

  @ApiProperty({ description: "Latitude", required: false, nullable: true })
  latitude: number | null;

  @ApiProperty({ description: "Longitude", required: false, nullable: true })
  longitude: number | null;

  @ApiProperty({ description: "Status", required: false })
  status?: string;

  @ApiProperty({ description: "Showtimes", required: false })
  showtimes?: {
    startTime: string;
  }[];

  @ApiProperty({
    description: "Whether this show only operates during certain seasons",
    required: false,
  })
  isSeasonal?: boolean;

  @ApiProperty({
    description:
      "Months (1–12) when this show typically runs. Null if not seasonal.",
    required: false,
    nullable: true,
    type: [Number],
  })
  seasonMonths?: number[] | null;

  @ApiProperty({
    description:
      "Whether the show is currently in its operating season. Null for non-seasonal shows.",
    required: false,
    nullable: true,
  })
  isCurrentlyInSeason?: boolean | null;
}

export class ParkRestaurantDto {
  @ApiProperty({ description: "Unique identifier" })
  id: string;

  @ApiProperty({ description: "Name" })
  name: string;

  @ApiProperty({ description: "Slug" })
  slug: string;

  @ApiProperty({ description: "Latitude", required: false, nullable: true })
  latitude: number | null;

  @ApiProperty({ description: "Longitude", required: false, nullable: true })
  longitude: number | null;

  @ApiProperty({ description: "Cuisine type", required: false, nullable: true })
  cuisineType: string | null;

  @ApiProperty({ description: "Requires reservation" })
  requiresReservation: boolean;

  @ApiProperty({ description: "Status", required: false })
  status?: string;

  @ApiProperty({ description: "Wait time", required: false, nullable: true })
  waitTime?: number | null;

  @ApiProperty({ description: "Party size", required: false, nullable: true })
  partySize?: number | null;

  @ApiProperty({ description: "Operating hours", required: false })
  operatingHours?: {
    type: string;
    startTime: string;
    endTime: string;
  }[];

  @ApiProperty({ description: "Last updated", required: false })
  lastUpdated?: string;
}

export class ParkOccupancyDto {
  @ApiProperty({ description: "Current occupancy" })
  current: number;

  @ApiProperty({ description: "Trend", enum: ["up", "stable", "down"] })
  trend: "up" | "stable" | "down";

  @ApiProperty({ description: "Compared to typical" })
  comparedToTypical: number;

  @ApiProperty({
    description: "Comparison status",
    enum: ["lower", "typical", "higher", "closed"],
  })
  comparisonStatus: "lower" | "typical" | "higher" | "closed";

  @ApiProperty({ description: "Baseline 90th percentile" })
  baseline90thPercentile: number;

  @ApiProperty({ description: "Updated at" })
  updatedAt: string;

  @ApiProperty({ description: "Breakdown", required: false })
  breakdown?: {
    currentAvgWait: number;
    typicalAvgWait: number;
    activeAttractions: number;
  };
}

export class ParkStatisticsDto {
  @ApiProperty({ description: "Average wait time" })
  avgWaitTime: number;

  @ApiProperty({ description: "Average wait today" })
  avgWaitToday: number;

  @ApiProperty({ description: "Peak wait today" })
  peakWaitToday: number;

  @ApiProperty({
    description:
      "Peak hour as ISO 8601 with timezone offset, e.g. " +
      '"2026-06-01T14:00:00+02:00". Null when no peak applies.',
    nullable: true,
  })
  peakHour: string | null;

  @ApiProperty({
    description: 'Peak hour in park-local "HH:MM" (no conversion needed)',
    example: "14:00",
    nullable: true,
  })
  peakHourLocal: string | null;

  @ApiProperty({
    description: "Confidence in the peak-hour value (0..1)",
    example: 0.9,
  })
  peakHourConfidence: number;

  @ApiProperty({
    description: "Origin of the peak-hour value",
    enum: ["prediction", "observed_today", "historical_fallback"],
    nullable: true,
  })
  peakHourSource:
    "prediction" | "observed_today" | "historical_fallback" | null;

  @ApiProperty({
    description:
      "Crowd level. `unknown` when the park is not ratable or has no live " +
      "sample at all — never a placeholder tier.",
    enum: CROWD_LEVEL_VALUES,
  })
  crowdLevel: CrowdLevel;

  @ApiProperty({ description: "Total attractions count" })
  totalAttractions: number;

  @ApiProperty({ description: "Operating attractions count" })
  operatingAttractions: number;

  @ApiProperty({ description: "Closed attractions count" })
  closedAttractions: number;

  @ApiProperty({ description: "Timestamp" })
  timestamp: string;
}

export class ParkAnalyticsDto {
  @ApiProperty({ description: "Occupancy data", type: ParkOccupancyDto })
  occupancy: ParkOccupancyDto;

  @ApiProperty({ description: "Statistics data", type: ParkStatisticsDto })
  statistics: ParkStatisticsDto;

  @ApiProperty({ description: "Percentiles", required: false })
  percentiles?: {
    p50: number;
    p75: number;
    p90: number;
    p95: number;
  };
}

/**
 * Park with Attractions Response DTO
 *
 * Used when returning park data with its attractions included.
 * Now includes integrated live data: weather, schedule, wait times, analytics.
 */
export class RopeDropHeadlinerDto {
  @ApiProperty({ description: "Attraction id" })
  attractionId: string;

  @ApiProperty({ description: "Attraction name" })
  name: string;

  @ApiProperty({ description: "Minutes saved by rope-dropping on a busy day" })
  savings: number;

  @ApiProperty({
    description: "Recommendation tier",
    enum: ["high", "moderate"],
  })
  strength: "high" | "moderate";
}

export class ParkWithAttractionsDto {
  @ApiProperty({ description: "Unique identifier of the park" })
  id: string;

  @ApiProperty({ description: "Name of the park" })
  name: string;

  @ApiProperty({ description: "URL-friendly slug" })
  slug: string;

  @ApiProperty({
    description: "This park's page on park.fan",
    required: false,
    nullable: true,
  })
  url: string | null;

  @ApiProperty({
    description:
      "Hand-curated facts no feed carries: the park's own site, tickets, " +
      "address, phone, opening year, area. Absent when nothing is curated.",
    required: false,
    nullable: true,
  })
  info: ParkInfo | null;

  @ApiProperty({ description: "Country name", required: false, nullable: true })
  country: string | null;

  @ApiProperty({ description: "City name", required: false, nullable: true })
  city: string | null;

  @ApiProperty({ description: "Timezone identifier" })
  timezone: string;

  @ApiProperty({
    description: "Whether the park provides official operating hours",
    example: true,
  })
  hasOperatingSchedule: boolean;

  @ApiProperty({
    description:
      "How far the park's published schedule reaches: first and last date with a park-level " +
      "OPERATING entry (YYYY-MM-DD, park timezone), null at both ends when the park has none. " +
      "Past `to`, a calendar `status` is inferred rather than published — for a park with a " +
      "history of seasonal closures it reads CLOSED, which is right just after a season ends " +
      "and wrong months later. Read it before publishing, paginating or indexing future " +
      "calendar dates.",
    type: ScheduleCoverage,
  })
  scheduleCoverage: ScheduleCoverage;

  @ApiProperty({
    description:
      "Whether this park's wait times are readable at all. Check it before " +
      "rendering wait times, operating counts or a crowd level — for a park " +
      "with no source those all collapse to zero.",
    type: LiveWaitTimesDto,
  })
  liveWaitTimes: LiveWaitTimesDto;

  @ApiProperty({
    description: "Current operating status",
    enum: ["OPERATING", "CLOSED"],
    required: false,
  })
  status?: "OPERATING" | "CLOSED";

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
    description: "Continent name",
    required: false,
    nullable: true,
  })
  continent: string | null;

  @ApiProperty({ description: "Region name", required: false, nullable: true })
  region: string | null;

  @ApiProperty({ description: "Region code", required: false, nullable: true })
  regionCode: string | null;

  // Live Data

  @ApiProperty({ description: "Weather information", required: false })
  weather?: {
    current: WeatherItemDto | null;
    now: {
      temperature: number;
      apparentTemperature: number | null;
      humidity: number | null;
      weatherCode: number | null;
      weatherDescription: string | null;
      isDay: boolean | null;
    } | null;
    forecast: WeatherItemDto[];
    /** Active severe-weather warnings (empty when none). */
    warnings?: WeatherWarningDto[];
  };

  // Content
  @ApiProperty({
    description: "List of attractions with live wait times",
    type: [ParkAttractionDto],
    required: false,
  })
  attractions: ParkAttractionDto[];

  @ApiProperty({
    description:
      "Headliners worth rope-dropping today (worth=true), sorted by minutes saved. " +
      "Quick 'is it worth arriving at opening' summary; full details on each attraction's ropeDrop.",
    type: [RopeDropHeadlinerDto],
    required: false,
  })
  ropeDropHeadliners?: RopeDropHeadlinerDto[];

  @ApiProperty({
    description: "List of shows",
    type: [ParkShowDto],
    required: false,
  })
  shows?: ParkShowDto[];

  @ApiProperty({
    description: "List of restaurants",
    type: [ParkRestaurantDto],
    required: false,
  })
  restaurants?: ParkRestaurantDto[];

  // Analytics & Planning
  @ApiProperty({
    description: "Park-wide analytics and occupancy",
    type: ParkAnalyticsDto,
    required: false,
    nullable: true,
  })
  analytics?: ParkAnalyticsDto | null;

  @ApiProperty({
    description: "Today's operating schedule",
    required: false,
    type: [ScheduleItemDto],
  })
  schedule?: ScheduleItemDto[];

  @ApiProperty({
    description: "Next scheduled opening day",
    required: false,
    nullable: true,
  })
  nextSchedule?: {
    openingTime: string;
    closingTime: string;
    scheduleType: string;
  };

  static fromEntity(park: Park): ParkWithAttractionsDto {
    const curatedPark = resolveCuratedPark(park);
    return {
      id: park.id,
      name: curatedPark.name,
      slug: park.slug,
      url: buildParkUrl(park),
      info: resolveParkInfo(park),

      country: park.country || null,
      city: park.city || null,
      region: park.region || null,
      regionCode: park.regionCode || null,
      continent: park.continent || null,
      timezone: park.timezone,
      hasOperatingSchedule: false, // Default, overwritten by service
      // Same deal: null/null is the honest default, and it is also the real answer for a park
      // with no OPERATING rows — a caller reading it before the service fills it in concludes
      // "no published schedule", never a window that does not exist.
      scheduleCoverage: { from: null, to: null },
      // Derived from the entity, not from live data: whether a source exists is
      // a property of the park, and it must be right even on the paths that
      // never reach ParkIntegrationService.
      liveWaitTimes: buildLiveWaitTimes(curatedPark.noWaitTimesReason),
      status: "CLOSED", // Default to ensure order
      latitude: park.latitude !== undefined ? park.latitude : null,
      longitude: park.longitude !== undefined ? park.longitude : null,

      attractions: park.attractions
        ? park.attractions.map((attraction) => {
            const curated = resolveCuratedFacts(attraction);
            return {
              id: attraction.id,
              name: curated.name,
              slug: attraction.slug,
              latitude:
                attraction.latitude !== undefined ? attraction.latitude : null,
              longitude:
                attraction.longitude !== undefined
                  ? attraction.longitude
                  : null,
              land: curated.landName,
              url: buildAttractionUrl(park, attraction) || null,
              isSeasonal: curated.isSeasonal,
              seasonMonths: curated.seasonMonths,
              // Seasonal but unknown when → null, deliberately: it must not
              // collapse into "not running", which would hide the ride.
              isCurrentlyInSeason: isCurrentlyInSeason(curated),
              minimumHeight: curated.minimumHeight,
              minimumHeightUnit: curated.minimumHeightUnit,
              maximumHeight: curated.maximumHeight,
              mayGetWet: curated.mayGetWet,
              hasSingleRider: attraction.hasSingleRider ?? null,
              rcdbId: attraction.rcdbId ?? null,
              // queue data, forecasts etc will be attached by service
            };
          })
        : [],
      shows: park.shows
        ? park.shows.map((show) => {
            const isSeasonal = show.isSeasonal || false;
            const seasonMonths = show.seasonMonths || null;
            let isCurrentlyInSeason: boolean | null = null;
            if (isSeasonal) {
              const currentMonth = new Date().getMonth() + 1;
              isCurrentlyInSeason =
                seasonMonths !== null && seasonMonths.length > 0
                  ? seasonMonths.includes(currentMonth)
                  : null;
            }
            return {
              id: show.id,
              name: show.name,
              slug: show.slug,
              status: "CLOSED", // Default to ensure order
              latitude: show.latitude !== undefined ? show.latitude : null,
              longitude: show.longitude !== undefined ? show.longitude : null,
              isSeasonal,
              seasonMonths,
              isCurrentlyInSeason,
            };
          })
        : [],
      restaurants: park.restaurants
        ? park.restaurants.map((restaurant) => ({
            id: restaurant.id,
            name: restaurant.name,
            slug: restaurant.slug,
            status: "CLOSED" as const,
            latitude:
              restaurant.latitude !== undefined ? restaurant.latitude : null,
            longitude:
              restaurant.longitude !== undefined ? restaurant.longitude : null,
            cuisineType: restaurant.cuisineType || null,
            requiresReservation: restaurant.requiresReservation,
          }))
        : [],
    };
  }
}
