import { ApiProperty } from "@nestjs/swagger";
import { ParkStatus } from "../../common/types/status.type";
import { CrowdLevel } from "../../common/types/crowd-level.type";

/**
 * Lean, precomputed best-days projection.
 *
 * This is the exact projection the frontend keeps today (derived from the full
 * `/calendar` response) — status, crowd level and the holiday/vacation flags a
 * "best days to visit" / crowd-FAQ block needs, and nothing else. It is served
 * from a materialized Redis snapshot refreshed by the daily forecast batch, so
 * it is small (target ≤ 15 KB) and cheap (single GET, never a lazy ML compute).
 *
 * Deliberately EXCLUDED (vs `/calendar`): `influencingHolidays`, weather,
 * hourly arrays, events, schedule/hours detail, `isToday` (the frontend derives
 * it from `date` + `timezone`; a baked flag goes stale in caches).
 */

/** Metadata for the best-days response. */
export class BestDaysMeta {
  @ApiProperty({ description: "Park slug", example: "phantasialand" })
  slug: string;

  @ApiProperty({
    description: "Park timezone (IANA format)",
    example: "Europe/Berlin",
  })
  timezone: string;

  @ApiProperty({
    description: "Whether the park provides official operating hours",
  })
  hasOperatingSchedule: boolean;

  @ApiProperty({
    description:
      "When the forecast batch produced this snapshot (ISO 8601 UTC). " +
      "Absent when no snapshot has been materialized yet.",
    required: false,
    example: "2026-07-14T03:10:00.000Z",
  })
  computedAt?: string;

  @ApiProperty({
    description: "First day of the returned window (YYYY-MM-DD, park timezone)",
    example: "2026-07-14",
  })
  windowFrom: string;

  @ApiProperty({
    description: "Last day of the returned window (YYYY-MM-DD, park timezone)",
    example: "2026-10-12",
  })
  windowTo: string;
}

/** One day in the best-days projection. */
export class BestDayEntry {
  @ApiProperty({
    description: "Date (YYYY-MM-DD) in the park's local timezone",
    example: "2026-07-14",
  })
  date: string;

  @ApiProperty({
    description: "Park status for the day",
    enum: ["OPERATING", "CLOSED", "UNKNOWN"],
  })
  status: ParkStatus;

  @ApiProperty({
    description:
      "Crowd level for the day. `closed` when the park is closed. On future " +
      "days this is the ML forecast; on today it is the live/last-computed level.",
    enum: [
      "very_low",
      "low",
      "moderate",
      "high",
      "very_high",
      "extreme",
      "unknown",
      "closed",
    ],
  })
  crowdLevel: CrowdLevel | "closed";

  @ApiProperty({
    description:
      "The ML FORWARD crowd prediction for the day. Equals `crowdLevel` on " +
      "future days; on today it may differ (crowdLevel carries the live level). " +
      "Absent when the day is not ratable (thin park / no baseline).",
    required: false,
    enum: ["very_low", "low", "moderate", "high", "very_high", "extreme"],
  })
  predictedCrowdLevel?: CrowdLevel;

  @ApiProperty({ description: "Local public holiday" })
  isHoliday: boolean;

  @ApiProperty({ description: "Local school vacation" })
  isSchoolVacation: boolean;

  @ApiProperty({ description: "Bridge day between a holiday and the weekend" })
  isBridgeDay: boolean;
}

/**
 * Per-weekday historical crowd aggregate (stats-quality, from the same source
 * as `/stats`). Lets the frontend render the weekday ranking without the (also
 * slow) `/stats` aggregate. Populated best-effort — omitted when the `/stats`
 * cache is cold.
 */
export class BestDaysByDayOfWeek {
  @ApiProperty({
    description: "Day of week (0 = Sunday … 6 = Saturday, Postgres DOW)",
    example: 1,
  })
  dayOfWeek: number;

  @ApiProperty({
    description: "Average crowd score (1.0–5.0)",
    example: 2.1,
  })
  avgCrowdScore: number;

  @ApiProperty({
    description: "Number of historical operating days behind this average",
    example: 98,
  })
  sampleDays: number;
}

/** Full best-days response. */
export class BestDaysResponse {
  @ApiProperty({ type: () => BestDaysMeta })
  meta: BestDaysMeta;

  @ApiProperty({
    description: "Days in the window (rolling today → +90 days by default)",
    type: () => [BestDayEntry],
  })
  days: BestDayEntry[];

  @ApiProperty({
    description:
      "Optional per-weekday historical aggregate. Omitted when unavailable.",
    type: () => [BestDaysByDayOfWeek],
    required: false,
  })
  byDayOfWeek?: BestDaysByDayOfWeek[];
}
