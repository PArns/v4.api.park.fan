import { ApiProperty, type ApiPropertyOptions } from "@nestjs/swagger";
import type { PerformanceSchedule } from "../utils/performance-schedule.util";

/**
 * When a ride that runs as performances starts them, for a run of park-local
 * days (PAR-1355). Curated by hand; no feed carries it.
 *
 * A client picks the schedule whose days hold the park's today (compare in the
 * park's timezone, never the reader's) and shows the next start instead of a
 * wait time. No schedule covering today means the ride runs as its live status
 * says.
 *
 * `from` and `to` are nullable for the reason `WorksPeriodDto` gives: every
 * object this codebase builds carries both keys, while on the wire a null key
 * is stripped by `ExcludeNullInterceptor`.
 */
export class PerformanceScheduleDto implements PerformanceSchedule {
  @ApiProperty({
    description:
      "First park-local day the schedule holds for, inclusive. Absent for a " +
      "schedule with no start.",
    example: "2026-11-14",
    required: false,
    nullable: true,
  })
  from: string | null;

  @ApiProperty({
    description:
      "Last park-local day the schedule holds for, inclusive. Absent for a " +
      "schedule with no end. A schedule with neither holds every day.",
    example: "2027-01-24",
    required: false,
    nullable: true,
  })
  to: string | null;

  @ApiProperty({
    description:
      "Park-local start times, `HH:MM` on a 24-hour clock, ascending and " +
      "never empty. Each belongs to the calendar day it is read on.",
    example: ["13:00", "14:00", "15:00", "16:00", "17:00"],
    type: [String],
  })
  times: string[];
}

/** The Swagger block of `performanceSchedules` on both attraction payloads. */
export const PERFORMANCE_SCHEDULES_API_PROPERTY: ApiPropertyOptions = {
  description:
    "The curated start times of a ride that runs as performances rather than " +
    "continuously (a 4D cinema, a show walk-through), per run of park-local " +
    "days, sorted by `from` (an open start first). Schedules never share a " +
    "day. Absent when none is curated, which is the state of nearly every " +
    "ride. The one whose days hold the park's today replaces the wait time: " +
    "show the next start. A live CLOSED between two performances is the feed " +
    "not knowing the programme, not a closure.",
  required: false,
  type: [PerformanceScheduleDto],
};
