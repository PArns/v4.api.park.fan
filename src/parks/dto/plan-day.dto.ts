import { ApiProperty } from "@nestjs/swagger";

/**
 * How a number in this response was arrived at. It travels with every curve
 * because the four are not equally trustworthy and nothing about a rendered bar
 * says which one produced it.
 *
 * The planner is asked about days months out — people book summer in January —
 * so going blank past the model's reach makes it useless, while a fabricated
 * number is worse than a blank. The tier is what lets a caller draw the
 * difference instead of choosing between those two.
 *
 * `observed` is the one that points BACKWARDS, and it is not a forecast at all:
 * a date in the past is answered from what the queues actually did, so a plan
 * somebody already walked stops predicting at itself. Before it, a past date
 * came back labelled `measured` with a negative `leadDays` and an empty ride
 * list — the model generates forwards, so nothing matched, and the response
 * claimed the most trustworthy tier for the emptiest possible answer.
 */
export type PlanDayTier = "observed" | "measured" | "composed" | "long_range";

export class PlanDayHourDto {
  @ApiProperty({ example: 14, description: "Park-local hour, 0–23." })
  hour: number;

  @ApiProperty({ example: 45, description: "Expected wait in minutes." })
  wait: number;
}

export class PlanDayRideDto {
  @ApiProperty({ example: "taron" })
  attractionSlug: string;

  @ApiProperty({ example: "Taron" })
  attractionName: string;

  @ApiProperty({ required: false, nullable: true, example: "Mystery" })
  land?: string | null;

  @ApiProperty({
    type: [PlanDayHourDto],
    description:
      "One entry per open hour. Absent rides are those with no measured " +
      "hourly shape to scale — the endpoint omits them rather than drawing a " +
      "flat line, which would assert the queue is the same all day.",
  })
  hours: PlanDayHourDto[];

  @ApiProperty({
    example: 50,
    description: "The day-level prediction this ride's curve was scaled to.",
  })
  dayPeak: number;

  @ApiProperty({
    required: false,
    nullable: true,
    example: 12,
    description:
      "Half-width of the uncertainty band in minutes, from the model's own " +
      "top quantile minus its median. Absent where the model reports no " +
      "spread — which is NOT a band of width zero and must not be drawn as one.",
  })
  uncertaintyMinutes?: number | null;

  @ApiProperty({
    example: 141,
    description: "Measured days behind the historical shape.",
  })
  sampleDays: number;

  @ApiProperty({
    required: false,
    nullable: true,
    example: 50.7987204,
    description:
      "Where the ride is. Carried so a planner can say how far apart two " +
      "consecutive entries are WITHOUT fetching 40 attraction payloads. A " +
      "geodesic distance is a LOWER BOUND on the walk and nothing more: park " +
      "paths bend around water, queues and one-way routes, so a caller may " +
      "state a floor and must not present the straight line as a walking time.",
  })
  latitude?: number | null;

  @ApiProperty({ required: false, nullable: true, example: 6.8807868 })
  longitude?: number | null;

  @ApiProperty({
    required: false,
    example: false,
    description:
      "The ride was observed all through the previous operating day and was " +
      "never OPERATING in any of it — i.e. down for the whole day rather than " +
      "unobserved. Absent past tomorrow: yesterday's downtime says nothing " +
      "actionable about a Tuesday in November, and the query is not worth its " +
      "cost there.",
  })
  downYesterday?: boolean;

  @ApiProperty({
    required: false,
    example: true,
    description:
      "Whether the park counts this ride among its headliners. The CURATED answer " +
      "(AnalyticsService.getHeadlinerAttractionIds), never re-derived from dayPeak — a " +
      "headliner having a quiet day is still a headliner. The planner uses it to point " +
      "out the ones a visitor has not planned.",
  })
  isHeadliner?: boolean;
}

export class PlanDayContextDto {
  @ApiProperty({ example: "2026-10-17" })
  date: string;

  @ApiProperty({ example: "OPERATING" })
  status: string;

  @ApiProperty({
    required: false,
    nullable: true,
    example: 9,
    description: "First park-local hour the park is open.",
  })
  openHour: number | null;

  @ApiProperty({ required: false, nullable: true, example: 20 })
  closeHour: number | null;

  @ApiProperty({ required: false, nullable: true, example: "high" })
  crowdLevel?: string | null;

  @ApiProperty({
    required: false,
    nullable: true,
    description:
      "Day weather. Absent past the forecast's reach (about 14 days) — the " +
      "endpoint does not substitute a climate normal, because a made-up rain " +
      "probability would silently move every bar on the day.",
  })
  weather?: Record<string, unknown> | null;

  @ApiProperty({ example: false })
  isHoliday: boolean;

  @ApiProperty({ example: false })
  isBridgeDay: boolean;

  @ApiProperty({ example: false })
  isSchoolVacation: boolean;

  @ApiProperty({
    example: true,
    description:
      "Saturday or Sunday in the park's own timezone. Derived here because " +
      "the calendar carries no such field and every surface was deriving it " +
      "separately.",
  })
  isWeekend: boolean;

  @ApiProperty({
    required: false,
    type: [Object],
    description:
      "Holidays in neighbouring regions whose day-trippers raise local crowds.",
  })
  neighborHolidays?: Array<Record<string, unknown>>;
}

export class PlanDayDto {
  @ApiProperty({ example: "phantasialand" })
  parkSlug: string;

  @ApiProperty({ example: "Europe/Berlin" })
  timezone: string;

  @ApiProperty({ type: PlanDayContextDto })
  context: PlanDayContextDto;

  @ApiProperty({
    enum: ["observed", "measured", "composed", "long_range"],
    description:
      "How the ride curves were produced. `observed` is not a forecast: for a " +
      "date in the past the hours are what the queues actually did, from the " +
      "nightly 15-minute rollup. `measured` is the model's own hourly " +
      "prediction (today and tomorrow only — it generates 24 hours ahead). " +
      "`composed` scales a day-level prediction by the ride's historical hour " +
      "shape. `long_range` is the same composition past the stored 60-day " +
      "daily horizon, where the day level itself is thinner.",
  })
  tier: PlanDayTier;

  @ApiProperty({
    example: 38,
    description: "Whole days from today to this date, in the park's timezone.",
  })
  leadDays: number;

  @ApiProperty({
    required: false,
    nullable: true,
    description:
      "Measured mean absolute error for predictions made this far ahead, in " +
      "minutes. Absent until the lead-time archive has been running that " +
      "long — and absent is the honest answer, because nothing measures how " +
      "wrong the model is at this distance yet. A caller should widen the " +
      "band with distance WITHOUT attaching a figure rather than invent one.",
  })
  leadTimeMae?: number | null;

  @ApiProperty({ type: [PlanDayRideDto] })
  rides: PlanDayRideDto[];

  @ApiProperty({
    type: [Object],
    description:
      "Showtimes for the day, when known. Shows are fixed points a plan is " +
      "arranged around rather than fitted between.",
  })
  shows: Array<Record<string, unknown>>;
}
