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
 *
 * The tier is derived from the curves that were actually built, never from the
 * distance alone. `measured` used to be claimed for today and tomorrow whatever
 * came back, so a day the model had no hourly answer for — the ML service
 * having a bad minute, a park it skipped — was served as composed data under
 * the most trustworthy label. Which is the one failure this whole design is
 * arranged against.
 */
export type PlanDayTier = "observed" | "measured" | "composed" | "long_range";

/** Where a single hour's number came from, when it is not the response's tier. */
export type PlanDayHourSource = "observed" | "measured" | "composed";

/** Where `openHour`/`closeHour` came from. */
export type PlanDayHoursSource = "schedule" | "observed";

/**
 * Where a show's times came from.
 *
 * `scheduled` is the operator's own answer for that day. `projected` is this
 * API's, built from the most recent day the show ran on the same weekday —
 * because **no feed publishes showtimes ahead of the current day**. Checked at
 * the source: ThemeParks.wiki answers for today and then a tail of entries it
 * never cleared, some from 2022; across every park we track, not one carries a
 * park-local showtime for a future day.
 */
export type PlanDayShowSource = "scheduled" | "projected";

export class PlanDayHourDto {
  @ApiProperty({ example: 14, description: "Park-local hour, 0–23." })
  hour: number;

  @ApiProperty({ example: 45, description: "Expected wait in minutes." })
  wait: number;

  @ApiProperty({
    required: false,
    enum: ["observed", "measured", "composed"],
    description:
      "Present only where this hour did NOT come from the response's `tier`. " +
      "The model generates hourly predictions for the next 24 hours and no " +
      "further, so a day inside that window is part measured and part " +
      "composed: today has no measured hours before the current one, and " +
      "tomorrow has none after it. Those hours used to be omitted, which cut " +
      "the evening off a park that closes at 22:00 and pulled `dayPeak` down " +
      "with it — a silent understatement of the busiest part of the day.",
  })
  source?: PlanDayHourSource;
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
    description:
      "The day's PEAK wait for this ride, and the same statistic on every " +
      "tier: the day-level prediction on a forecast day, the realised day-P90 " +
      "on an observed one — the pair the calendar already scores against each " +
      "other. It is deliberately NOT the maximum of `hours`: those are " +
      "typical-hour numbers (a median forecast, a measured mean), and taking " +
      "their maximum made the field mean something different on each tier. " +
      "Today read 20 where the same ride read 42 five days out, and the whole " +
      "difference was the statistic.",
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
      "The ride was reported DOWN at some point in the previous operating day " +
      "and was never OPERATING in any of it — a breakdown that lasted the " +
      "whole day. A ride the feed called CLOSED all day is NOT flagged: that " +
      "is a season or a refurbishment, not a fault, and flagging it put a " +
      "warning on nine winter-only attractions at Phantasialand every day of " +
      "the summer. Absent past tomorrow: yesterday's downtime says nothing " +
      "actionable about a Tuesday in November.",
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

export class PlanDayShowDto {
  @ApiProperty({ example: "big-moments-the-celebration-show" })
  showSlug: string;

  @ApiProperty({ example: "Big Moments – The Celebration-Show" })
  showName: string;

  @ApiProperty({
    type: [String],
    example: ["12:30", "14:30", "17:45"],
    description: "Park-local start times, ascending.",
  })
  times: string[];

  @ApiProperty({
    enum: ["scheduled", "projected"],
    description:
      "`scheduled` is the operator's own answer for this day and exists for " +
      "today only — no feed publishes showtimes further ahead. `projected` is " +
      "ours, and a caller MUST render it differently: it is what the show did " +
      "on the most recent matching weekday, not a promise that it runs.",
  })
  source: PlanDayShowSource;

  @ApiProperty({
    required: false,
    example: "2026-08-29",
    description:
      "Projections only: the day these times were actually observed on. A " +
      "projection that cannot say what it was projected FROM is " +
      "indistinguishable from a schedule.",
  })
  observedOn?: string;

  @ApiProperty({
    required: false,
    example: 7,
    description:
      "Projections only: how many days in the last eight weeks had this show " +
      "on this weekday. One sighting is an event, not a schedule — a single " +
      "summer concert must not become every Thursday of the autumn — so a " +
      "projection needs more than one, and the count travels so a reader can " +
      "weigh it.",
  })
  sampleDays?: number;
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

  @ApiProperty({
    required: false,
    enum: ["schedule", "observed"],
    description:
      "Where the two hours above came from. `schedule` is the operator's own " +
      "published day. `observed` means they were derived from the hours this " +
      "ride's queues have actually been measured in over the last year, " +
      "because the operator has not published that far out — of 177 parks " +
      "with published hours, 91 reach 60 days and 38 reach 120, so a summer " +
      "date asked in January has none. Without the fallback the whole " +
      "response was an empty shell past ~2 months; with it, a caller must " +
      "present the window as approximate and must not read `status` as a " +
      "promise that the park is open.",
  })
  hoursSource?: PlanDayHoursSource;

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
      "How the ride curves were produced, derived from the curves that were " +
      "actually built rather than from the distance. `observed` is not a " +
      "forecast: for a date in the past the hours are what the queues actually " +
      "did, from the nightly 15-minute rollup. `measured` means the day " +
      "carries the model's own hourly predictions, which exist for the next 24 " +
      'hours only — the hours outside that window carry `source: "composed"`. ' +
      "`composed` scales a day-level prediction by the ride's historical hour " +
      "shape. `long_range` means the model has produced no day level for this " +
      "date at all, so there are no ride curves to give: the daily horizon is " +
      "the park's own schedule coverage (about 6 months) rather than a fixed " +
      "number of days.",
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
      "minutes, from the lead-time archive (`prediction_lead_snapshots`): the " +
      "nearest sampled lead distance at or below `leadDays`, over scored " +
      "headliner days. Absent until that distance has enough scored rows — " +
      "the 60-day bucket says nothing until the archive has been running 60 " +
      "days — and absent is the honest answer, because nothing measures how " +
      "wrong the model is at this distance yet. A caller should widen the band " +
      "with distance WITHOUT attaching a figure rather than invent one.",
  })
  leadTimeMae?: number | null;

  @ApiProperty({ type: [PlanDayRideDto] })
  rides: PlanDayRideDto[];

  @ApiProperty({
    type: [PlanDayShowDto],
    description:
      "Shows for the day — fixed points a plan is arranged around rather than " +
      "fitted between. `source` says whether the times are the operator's " +
      "(today only) or this API's projection from the same weekday, and the " +
      "two must not be drawn alike. Empty for a park whose shows we have never " +
      "watched, which is a different statement from a park with no shows.",
  })
  shows: PlanDayShowDto[];
}
