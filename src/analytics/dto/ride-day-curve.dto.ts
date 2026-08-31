import { ApiProperty } from "@nestjs/swagger";

/**
 * One ride's day: what it normally does, what it has done so far today, and
 * what the model expects for the rest of it.
 *
 * A projection built for the day-curve chart, so everything it carries is
 * POSITIONAL against `hours` and nothing else is included. The alternative was
 * the attraction detail endpoint, which answers ~53 KB for one ride — most of it
 * a schedule and a queue list the chart does not draw — and which the homepage
 * therefore could not afford to mount at all.
 *
 * The three sources behind it are deliberately different, and the reason matters
 * for anyone extending this:
 *
 * - `p25`/`p50`/`p90` come from `queue_data_aggregates`, the nightly hourly
 *   rollup. It is computed for YESTERDAY and back, never for today.
 * - `today` therefore CANNOT come from that table. It is bucketed live out of
 *   raw `queue_data`.
 * - `forecast` comes from `wait_time_predictions` (`predictionType: 'hourly'`),
 *   taking the most recent prediction made for each hour.
 */
export class RideDayCurveDto {
  @ApiProperty({
    type: [Number],
    example: [9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
    description:
      "Hours of the operating day, park-local and ascending. Derived from " +
      "the data, so a park that opens at 11 starts at 11.",
  })
  hours: number[];

  @ApiProperty({ example: "voltron-nevera-powered-by-rimac" })
  attractionSlug: string;

  @ApiProperty({ example: "Voltron Nevera powered by Rimac" })
  attractionName: string;

  @ApiProperty({
    type: [Number],
    description:
      "Quiet-hour wait (P25) per hour. Lower edge of the historical spread.",
  })
  p25: Array<number | null>;

  @ApiProperty({
    type: [Number],
    description: "Median wait per hour. `null` is a gap, never a zero.",
  })
  p50: Array<number | null>;

  @ApiProperty({
    type: [Number],
    description: "Busy-hour wait (P90) per hour. Upper edge of the spread.",
  })
  p90: Array<number | null>;

  @ApiProperty({
    type: [Number],
    example: [30, 45, 50, null, null],
    description:
      "What the ride ACTUALLY showed today, per hour, park-local. `null` " +
      "for an hour that has not happened yet, and for one the ride reported " +
      "nothing in — a closed ride has an all-null row, which is how a caller " +
      "tells 'nothing to draw today' from 'quiet today'. Read live out of raw " +
      "queue data: the nightly hourly rollup only ever covers yesterday.",
  })
  today: Array<number | null>;

  @ApiProperty({
    type: [Number],
    example: [null, null, null, 48, 40],
    description:
      "Expected wait per hour for the REST of today, from the hourly model. " +
      "`null` for an hour already measured (see `today`) and for one the " +
      "model made no prediction for.",
  })
  forecast: Array<number | null>;

  @ApiProperty({
    type: [Number],
    example: [30, 40, 45, 50, 45],
    description:
      "What the model said for each hour BEFORE that hour happened — the " +
      "oldest prediction it holds per hour, over the whole day rather than " +
      "only the part still ahead. Set next to `today` it shows how the model " +
      "actually did; `forecast` cannot do that, because it drops every hour " +
      "that has since been measured. `null` for an hour the model never " +
      "predicted.",
  })
  predicted: Array<number | null>;

  @ApiProperty({
    example: 6.8,
    nullable: true,
    description:
      "The model's current mean absolute error in minutes, for a caller that " +
      "wants to draw the forecast as a band rather than a line. Null when the " +
      "model has not been scored yet. It is a published, measured figure — do " +
      "NOT widen it with the forecast horizon, which nothing here measures.",
  })
  forecastError: number | null;

  @ApiProperty({
    example: true,
    description:
      "Whether the ride reported at least one wait time today. False for a " +
      "ride that is closed, out of season, or in a park that has not opened " +
      "yet — a caller picking something to display should prefer a true.",
  })
  measuredToday: boolean;

  @ApiProperty({ example: 163, description: "Measured days behind the curve." })
  sampleDays: number;

  @ApiProperty({ example: "Europe/Berlin" })
  timezone: string;

  @ApiProperty({ example: "2026-08-31T03:14:00.000Z" })
  generatedAt: string;

  @ApiProperty({ example: 2, description: "2 since `predicted` was added." })
  schemaVersion: number;
}
