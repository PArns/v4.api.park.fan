import { ApiProperty } from "@nestjs/swagger";
import type { WorksPeriod } from "../utils/curated-out-of-service.util";

/**
 * A hand-written works period: the rebuild or refit a ride is closed for.
 *
 * Present only where an editor has written the window down. It is absent for
 * nearly every ride, and absent says nothing at all — no feed announces a
 * rebuild, so "no works period here" means nobody has curated one, never that
 * the ride is running.
 *
 * **Not an outage, and never rendered as one.** `outage` says a ride stopped
 * working and the site noticed; this says a ride was taken out of service on
 * purpose, months ahead, by people who scheduled it. Inside this window the API
 * reports no outage at all — the feed cannot tell a rebuild from a breakdown,
 * so a running works period is the only thing that can.
 *
 * Dates are park-local `YYYY-MM-DD` and both bounds are inclusive. Whether the
 * window covers today is a question about the park's calendar, so a client
 * comparing them must do it in the park's timezone, which the park payload
 * carries — the reader's own clock is the wrong one for every park in another
 * zone.
 */
export class WorksPeriodDto implements WorksPeriod {
  @ApiProperty({
    description:
      "First park-local day of the works period, inclusive. Null for a window " +
      "that was already running when somebody wrote it down.",
    example: "2026-01-16",
    nullable: true,
  })
  from: string | null;

  @ApiProperty({
    description:
      "Last park-local day, inclusive. Null while the work is running and " +
      "nobody has been told when it ends — the usual case, and NOT a claim " +
      "that the ride is gone for good.",
    example: "2026-03-03",
    nullable: true,
  })
  to: string | null;

  @ApiProperty({
    description:
      "Whether `to` is an estimate rather than a date the park published. " +
      'True means a client must hedge it ("probably until 3 March"); false ' +
      "covers both a published date and one nobody has checked, because a " +
      "page has no third way to write it. Always false when `to` is null.",
    example: false,
  })
  toUncertain: boolean;
}
