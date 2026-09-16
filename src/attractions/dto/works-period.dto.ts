import { ApiProperty } from "@nestjs/swagger";

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
 *
 * It does NOT say whether the window is running: a period that ended in March
 * stays on the row and in this block, and a rebuild starting next winter looks
 * the same from here. That is deliberate — the window is a fact about a date
 * range, and a page wanting "closed for works right now" makes the comparison
 * above, while one wanting "reopens 3 March" reads `to` on a window that has
 * not started.
 *
 * Not declared `implements WorksPeriod`: the two keys are optional here because
 * the interceptor strips their nulls, and the interface's are not, since every
 * object this codebase builds carries them. The link that matters is checked
 * anyway — `resolveWorksPeriod()`'s result is assigned to this type at both
 * mappers, so a renamed or retyped field is a compile error there.
 */
export class WorksPeriodDto {
  @ApiProperty({
    description:
      "First park-local day of the works period, inclusive. Absent for a " +
      "window that was already running when somebody wrote it down — null " +
      "here or a stripped key on the wire, since `ExcludeNullInterceptor` " +
      "removes null-valued keys outside `/v1/admin/*`.",
    example: "2026-01-16",
    required: false,
    nullable: true,
  })
  from?: string | null;

  @ApiProperty({
    description:
      "Last park-local day, inclusive. Absent while the work is running and " +
      "nobody has been told when it ends — the usual case, and NOT a claim " +
      "that the ride is gone for good. Stripped on the wire like `from`.",
    example: "2026-03-03",
    required: false,
    nullable: true,
  })
  to?: string | null;

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
