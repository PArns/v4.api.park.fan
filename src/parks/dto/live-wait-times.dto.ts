import { ApiProperty } from "@nestjs/swagger";
import type { NoLiveWaitTimesReason } from "../data/live-wait-time-sources";

export const NO_LIVE_WAIT_TIMES_REASON_VALUES = [
  "in_park_app_only",
  "not_published",
] as const;

/**
 * Whether this park's wait times are readable at all.
 *
 * `available: false` is a statement about the *source*, not about right now: it
 * says no number will ever arrive for this park, so an empty ride list means
 * "we don't know" and never "nothing is running". Every other park in the
 * catalog reports `available: true` — including one whose feed happens to be
 * down, because that is a temporary condition the live queries handle.
 *
 * A client rendering wait times must check this before showing zeros, counts of
 * operating rides, or a crowd level. The API already withholds what it can
 * (predictions, ratings), but the shape of the response is unchanged, so a
 * client that ignores the flag will render an empty park as a quiet one.
 */
export class LiveWaitTimesDto {
  @ApiProperty({
    description:
      "False when this park publishes wait times somewhere we cannot read. " +
      "Not a freshness signal — a park with a temporarily silent feed stays true.",
    example: true,
  })
  available: boolean;

  @ApiProperty({
    description:
      "Why the wait times are unreadable; null whenever available is true. " +
      "`in_park_app_only`: served only to devices inside the park, typically " +
      "on its own WLAN. `not_published`: the park publishes them nowhere.",
    enum: NO_LIVE_WAIT_TIMES_REASON_VALUES,
    required: false,
    nullable: true,
    example: null,
  })
  reason: NoLiveWaitTimesReason | null;
}

/** The value every park with a readable source carries. */
export const LIVE_WAIT_TIMES_AVAILABLE: LiveWaitTimesDto = {
  available: true,
  reason: null,
};

export function buildLiveWaitTimes(
  reason: NoLiveWaitTimesReason | null,
): LiveWaitTimesDto {
  return reason ? { available: false, reason } : LIVE_WAIT_TIMES_AVAILABLE;
}
