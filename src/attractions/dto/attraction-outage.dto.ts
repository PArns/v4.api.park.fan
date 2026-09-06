import { ApiProperty } from "@nestjs/swagger";
import type { CurrentOutage } from "../services/attraction-outage.service";

/**
 * The running outage on a ride that is reported `DOWN` right now.
 *
 * Present only while the ride reads `DOWN`, only in a park whose configuration
 * lets a source say so at all, and only outside a curated works period. Absent
 * means one of those three, and a client may not read its absence as "running".
 *
 * Deliberately not a duration. `queue_data` is a change log with an hourly
 * heartbeat that copies the previous row's status *and* its `data_source`, so a
 * carried `DOWN` is indistinguishable from an observed one; minutes taken from
 * it would be wrong upward exactly on the long outages. The client renders the
 * clock time this carries, and computing an elapsed figure from it is the one
 * thing this object is not for.
 */
export class AttractionOutageDto {
  @ApiProperty({
    description:
      "First DOWN reading of the run that is still open, ISO 8601 UTC. " +
      "When `startObserved` is false this is the oldest reading inside the " +
      "seven-day window rather than the moment the ride stopped.",
    example: "2026-09-06T12:20:00.000Z",
  })
  startedAt: string;

  @ApiProperty({
    description:
      "Whether the transition into DOWN was seen. False means the run already " +
      "covered the oldest reading in the window, so the outage began earlier " +
      "than `startedAt` and a client must name the day rather than a clock time.",
    example: true,
  })
  startObserved: boolean;
}

/**
 * @param outage - What the reconstruction found, or undefined.
 * @returns The DTO, or undefined so the key is absent rather than null.
 */
export function toOutageDto(
  outage: CurrentOutage | undefined,
): AttractionOutageDto | undefined {
  if (!outage) return undefined;
  return {
    startedAt: outage.startedAt.toISOString(),
    startObserved: outage.startObserved,
  };
}
