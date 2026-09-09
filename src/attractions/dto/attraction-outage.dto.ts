import { ApiProperty } from "@nestjs/swagger";
import type { CurrentOutage } from "../services/attraction-outage.service";
import {
  OUTAGE_SIGNALS,
  type OutageSignal,
} from "../../analytics/entities/attraction-outage.entity";

/**
 * When a running outage is expected to be over, on the wall clock.
 *
 * The quartiles of `OutageEstimateDto.remaining` placed on the park's opening
 * calendar. Its own class rather than an inline shape so the pair has one
 * published description, and so a client generated off the spec gets a named
 * type for the one part of the estimate it may render as a time.
 */
export class RecoveryWindowDto {
  @ApiProperty({
    description:
      "When the 25th percentile falls, ISO 8601 UTC — the earliest end this " +
      "estimate is willing to name. Placed on the park's opening calendar, so " +
      "an outage with more operating minutes left than the park has hours " +
      "left today lands on the next opening day.",
    example: "2026-09-09T14:35:00.000Z",
  })
  from: string;

  @ApiProperty({
    description:
      "When the 75th percentile falls, ISO 8601 UTC. **The key is absent " +
      "whenever there is no upper bound to give** — past roughly two hours the " +
      "curve stops resolving the upper quartile, and the park's published " +
      "calendar may not reach far enough either. Absent and never null: a null " +
      "could not survive `ExcludeNullInterceptor` on this surface anyway, so " +
      "the type is written the way the wire behaves. Render it as an open " +
      "range, never as a missing value to fill in.",
    required: false,
  })
  to?: string;
}

/**
 * How much longer a running outage usually lasts, measured and never predicted.
 *
 * ## What this is, and what it is not
 *
 * `docs/analytics/ride-downtime.md` §6 refuses "when will this ride break next,
 * and for how long". This is the other question — *it is broken now; what
 * happened to outages that got this far* — and it survives the four objections
 * that sank the first one. The full argument is on `DowntimeRecoveryCurve`; the
 * short version is that the conditioning event is observed rather than
 * forecast, the sample per bucket runs from 5900 to 128000 intervals rather
 * than the ~1300 a rate would need, and counting in operating minutes takes
 * censoring from 68 % to 15.6 %.
 *
 * Out-of-sample calibration error, fit on 120 days and scored on the next 60:
 * **2.55 percentage points**.
 *
 * ## For a client
 *
 * The probabilities are the load-bearing figures and the pair `remaining` is
 * context. Render the spread whenever you render the median: the distribution is
 * heavy-tailed, and a median on its own reads as a promise. `remaining` is
 * absent past about two hours precisely because the spread stops resolving
 * there, so a client that renders only the median will silently show nothing on
 * exactly the long outages a visitor most wants to know about.
 *
 * `remaining` counts **operating** minutes and may never be added to a clock.
 * `recoveryWindow` is that same pair already placed on the park's calendar, and
 * it is the only one of the two a client may render as a time.
 *
 * Every sentence built on this says *reported* — the numerator is
 * "themeparks-wiki said DOWN", not "the ride was broken".
 */
export class OutageEstimateDto {
  @ApiProperty({
    description:
      "Operating minutes already elapsed — the park's own open minutes since " +
      "the outage started, NOT wall-clock time since `startedAt`.",
    example: 45,
  })
  elapsedMinutes: number;

  @ApiProperty({
    description:
      "Probability the ride is reported running again within 30 more " +
      "operating minutes, 0-1.",
    example: 0.317,
  })
  recoveryWithin30: number;

  @ApiProperty({
    description:
      "Probability the ride is reported running again within 60 more " +
      "operating minutes, 0-1.",
    example: 0.504,
  })
  recoveryWithin60: number;

  @ApiProperty({
    description:
      "Remaining **operating** minutes at the 25th, 50th and 75th percentile — " +
      "the park's own open minutes, so this is not a duration that may be " +
      "added to a clock. Use `recoveryWindow` for that. `p75` is **absent** " +
      "past roughly two hours, where the curve stops resolving the upper " +
      'quartile — render that as an open range ("ab 2:45 h"), and never render ' +
      "the median without the spread around it.",
    required: false,
  })
  remaining?: { p25: number; median: number; p75: number | null };

  @ApiProperty({
    description:
      "The same quartiles as instants: when the outage is expected to be over, " +
      "on the wall clock. Derived from `remaining.p25`/`p75` over the park's " +
      "opening calendar, because operating minutes cannot be turned into a " +
      "clock time without it — an outage with two operating hours left, in a " +
      "park shutting in twenty minutes, ends tomorrow morning. **Absent when " +
      "the park publishes no opening hours**, and absent when its calendar " +
      "does not reach far enough; there is deliberately no wall-clock fallback, " +
      "because that would answer a different question.",
    required: false,
    type: () => RecoveryWindowDto,
  })
  recoveryWindow?: RecoveryWindowDto;

  @ApiProperty({
    description:
      "Whether this park carried its own curve at this elapsed bucket, or the " +
      "pooled one was used. Diagnostic; not something to render.",
    enum: ["park", "pooled"],
    example: "park",
  })
  basis: "park" | "pooled";
}

/**
 * @param outage - What the reconstruction found, or undefined.
 * @returns The DTO, or undefined so the key is absent rather than null.
 */

/**
 * The running outage on a ride that is reported `DOWN` right now.
 *
 * Present only while the ride reads `DOWN`, only in a park whose configuration
 * lets a source say so at all, and only outside a curated works period. Absent
 * means one of those three, and a client may not read its absence as "running".
 *
 * The elapsed figure a client may need is on `estimate.elapsedMinutes`, and it
 * is NOT `now - startedAt`. It counts the park's own OPERATING minutes, so an
 * outage that began at 18:00 in a park that shut at 20:00 reads two hours the
 * next morning rather than sixteen. Subtracting the two timestamps is the one
 * arithmetic this object is not for.
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

  @ApiProperty({
    description:
      "Which signal placed this outage. `down` is the operator's own feed " +
      "saying the ride is not running. `closed_gap` is INFERRED — the ride was " +
      "open earlier the same day, shut inside opening hours, and did not shut " +
      "together with the rest of the park. It appears only for parks whose " +
      "feed never emits DOWN (102 of 182 scheduled parks), where the " +
      "alternative is silence rather than a stronger signal. **A client MUST " +
      "word the two differently**: nobody reported a `closed_gap`, so no " +
      "sentence built on it may say 'reported'.",
    enum: OUTAGE_SIGNALS,
    example: "down",
  })
  signal: OutageSignal;

  @ApiProperty({
    description:
      "How long outages like this one usually still take from here. Absent " +
      "whenever the measured curve cannot answer — too short to have a " +
      "bucket, too thin a sample, or a park that publishes no opening hours " +
      "so there is no operating clock to count on. Absence NEVER means the " +
      "outage is about to end.",
    required: false,
    type: () => OutageEstimateDto,
  })
  estimate?: OutageEstimateDto;
}

export function toOutageDto(
  outage: CurrentOutage | undefined,
): AttractionOutageDto | undefined {
  if (!outage) return undefined;
  return {
    startedAt: outage.startedAt.toISOString(),
    startObserved: outage.startObserved,
    signal: outage.signal,
    estimate: outage.estimate
      ? {
          elapsedMinutes: outage.estimate.elapsedMinutes,
          recoveryWithin30: outage.estimate.recoveryWithin30,
          recoveryWithin60: outage.estimate.recoveryWithin60,
          remaining: outage.estimate.remaining,
          recoveryWindow: outage.estimate.recoveryWindow,
          basis: outage.estimate.basis,
          // `bucketMinutes` and `sampleSize` deliberately do NOT travel: they
          // are inputs to the decision, and this endpoint answers ~40 000 ride
          // pages. The same rule the profile DTO's discriminated union follows.
        }
      : undefined,
  };
}
