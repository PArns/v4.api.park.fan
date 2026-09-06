import { ApiProperty } from "@nestjs/swagger";
import { DOWNTIME_WITHHELD_REASONS } from "../../analytics/entities/attraction-downtime-profile.entity";
import type { DowntimeWithheldReason } from "../../analytics/entities/attraction-downtime-profile.entity";
import { DOWNTIME_REGIMES } from "../../analytics/entities/park-downtime-coverage.entity";
import type { DowntimeRegime } from "../../analytics/entities/park-downtime-coverage.entity";
import type { AttractionDowntimeProfile } from "../../analytics/entities/attraction-downtime-profile.entity";

/**
 * What we can say about how often this ride is reported down, or why we cannot.
 *
 * A **discriminated union**, and that is the point rather than a style
 * preference. The alternative — one object with nullable figures plus the counts
 * and thresholds that produced them — puts every input to the decision on the
 * ride page of forty thousand rides, and invites a client to re-derive the
 * verdict from them and get a different one. The gates live in
 * `DowntimeProfileService` and their output is a verdict, not evidence.
 *
 * The `withheld` branch carries a REASON and no numbers at all. Four of those
 * reasons are about us rather than about the ride, and a reader is owed the
 * difference: "no source here reports outages" is not "this ride never breaks".
 */
export const DOWNTIME_BLOCK_KINDS = ["figures", "withheld"] as const;

export class DowntimeFiguresDto {
  @ApiProperty({ enum: ["figures"], example: "figures" })
  kind: "figures";

  @ApiProperty({
    description: "Days the window covers.",
    example: 90,
  })
  windowDays: number;

  @ApiProperty({
    description:
      "Outages the data source REPORTED in the window, works periods excluded. " +
      "Not 'times the ride broke': only ThemeParks.wiki emits the status at " +
      "all, and the merge can overwrite it.",
    example: 34,
  })
  outages: number;

  @ApiProperty({
    description: "Park operating days on which this ride was observed running.",
    example: 61,
  })
  observedDays: number;

  @ApiProperty({
    description:
      "Median length of the outages with an observed end, in minutes, rounded " +
      "to five. The EMPIRICAL median of that set — not a Kaplan-Meier estimate, " +
      "so the sentence 'half of the N observed outages' beside it is checkable " +
      "against `usableDurations`.",
    example: 25,
  })
  medianMinutes: number;

  @ApiProperty({
    description: "How many outages the median is taken over.",
    example: 28,
  })
  usableDurations: number;

  @ApiProperty({
    description: "Longest observed outage in the window, in minutes.",
    example: 200,
  })
  longestMinutes: number;

  @ApiProperty({
    description:
      "Down minutes over (down + operating) minutes inside the park's hours. " +
      "The only denominator on this block: a second one would be a division a " +
      "reader performs and gets a different answer from.",
    example: 0.021,
  })
  downShare: number;
}

export class DowntimeWithheldDto {
  @ApiProperty({ enum: ["withheld"], example: "withheld" })
  kind: "withheld";

  @ApiProperty({
    description:
      "Why nothing is published. `not_down_capable`, `artefact_regime` and " +
      "`no_schedule` are statements about OUR data and say nothing about the " +
      "ride; the rest are about how much of it there is.",
    enum: DOWNTIME_WITHHELD_REASONS,
    example: "thin_events",
  })
  reason: DowntimeWithheldReason;

  @ApiProperty({
    description:
      "Reported outages in the window. Present even when withheld, because a " +
      "count needs no estimator — but it is 0 for the three reasons above, " +
      "where a zero means 'we cannot see' and not 'none happened'.",
    example: 6,
  })
  outages: number;

  @ApiProperty({
    description: "Days the window covers.",
    example: 90,
  })
  windowDays: number;
}

export class ParkDowntimeCoverageDto {
  @ApiProperty({
    description:
      "What this park's outage readings are worth. `not_capable` is read from " +
      "the park's source configuration and never from the outcome.",
    enum: DOWNTIME_REGIMES,
    example: "reports",
  })
  regime: DowntimeRegime;
}

export type DowntimeBlockDto = DowntimeFiguresDto | DowntimeWithheldDto;

/**
 * Turn a stored profile into the block, or into the reason there is none.
 *
 * A missing profile row is `not_down_capable` rather than an empty object: the
 * nightly job writes a row for every tracked ride, so its absence means the ride
 * is not tracked, which is the same statement.
 */
export function toDowntimeBlock(
  profile: AttractionDowntimeProfile | null | undefined,
): DowntimeBlockDto {
  if (!profile) {
    return {
      kind: "withheld",
      reason: "not_down_capable",
      outages: 0,
      windowDays: 90,
    };
  }

  if (
    !profile.publishable ||
    profile.medianMinutes == null ||
    profile.longestMinutes == null ||
    profile.downShare == null
  ) {
    return {
      kind: "withheld",
      reason: profile.withheldReason ?? "thin_events",
      // Zero for the three reasons that are about us. The client must not read
      // it as "no outages happened" — the reason says which kind of zero it is.
      outages: profile.outages ?? 0,
      windowDays: profile.windowDays,
    };
  }

  return {
    kind: "figures",
    windowDays: profile.windowDays,
    outages: profile.outages,
    observedDays: profile.observedDays,
    medianMinutes: profile.medianMinutes,
    usableDurations: profile.usableDurations,
    longestMinutes: profile.longestMinutes,
    downShare: Number(profile.downShare),
  };
}
