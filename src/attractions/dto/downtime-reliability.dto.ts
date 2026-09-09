import { ApiProperty } from "@nestjs/swagger";
import {
  DOWNTIME_WITHHELD_REASONS,
  PERMANENT_WITHHELD_REASONS,
} from "../../analytics/entities/attraction-downtime-profile.entity";
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
      "Why nothing is published. `not_down_capable`, `park_never_reports`, " +
      "`artefact_regime` and `no_schedule` are statements about OUR data and " +
      "say nothing about the ride; the rest are about how much of it there is. " +
      "`park_never_reports` is the common one — 91 parks are listed at the " +
      "source and have still never emitted a single DOWN reading.",
    enum: DOWNTIME_WITHHELD_REASONS,
    example: "thin_events",
  })
  reason: DowntimeWithheldReason;

  @ApiProperty({
    description:
      "Reported outages in the window. Present even when withheld, because a " +
      "count needs no estimator — but it is 0 for the four reasons above, " +
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
      "the park's source configuration. `never_reports` is the one value read " +
      "from the outcome, and only past an evidence threshold at which silence " +
      "is no longer compatible with a working feed: the park has been watched " +
      "for 1500+ operating hours and has never emitted a DOWN. Both mean the " +
      "same thing for a ride — its outages are invisible to us — and neither " +
      "may be rendered as 'no outages'.",
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
/**
 * Days a profile may be old before it stops describing "the last 90 days".
 *
 * The job runs nightly, so two days is already two missed runs. Without this
 * check a dead job keeps publishing its last successful night as current, and a
 * retired ride keeps its last profile forever — the profile table is upsert-only
 * and never deletes. That is the same silent-success shape as the recovery-curve
 * bug, one layer out and this time facing a reader.
 *
 * Withholding is the right failure: the numbers themselves may still be roughly
 * true, but the sentence built on them („in den letzten 90 Tagen") is not, and
 * there is no way to tell a visitor which part to discount.
 */
export const MAX_PROFILE_AGE_DAYS = 2;

/**
 * Reasons that describe what we can never see, rather than what we do not yet
 * have enough of.
 *
 * Staleness cannot override these: a park whose source has no DOWN status does
 * not start reporting because a nightly job caught up, and telling a reader the
 * figures are "not current" would promise a resolution that cannot arrive.
 *
 * Defined on the entity rather than here, because the WRITE side needs the same
 * list: a row carrying one of these can never be corrected by ageing, so the
 * rebuild may not leave one behind for a ride it can no longer re-derive.
 */
const PERMANENT_REASONS = PERMANENT_WITHHELD_REASONS;

export function toDowntimeBlock(
  profile: AttractionDowntimeProfile | null | undefined,
  now: Date = new Date(),
): DowntimeBlockDto {
  if (!profile) {
    return {
      kind: "withheld",
      reason: "not_down_capable",
      outages: 0,
      windowDays: 90,
    };
  }

  // A profile older than the job's own cadence describes a window that has
  // moved on. See MAX_PROFILE_AGE_DAYS.
  const ageDays = profile.generatedAt
    ? (now.getTime() - new Date(profile.generatedAt).getTime()) / 86_400_000
    : Number.POSITIVE_INFINITY;
  const stale = !(ageDays <= MAX_PROFILE_AGE_DAYS);

  if (
    stale ||
    !profile.publishable ||
    profile.medianMinutes == null ||
    profile.longestMinutes == null ||
    profile.downShare == null
  ) {
    return {
      kind: "withheld",
      // Staleness gets its own reason. Reusing `thin_events` kept the stored
      // count and rendered „34 Störungen gemeldet. Für eine belastbare Zahl
      // sind das zu wenige" — refuted by its own number, and guaranteed for
      // every stale publishable ride because the event floor is 24.
      // A stored reason WINS over staleness when it is permanent. Preferring
      // stale_data unconditionally told every ride in a blind park that "these
      // numbers are not current" — implying they will catch up — when the truth
      // is that this park's source cannot report an outage at all and never
      // will. Those parks outnumber the near-miss publishable ones by far, so
      // the wrong branch was also the common one.
      reason:
        profile.withheldReason && PERMANENT_REASONS.has(profile.withheldReason)
          ? profile.withheldReason
          : stale
            ? "stale_data"
            : (profile.withheldReason ?? "thin_events"),
      // Zero for the reasons that are about us. The client must not read it as
      // "no outages happened" — the reason says which kind of zero it is. A
      // stale profile sends its real count: `stale_data`'s copy does not use
      // it, and zeroing it would state something false about the ride.
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
