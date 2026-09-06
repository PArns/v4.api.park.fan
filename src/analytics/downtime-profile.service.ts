import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { DataSource, Repository } from "typeorm";
import { AttractionDowntimeProfile } from "./entities/attraction-downtime-profile.entity";
import type { DowntimeWithheldReason } from "./entities/attraction-downtime-profile.entity";
import { ParkDowntimeCoverage } from "./entities/park-downtime-coverage.entity";
import type { DowntimeRegime } from "./entities/park-downtime-coverage.entity";

/**
 * Thresholds, all of them provisional.
 *
 * Every number here is a placeholder until `GET /v1/admin/downtime-measurement`
 * has been run against production and the event distribution is known. They are
 * written down in one object so re-deriving them is an edit to a constant rather
 * than a hunt through a service, and `docs/analytics/ride-downtime.md` records
 * which of them the measurement is supposed to replace.
 *
 * They are deliberately strict. Publishing nothing is the correct default here,
 * and every one of them was chosen so that the first production run withholds
 * more than it should rather than less.
 */
export const DOWNTIME_GATES = {
  /** Window the profile describes. */
  windowDays: 90,

  /**
   * Events before anything beyond the bare count is published.
   *
   * 24 gives roughly 40 % relative half-width on a count (1.96/√24). It is NOT
   * derived from a two-sample power calculation — that would want about 33 per
   * arm — because the comparison such a calculation licenses is not rendered
   * anywhere. Precision of the single figure is the whole justification.
   */
  minOutages: 24,

  /** Of those, how many need a usable length before a median is published. */
  minUsableDurations: 12,

  /** Park operating days on which the ride was observed at all. */
  minObservedDays: 40,

  /** Observed operating hours behind the window. */
  minExposureHours: 150,

  /** Above this share of censored intervals, no duration is published. */
  maxCensoredShare: 0.25,

  /**
   * A park where this share of intervals rests on a single reading is in the
   * artefact regime: the readings are being erased between polls.
   */
  artefactSingleReadingShare: 0.7,

  /**
   * A ride's first operating days are excluded.
   *
   * A new ride's teething failures are real and are not its steady state, and a
   * figure that mixes the two describes neither.
   */
  newRideOperatingDays: 30,

  /**
   * Ratio between the two halves of the window beyond which they are treated as
   * describing different things.
   *
   * A ride that broke eleven times in October and twice since is not a ride with
   * a rate; one number over the whole window would hide exactly the change a
   * reader wants.
   */
  maxHalfRatio: 3,

  /** Both halves need this many events before the check can say anything. */
  minEventsPerHalf: 6,
} as const;

/**
 * Turn intervals and exposure into what may be published, and mostly refuse.
 *
 * The gates live here and nowhere else. A surface that computed its own
 * aggregate would be a second place they could be forgotten, and the difference
 * between a figure and a claim about a named operator is exactly these
 * comparisons.
 */
@Injectable()
export class DowntimeProfileService {
  private readonly logger = new Logger(DowntimeProfileService.name);

  constructor(
    private readonly dataSource: DataSource,
    @InjectRepository(AttractionDowntimeProfile)
    private readonly profiles: Repository<AttractionDowntimeProfile>,
    @InjectRepository(ParkDowntimeCoverage)
    private readonly coverage: Repository<ParkDowntimeCoverage>,
  ) {}

  /**
   * Recompute coverage for every park and profiles for every tracked ride.
   *
   * @param parkIds - Restrict to these parks, or null for the whole catalogue.
   */
  async rebuild(parkIds: string[] | null): Promise<void> {
    const generatedAt = new Date();
    const windowTo = generatedAt;
    const windowFrom = new Date(
      windowTo.getTime() - DOWNTIME_GATES.windowDays * 24 * 60 * 60 * 1000,
    );

    const regimes = await this.rebuildCoverage(parkIds, generatedAt);
    await this.rebuildProfiles(
      parkIds,
      regimes,
      windowFrom,
      windowTo,
      generatedAt,
    );
  }

  /**
   * The per-park honesty label.
   *
   * Capability comes first and comes from `parks.wiki_entity_id`. Reading it off
   * the outcome instead — "did we see any outages here" — calls a quiet quarter a
   * blind park and, worse, a blind park a flawless one.
   */
  private async rebuildCoverage(
    parkIds: string[] | null,
    generatedAt: Date,
  ): Promise<Map<string, DowntimeRegime>> {
    const rows: CoverageRow[] = await this.dataSource.query(
      `
      SELECT p.id                                        AS "parkId",
             (p.wiki_entity_id IS NOT NULL)              AS "downCapable",
             EXISTS (
               SELECT 1 FROM schedule_entries se
                WHERE se."parkId" = p.id
                  AND se."attractionId" IS NULL
                  AND se."scheduleType" = 'OPERATING'
             )                                           AS "hasSchedule",
             COUNT(DISTINCT a.id) FILTER (WHERE a.retired_at IS NULL)::int
                                                         AS "ridesTracked",
             COUNT(DISTINCT o."attractionId")::int       AS "ridesWithOutages",
             COUNT(o.*)::int                             AS "outages",
             COUNT(o.*) FILTER (WHERE o.rows_in_spell <= 1)::int
                                                         AS "singleReadingSpells",
             PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o.operating_minutes)
               FILTER (WHERE o.duration_usable)          AS "medianSpellMinutes"
        FROM parks p
        LEFT JOIN attractions a ON a."parkId" = p.id
        LEFT JOIN attraction_outages o
               ON o."attractionId" = a.id
              AND NOT o.likely_works_period
       WHERE ($1::uuid[] IS NULL OR p.id = ANY($1::uuid[]))
       GROUP BY p.id, p.wiki_entity_id
      `,
      [parkIds],
    );

    const regimes = new Map<string, DowntimeRegime>();
    const toSave: Partial<ParkDowntimeCoverage>[] = [];

    for (const row of rows) {
      const outages = Number(row.outages) || 0;
      const singleShare =
        outages > 0 ? Number(row.singleReadingSpells) / outages : 0;

      const regime: DowntimeRegime = !row.downCapable
        ? "not_capable"
        : !row.hasSchedule
          ? "no_schedule"
          : outages > 0 &&
              singleShare >= DOWNTIME_GATES.artefactSingleReadingShare
            ? "artefact"
            : "reports";

      regimes.set(row.parkId, regime);
      toSave.push({
        parkId: row.parkId,
        regime,
        downCapable: row.downCapable,
        ridesTracked: clampSmallint(Number(row.ridesTracked) || 0),
        ridesWithOutages: clampSmallint(Number(row.ridesWithOutages) || 0),
        outages,
        oneIntervalSpellShare: singleShare.toFixed(3),
        medianSpellMinutes:
          row.medianSpellMinutes == null
            ? null
            : Math.round(Number(row.medianSpellMinutes)),
        generatedAt,
      });
    }

    if (toSave.length > 0) {
      await this.coverage.upsert(toSave as ParkDowntimeCoverage[], ["parkId"]);
    }
    return regimes;
  }

  private async rebuildProfiles(
    parkIds: string[] | null,
    regimes: Map<string, DowntimeRegime>,
    windowFrom: Date,
    windowTo: Date,
    generatedAt: Date,
  ): Promise<void> {
    const rows: ProfileRow[] = await this.dataSource.query(
      `
      WITH ex AS (
        SELECT e."attractionId" AS aid, e."parkId" AS pid,
               SUM(e.operating_minutes)::int      AS operating_minutes,
               SUM(e.down_minutes)::int           AS down_minutes,
               COUNT(*) FILTER (WHERE e.operating_minutes > 0)::int AS observed_days
          FROM attraction_exposure_days e
         WHERE e.op_day >= $2::date AND e.op_day <= $3::date
           AND ($1::uuid[] IS NULL OR e."parkId" = ANY($1::uuid[]))
         GROUP BY e."attractionId", e."parkId"
      ),
      ou AS (
        SELECT o."attractionId" AS aid,
               COUNT(*)::int                                        AS outages,
               COUNT(*) FILTER (WHERE o.duration_usable)::int       AS usable,
               COUNT(*) FILTER (WHERE NOT o.duration_usable)::int   AS censored,
               COUNT(*) FILTER (WHERE o.started_at < $4::timestamptz)::int AS first_half,
               COUNT(*) FILTER (WHERE o.started_at >= $4::timestamptz)::int AS second_half,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o.operating_minutes)
                 FILTER (WHERE o.duration_usable)                   AS median_minutes,
               MAX(o.operating_minutes) FILTER (WHERE o.duration_usable) AS longest_minutes
          FROM attraction_outages o
         WHERE o.started_at >= $2::timestamptz AND o.started_at <= $3::timestamptz
           AND NOT o.likely_works_period
           AND ($1::uuid[] IS NULL OR o."parkId" = ANY($1::uuid[]))
         GROUP BY o."attractionId"
      )
      SELECT ex.aid                       AS "attractionId",
             ex.pid                       AS "parkId",
             ex.operating_minutes         AS "operatingMinutes",
             ex.down_minutes              AS "downMinutes",
             ex.observed_days             AS "observedDays",
             COALESCE(ou.outages, 0)      AS "outages",
             COALESCE(ou.usable, 0)       AS "usableDurations",
             COALESCE(ou.censored, 0)     AS "censored",
             COALESCE(ou.first_half, 0)   AS "firstHalf",
             COALESCE(ou.second_half, 0)  AS "secondHalf",
             ou.median_minutes            AS "medianMinutes",
             ou.longest_minutes           AS "longestMinutes",
             a.last_merged_at             AS "lastMergedAt",
             (SELECT MIN(e2.op_day) FROM attraction_exposure_days e2
               WHERE e2."attractionId" = ex.aid AND e2.operating_minutes > 0)
                                          AS "firstObservedDay",
             (SELECT COUNT(*) FROM attraction_exposure_days e3
               WHERE e3."attractionId" = ex.aid AND e3.operating_minutes > 0)::int
                                          AS "lifetimeObservedDays"
        FROM ex
        JOIN attractions a ON a.id = ex.aid
        LEFT JOIN ou ON ou.aid = ex.aid
      `,
      [
        parkIds,
        isoDay(windowFrom),
        isoDay(windowTo),
        midpoint(windowFrom, windowTo),
      ],
    );

    const toSave: Partial<AttractionDowntimeProfile>[] = [];
    for (const row of rows) {
      const decision = decideProfile(row, regimes.get(row.parkId));
      const longest =
        row.longestMinutes == null ? null : Number(row.longestMinutes);
      toSave.push({
        attractionId: row.attractionId,
        parkId: row.parkId,
        windowDays: DOWNTIME_GATES.windowDays,
        windowFrom: isoDay(windowFrom),
        windowTo: isoDay(windowTo),
        exposureHours: (Number(row.operatingMinutes) / 60).toFixed(2),
        observedDays: clampSmallint(Number(row.observedDays) || 0),
        outages: clampSmallint(Number(row.outages) || 0),
        usableDurations: clampSmallint(Number(row.usableDurations) || 0),
        medianMinutes: decision.figures
          ? row.medianMinutes == null
            ? null
            : Math.round(Number(row.medianMinutes))
          : null,
        longestMinutes: decision.figures ? longest : null,
        longestStartedAt: null,
        downShare: decision.figures ? decision.downShare : null,
        censoredShare: decision.censoredShare.toFixed(3),
        publishable: decision.figures,
        withheldReason: decision.reason,
        generatedAt,
      });
    }

    if (toSave.length === 0) return;
    for (let i = 0; i < toSave.length; i += 500) {
      await this.profiles.upsert(
        toSave.slice(i, i + 500) as AttractionDowntimeProfile[],
        ["attractionId"],
      );
    }
    const published = toSave.filter((p) => p.publishable).length;
    this.logger.log(
      `📉 Downtime profiles: ${toSave.length} ride(s), ${published} publishable`,
    );
  }
}

// ── the gates ────────────────────────────────────────────────────────────────

export interface ProfileInputs {
  parkId: string;
  operatingMinutes: number | string;
  downMinutes: number | string;
  observedDays: number | string;
  outages: number | string;
  usableDurations: number | string;
  censored: number | string;
  firstHalf: number | string;
  secondHalf: number | string;
  medianMinutes: number | string | null;
  longestMinutes: number | string | null;
  lastMergedAt: Date | string | null;
  lifetimeObservedDays: number | string;
}

export interface ProfileDecision {
  /** Whether median / longest / share may be published at all. */
  figures: boolean;
  reason: DowntimeWithheldReason | null;
  downShare: string | null;
  censoredShare: number;
}

/**
 * The order of these checks is the whole design.
 *
 * Capability first, because "this park cannot report an outage" is a different
 * sentence from "this ride had none" and the second must never be shown in place
 * of the first. Then the things that make a number meaningless (artefact regime,
 * a merged history, a ride too new), then the things that make it imprecise
 * (thin events, thin exposure, censoring), and last the homogeneity check, which
 * is the only one that can reject a ride with plenty of data.
 */
export function decideProfile(
  row: ProfileInputs,
  regime: DowntimeRegime | undefined,
): ProfileDecision {
  const outages = num(row.outages);
  const usable = num(row.usableDurations);
  const censored = num(row.censored);
  const observedDays = num(row.observedDays);
  const operatingMinutes = num(row.operatingMinutes);
  const downMinutes = num(row.downMinutes);
  const censoredShare = outages > 0 ? censored / outages : 0;

  const withhold = (reason: DowntimeWithheldReason): ProfileDecision => ({
    figures: false,
    reason,
    downShare: null,
    censoredShare,
  });

  if (!regime || regime === "not_capable") return withhold("not_down_capable");
  if (regime === "no_schedule") return withhold("no_schedule");
  if (regime === "artefact") return withhold("artefact_regime");

  if (row.lastMergedAt) return withhold("recently_merged");
  if (num(row.lifetimeObservedDays) < DOWNTIME_GATES.newRideOperatingDays) {
    return withhold("new_ride");
  }

  if (outages < DOWNTIME_GATES.minOutages) return withhold("thin_events");
  if (usable < DOWNTIME_GATES.minUsableDurations)
    return withhold("thin_events");
  if (observedDays < DOWNTIME_GATES.minObservedDays) {
    return withhold("thin_exposure");
  }
  if (operatingMinutes / 60 < DOWNTIME_GATES.minExposureHours) {
    return withhold("thin_exposure");
  }
  if (censoredShare > DOWNTIME_GATES.maxCensoredShare) {
    return withhold("thin_events");
  }

  // Split-half: a ride whose two halves disagree by more than the ratio is not
  // a ride with one rate, and a single number over the window would hide the
  // change rather than describe it.
  const a = num(row.firstHalf);
  const b = num(row.secondHalf);
  if (
    a >= DOWNTIME_GATES.minEventsPerHalf &&
    b >= DOWNTIME_GATES.minEventsPerHalf
  ) {
    const ratio = Math.max(a, b) / Math.max(1, Math.min(a, b));
    if (ratio > DOWNTIME_GATES.maxHalfRatio) return withhold("inhomogeneous");
  }

  const denominator = downMinutes + operatingMinutes;
  return {
    figures: true,
    reason: null,
    // Down over (down + operating). The ONE denominator on the card: a second
    // one beside it is a division a reader performs and gets a different answer
    // from.
    downShare: denominator > 0 ? (downMinutes / denominator).toFixed(4) : null,
    censoredShare,
  };
}

// ── shapes and arithmetic ────────────────────────────────────────────────────

interface CoverageRow {
  parkId: string;
  downCapable: boolean;
  hasSchedule: boolean;
  ridesTracked: number | string;
  ridesWithOutages: number | string;
  outages: number | string;
  singleReadingSpells: number | string;
  medianSpellMinutes: number | string | null;
}

interface ProfileRow extends ProfileInputs {
  attractionId: string;
  firstObservedDay: string | null;
}

function num(value: number | string | null | undefined): number {
  const parsed = typeof value === "string" ? Number(value) : (value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function isoDay(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function midpoint(from: Date, to: Date): Date {
  return new Date((from.getTime() + to.getTime()) / 2);
}

/** Postgres smallint tops out at 32767, and a counter must not overflow a column. */
function clampSmallint(value: number): number {
  return Math.max(-32768, Math.min(32767, Math.round(value)));
}
