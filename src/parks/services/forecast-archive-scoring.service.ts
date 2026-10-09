import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Redis } from "ioredis";
import { fromZonedTime } from "date-fns-tz";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { Park } from "../entities/park.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { AnalyticsService } from "../../analytics/analytics.service";
import {
  ARCHIVE_ORIGIN_KINDS,
  ARCHIVE_SURFACES,
  ArchiveOriginKind,
  ArchiveSurface,
  ForecastArchiveCurve,
} from "../../ml/entities/forecast-archive-curve.entity";
import { ForecastArchiveParkDay } from "../../ml/entities/forecast-archive-park-day.entity";
import { ForecastArchiveScore } from "../../ml/entities/forecast-archive-score.entity";
import {
  ArchivedCurve,
  CrowdObservation,
  OperatingWindow,
  ScoreAccumulator,
  TruthReading,
  buildTruthSlots,
  calendarDayValue,
  regionOf,
  scoreCrossParkCrowd,
  scoreParkDay,
} from "../../ml/utils/forecast-archive-scoring.util";
import { mapWithDbBudget } from "../../common/utils/db-job-budget";
import { addIsoDays, formatInParkTimezone } from "../../common/utils/date.util";
import {
  StatementLimits,
  queryWithLimits,
} from "../../common/utils/statement-limits.util";

/** One park-day's reads, and the board's archive summary. */
const READ_LIMITS: StatementLimits = {
  statementTimeoutMs: 60_000,
  lockTimeoutMs: 5_000,
  idleInTransactionTimeoutMs: 10_000,
};

/** Bumped by every scoring run; the board cache is keyed on it, so one
 *  INCR retires every cached `days` × region variant at once. */
const BOARD_VERSION_KEY = "forecast-archive:board:version";

/** Parks scored side by side. */
const BATCH_SIZE = 4;
/** How many missed days a run catches up on. */
const CATCH_UP_DAYS = 3;
/** Scores are ~0.35 MB a day; 400 days lets a season be read against the
 *  same season a year earlier. */
const SCORE_RETENTION_DAYS = 400;
/** The board moves once a day; well above any admin poll interval. */
const BOARD_TTL_SECONDS = 2 * 3600;

const SURFACE_BY_ID = new Map<number, ArchiveSurface>(
  Object.entries(ARCHIVE_SURFACES).map(([k, v]) => [v, k as ArchiveSurface]),
);
const ORIGIN_BY_ID = new Map<number, ArchiveOriginKind>(
  Object.entries(ARCHIVE_ORIGIN_KINDS).map(([k, v]) => [
    v,
    k as ArchiveOriginKind,
  ]),
);

/** One line of the admin board: pooled counters plus the derived metrics. */
export interface ForecastArchiveBoardRow {
  useCase: string;
  lead: string;
  source: string;
  segment: string;
  days: number;
  metrics: Record<string, number | null>;
  sums: Record<string, number>;
}

/**
 * The forward archive's read side (PAR-831): scores yesterday's archived
 * curves against BENCH-SPEC truth and serves the board.
 *
 * Truth is read from `queue_data` per park for ONE day — the rides of one park,
 * from three hours before its first published window (the forward-fill reach)
 * to its last close. Yesterday's chunk is not compressed yet, and the read is
 * a few thousand rows per park on the `(attractionId, queueType, …)` index.
 */
@Injectable()
export class ForecastArchiveScoringService {
  private readonly logger = new Logger(ForecastArchiveScoringService.name);

  constructor(
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
    @InjectRepository(Attraction)
    private readonly attractionRepository: Repository<Attraction>,
    @InjectRepository(ForecastArchiveCurve)
    private readonly curveRepository: Repository<ForecastArchiveCurve>,
    @InjectRepository(ForecastArchiveParkDay)
    private readonly parkDayRepository: Repository<ForecastArchiveParkDay>,
    @InjectRepository(ForecastArchiveScore)
    private readonly scoreRepository: Repository<ForecastArchiveScore>,
    private readonly analyticsService: AnalyticsService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * Scores every target date that is over everywhere and not scored yet.
   *
   * The job runs at 12:00 UTC: the previous UTC date is then over in every park
   * timezone (UTC−10 ends it at 10:00 UTC), including the hour or two a
   * midnight-crossing park keeps going. Up to {@link CATCH_UP_DAYS} earlier
   * dates are picked up if a run was missed.
   */
  async scoreDue(now: Date = new Date()): Promise<{ dates: string[] }> {
    const latest = addIsoDays(now.toISOString().slice(0, 10), -1);
    const earliest = addIsoDays(latest, -(CATCH_UP_DAYS - 1));
    const scored: Array<{ d: string }> =
      await this.scoreRepository.manager.query(
        `SELECT DISTINCT target_date::text AS d FROM forecast_archive_scores
        WHERE target_date BETWEEN $1::date AND $2::date`,
        [earliest, latest],
      );
    const done = new Set(scored.map((r) => r.d));
    const dates: string[] = [];
    for (let d = earliest; d <= latest; d = addIsoDays(d, 1)) {
      if (done.has(d)) continue;
      const rows = await this.scoreDate(d, now);
      if (rows > 0) dates.push(d);
    }
    await this.scoreRepository.manager
      .query(
        `DELETE FROM forecast_archive_scores WHERE target_date < $1::date`,
        [addIsoDays(latest, -SCORE_RETENTION_DAYS)],
      )
      .catch((err: Error) =>
        this.logger.warn(
          `Forward archive: score retention failed: ${err.message}`,
        ),
      );
    if (dates.length > 0) {
      await this.redis.incr(BOARD_VERSION_KEY).catch(() => undefined);
    }
    return { dates };
  }

  /** Scores one target date over all parks; returns the score rows written. */
  async scoreDate(targetDate: string, now: Date = new Date()): Promise<number> {
    const parks = await this.parkRepository.find({
      select: ["id", "slug", "timezone", "continentSlug"],
    });
    const acc = new ScoreAccumulator();
    const observations: CrowdObservation[] = [];
    let parksScored = 0;

    for (let i = 0; i < parks.length; i += BATCH_SIZE) {
      const batch = parks.slice(i, i + BATCH_SIZE);
      const results = await mapWithDbBudget(batch, (park) =>
        this.scorePark(park, targetDate, acc).catch((err: Error) => {
          // Unscored, not zero: this park's day is simply absent tonight.
          this.logger.warn(
            `Forward archive scoring: ${park.slug} ${targetDate} failed: ${err.message}`,
          );
          return null;
        }),
      );
      for (const r of results) {
        if (!r) continue;
        parksScored++;
        observations.push(...r);
      }
    }
    scoreCrossParkCrowd(observations, acc);

    const entries = acc.entries();
    if (entries.length === 0) return 0;
    const rows = entries.map((e) =>
      this.scoreRepository.create({ targetDate, ...e, scoredAt: now }),
    );
    await this.scoreRepository.manager.transaction(async (em) => {
      await em.delete(ForecastArchiveScore, { targetDate });
      for (let i = 0; i < rows.length; i += 500) {
        await em.insert(ForecastArchiveScore, rows.slice(i, i + 500));
      }
    });
    this.logger.log(
      `📐 Forward archive ${targetDate}: ${parksScored} parks, ${rows.length} score rows`,
    );
    return rows.length;
  }

  /** One park-day; null when there was nothing to score. */
  private async scorePark(
    park: Park,
    targetDate: string,
    acc: ScoreAccumulator,
  ): Promise<CrowdObservation[] | null> {
    const curveRows = await this.curveRepository.find({
      where: { parkId: park.id, targetDate },
    });
    if (curveRows.length === 0) return null;
    const parkDayRows = await this.parkDayRepository.find({
      where: { parkId: park.id, targetDate },
    });

    const windows = await this.operatingWindows(park.id, targetDate);
    const attractionIds = (
      await this.attractionRepository.find({
        where: { parkId: park.id },
        select: ["id"],
      })
    ).map((a) => a.id);

    // The whole park-local day (D6's calendar statistic has no window) plus
    // the 3 h of forward-fill reach before the first window.
    const dayStart = fromZonedTime(`${targetDate}T00:00:00`, park.timezone);
    const dayEnd = fromZonedTime(
      `${addIsoDays(targetDate, 1)}T00:00:00`,
      park.timezone,
    );
    const readings =
      attractionIds.length > 0
        ? await this.truthReadings(
            attractionIds,
            Math.min(
              dayStart.getTime(),
              ...windows.map((w) => w.open - 3 * 3_600_000),
            ),
            Math.max(dayEnd.getTime(), ...windows.map((w) => w.close)),
          )
        : new Map<string, TruthReading[]>();

    const truth = new Map<string, Map<number, number>>();
    if (windows.length > 0) {
      for (const [id, list] of readings) {
        truth.set(id, buildTruthSlots(list, windows));
      }
    }

    const headliners = await this.analyticsService
      .getHeadlinerAttractions(park.id)
      .then((h) => new Set(h.map((x) => x.attractionId)))
      .catch(() => new Set<string>());

    return scoreParkDay(
      {
        region: regionOf(park.continentSlug),
        curves: curveRows.map((c) =>
          ForecastArchiveScoringService.toArchived(c),
        ),
        parkDays: parkDayRows.map((d) => ({
          originAt: new Date(d.originAt).getTime(),
          leadDays: d.leadDays,
          tier: d.tier,
          crowdLevel: d.crowdLevel,
          predictedCrowdLevel: d.predictedCrowdLevel,
          levelSource: d.levelSource,
          typicalDayPeak:
            d.typicalDayPeak === null ? null : Number(d.typicalDayPeak),
          crowdLevelFallback: d.crowdLevelFallback,
        })),
        truth,
        hadWindows: windows.length > 0,
        calendarDayValue: calendarDayValue(
          readings,
          headliners,
          (ts) =>
            formatInParkTimezone(new Date(ts), park.timezone) === targetDate,
        ),
      },
      acc,
    );
  }

  static toArchived(c: ForecastArchiveCurve): ArchivedCurve {
    return {
      attractionId: c.attractionId,
      surface: SURFACE_BY_ID.get(c.surface) ?? "park_hourly",
      originKind: ORIGIN_BY_ID.get(c.originKind) ?? "daily",
      originAt: new Date(c.originAt).getTime(),
      leadDays: c.leadDays,
      slotStart: new Date(c.slotStart).getTime(),
      slotMinutes: c.slotMinutes,
      waits: c.waits ?? [],
      sources: c.sources ?? "",
      bands: c.bands,
      dayPeak: c.dayPeak,
      expectedError: c.expectedError,
      rideQ90: c.rideQ90,
      isHeadliner: c.isHeadliner,
      peakBand: c.peakBand,
      liveWait: c.liveWait,
      laterWait: c.laterWait,
      laterAt: c.laterAt ? new Date(c.laterAt).getTime() : null,
      levelSource: c.levelSource,
    };
  }

  /** The park's published OPERATING windows for the date (park-wide rows). */
  private async operatingWindows(
    parkId: string,
    date: string,
  ): Promise<OperatingWindow[]> {
    const rows = await queryWithLimits<Array<{ open: Date; close: Date }>>(
      this.parkRepository.manager.connection,
      `SELECT "openingTime" AS open, "closingTime" AS close
         FROM schedule_entries
        WHERE "parkId" = $1 AND date = $2::date
          AND "scheduleType" = 'OPERATING' AND "attractionId" IS NULL
          AND "openingTime" IS NOT NULL AND "closingTime" IS NOT NULL`,
      [parkId, date],
      READ_LIMITS,
    );
    return rows
      .map((r) => ({
        open: new Date(r.open).getTime(),
        close: new Date(r.close).getTime(),
      }))
      .filter((w) => w.close > w.open);
  }

  /** STANDBY change-log rows in [fromMs, toMs), grouped per ride. */
  private async truthReadings(
    attractionIds: string[],
    fromMs: number,
    toMs: number,
  ): Promise<Map<string, TruthReading[]>> {
    const rows = await queryWithLimits<
      Array<{ id: string; ts: Date; status: string; wait: number | null }>
    >(
      this.curveRepository.manager.connection,
      `SELECT "attractionId" AS id, timestamp AS ts, status::text AS status, "waitTime" AS wait
         FROM queue_data
        WHERE "attractionId" = ANY($1::uuid[])
          AND "queueType" = 'STANDBY'
          AND timestamp >= $2 AND timestamp < $3
        ORDER BY "attractionId", timestamp`,
      [attractionIds, new Date(fromMs), new Date(toMs)],
      READ_LIMITS,
    );
    const out = new Map<string, TruthReading[]>();
    for (const r of rows) {
      const list = out.get(r.id) ?? [];
      list.push({
        timestamp: new Date(r.ts).getTime(),
        status: r.status,
        waitTime: r.wait === null ? null : Number(r.wait),
      });
      out.set(r.id, list);
    }
    return out;
  }

  /**
   * The admin board: counters pooled over the last `days` scored target dates
   * for one region, with the derived metrics. Ordered use case → lead → source
   * → segment. Cached for {@link BOARD_TTL_SECONDS}; scoring evicts it.
   */
  async getBoard(
    days: number,
    region = "ALL",
  ): Promise<{
    region: string;
    days: number;
    targetDates: { first: string | null; last: string | null; count: number };
    archive: Array<Record<string, unknown>>;
    rows: ForecastArchiveBoardRow[];
  }> {
    const version = await this.redis.get(BOARD_VERSION_KEY).catch(() => null);
    const cacheKey = `forecast-archive:board:v${version ?? 0}:${region}:${days}`;
    const cached = await this.redis.get(cacheKey).catch(() => null);
    if (cached) {
      try {
        return JSON.parse(cached);
      } catch {
        // rebuild
      }
    }

    const scores: ForecastArchiveScore[] = await this.scoreRepository
      .createQueryBuilder("s")
      .where("s.region = :region", { region })
      .andWhere(
        "s.targetDate >= (SELECT max(target_date) FROM forecast_archive_scores) - (:days)::int + 1",
        { days },
      )
      .getMany();

    const groups = new Map<
      string,
      { dates: Set<string>; sums: Record<string, number> }
    >();
    const allDates = new Set<string>();
    for (const s of scores) {
      allDates.add(String(s.targetDate));
      const key = [s.useCase, s.lead, s.source, s.segment].join("|");
      const g = groups.get(key) ?? { dates: new Set<string>(), sums: {} };
      g.dates.add(String(s.targetDate));
      for (const [k, v] of Object.entries(s.sums ?? {})) {
        g.sums[k] = (g.sums[k] ?? 0) + Number(v);
      }
      groups.set(key, g);
    }

    const rows: ForecastArchiveBoardRow[] = [...groups.entries()]
      .map(([key, g]) => {
        const [useCase, lead, source, segment] = key.split("|");
        return {
          useCase,
          lead,
          source,
          segment,
          days: g.dates.size,
          metrics: ForecastArchiveScoringService.deriveMetrics(g.sums),
          sums: g.sums,
        };
      })
      .sort(
        (a, b) =>
          a.useCase.localeCompare(b.useCase) ||
          ForecastArchiveScoringService.leadOrder(a.lead) -
            ForecastArchiveScoringService.leadOrder(b.lead) ||
          a.source.localeCompare(b.source) ||
          a.segment.localeCompare(b.segment),
      );

    const archive = await queryWithLimits<Array<Record<string, unknown>>>(
      this.curveRepository.manager.connection,
      // The last 24 h only, so the PK's leading `origin_at` bounds the read;
      // the oldest origin is one PK probe.
      `SELECT surface, origin_kind AS "originKind", count(*)::int AS "rowsLast24h",
              max(origin_at) AS "lastOrigin",
              (SELECT min(origin_at) FROM forecast_archive_curves) AS "firstOrigin"
         FROM forecast_archive_curves
        WHERE origin_at > now() - interval '24 hours'
        GROUP BY 1, 2 ORDER BY 1, 2`,
      [],
      READ_LIMITS,
    ).catch(() => []);

    const sortedDates = [...allDates].sort();
    const board = {
      region,
      days,
      targetDates: {
        first: sortedDates[0] ?? null,
        last: sortedDates[sortedDates.length - 1] ?? null,
        count: sortedDates.length,
      },
      archive,
      rows,
    };
    await this.redis
      .set(cacheKey, JSON.stringify(board), "EX", BOARD_TTL_SECONDS)
      .catch(() => undefined);
    return board;
  }

  /** `h0-1` … `h24-48` first, then `d0` … `d7`. */
  static leadOrder(lead: string): number {
    if (lead.startsWith("h")) return Number(lead.slice(1).split("-")[0]);
    if (lead.startsWith("d")) return 1000 + Number(lead.slice(1));
    return 9999;
  }

  /** Means from pooled counters; null where the denominator is zero. */
  static deriveMetrics(
    s: Record<string, number>,
  ): Record<string, number | null> {
    const ratio = (num: string, den: string, digits = 3): number | null => {
      const d = s[den] ?? 0;
      // Counter names repeat across use cases (`n` is slots on UC rows and
      // park-days on D6), so a missing numerator means "not this kind of row".
      if (!d || s[num] === undefined) return null;
      const f = 10 ** digits;
      return Math.round(((s[num] ?? 0) / d) * f) / f;
    };
    return {
      mae: ratio("sae", "n", 2),
      bias: ratio("se", "n", 2),
      bandCoverage: ratio("nBandCov", "nBand"),
      bandFieldCoverage: ratio("nBand", "n"),
      // Regret is the D3 headline; hit rates exclude flat ride-days.
      bestTimeRegret: ratio("regret", "rd", 2),
      bestTimeHitTop2: ratio("hitTop2", "rdHit"),
      bestTimeHit30: ratio("hit30", "rdHit"),
      bestTimeFlatShare: ratio("rdFlat", "rd"),
      slotSpearman: ratio("sRho", "nRho"),
      dayPeakMae: ratio("sPeakAe", "nPeak", 2),
      dayPeakBias: ratio("sPeakE", "nPeak", 2),
      statedError: ratio("sStated", "nStated", 2),
      realisedErrorWhereStated: ratio("sStatedAe", "nStated", 2),
      statedFieldCoverage: ratio("nStated", "nPeak"),
      dayPeakBandCoverage: ratio("nPeakBandCov", "nPeakBand"),
      dayPeakBandFieldCoverage: ratio("nPeakBand", "nPeak"),
      dayPeakRankSpearman: ratio("sRank", "nRank"),
      dayPeakPairOrder: ratio("pairsOk", "pairs"),
      rideCoverage: ratio("truthRidesOffered", "truthRides"),
      offeredNotOperating: ratio("offeredNotOperating", "offered"),
      emptyParkDays: ratio("parkDaysEmpty", "parkDays"),
      crowdExact: ratio("exact", "n"),
      crowdWithin1: ratio("within1", "n"),
      busyDayRecall: ratio("busyBoth", "busyTrue"),
      busyDayPrecision: ratio("busyBoth", "busyPred"),
      crossParkOrder: ratio("crossPairsOk", "crossPairs"),
      nextBestRidePrecision: ratio("d1SuggOk", "d1Sugg"),
      nextBestRideFalseRate:
        s.d1Sugg && s.d1SuggOk !== undefined
          ? Math.round((1 - s.d1SuggOk / s.d1Sugg) * 1000) / 1000
          : null,
      nextBestRideBaseRate: ratio("d1NoneWorse", "d1None"),
    };
  }
}
