import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Redis } from "ioredis";
import { formatInTimeZone, fromZonedTime } from "date-fns-tz";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { Park } from "../entities/park.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { MLService } from "../../ml/ml.service";
import { PredictionDto } from "../../ml/dto/prediction-response.dto";
import { AnalyticsService } from "../../analytics/analytics.service";
import {
  ARCHIVE_ORIGIN_KINDS,
  ARCHIVE_SURFACES,
  ArchiveOriginKind,
  ForecastArchiveCurve,
} from "../../ml/entities/forecast-archive-curve.entity";
import { ForecastArchiveParkDay } from "../../ml/entities/forecast-archive-park-day.entity";
import { PlanDayService } from "./plan-day.service";
import { CalendarService } from "./calendar.service";
import { PlanDayDto } from "../dto/plan-day.dto";
import { formatInParkTimezone } from "../../common/utils/date.util";
import { currentSlotStartMs } from "../../common/utils/best-visit-times.util";
import { mapWithDbBudget } from "../../common/utils/db-job-budget";
import {
  addIsoDays,
  withStatementLimits,
} from "../utils/forecast-archive-limits";

const SLOT_MS = 15 * 60_000;

/** Parks captured side by side; the shared DB budget caps the sum anyway. */
const BATCH_SIZE = 3;

/** Insert chunk: ~1,000 rows × 16 columns stays far below the 65,535 bind cap. */
const INSERT_CHUNK = 1000;

/** What one capture wrote. */
export interface ArchiveCaptureResult {
  parks: number;
  curves: number;
  parkDays: number;
  failed: number;
}

/** A curve row before it is an entity — what the pure builders return. */
export type CurveDraft = Omit<
  ForecastArchiveCurve,
  | "createdAt"
  | "parkId"
  | "originAt"
  | "originKind"
  | "rideQ90"
  | "isHeadliner"
  | "liveWait"
  | "levelSource"
>;

/**
 * The forward archive's write side (PAR-831): reads the curves the endpoints
 * SERVE and stores them before the next run overwrites them.
 *
 * - **daily origin, 06:00 park-local**: the served 15-min curve for the next
 *   48 h (`MLService.getParkPredictions`, i.e. CatBoost with the PCN override
 *   and persistence blend applied — exactly what the park page, favorites and
 *   `/plan/day`'s measured hours read) and the planner's hourly curve for
 *   d0…d7 (`PlanDayService.buildPlanDay`, the endpoint's own method), plus one
 *   park-day row per lead with the crowd bucket, tier and coverage.
 * - **long leads, 07:00 park-local**: the planner curve and park-day row for
 *   d10, d14, d21, d30, d45, d60 and d90 only (BENCH-SPEC "Horizon"), an hour
 *   after the 06:00 origin so the Europe burst is spread over two hours. Leads
 *   count from the same park-local date.
 * - **intraday origins, 10/12/14/16/18 park-local**: the served 15-min curve
 *   for the next 4 h only — UC1 (≤ 2 h) and the short end of UC2. Only for
 *   parks whose daily capture found a curve for today, so a closed park costs
 *   nothing all day.
 *
 * Nothing here computes a forecast. A second implementation would measure
 * itself, and the first time it drifted from the endpoint the archive would be
 * scoring a product nobody is served.
 */
@Injectable()
export class ForecastArchiveService {
  private readonly logger = new Logger(ForecastArchiveService.name);

  static readonly DAILY_ORIGIN_HOUR = 6;
  /** The sparse long leads, captured an hour after the daily origin. */
  static readonly LONG_ORIGIN_HOUR = 7;
  static readonly LONG_LEADS: readonly number[] = [10, 14, 21, 30, 45, 60, 90];
  /** D1's live anchor: the latest STANDBY reading no older than this. */
  static readonly LIVE_ANCHOR_MAX_AGE_MINUTES = 30;
  static readonly INTRADAY_ORIGIN_HOURS: readonly number[] = [
    10, 12, 14, 16, 18,
  ];
  /** plan_day leads captured: d0 … d7 (BENCH-SPEC UC3). */
  static readonly PLAN_DAY_MAX_LEAD = 7;
  /** How far ahead an intraday origin keeps the served curve. */
  static readonly INTRADAY_WINDOW_MINUTES = 240;
  /**
   * Curves are kept this many days past their TARGET date — never counted from
   * the origin, or a d90 row would be gone before its day could be scored.
   * Two weeks leaves room for re-scoring and the bench runner's per-park-day
   * CIs. Sized in forward-archive.md.
   */
  static readonly CURVE_RETENTION_AFTER_TARGET_DAYS = 14;
  /** Park-day rows, past their target date: ~3.2 k a day at ~240 B with
   *  indexes, so half a year is ~140 MB. The scores are kept longer. */
  static readonly PARK_DAY_RETENTION_DAYS = 180;

  constructor(
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
    @InjectRepository(Attraction)
    private readonly attractionRepository: Repository<Attraction>,
    @InjectRepository(ForecastArchiveCurve)
    private readonly curveRepository: Repository<ForecastArchiveCurve>,
    @InjectRepository(ForecastArchiveParkDay)
    private readonly parkDayRepository: Repository<ForecastArchiveParkDay>,
    private readonly mlService: MLService,
    private readonly planDayService: PlanDayService,
    private readonly calendarService: CalendarService,
    private readonly analyticsService: AnalyticsService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * Which origin, if any, a park is at right now. Read from the park's own
   * clock: an hourly job runs once per UTC hour, and each park is captured in
   * the hour its local clock shows 06, 10, 12 … — whatever its offset.
   */
  static originKindAt(now: Date, timezone: string): ArchiveOriginKind | null {
    const hour = Number(formatInTimeZone(now, timezone, "H"));
    if (hour === ForecastArchiveService.DAILY_ORIGIN_HOUR) return "daily";
    if (hour === ForecastArchiveService.LONG_ORIGIN_HOUR) return "long";
    if (ForecastArchiveService.INTRADAY_ORIGIN_HOURS.includes(hour)) {
      return "intraday";
    }
    return null;
  }

  /** The hourly job: capture every park that sits at an origin now. */
  async captureDue(now: Date = new Date()): Promise<ArchiveCaptureResult> {
    const parks = await this.parkRepository.find();
    const due: Array<{ park: Park; kind: ArchiveOriginKind }> = [];
    for (const park of parks) {
      if (!park.timezone) continue;
      const kind = ForecastArchiveService.originKindAt(now, park.timezone);
      if (!kind) continue;
      const localDate = formatInParkTimezone(now, park.timezone);
      if (kind === "intraday") {
        const active = await this.redis
          .get(this.activeKey(park.id, localDate))
          .catch(() => null);
        if (!active) continue;
      }
      // One capture per park and origin hour, whatever re-runs the job.
      const hour = formatInTimeZone(now, park.timezone, "HH");
      const claimed = await this.redis
        .set(
          `forecast-archive:origin:${park.id}:${localDate}:${hour}`,
          "1",
          "EX",
          3 * 3600,
          "NX",
        )
        .catch(() => "OK");
      if (claimed !== "OK") continue;
      due.push({ park, kind });
    }

    const result: ArchiveCaptureResult = {
      parks: 0,
      curves: 0,
      parkDays: 0,
      failed: 0,
    };
    for (let i = 0; i < due.length; i += BATCH_SIZE) {
      const batch = due.slice(i, i + BATCH_SIZE);
      const done = await mapWithDbBudget(batch, ({ park, kind }) =>
        this.capturePark(park, kind, now).catch((err: Error) => {
          this.logger.warn(
            `Forward archive: ${park.slug} (${kind}) failed: ${err.message}`,
          );
          return null;
        }),
      );
      for (const d of done) {
        if (!d) {
          result.failed++;
          continue;
        }
        result.parks++;
        result.curves += d.curves;
        result.parkDays += d.parkDays;
      }
    }
    return result;
  }

  /** Captures one park at one origin. Exposed for the spec and admin reruns. */
  async capturePark(
    park: Park,
    kind: ArchiveOriginKind,
    now: Date,
  ): Promise<{ curves: number; parkDays: number }> {
    // One instant for every row of this capture — leads are measured from it.
    const originAt = new Date(Math.floor(now.getTime() / 60_000) * 60_000);
    const localToday = formatInParkTimezone(originAt, park.timezone);

    const [headliners, q90] = await Promise.all([
      this.analyticsService
        .getHeadlinerAttractions(park.id)
        .then((h) => new Set(h.map((x) => x.attractionId)))
        .catch(() => new Set<string>()),
      this.rideQ90(park.id, localToday),
    ]);

    const drafts: CurveDraft[] = [];
    let live = new Map<string, number>();

    // ---- served 15-min curve (not on the long-lead origin) ----
    const served = await (kind === "long"
      ? Promise.resolve({ predictions: [] as PredictionDto[] })
      : this.mlService
          .getParkPredictions(park.id, "hourly")
          .catch((err: Error) => {
            this.logger.debug(
              `Forward archive: no hourly curve for ${park.slug}: ${err.message}`,
            );
            return { predictions: [] as PredictionDto[] };
          }));
    const fromMs = currentSlotStartMs();
    const untilMs =
      kind === "intraday"
        ? originAt.getTime() +
          ForecastArchiveService.INTRADAY_WINDOW_MINUTES * 60_000
        : Number.POSITIVE_INFINITY;
    drafts.push(
      ...ForecastArchiveService.curvesFromServed(
        served.predictions ?? [],
        park.timezone,
        localToday,
        fromMs,
        untilMs,
      ),
    );

    if (drafts.length > 0) {
      live = await this.liveWaits(
        [...new Set(drafts.map((d) => d.attractionId))],
        originAt,
      );
    }

    // ---- planner curve and the park-day rows ----
    const parkDays: ForecastArchiveParkDay[] = [];
    const levelOf = new Map<string, string>();
    if (kind === "daily" || kind === "long") {
      const leads =
        kind === "daily"
          ? Array.from(
              { length: ForecastArchiveService.PLAN_DAY_MAX_LEAD + 1 },
              (_, i) => i,
            )
          : [...ForecastArchiveService.LONG_LEADS];
      const lastDate = addIsoDays(localToday, leads[leads.length - 1]);
      const [slugToId, calendar, levelSources] = await Promise.all([
        this.attractionRepository
          .find({ where: { parkId: park.id }, select: ["id", "slug"] })
          .then((rows) => new Map(rows.map((a) => [a.slug, a.id]))),
        this.predictedCrowdLevels(
          park,
          addIsoDays(localToday, leads[0]),
          lastDate,
        ),
        this.levelSources(park),
      ]);
      for (const lead of leads) {
        const date = addIsoDays(localToday, lead);
        let plan: PlanDayDto;
        try {
          plan = await this.planDayService.buildPlanDay(park, date);
        } catch (err) {
          // Unmeasured, not empty: no park-day row, so coverage is not
          // understated by our own failure.
          this.logger.warn(
            `Forward archive: plan/day ${park.slug} ${date} failed: ${(err as Error).message}`,
          );
          continue;
        }
        const planned = ForecastArchiveService.curvesFromPlanDay(
          plan,
          slugToId,
          park.timezone,
          date,
          lead,
        );
        const sourcesOfDay = new Set<string>();
        for (const d of planned) {
          const src =
            plan.tier === "climatology"
              ? "climatology"
              : levelSources.get(`${d.attractionId}|${date}`);
          if (src) {
            levelOf.set(`${d.attractionId}|${date}`, src);
            sourcesOfDay.add(src);
          }
        }
        drafts.push(...planned);
        parkDays.push(
          this.parkDayRepository.create({
            originAt,
            parkId: park.id,
            targetDate: date,
            leadDays: lead,
            tier: plan.tier ?? null,
            status: plan.context?.status ?? null,
            crowdLevel: plan.context?.crowdLevel ?? null,
            predictedCrowdLevel: calendar.get(date) ?? null,
            openHour: plan.context?.openHour ?? null,
            closeHour: plan.context?.closeHour ?? null,
            hoursSource: plan.context?.hoursSource ?? null,
            ridesOffered: plan.rides.length,
            ridesUnavailable: plan.ridesUnavailable?.reason ?? null,
            accuracyBasis: plan.accuracy?.basis ?? null,
            typicalError: plan.accuracy?.typicalError ?? null,
            leadTimeMae: plan.leadTimeMae ?? null,
            levelSource:
              sourcesOfDay.size === 0
                ? "none"
                : sourcesOfDay.size === 1
                  ? [...sourcesOfDay][0]
                  : "mixed",
          }),
        );
      }
    }

    const originKind = ARCHIVE_ORIGIN_KINDS[kind];
    const rows = drafts.map((d) =>
      this.curveRepository.create({
        ...d,
        originAt,
        originKind,
        parkId: park.id,
        rideQ90: q90.get(d.attractionId) ?? null,
        isHeadliner: headliners.has(d.attractionId),
        liveWait:
          d.surface === ARCHIVE_SURFACES.park_hourly
            ? (live.get(d.attractionId) ?? null)
            : null,
        levelSource:
          d.surface === ARCHIVE_SURFACES.plan_day
            ? (levelOf.get(`${d.attractionId}|${d.targetDate}`) ?? null)
            : null,
      }),
    );

    for (let i = 0; i < rows.length; i += INSERT_CHUNK) {
      await this.curveRepository
        .createQueryBuilder()
        .insert()
        .into(ForecastArchiveCurve)
        .values(rows.slice(i, i + INSERT_CHUNK))
        .orIgnore()
        .execute();
    }
    if (parkDays.length > 0) {
      await this.parkDayRepository
        .createQueryBuilder()
        .insert()
        .into(ForecastArchiveParkDay)
        .values(parkDays)
        .orIgnore()
        .execute();
    }

    if (
      kind === "daily" &&
      rows.some(
        (r) =>
          r.surface === ARCHIVE_SURFACES.park_hourly &&
          r.targetDate === localToday,
      )
    ) {
      await this.redis
        .set(this.activeKey(park.id, localToday), "1", "EX", 26 * 3600)
        .catch(() => undefined);
    }

    return { curves: rows.length, parkDays: parkDays.length };
  }

  /**
   * The served hourly predictions as one row per ride and park-local date.
   * Slots the payload did not carry stay NULL with source `-`.
   */
  static curvesFromServed(
    predictions: PredictionDto[],
    timezone: string,
    localToday: string,
    fromMs: number,
    untilMs: number,
  ): CurveDraft[] {
    const groups = new Map<string, PredictionDto[]>();
    for (const p of predictions) {
      if (p.predictionType !== "hourly") continue;
      const t = Date.parse(p.predictedTime);
      if (!Number.isFinite(t) || t < fromMs || t >= untilMs) continue;
      const date = formatInParkTimezone(new Date(t), timezone);
      const key = `${p.attractionId}|${date}`;
      const list = groups.get(key) ?? [];
      list.push(p);
      groups.set(key, list);
    }

    const out: CurveDraft[] = [];
    for (const [key, list] of groups) {
      const [attractionId, date] = key.split("|");
      list.sort(
        (a, b) => Date.parse(a.predictedTime) - Date.parse(b.predictedTime),
      );
      const first = Date.parse(list[0].predictedTime);
      const last = Date.parse(list[list.length - 1].predictedTime);
      const n = Math.round((last - first) / SLOT_MS) + 1;
      const waits: (number | null)[] = new Array(n).fill(null);
      const bands: (number | null)[] = new Array(n).fill(null);
      const sources = new Array<string>(n).fill("-");
      let anyBand = false;
      let modelVersion: string | null = null;
      for (const p of list) {
        const idx = Math.round((Date.parse(p.predictedTime) - first) / SLOT_MS);
        if (idx < 0 || idx >= n) continue;
        waits[idx] = Math.round(p.predictedWaitTime);
        const pcn = (p.modelVersion ?? "").endsWith("+pcn");
        sources[idx] = pcn ? "p" : "c";
        if (p.uncertaintyMinutes != null) {
          bands[idx] = Math.round(p.uncertaintyMinutes);
          anyBand = true;
        }
        if (!modelVersion && p.modelVersion) {
          modelVersion = p.modelVersion.replace(/\+pcn$/, "").slice(0, 64);
        }
      }
      out.push({
        attractionId,
        surface: ARCHIVE_SURFACES.park_hourly,
        targetDate: date,
        leadDays: ForecastArchiveService.daysBetween(localToday, date),
        slotStart: new Date(first),
        slotMinutes: 15,
        waits,
        sources: sources.join(""),
        bands: anyBand ? bands : null,
        dayPeak: null,
        expectedError: null,
        modelVersion,
      });
    }
    return out;
  }

  /**
   * The planner's rides as hourly rows. `hour` may run past 23 on a day that
   * crosses midnight (24 = that day's midnight); the instant is resolved per
   * day in the park's zone, so DST is honoured at the first slot. Hours are
   * then stepped 60 min in absolute time, which is off by one only for a park
   * open across a DST switch at 02:00–03:00 — none is on the switch nights.
   */
  static curvesFromPlanDay(
    plan: PlanDayDto,
    slugToId: ReadonlyMap<string, string>,
    timezone: string,
    date: string,
    lead: number,
  ): CurveDraft[] {
    const tierCode = ForecastArchiveService.planSourceCode(plan.tier);
    const out: CurveDraft[] = [];
    for (const ride of plan.rides ?? []) {
      const attractionId = slugToId.get(ride.attractionSlug);
      if (!attractionId || ride.hours.length === 0) continue;
      const hours = [...ride.hours].sort((a, b) => a.hour - b.hour);
      const firstHour = hours[0].hour;
      const n = hours[hours.length - 1].hour - firstHour + 1;
      const waits: (number | null)[] = new Array(n).fill(null);
      const sources = new Array<string>(n).fill("-");
      for (const h of hours) {
        const idx = h.hour - firstHour;
        waits[idx] = Math.round(h.wait);
        sources[idx] = h.source
          ? ForecastArchiveService.planSourceCode(h.source)
          : tierCode;
      }
      const band =
        ride.uncertaintyMinutes != null
          ? Math.round(ride.uncertaintyMinutes)
          : null;
      const dayOffset = Math.floor(firstHour / 24);
      const hh = String(firstHour % 24).padStart(2, "0");
      out.push({
        attractionId,
        surface: ARCHIVE_SURFACES.plan_day,
        targetDate: date,
        leadDays: lead,
        slotStart: fromZonedTime(
          `${addIsoDays(date, dayOffset)}T${hh}:00:00`,
          timezone,
        ),
        slotMinutes: 60,
        waits,
        sources: sources.join(""),
        bands:
          band === null ? null : waits.map((w) => (w === null ? null : band)),
        dayPeak: Number.isFinite(ride.dayPeak)
          ? Math.round(ride.dayPeak)
          : null,
        expectedError:
          ride.expectedError != null ? Math.round(ride.expectedError) : null,
        modelVersion: null,
      });
    }
    return out;
  }

  private static planSourceCode(source: string | undefined): string {
    switch (source) {
      case "measured":
        return "m";
      case "composed":
        return "k";
      case "climatology":
        return "l";
      case "observed":
        return "o";
      default:
        return "-";
    }
  }

  private static daysBetween(from: string, to: string): number {
    return Math.round(
      (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) /
        86_400_000,
    );
  }

  private activeKey(parkId: string, localDate: string): string {
    return `forecast-archive:active:${parkId}:${localDate}`;
  }

  /**
   * The calendar's `predictedCrowdLevel` for today … today+7 — the forecast
   * bucket the "Prognose heute" hero reads. One calendar call per park and day.
   */
  private async predictedCrowdLevels(
    park: Park,
    fromDate: string,
    toDate: string,
  ): Promise<Map<string, string>> {
    const out = new Map<string, string>();
    try {
      const from = new Date(`${fromDate}T12:00:00Z`);
      const to = new Date(`${toDate}T12:00:00Z`);
      const cal = await this.calendarService.buildCalendarResponse(
        park,
        from,
        to,
        "none",
      );
      for (const day of cal?.days ?? []) {
        if (day.predictedCrowdLevel) out.set(day.date, day.predictedCrowdLevel);
      }
    } catch (err) {
      this.logger.debug(
        `Forward archive: calendar unavailable for ${park.slug}: ${(err as Error).message}`,
      );
    }
    return out;
  }

  /**
   * Which model the planner's day level comes from, per `ride|date`:
   * `getServingDailyPredictions` is the call `PlanDayService.dayLevels` makes,
   * matched on `predictedTime.slice(0, 10)` exactly as it does (freshest row
   * wins), and the merged rows carry `modelVersion: "tft"` on the TFT side.
   * Reconstructed rather than exposed, because the payload has no field for it.
   */
  private async levelSources(park: Park): Promise<Map<string, string>> {
    const out = new Map<string, string>();
    const serving = await this.mlService
      .getServingDailyPredictions(park.id)
      .catch(() => ({ predictions: [] as PredictionDto[] }));
    for (const p of serving.predictions ?? []) {
      out.set(
        `${p.attractionId}|${p.predictedTime.slice(0, 10)}`,
        p.modelVersion === "tft" ? "tft" : "catboost",
      );
    }
    return out;
  }

  /**
   * D1's anchor: each ride's latest STANDBY reading at the origin, when it is
   * OPERATING with a wait and no older than {@link LIVE_ANCHOR_MAX_AGE_MINUTES}.
   */
  private async liveWaits(
    attractionIds: string[],
    originAt: Date,
  ): Promise<Map<string, number>> {
    const out = new Map<string, number>();
    try {
      const rows: Array<{ id: string; status: string; wait: number | null }> =
        await withStatementLimits(
          this.curveRepository.manager,
          { statementTimeoutMs: 20_000, lockTimeoutMs: 2_000 },
          (em) =>
            em.query(
              `SELECT DISTINCT ON ("attractionId") "attractionId" AS id,
                      status::text AS status, "waitTime" AS wait
                 FROM queue_data
                WHERE "attractionId" = ANY($1::uuid[])
                  AND "queueType" = 'STANDBY'
                  AND timestamp > $2 AND timestamp <= $3
                ORDER BY "attractionId", timestamp DESC`,
              [
                attractionIds,
                new Date(
                  originAt.getTime() -
                    ForecastArchiveService.LIVE_ANCHOR_MAX_AGE_MINUTES * 60_000,
                ),
                originAt,
              ],
            ),
        );
      for (const r of rows) {
        if (r.status === "OPERATING" && r.wait !== null) {
          out.set(r.id, Number(r.wait));
        }
      }
    } catch (err) {
      this.logger.debug(
        `Forward archive: live anchor unavailable: ${(err as Error).message}`,
      );
    }
    return out;
  }

  /**
   * Each ride's q90 of 15-min average waits over the 56 days before today —
   * BENCH-SPEC's ex-ante busy input — from the hourly-history rollup rather
   * than `queue_data`: ~40 rides × 56 rows of jsonb per park instead of a
   * compressed-hypertable scan. Cached for the park-local day.
   */
  private async rideQ90(
    parkId: string,
    localToday: string,
  ): Promise<Map<string, number>> {
    const key = `forecast-archive:q90:${parkId}:${localToday}`;
    const cached = await this.redis.get(key).catch(() => null);
    if (cached) {
      try {
        return new Map(
          Object.entries(JSON.parse(cached) as Record<string, number>),
        );
      } catch {
        // fall through and recompute
      }
    }
    const out = new Map<string, number>();
    try {
      const rows: Array<{ id: string; q90: number | string | null }> =
        await withStatementLimits(
          this.curveRepository.manager,
          { statementTimeoutMs: 30_000, lockTimeoutMs: 2_000 },
          (em) =>
            em.query(
              `SELECT h."attractionId" AS id,
                      percentile_cont(0.9) WITHIN GROUP (ORDER BY (s->>'avgWait')::float) AS q90
                 FROM attraction_hourly_history h
                 CROSS JOIN LATERAL jsonb_array_elements(h.slots) s
                WHERE h."parkId" = $1
                  AND h.date >= $2::date - 56
                  AND h.date < $2::date
                  AND (s->>'avgWait')::float >= 5
                GROUP BY h."attractionId"`,
              [parkId, localToday],
            ),
        );
      for (const r of rows) {
        const v = Number(r.q90);
        if (Number.isFinite(v)) out.set(r.id, Math.round(v));
      }
      await this.redis
        .set(key, JSON.stringify(Object.fromEntries(out)), "EX", 26 * 3600)
        .catch(() => undefined);
    } catch (err) {
      // Without it the busy segment is empty for this capture, nothing else.
      this.logger.debug(
        `Forward archive: q90 unavailable for ${parkId}: ${(err as Error).message}`,
      );
    }
    return out;
  }

  /**
   * App-side retention, in short batches with a lock deadline — never a
   * TimescaleDB policy (db-health-runbook §0b). The tables have no foreign key,
   * so a DELETE locks nothing but its own rows.
   */
  async pruneExpired(
    now: Date = new Date(),
  ): Promise<{ curves: number; parkDays: number }> {
    const today = now.toISOString().slice(0, 10);
    const curves = await this.pruneTable(
      "forecast_archive_curves",
      "target_date",
      addIsoDays(
        today,
        -ForecastArchiveService.CURVE_RETENTION_AFTER_TARGET_DAYS,
      ),
    );
    const parkDays = await this.pruneTable(
      "forecast_archive_park_days",
      "target_date",
      addIsoDays(today, -ForecastArchiveService.PARK_DAY_RETENTION_DAYS),
    );
    return { curves, parkDays };
  }

  private async pruneTable(
    table: string,
    column: string,
    before: string,
  ): Promise<number> {
    let total = 0;
    // 50 × 20,000 rows covers a day's backlog several times over; a longer
    // backlog drains over the following nights.
    for (let i = 0; i < 50; i++) {
      const deleted: number = await withStatementLimits(
        this.curveRepository.manager,
        { statementTimeoutMs: 60_000, lockTimeoutMs: 2_000 },
        async (em) => {
          const res: Array<{ n: string }> = await em.query(
            `WITH gone AS (
               DELETE FROM ${table}
                WHERE ctid IN (SELECT ctid FROM ${table} WHERE ${column} < $1::date LIMIT 20000)
                RETURNING 1)
             SELECT count(*)::text AS n FROM gone`,
            [before],
          );
          return Number(res?.[0]?.n ?? 0);
        },
      );
      total += deleted;
      if (deleted < 20000) break;
    }
    return total;
  }
}
