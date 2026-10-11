import { Inject, Injectable, Logger, NotFoundException } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { DataSource, Repository } from "typeorm";
import { Redis } from "ioredis";
import { formatInTimeZone } from "date-fns-tz";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import {
  ARCHIVE_ORIGIN_KINDS,
  ARCHIVE_SURFACES,
  ArchiveSurface,
  ForecastArchiveCurve,
} from "../../ml/entities/forecast-archive-curve.entity";
import {
  ArchivedCurve,
  OperatingWindow,
  TRUTH_SLOT_MS,
  TruthReading,
  buildTruthSlots,
  valueAt,
  weekdayAverageArchived,
} from "../../ml/utils/forecast-archive-scoring.util";
import {
  DaySlots,
  forwardFillDay,
  weekdayHistoryDates,
} from "../../ml/utils/weekday-average.util";
import {
  COMPARISON_MODELS,
  OfflineVerdictFile,
  PairedAggregate,
  buildModelComparison,
} from "../../ml/utils/model-comparison.util";
import {
  ModelComparisonDayDto,
  ModelComparisonDayOptionsDto,
  ModelComparisonOriginId,
  ModelComparisonResponseDto,
} from "../../ml/dto/model-comparison.dto";
import verdictsJson from "../../ml/data/offline-model-verdicts.json";
import {
  BOARD_VERSION_KEY,
  ForecastArchiveScoringService,
} from "./forecast-archive-scoring.service";
import { getCurrentDateInTimezone } from "../../common/utils/date.util";
import {
  StatementLimits,
  queryWithLimits,
} from "../../common/utils/statement-limits.util";

const READ_LIMITS: StatementLimits = {
  statementTimeoutMs: 60_000,
  lockTimeoutMs: 5_000,
  idleInTransactionTimeoutMs: 10_000,
};

/** The matrix moves once a day (the 12:00 UTC scoring); above any admin poll. */
const MATRIX_TTL_SECONDS = 2 * 3600;
/** The day view of today changes with every reading. */
const DAY_TTL_SECONDS = 15 * 60;
/** Admin dates ("expected by") are read on the owner's calendar. */
const ADMIN_TZ = "Europe/Berlin";
const MAX_DATES = 30;

export const DAY_ORIGINS: Array<{
  id: ModelComparisonOriginId;
  label: string;
  leadDays: number;
}> = [
  { id: "morning", label: "Am Morgen des Tages", leadDays: 0 },
  { id: "d1", label: "Am Vortag", leadDays: 1 },
  { id: "d3", label: "3 Tage vorher", leadDays: 3 },
  { id: "d7", label: "7 Tage vorher", leadDays: 7 },
];

/** Day-view series per archived surface, in the order they are listed. */
const SERIES_BY_SURFACE: Array<[ArchiveSurface, string]> = [
  ["park_hourly", "pcn"],
  ["plan_day", "served"],
  ["plan_day_shadow_peak", "peak"],
  ["plan_day_shadow_h5", "h5"],
  ["plan_day_shadow_routed", "tfth5"],
  ["chronos_grid", "chronos"],
];

const LABEL = new Map(COMPARISON_MODELS.map((m) => [m.id, m.label]));

/**
 * The admin model comparison (PAR-855): the question × model matrix from the
 * paired rows of the forward archive, and one real day replayed per ride.
 * Read-only; every read runs under statement limits.
 */
@Injectable()
export class ModelComparisonService {
  private readonly logger = new Logger(ModelComparisonService.name);
  private readonly verdicts = verdictsJson as unknown as OfflineVerdictFile;

  constructor(
    @InjectRepository(ForecastArchiveCurve)
    private readonly curveRepository: Repository<ForecastArchiveCurve>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  private get db(): DataSource {
    return this.curveRepository.manager.connection;
  }

  private q<T>(sql: string, params: unknown[]): Promise<T> {
    return queryWithLimits<T>(this.db, sql, params, READ_LIMITS);
  }

  private async cached<T>(
    key: string,
    ttl: number,
    build: () => Promise<T>,
  ): Promise<T> {
    const hit = await this.redis.get(key).catch(() => null);
    if (hit) {
      try {
        return JSON.parse(hit) as T;
      } catch {
        // rebuild
      }
    }
    const value = await build();
    await this.redis
      .set(key, JSON.stringify(value), "EX", ttl)
      .catch(() => undefined);
    return value;
  }

  private async version(): Promise<string> {
    return (await this.redis.get(BOARD_VERSION_KEY).catch(() => null)) ?? "0";
  }

  /** The matrix over the last `days` scored target dates. */
  async getMatrix(
    days: number,
    region = "ALL",
    now: Date = new Date(),
  ): Promise<ModelComparisonResponseDto> {
    const v = await this.version();
    return this.cached(
      `model-comparison:v${v}:${region}:${days}`,
      MATRIX_TTL_SECONDS,
      async () => {
        const [win] = await this.q<
          Array<{ first: string | null; last: string | null }>
        >(
          `SELECT min(target_date)::text AS first, max(target_date)::text AS last
             FROM forecast_archive_scores
            WHERE target_date > (SELECT max(target_date) FROM forecast_archive_scores) - $1::int`,
          [days],
        );
        const window = {
          days,
          firstDate: win?.first ?? null,
          lastDate: win?.last ?? null,
        };
        const aggregates =
          window.firstDate && window.lastDate
            ? await this.aggregates(window.firstDate, window.lastDate, region)
            : [];
        const since = await this.since(v);
        return buildModelComparison({
          aggregates,
          since,
          verdicts: this.verdicts,
          window,
          today: getCurrentDateInTimezone(ADMIN_TZ),
          generatedAt: now.toISOString(),
        });
      },
    );
  }

  /** Per park, the pooled paired sums vs `served` of each key in the window. */
  private async aggregates(
    first: string,
    last: string,
    region: string,
  ): Promise<PairedAggregate[]> {
    const params: unknown[] = [first, last];
    let regionSql = "";
    if (region !== "ALL") {
      params.push(region);
      regionSql = "AND region = $3";
    }
    const rows = await this.q<
      Array<{
        useCase: string;
        lead: string;
        model: string;
        segment: string;
        parkId: string;
        dates: string[];
        n: string | number;
        saeModel: string | number;
        saeRef: string | number;
      }>
    >(
      `SELECT use_case AS "useCase", lead, model, segment, park_id AS "parkId",
              array_agg(DISTINCT target_date::text) AS dates,
              sum(n)::bigint AS n, sum(sae_model) AS "saeModel", sum(sae_ref) AS "saeRef"
         FROM forecast_archive_paired
        WHERE target_date BETWEEN $1::date AND $2::date
          AND reference = 'served' AND segment IN ('all', 'headliner')
          ${regionSql}
        GROUP BY 1, 2, 3, 4, 5`,
      params,
    );
    return rows.map((r) => ({
      ...r,
      dates: r.dates ?? [],
      n: Number(r.n),
      saeModel: Number(r.saeModel),
      saeRef: Number(r.saeRef),
    }));
  }

  /** First scored target date per key, all time; once per scoring run. */
  private async since(version: string): Promise<Map<string, string>> {
    const rows = await this.cached(
      `model-comparison:since:v${version}`,
      MATRIX_TTL_SECONDS,
      () =>
        this.q<
          Array<{
            useCase: string;
            lead: string;
            model: string;
            segment: string;
            since: string;
          }>
        >(
          `SELECT use_case AS "useCase", lead, model, segment, min(target_date)::text AS since
             FROM forecast_archive_paired
            WHERE reference = 'served' AND segment IN ('all', 'headliner')
            GROUP BY 1, 2, 3, 4`,
          [],
        ).catch((err: Error) => {
          this.logger.warn(
            `Model comparison: since unavailable: ${err.message}`,
          );
          return [];
        }),
    );
    return new Map(
      rows.map((r) => [
        [r.useCase, r.lead, r.model, r.segment].join("|"),
        r.since,
      ]),
    );
  }

  /** Dates, parks and rides the day view can show. */
  async getDayOptions(date?: string): Promise<ModelComparisonDayOptionsDto> {
    const today = getCurrentDateInTimezone(ADMIN_TZ);
    const v = await this.version();
    return this.cached(
      `model-comparison:day-options:v${v}:${today}:${date ?? "-"}`,
      DAY_TTL_SECONDS,
      async () => {
        // Archive rows live 14 days past their target date; a target date
        // after today has no truth. 30 index probes, newest first.
        const dateRows = await this.q<Array<{ d: string }>>(
          `SELECT d::date::text AS d
             FROM generate_series($1::date - ${MAX_DATES - 1}, $1::date, interval '1 day') d
            WHERE EXISTS (SELECT 1 FROM forecast_archive_curves c
                           WHERE c.target_date = d::date AND c.origin_kind = $2)
            ORDER BY d DESC`,
          [today, ARCHIVE_ORIGIN_KINDS.daily],
        );
        const dates = dateRows.map((r) => r.d);
        const chosen = date ?? dates[0] ?? today;
        const rides = await this.q<
          Array<{ parkId: string; id: string; hl: boolean }>
        >(
          `SELECT park_id AS "parkId", attraction_id AS id, bool_or(is_headliner) AS hl
             FROM forecast_archive_curves
            WHERE target_date = $1::date AND origin_kind = $2 AND surface IN ($3, $4)
            GROUP BY 1, 2`,
          [
            chosen,
            ARCHIVE_ORIGIN_KINDS.daily,
            ARCHIVE_SURFACES.park_hourly,
            ARCHIVE_SURFACES.plan_day,
          ],
        );
        const parkIds = [...new Set(rides.map((r) => r.parkId))];
        const parks =
          parkIds.length === 0
            ? []
            : await this.q<
                Array<{ id: string; name: string; timezone: string }>
              >(
                `SELECT id, name, timezone FROM parks WHERE id = ANY($1::uuid[])`,
                [parkIds],
              );
        const names =
          rides.length === 0
            ? []
            : await this.q<Array<{ id: string; name: string }>>(
                `SELECT id, name FROM attractions WHERE id = ANY($1::uuid[])`,
                [rides.map((r) => r.id)],
              );
        const nameOf = new Map(names.map((n) => [n.id, n.name]));
        return {
          date: chosen,
          dates,
          parks: parks
            .map((p) => ({
              id: p.id,
              name: p.name,
              timezone: p.timezone,
              rides: rides
                .filter((r) => r.parkId === p.id && nameOf.has(r.id))
                .map((r) => ({
                  id: r.id,
                  name: nameOf.get(r.id)!,
                  isHeadliner: Boolean(r.hl),
                }))
                .sort(
                  (a, b) =>
                    Number(b.isHeadliner) - Number(a.isHeadliner) ||
                    a.name.localeCompare(b.name, "de"),
                ),
            }))
            .filter((p) => p.rides.length > 0)
            .sort((a, b) => a.name.localeCompare(b.name, "de")),
          origins: DAY_ORIGINS.map(({ id, label }) => ({ id, label })),
        };
      },
    );
  }

  /** One ride, one day, one origin: every archived curve against the truth. */
  async getDay(
    parkId: string,
    date: string,
    attractionId: string,
    originId: ModelComparisonOriginId,
  ): Promise<ModelComparisonDayDto> {
    const v = await this.version();
    return this.cached(
      `model-comparison:day:v${v}:${parkId}:${date}:${attractionId}:${originId}`,
      DAY_TTL_SECONDS,
      () => this.buildDay(parkId, date, attractionId, originId),
    );
  }

  private async buildDay(
    parkId: string,
    date: string,
    attractionId: string,
    originId: ModelComparisonOriginId,
  ): Promise<ModelComparisonDayDto> {
    const origin = DAY_ORIGINS.find((o) => o.id === originId) ?? DAY_ORIGINS[0];
    const [park] = await this.q<
      Array<{ id: string; name: string; timezone: string }>
    >(`SELECT id, name, timezone FROM parks WHERE id = $1::uuid`, [parkId]);
    if (!park) throw new NotFoundException("park not found");
    const [ride] = await this.q<Array<{ id: string; name: string }>>(
      `SELECT id, name FROM attractions WHERE id = $1::uuid AND "parkId" = $2::uuid`,
      [attractionId, parkId],
    );
    if (!ride) throw new NotFoundException("attraction not found in park");

    const curveRows = await this.q<Array<Record<string, unknown>>>(
      `SELECT origin_at AS "originAt", attraction_id AS "attractionId", surface,
              target_date::text AS "targetDate", park_id AS "parkId",
              origin_kind AS "originKind", lead_days AS "leadDays",
              slot_start AS "slotStart", slot_minutes AS "slotMinutes", waits,
              sources, bands, day_peak AS "dayPeak", expected_error AS "expectedError",
              ride_q90_56d AS "rideQ90", is_headliner AS "isHeadliner",
              live_wait AS "liveWait", peak_band AS "peakBand", level_source AS "levelSource"
         FROM forecast_archive_curves
        WHERE park_id = $1::uuid AND target_date = $2::date AND attraction_id = $3::uuid
          AND origin_kind IN ($4, $5) AND lead_days = $6`,
      [
        parkId,
        date,
        attractionId,
        ARCHIVE_ORIGIN_KINDS.daily,
        ARCHIVE_ORIGIN_KINDS.long,
        origin.leadDays,
      ],
    );
    const curves = curveRows.map((r) =>
      ForecastArchiveScoringService.toArchived(
        r as unknown as ForecastArchiveCurve,
      ),
    );
    // One capture per origin; the latest if a re-run left two.
    const originAt = curves.length
      ? Math.max(...curves.map((c) => c.originAt))
      : null;
    const atOrigin = curves.filter((c) => c.originAt === originAt);

    const windows = await this.operatingWindows(parkId, date);
    const slotTimes = ModelComparisonService.daySlots(windows, atOrigin);
    const tz = park.timezone;

    let truthSlots = new Map<number, number>();
    if (windows.length > 0 && slotTimes.length > 0) {
      const readings = await this.q<
        Array<{ ts: Date; status: string; wait: number | null }>
      >(
        `SELECT timestamp AS ts, status::text AS status, "waitTime" AS wait
           FROM queue_data
          WHERE "attractionId" = $1::uuid AND "queueType" = 'STANDBY'
            AND timestamp >= $2 AND timestamp < $3
          ORDER BY timestamp`,
        [
          attractionId,
          new Date(Math.min(...windows.map((w) => w.open)) - 3 * 3_600_000),
          new Date(Math.max(...windows.map((w) => w.close))),
        ],
      );
      const list: TruthReading[] = readings.map((r) => ({
        timestamp: new Date(r.ts).getTime(),
        status: r.status,
        waitTime: r.wait === null ? null : Number(r.wait),
      }));
      truthSlots = buildTruthSlots(list, windows);
    }

    const seriesCurves: Array<{ model: string; curve: ArchivedCurve }> = [];
    for (const [surface, model] of SERIES_BY_SURFACE) {
      const c = atOrigin.find((x) => x.surface === surface);
      if (c) seriesCurves.push({ model, curve: c });
    }
    const plan = atOrigin.find((c) => c.surface === "plan_day");
    if (plan) {
      const history = await this.rideWeekdayHistory(
        attractionId,
        weekdayHistoryDates(date, origin.leadDays),
      ).catch(() => null);
      const wkd = history
        ? weekdayAverageArchived(plan, {
            targetDate: date,
            history: new Map([[attractionId, history]]),
            clock: (ms) => formatInTimeZone(new Date(ms), tz, "HH:mm"),
          })
        : null;
      if (wkd) seriesCurves.push({ model: "wkd", curve: wkd });
    }
    const hourly = atOrigin.find((c) => c.surface === "park_hourly");

    const truth = slotTimes.map((s) => truthSlots.get(s) ?? null);
    const series = seriesCurves.map(({ model, curve }) => ({
      model,
      label: LABEL.get(model) ?? model,
      values: slotTimes.map((s) => valueAt(curve, s)?.wait ?? null),
    }));
    if (hourly?.liveWait !== null && hourly?.liveWait !== undefined) {
      const anchor = hourly.liveWait;
      series.unshift({
        model: "persist",
        label: LABEL.get("persist")!,
        values: slotTimes.map((s) => (s >= hourly.originAt ? anchor : null)),
      });
    }
    const scores = ModelComparisonService.dayScores(truth, series);

    const firstOpen = windows.length
      ? Math.min(...windows.map((w) => w.open))
      : null;
    const lastClose = windows.length
      ? Math.max(...windows.map((w) => w.close))
      : null;
    return {
      park: { id: park.id, name: park.name, timezone: tz },
      ride: { id: ride.id, name: ride.name },
      date,
      origin: {
        id: origin.id,
        label: origin.label,
        at: originAt === null ? null : new Date(originAt).toISOString(),
        localLabel:
          originAt === null
            ? "keine Prognose gespeichert"
            : formatInTimeZone(new Date(originAt), tz, "dd.MM. HH:mm 'Uhr'"),
      },
      openingHours:
        firstOpen === null || lastClose === null
          ? null
          : {
              open: formatInTimeZone(new Date(firstOpen), tz, "HH:mm"),
              close: formatInTimeZone(new Date(lastClose), tz, "HH:mm"),
            },
      slots: slotTimes.map((s) => formatInTimeZone(new Date(s), tz, "HH:mm")),
      truth,
      series,
      scores,
    };
  }

  /**
   * The day's 15-min slot instants: the published window(s) from the first
   * opening (floored to the quarter-hour) to the last closing. Without a
   * published window, the span the archived curves cover — truth is then null
   * throughout, as the scorer would have it.
   */
  static daySlots(
    windows: OperatingWindow[],
    curves: ArchivedCurve[],
  ): number[] {
    let from: number;
    let to: number;
    if (windows.length > 0) {
      from = Math.min(...windows.map((w) => w.open));
      to = Math.max(...windows.map((w) => w.close));
    } else if (curves.length > 0) {
      from = Math.min(...curves.map((c) => c.slotStart));
      to = Math.max(
        ...curves.map(
          (c) => c.slotStart + c.waits.length * c.slotMinutes * 60_000,
        ),
      );
    } else {
      return [];
    }
    const out: number[] = [];
    for (
      let s = Math.floor(from / TRUTH_SLOT_MS) * TRUTH_SLOT_MS;
      s < to;
      s += TRUTH_SLOT_MS
    ) {
      out.push(s);
    }
    return out;
  }

  /** MAE per series over the slots where both truth and the series have a value. */
  static dayScores(
    truth: (number | null)[],
    series: Array<{ model: string; label: string; values: (number | null)[] }>,
  ): ModelComparisonDayDto["scores"] {
    const out: ModelComparisonDayDto["scores"] = [];
    for (const s of series) {
      let n = 0;
      let sae = 0;
      truth.forEach((t, i) => {
        const v = s.values[i];
        if (t === null || v === null || v === undefined) return;
        n++;
        sae += Math.abs(v - t);
      });
      if (n === 0) continue;
      out.push({
        model: s.model,
        label: s.label,
        mae: Math.round((sae / n) * 100) / 100,
        n,
      });
    }
    return out.sort((a, b) => a.mae - b.mae);
  }

  private async operatingWindows(
    parkId: string,
    date: string,
  ): Promise<OperatingWindow[]> {
    const rows = await this.q<Array<{ open: Date; close: Date }>>(
      `SELECT "openingTime" AS open, "closingTime" AS close
         FROM schedule_entries
        WHERE "parkId" = $1::uuid AND date = $2::date
          AND "scheduleType" = 'OPERATING' AND "attractionId" IS NULL
          AND "openingTime" IS NOT NULL AND "closingTime" IS NOT NULL`,
      [parkId, date],
    );
    return rows
      .map((r) => ({
        open: new Date(r.open).getTime(),
        close: new Date(r.close).getTime(),
      }))
      .filter((w) => w.close > w.open);
  }

  private async rideWeekdayHistory(
    attractionId: string,
    dates: string[],
  ): Promise<Map<string, DaySlots>> {
    const rows = await this.q<
      Array<{
        d: string;
        slots: Array<{ time_slot: string; avgWait: number }> | null;
      }>
    >(
      `SELECT date::text AS d, slots FROM attraction_hourly_history
        WHERE "attractionId" = $1::uuid AND date = ANY($2::date[])`,
      [attractionId, dates],
    );
    const out = new Map<string, DaySlots>();
    for (const r of rows) {
      const day = forwardFillDay(r.slots ?? []);
      if (day.size > 0) out.set(r.d, day);
    }
    return out;
  }
}
