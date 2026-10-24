import { Inject, Injectable, Logger, NotFoundException } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { DataSource, Repository } from "typeorm";
import { Redis } from "ioredis";
import { formatInTimeZone, fromZonedTime } from "date-fns-tz";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import {
  ARCHIVE_ORIGIN_KINDS,
  ARCHIVE_SURFACES,
  ArchiveSurface,
  ForecastArchiveCurve,
} from "../../ml/entities/forecast-archive-curve.entity";
import { PAIRED_DEFINITION } from "../../ml/entities/forecast-archive-paired.entity";
import {
  ArchivedCurve,
  OperatingWindow,
  TruthReading,
  buildTruthSlots,
  truthRowsSql,
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
  MATRIX_REFERENCES,
  OfflineVerdictFile,
  PairedAggregate,
  buildModelComparison,
  d7Aggregates,
} from "../../ml/utils/model-comparison.util";
import {
  ModelComparisonDayDto,
  ModelComparisonDayOptionsDto,
  ModelComparisonOriginId,
  ModelComparisonResponseDto,
} from "../../ml/dto/model-comparison.dto";
import verdictsJson from "../../ml/data/offline-model-verdicts.json";
import {
  DayOptionRow,
  dayViewSlots,
  lastCompleteDate,
  rankDayOptions,
  truthCoverage,
} from "../../ml/utils/model-comparison-day.util";
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
/** Without a date, the newest complete dates tried for a suggestion. */
const SUGGESTION_DATE_TRIES = 3;

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

  /**
   * The matrix over the last `days` scored target dates — ending at `until`
   * (YYYY-MM-DD, inclusive) when given, so a decision evaluated on a fixed
   * date reads the same window every day after it (ML overview, PAR-1036).
   */
  async getMatrix(
    days: number,
    region = "ALL",
    now: Date = new Date(),
    until?: string,
  ): Promise<ModelComparisonResponseDto> {
    const v = await this.version();
    return this.cached(
      `model-comparison:v${v}:${region}:${days}${until ? `:until${until}` : ""}`,
      MATRIX_TTL_SECONDS,
      async () => {
        const [win] = await this.q<
          Array<{ first: string | null; last: string | null }>
        >(
          `WITH s AS (
             SELECT target_date FROM forecast_archive_scores
              WHERE ($2::date IS NULL OR target_date <= $2::date))
           SELECT min(target_date)::text AS first, max(target_date)::text AS last
             FROM s
            WHERE target_date > (SELECT max(target_date) FROM s) - $1::int`,
          [days, until ?? null],
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

  /**
   * Per park, the pooled paired sums of each key the matrix reads in the
   * window; D7's per-date level rows are turned into day-pair counts here
   * (`d7Aggregates`, PAR-1037), since pairs need the dates apart.
   */
  private async aggregates(
    first: string,
    last: string,
    region: string,
  ): Promise<PairedAggregate[]> {
    const params: unknown[] = [first, last, MATRIX_REFERENCES];
    let regionSql = "";
    if (region !== "ALL") {
      params.push(region);
      regionSql = "AND region = $4";
    }
    const rows = await this.q<
      Array<{
        useCase: string;
        lead: string;
        model: string;
        reference: string;
        segment: string;
        parkId: string;
        dates: string[];
        n: string | number;
        saeModel: string | number;
        saeRef: string | number;
      }>
    >(
      // One row per park × date × key (the table's own grain): the gate's
      // two-way bootstrap resamples dates too (PAR-1036). Bounded by the
      // window (≤ 90 days) and the matrix's references and segments.
      `SELECT use_case AS "useCase", lead, model, reference, segment, park_id AS "parkId",
              ARRAY[target_date::text] AS dates,
              sum(n)::bigint AS n, sum(sae_model) AS "saeModel", sum(sae_ref) AS "saeRef"
         FROM forecast_archive_paired
        WHERE target_date BETWEEN $1::date AND $2::date
          AND reference = ANY($3::text[]) AND segment IN ('all', 'headliner')
          AND use_case <> 'D7'
          AND definition = ${PAIRED_DEFINITION}
          ${regionSql}
        GROUP BY 1, 2, 3, 4, 5, 6, target_date`,
      params,
    );
    const d7 = await this.q<
      Array<{
        parkId: string;
        lead: string;
        date: string;
        n: string | number;
        sumModel: string | number;
        sumRef: string | number;
        sumTruth: string | number;
      }>
    >(
      `SELECT park_id AS "parkId", lead, target_date::text AS date, n,
              sae_model AS "sumModel", sae_ref AS "sumRef", se_model AS "sumTruth"
         FROM forecast_archive_paired
        WHERE target_date BETWEEN $1::date AND $2::date
          AND use_case = 'D7' AND reference = ANY($3::text[])
          AND definition = ${PAIRED_DEFINITION}
          ${regionSql}`,
      params,
    );
    return [
      ...rows.map((r) => ({
        ...r,
        dates: r.dates ?? [],
        n: Number(r.n),
        saeModel: Number(r.saeModel),
        saeRef: Number(r.saeRef),
      })),
      ...d7Aggregates(
        d7.map((r) => ({
          parkId: r.parkId,
          lead: r.lead,
          date: r.date,
          n: Number(r.n),
          sumModel: Number(r.sumModel),
          sumRef: Number(r.sumRef),
          sumTruth: Number(r.sumTruth),
        })),
      ),
    ];
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
            reference: string;
            segment: string;
            since: string;
          }>
        >(
          `SELECT use_case AS "useCase", lead, model, reference, segment, min(target_date)::text AS since
             FROM forecast_archive_paired
            WHERE reference = ANY($1::text[]) AND segment IN ('all', 'headliner')
              AND definition = ${PAIRED_DEFINITION}
            GROUP BY 1, 2, 3, 4, 5`,
          [MATRIX_REFERENCES],
        ).catch((err: Error) => {
          this.logger.warn(
            `Model comparison: since unavailable: ${err.message}`,
          );
          return [];
        }),
    );
    return new Map(
      rows.map((r) => [
        [r.useCase, r.lead, r.model, r.reference, r.segment].join("|"),
        r.since,
      ]),
    );
  }

  /**
   * Dates, parks and rides the day view can show, and the ride-day to open
   * first. Only complete dates are listed (over in every park's timezone);
   * without a `date`, the newest of them that has a suggestion is described.
   */
  async getDayOptions(
    date?: string,
    now: Date = new Date(),
  ): Promise<ModelComparisonDayOptionsDto> {
    const tzRows = await this.q<Array<{ timezone: string }>>(
      `SELECT DISTINCT timezone FROM parks WHERE timezone IS NOT NULL`,
      [],
    );
    const cutoff = lastCompleteDate(
      tzRows.map((r) => r.timezone),
      now,
    );
    const v = await this.version();
    return this.cached(
      `model-comparison:day-options:v${v}:${cutoff}:${date ?? "-"}`,
      DAY_TTL_SECONDS,
      async () => {
        // Archive rows live 14 days past their target date. 30 index probes,
        // newest first, ending at the last date that is over everywhere.
        const dateRows = await this.q<Array<{ d: string }>>(
          `SELECT d::date::text AS d
             FROM generate_series($1::date - ${MAX_DATES - 1}, $1::date, interval '1 day') d
            WHERE EXISTS (SELECT 1 FROM forecast_archive_curves c
                           WHERE c.target_date = d::date AND c.origin_kind = $2)
            ORDER BY d DESC`,
          [cutoff, ARCHIVE_ORIGIN_KINDS.daily],
        );
        const dates = dateRows.map((r) => r.d);
        const newest = dates.slice(0, SUGGESTION_DATE_TRIES);
        const tries = date ? [date] : newest.length ? newest : [cutoff];
        let chosen = tries[0];
        let ranked = rankDayOptions(chosen, []);
        for (const d of tries) {
          const r = rankDayOptions(d, await this.dayOptionRows(d));
          if (d === tries[0]) ranked = r;
          if (r.suggested) {
            chosen = d;
            ranked = r;
            break;
          }
        }
        return {
          date: chosen,
          dates,
          parks: ranked.parks,
          origins: DAY_ORIGINS.map(({ id, label }) => ({ id, label })),
          suggested: ranked.suggested,
        };
      },
    );
  }

  /**
   * Every ride with a `park_hourly` or `plan_day` curve on `date`, with its
   * model count at the morning origin and its truth coverage: the share of the
   * operating window (published hours, else the park-local day) in which an
   * OPERATING STANDBY reading ≥ 5 min was in force (≤ 3 h old — the truth's
   * own rule, time-weighted instead of per slot midpoint). One pass over that
   * day's change log of the archived rides: 0.6 s on production for
   * 2026-10-10 (5,645 rides, 356 k rows).
   */
  private async dayOptionRows(date: string): Promise<DayOptionRow[]> {
    const day = new Date(`${date}T00:00:00Z`).getTime();
    const rows = await this.q<
      Array<
        Omit<DayOptionRow, "coverage" | "models"> & {
          models: string | number;
          coverage: string | number | null;
        }
      >
    >(
      `WITH rides AS (
         SELECT park_id, attraction_id, bool_or(is_headliner) AS hl,
                count(DISTINCT surface) FILTER (
                  WHERE lead_days = 0 AND surface = ANY($5::smallint[])) AS models
           FROM forecast_archive_curves
          WHERE target_date = $1::date AND origin_kind = $2
          GROUP BY 1, 2
         HAVING bool_or(surface IN ($3, $4))
       ), win AS (
         SELECT "parkId" AS park_id, min("openingTime") AS open, max("closingTime") AS close
           FROM schedule_entries
          WHERE date = $1::date AND "scheduleType" = 'OPERATING' AND "attractionId" IS NULL
            AND "openingTime" IS NOT NULL AND "closingTime" > "openingTime"
          GROUP BY 1
       ), dw AS (
         SELECT p.id AS park_id, p.name, p.timezone,
                coalesce(w.open, $1::date::timestamp AT TIME ZONE p.timezone) AS open,
                coalesce(w.close, ($1::date + 1)::timestamp AT TIME ZONE p.timezone) AS close
           FROM parks p LEFT JOIN win w ON w.park_id = p.id
          WHERE p.id IN (SELECT park_id FROM rides)
       ), q AS (
         SELECT qd."attractionId" AS id, qd.timestamp AS ts, qd.status::text AS status,
                qd."waitTime" AS wait,
                lead(qd.timestamp) OVER (PARTITION BY qd."attractionId" ORDER BY qd.timestamp) AS nxt
           FROM queue_data qd
          WHERE qd."attractionId" = ANY(ARRAY(SELECT attraction_id FROM rides))
            AND qd."queueType" = 'STANDBY'
            AND qd.timestamp >= $6 AND qd.timestamp < $7
            AND ${truthRowsSql("qd")}
       )
       SELECT r.park_id AS "parkId", dw.name AS "parkName", dw.timezone,
              r.attraction_id AS "attractionId", a.name, r.hl AS "isHeadliner",
              r.models,
              coalesce(sum(GREATEST(0, EXTRACT(EPOCH FROM
                  LEAST(coalesce(q.nxt, dw.close), q.ts + interval '3 hours', dw.close)
                  - GREATEST(q.ts, dw.open))))
                FILTER (WHERE q.status = 'OPERATING' AND q.wait >= 5), 0)
                / NULLIF(EXTRACT(EPOCH FROM dw.close - dw.open), 0) AS coverage
         FROM rides r
         JOIN dw ON dw.park_id = r.park_id
         JOIN attractions a ON a.id = r.attraction_id
         LEFT JOIN q ON q.id = r.attraction_id AND q.ts < dw.close
        GROUP BY r.park_id, dw.name, dw.timezone, r.attraction_id, a.name, r.hl,
                 r.models, dw.open, dw.close`,
      [
        date,
        ARCHIVE_ORIGIN_KINDS.daily,
        ARCHIVE_SURFACES.park_hourly,
        ARCHIVE_SURFACES.plan_day,
        SERIES_BY_SURFACE.map(([surface]) => ARCHIVE_SURFACES[surface]),
        // Covers the park-local day of every timezone in use (Asia to the
        // US west coast), the 3 h a reading stays in force before the
        // opening, and closings past midnight.
        new Date(day - 24 * 3_600_000),
        new Date(day + 36 * 3_600_000),
      ],
    );
    return rows.map((r) => ({
      ...r,
      isHeadliner: Boolean(r.isHeadliner),
      models: Number(r.models),
      coverage: Math.min(1, Number(r.coverage ?? 0)),
    }));
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

    const tz = park.timezone;
    const windows = await this.operatingWindows(parkId, date);
    // Without published hours the truth is read over the park-local day, and
    // the slots then follow it (dayViewSlots).
    const truthWindows =
      windows.length > 0
        ? windows
        : [ModelComparisonService.localDay(date, tz)];

    const readings = await this.q<
      Array<{ ts: Date; status: string; wait: number | null }>
    >(
      `SELECT timestamp AS ts, status::text AS status, "waitTime" AS wait
         FROM queue_data
        WHERE "attractionId" = $1::uuid AND "queueType" = 'STANDBY'
          AND timestamp >= $2 AND timestamp < $3
          AND ${truthRowsSql()}
        ORDER BY timestamp`,
      [
        attractionId,
        new Date(Math.min(...truthWindows.map((w) => w.open)) - 3 * 3_600_000),
        new Date(Math.max(...truthWindows.map((w) => w.close))),
      ],
    );
    const list: TruthReading[] = readings.map((r) => ({
      timestamp: new Date(r.ts).getTime(),
      status: r.status,
      waitTime: r.wait === null ? null : Number(r.wait),
    }));
    const truthSlots = buildTruthSlots(list, truthWindows);
    const slotTimes = dayViewSlots({
      windows,
      truthSlots: [...truthSlots.keys()],
      served:
        atOrigin.find((c) => c.surface === "plan_day") ??
        atOrigin.find((c) => c.surface === "park_hourly") ??
        null,
    });

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
      truthCoverage: truthCoverage(truth),
      series,
      scores,
    };
  }

  /** The park-local calendar day `date` as an instant window. */
  static localDay(date: string, timezone: string): OperatingWindow {
    const next = new Date(`${date}T12:00:00Z`);
    next.setUTCDate(next.getUTCDate() + 1);
    return {
      open: fromZonedTime(`${date}T00:00:00`, timezone).getTime(),
      close: fromZonedTime(
        `${next.toISOString().slice(0, 10)}T00:00:00`,
        timezone,
      ).getTime(),
    };
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
