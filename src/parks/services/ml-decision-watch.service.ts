import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectDataSource } from "@nestjs/typeorm";
import { DataSource } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { addIsoDays, formatInParkTimezone } from "../../common/utils/date.util";
import { dayHolidayFlags } from "../../common/utils/holiday.utils";
import { MAX_PLAUSIBLE_WAIT_TIME } from "../../common/utils/wait-time.utils";
import {
  StatementLimits,
  queryWithLimits,
} from "../../common/utils/statement-limits.util";
import { HolidaysService, HolidayRow } from "../../holidays/holidays.service";
import { PAIRED_DEFINITION } from "../../ml/entities/forecast-archive-paired.entity";
import { scoredSlotSql } from "../../ml/entities/prediction-accuracy.entity";
import {
  MlDecisionReportDto,
  MlDriftWatchDto,
} from "../../ml/dto/ml-overview.dto";
import { DecisionFile } from "../../ml/utils/ml-overview.util";
import {
  PairedParkDate,
  buildDecisionReport,
  decisionPairedKeys,
  reportDue,
} from "../../ml/utils/ml-decision-report.util";
import {
  DriftWindowSums,
  buildDriftWatch,
} from "../../ml/utils/ml-drift-watch.util";
import decisionsJson from "../../ml/data/ml-decisions.json";
import { ModelComparisonService } from "./model-comparison.service";

const ADMIN_TZ = "Europe/Berlin";
/** The window the overview's decisions read (`MATRIX_DAYS` there). */
const MATRIX_DAYS = 28;
export const DRIFT_WATCH_KEY = "ml-overview:drift:v1";
/** Three days: a missed run or two leaves the last figure up, a stopped job removes it. */
const DRIFT_TTL_SECONDS = 3 * 24 * 3600;
/** The overview's own cache, cleared after a new report or drift figure. */
const OVERVIEW_CACHE_KEY = "ml-overview:v1";

/** Once a day, ~63 days of hourly accuracy rows; minutes, not seconds, is the alarm. */
const WATCH_LIMITS: StatementLimits = {
  statementTimeoutMs: 5 * 60_000,
  lockTimeoutMs: 5_000,
  idleInTransactionTimeoutMs: 30_000,
};

interface DriftRow {
  model: string;
  recent: boolean;
  n: string | number;
  sae: string | number | null;
  nBase: string | number;
  saeOnBase: string | number | null;
  saeBase: string | number | null;
  nQ: string | number;
  hit80: string | number;
  hit95: string | number;
}

/**
 * Daily, after the forward archive's 12:00 UTC scoring (PAR-1059): freeze
 * the report of every decision whose evaluation date has arrived, and
 * compute the drift watch of the served models. Both end up on the admin ML
 * overview; nothing is sent anywhere (owner decision: no alerts for now).
 */
@Injectable()
export class MlDecisionWatchService {
  private readonly logger = new Logger(MlDecisionWatchService.name);
  private readonly decisionFile = decisionsJson as unknown as DecisionFile;

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly modelComparison: ModelComparisonService,
    private readonly holidaysService: HolidaysService,
  ) {}

  private q<T>(sql: string, params: unknown[]): Promise<T> {
    return queryWithLimits<T>(this.dataSource, sql, params, WATCH_LIMITS);
  }

  /**
   * Freezes every due report that does not exist yet. Idempotent: an existing
   * (decision, evaluateOn) row is never read back into a computation, and the
   * INSERT does nothing on conflict, so a second run changes nothing.
   */
  async freezeDueReports(now: Date = new Date()): Promise<string[]> {
    const today = formatInParkTimezone(now, ADMIN_TZ);
    const candidates = this.decisionFile.decisions.filter(
      (d) => !d.state && d.evaluateOn && d.evaluateOn <= today,
    );
    if (candidates.length === 0) return [];
    const existing = await this.q<Array<{ id: string; on: string }>>(
      `SELECT decision_id AS id, evaluate_on::text AS on FROM ml_decision_reports`,
      [],
    );
    const have = new Set(existing.map((r) => `${r.id}|${r.on}`));
    const written: string[] = [];
    for (const def of candidates) {
      const evaluateOn = def.evaluateOn!;
      if (have.has(`${def.id}|${evaluateOn}`)) continue;
      const until = addIsoDays(evaluateOn, -1);
      const matrix = await this.modelComparison.getMatrix(
        MATRIX_DAYS,
        "ALL",
        now,
        until,
      );
      if (!reportDue(def, today, matrix.window.lastDate)) continue;
      const paired = await this.pairedParkDates(
        decisionPairedKeys(def),
        matrix.window.firstDate,
        matrix.window.lastDate,
      );
      const report = buildDecisionReport(
        { ...def, evaluateOn },
        matrix,
        paired,
        now,
      );
      await this.store(report);
      written.push(def.id);
    }
    if (written.length) await this.redis.del(OVERVIEW_CACHE_KEY).catch(() => 0);
    return written;
  }

  private async store(report: MlDecisionReportDto): Promise<void> {
    await this.q(
      `INSERT INTO ml_decision_reports
              (decision_id, evaluate_on, verdict, warning, report, created_at)
       VALUES ($1, $2::date, $3, $4, $5::jsonb, $6)
       ON CONFLICT (decision_id, evaluate_on) DO NOTHING`,
      [
        report.decisionId,
        report.evaluateOn,
        report.verdict,
        report.warning,
        JSON.stringify(report),
        report.createdAt,
      ],
    );
  }

  /** The decision's paired park × date cells in the window, with each park's holiday flag. */
  private async pairedParkDates(
    keys: string[],
    first: string | null,
    last: string | null,
  ): Promise<PairedParkDate[]> {
    if (!first || !last || keys.length === 0) return [];
    const rows = await this.q<Array<{ parkId: string; date: string }>>(
      `SELECT DISTINCT park_id AS "parkId", target_date::text AS date
         FROM forecast_archive_paired
        WHERE target_date BETWEEN $1::date AND $2::date
          AND definition = ${PAIRED_DEFINITION}
          AND (use_case || '|' || lead || '|' || model || '|' || reference || '|' || segment)
              = ANY($3::text[])`,
      [first, last, keys],
    );
    if (rows.length === 0) return [];
    const parks = await this.q<
      Array<{
        id: string;
        countryCode: string | null;
        regionCode: string | null;
        timezone: string;
      }>
    >(
      `SELECT id, "countryCode", "regionCode", timezone FROM parks WHERE id = ANY($1::uuid[])`,
      [[...new Set(rows.map((r) => r.parkId))]],
    );
    const byCountry = new Map<string, HolidayRow[] | null>();
    const flags = new Map<
      string,
      Map<
        string,
        { isHoliday: boolean; isSchoolVacation: boolean; isBridgeDay: boolean }
      > | null
    >();
    for (const p of parks) {
      if (!p.countryCode) {
        flags.set(p.id, null);
        continue;
      }
      if (!byCountry.has(p.countryCode)) {
        byCountry.set(
          p.countryCode,
          await this.holidaysService
            .getHolidays(
              p.countryCode,
              addIsoDays(first, -3),
              addIsoDays(last, 3),
            )
            .catch(() => null),
        );
      }
      const hol = byCountry.get(p.countryCode);
      const dates = rows.filter((r) => r.parkId === p.id).map((r) => r.date);
      flags.set(
        p.id,
        hol
          ? dayHolidayFlags(dates, hol, {
              countryCode: p.countryCode,
              regionCode: p.regionCode,
              timezone: p.timezone,
            })
          : null,
      );
    }
    return rows.map((r) => {
      const f = flags.get(r.parkId);
      const day = f?.get(r.date);
      return {
        parkId: r.parkId,
        date: r.date,
        holiday: f
          ? !!day && (day.isHoliday || day.isSchoolVacation || day.isBridgeDay)
          : null,
      };
    });
  }

  /**
   * Per served model, the last 7 days against the 28 before: MAE, the MAE of
   * the weekday median (same ride, park-local weekday and hour, the 4 weeks
   * before each slot — ex ante for every slot, so both windows are judged
   * alike), and q0.8 / q0.95 coverage. Stored in Redis for the overview.
   */
  async computeDrift(now: Date = new Date()): Promise<MlDriftWatchDto> {
    const rows = await this.q<DriftRow[]>(
      `WITH r AS (
         SELECT pa.attraction_id AS a,
                date_trunc('hour', pa.target_time AT TIME ZONE p.timezone) AS lt,
                pa.target_time >= $1::timestamptz - interval '7 days' AS recent,
                pa.target_time >= $1::timestamptz - interval '35 days' AS judged,
                pa.actual_wait_time AS actual, pa.absolute_error AS err,
                pa.served_source AS src, pa.served_absolute_error AS serr,
                pa.predicted_q80 AS q80, pa.predicted_q95 AS q95
           FROM prediction_accuracy pa
           JOIN attractions att ON att.id = pa.attraction_id
           JOIN parks p ON p.id = att."parkId"
          WHERE pa.comparison_status = 'COMPLETED'
            AND pa.target_time >= $1::timestamptz - interval '63 days'
            AND pa.target_time < $1::timestamptz
            AND pa."predictionType" = 'hourly'
            AND ${scoredSlotSql("pa", MAX_PLAUSIBLE_WAIT_TIME)}
       ),
       base AS (
         SELECT s.a, s.lt,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY h.actual) AS b
           FROM r s
           JOIN r h ON h.a = s.a
            AND h.lt IN (s.lt - interval '7 days', s.lt - interval '14 days',
                         s.lt - interval '21 days', s.lt - interval '28 days')
          WHERE s.judged
          GROUP BY s.a, s.lt
         HAVING count(*) >= 2
       ),
       e AS (
         SELECT r.recent, m.model, m.err, m.withq, r.actual, r.q80, r.q95, b.b
           FROM r
           LEFT JOIN base b ON b.a = r.a AND b.lt = r.lt
          CROSS JOIN LATERAL (VALUES
                ('catboost', r.err, true),
                (CASE WHEN r.src IN ('pcn', 'chronos') THEN r.src END, r.serr, false)
          ) m(model, err, withq)
          WHERE r.judged AND m.model IS NOT NULL AND m.err IS NOT NULL
       )
       SELECT model, recent,
              count(*) AS n, sum(err) AS sae,
              count(b) AS "nBase",
              sum(err) FILTER (WHERE b IS NOT NULL) AS "saeOnBase",
              sum(abs(actual - b)) AS "saeBase",
              count(*) FILTER (WHERE withq AND q80 IS NOT NULL AND q95 IS NOT NULL) AS "nQ",
              count(*) FILTER (WHERE withq AND q80 IS NOT NULL AND q95 IS NOT NULL AND actual <= q80) AS hit80,
              count(*) FILTER (WHERE withq AND q80 IS NOT NULL AND q95 IS NOT NULL AND actual <= q95) AS hit95
         FROM e
        GROUP BY model, recent`,
      [now.toISOString()],
    );
    const sums = new Map<
      string,
      { recent?: DriftWindowSums; previous?: DriftWindowSums }
    >();
    for (const row of rows) {
      const s: DriftWindowSums = {
        n: Number(row.n),
        sae: Number(row.sae ?? 0),
        nBase: Number(row.nBase),
        saeOnBase: Number(row.saeOnBase ?? 0),
        saeBase: Number(row.saeBase ?? 0),
        nQ: Number(row.nQ),
        hitQ80: Number(row.hit80),
        hitQ95: Number(row.hit95),
      };
      const entry = sums.get(row.model) ?? {};
      entry[row.recent ? "recent" : "previous"] = s;
      sums.set(row.model, entry);
    }
    const watch = buildDriftWatch(sums, now);
    await this.redis
      .set(DRIFT_WATCH_KEY, JSON.stringify(watch), "EX", DRIFT_TTL_SECONDS)
      .catch(() => undefined);
    await this.redis.del(OVERVIEW_CACHE_KEY).catch(() => 0);
    return watch;
  }

  /** Both, each on its own: a failing drift query never costs a report, nor the other way round. */
  async runDaily(now: Date = new Date()): Promise<void> {
    try {
      const written = await this.freezeDueReports(now);
      if (written.length) {
        this.logger.log(`🧊 Decision reports frozen: ${written.join(", ")}`);
      }
    } catch (err) {
      this.logger.warn(
        `Decision reports deferred: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
    try {
      const watch = await this.computeDrift(now);
      if (watch.status === "drift") {
        this.logger.warn(
          `ML drift watch: ${watch.models
            .filter((m) => m.status === "drift")
            .map((m) => m.model)
            .join(", ")}`,
        );
      }
    } catch (err) {
      this.logger.warn(
        `ML drift watch deferred: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }
}
