import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectDataSource } from "@nestjs/typeorm";
import { DataSource } from "typeorm";
import { Redis } from "ioredis";
import axios from "axios";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { getMlServiceUrl, getNfServiceUrl } from "../config/ml-services.config";
import {
  addIsoDays,
  getCurrentDateInTimezone,
} from "../common/utils/date.util";
import {
  StatementLimits,
  queryWithLimits,
} from "../common/utils/statement-limits.util";
import { ModelComparisonService } from "../parks/services/model-comparison.service";
import {
  CHRONOS_LAST_RUN_KEY,
  ChronosRunSummary,
  ChronosShadowService,
} from "../parks/services/chronos-shadow.service";
import { ARCHIVE_SURFACES } from "../ml/entities/forecast-archive-curve.entity";
import { serveChronosIntraday } from "../ml/chronos-serving";
import { servePcnIntraday } from "../ml/pcn-serving.constants";
import { ModelComparisonResponseDto } from "../ml/dto/model-comparison.dto";
import { MlComponentDto, MlOverviewDto } from "../ml/dto/ml-overview.dto";
import {
  DecisionFile,
  LATE_AFTER_MS,
  buildDecisions,
  componentStatus,
  versionNote,
} from "../ml/utils/ml-overview.util";
import { dayMonthDe } from "../ml/utils/model-comparison.util";
import decisionsJson from "../ml/data/ml-decisions.json";

/**
 * Each probe is an index lookup measured in milliseconds on production
 * (docs/ml/ml-overview.md); 10 s means something is wrong, not slow.
 */
const PROBE_LIMITS: StatementLimits = {
  statementTimeoutMs: 10_000,
  lockTimeoutMs: 2_000,
  idleInTransactionTimeoutMs: 5_000,
};

/** The page is polled by hand; five minutes is fresh enough for "is it running". */
const OVERVIEW_TTL_SECONDS = 5 * 60;
export const ML_OVERVIEW_CACHE_KEY = "ml-overview:v1";
/** A service status call must never hold the page up. */
const SERVICE_TIMEOUT_MS = 3_000;
const ADMIN_TZ = "Europe/Berlin";
/** The matrix the summary and the decisions read: the board's default window. */
const MATRIX_DAYS = 28;

interface LatestRow {
  at: Date | string | null;
  version?: string | null;
}

interface TrainStatus {
  status?: string;
  is_training?: boolean;
  finished_at?: string | null;
  error?: string | null;
  enabled?: boolean;
  loaded?: boolean;
}

const toDate = (v: Date | string | null | undefined): Date | null =>
  v ? new Date(v) : null;

/**
 * The admin ML overview (PAR-861): what is up for decision, the model
 * comparison's summary, and whether every component ran when it should have.
 * Read-only; every query is a bounded index lookup under statement limits,
 * every service call has a short timeout, and any failing source degrades to
 * `unknown` instead of failing the page.
 */
@Injectable()
export class MlOverviewService {
  private readonly logger = new Logger(MlOverviewService.name);
  private readonly decisionFile = decisionsJson as unknown as DecisionFile;

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly modelComparison: ModelComparisonService,
  ) {}

  async getOverview(now: Date = new Date()): Promise<MlOverviewDto> {
    const hit = await this.redis.get(ML_OVERVIEW_CACHE_KEY).catch(() => null);
    if (hit) {
      try {
        return JSON.parse(hit) as MlOverviewDto;
      } catch {
        // rebuild
      }
    }
    const value = await this.build(now);
    await this.redis
      .set(
        ML_OVERVIEW_CACHE_KEY,
        JSON.stringify(value),
        "EX",
        OVERVIEW_TTL_SECONDS,
      )
      .catch(() => undefined);
    return value;
  }

  async build(now: Date): Promise<MlOverviewDto> {
    const today = getCurrentDateInTimezone(ADMIN_TZ);
    // A decision past its evaluation date reads the window ending the day
    // before it, so it does not drift (PAR-1036). Cached per `until` by the
    // comparison service.
    const due = [
      ...new Set(
        this.decisionFile.decisions
          .map((d) => d.evaluateOn)
          .filter((d): d is string => !!d && d <= today),
      ),
    ];
    const frozen = new Map<string, ModelComparisonResponseDto | null>(
      await Promise.all(
        due.map(
          async (d): Promise<[string, ModelComparisonResponseDto | null]> => [
            d,
            await this.modelComparison
              .getMatrix(MATRIX_DAYS, "ALL", now, addIsoDays(d, -1))
              .catch(() => null),
          ],
        ),
      ),
    );
    const [matrix, components] = await Promise.all([
      // Cached 2 h per scoring run by the comparison service itself.
      this.modelComparison
        .getMatrix(MATRIX_DAYS, "ALL", now)
        .catch((err: Error): ModelComparisonResponseDto | null => {
          this.logger.warn(
            `ML overview: model comparison unavailable: ${err.message}`,
          );
          return null;
        }),
      this.components(now),
    ]);
    return {
      generatedAt: now.toISOString(),
      decisions: buildDecisions(this.decisionFile, matrix, today, frozen),
      matrixSummary: matrix?.summary ?? {
        better: 0,
        pending: 0,
        unclear: 0,
        worse: 0,
      },
      components,
    };
  }

  private probe<T>(name: string, sql: string): Promise<T | null> {
    return queryWithLimits<T>(this.dataSource, sql, [], PROBE_LIMITS).catch(
      (err: Error) => {
        this.logger.warn(`ML overview: ${name} probe failed: ${err.message}`);
        return null;
      },
    );
  }

  private service(url: string): Promise<TrainStatus | null> {
    return axios
      .get<TrainStatus>(url, { timeout: SERVICE_TIMEOUT_MS })
      .then((r) => r.data ?? null)
      .catch(() => null);
  }

  async components(now: Date): Promise<MlComponentDto[]> {
    const [
      pcn,
      tft,
      catboost,
      surfaces,
      scoring,
      chronosRunRaw,
      nfTrain,
      mlTrain,
      chronosStatus,
    ] = await Promise.all([
      // Backward scan of idx_pcn_forecasts_created_at, recent window only.
      this.probe<LatestRow[]>(
        "pcn",
        `SELECT created_at AS at, model_version AS version
           FROM pcn_forecasts
          WHERE created_at > now() - interval '2 days'
          ORDER BY created_at DESC LIMIT 1`,
      ),
      // tft_forecasts has no index on created_at / forecast_date (3.4 GB), so
      // the newest run is read through the PK: per ride, its newest forecast
      // for the day after tomorrow. A stopped TFT leaves older rows there.
      this.probe<LatestRow[]>(
        "tft",
        `SELECT t.created_at AS at, t.model_version AS version
           FROM attractions a
          CROSS JOIN LATERAL (
                SELECT f.created_at, f.model_version
                  FROM tft_forecasts f
                 WHERE f.attraction_id = a.id
                   AND f.target_date = (now() AT TIME ZONE 'UTC')::date + 2
                 ORDER BY f.forecast_date DESC
                 LIMIT 1) t
          ORDER BY t.created_at DESC LIMIT 1`,
      ),
      this.probe<LatestRow[]>(
        "catboost",
        `SELECT "trainedAt" AS at, version
           FROM ml_models WHERE "isActive" = true
          ORDER BY "trainedAt" DESC LIMIT 1`,
      ),
      // Index-only scan of the PK (origin_at leads), two days of origins.
      this.probe<Array<{ surface: number; at: Date | string }>>(
        "archive",
        `SELECT surface, max(origin_at) AS at
           FROM forecast_archive_curves
          WHERE origin_at > now() - interval '2 days'
          GROUP BY surface`,
      ),
      this.probe<Array<{ at: Date | string | null; date: string | null }>>(
        "scoring",
        `SELECT max(scored_at) AS at, max(target_date)::text AS date
           FROM forecast_archive_scores
          WHERE target_date >= (now() AT TIME ZONE 'UTC')::date - 30`,
      ),
      this.redis.get(CHRONOS_LAST_RUN_KEY).catch(() => null),
      this.service(`${getNfServiceUrl()}/train/status`),
      this.service(`${getMlServiceUrl()}/train/status`),
      this.service(`${getNfServiceUrl()}/chronos/status`),
    ]);

    let chronosRun: ChronosRunSummary | null = null;
    try {
      chronosRun = chronosRunRaw
        ? (JSON.parse(chronosRunRaw) as ChronosRunSummary)
        : null;
    } catch {
      chronosRun = null;
    }

    const surfaceAt = (s: number): Date | null => {
      const row = surfaces?.find((r) => Number(r.surface) === s);
      return toDate(row?.at);
    };
    const latest = (...ds: Array<Date | null>): Date | null =>
      ds.reduce<Date | null>((m, d) => (d && (!m || d > m) ? d : m), null);

    // --- PCN -----------------------------------------------------------
    const pcnAt = toDate(pcn?.[0]?.at);
    const pcnServed = servePcnIntraday();

    // --- TFT -----------------------------------------------------------
    const tftAt = toDate(tft?.[0]?.at);
    const tftFailed = trainingFailedAfter(nfTrain, tftAt);
    const tftNote = trainingNote(nfTrain, tftFailed, tft?.[0]?.version);

    // --- CatBoost ------------------------------------------------------
    const cbAt = toDate(catboost?.[0]?.at);
    const cbFailed = trainingFailedAfter(mlTrain, cbAt);
    const cbNote = trainingNote(mlTrain, cbFailed, catboost?.[0]?.version);

    // --- Chronos-2 -----------------------------------------------------
    const chronosServed = serveChronosIntraday();
    const chronosAt = latest(
      surfaceAt(ARCHIVE_SURFACES.chronos_live),
      surfaceAt(ARCHIVE_SURFACES.chronos_grid),
    );
    const chronos = chronosState(
      chronosRun,
      chronosStatus,
      chronosAt,
      now,
      ChronosShadowService.enabled(),
    );

    // --- Archive / scoring ---------------------------------------------
    const archiveAt = latest(
      ...Object.values(ARCHIVE_SURFACES).map((s) => surfaceAt(s)),
    );
    const scoringAt = toDate(scoring?.[0]?.at);
    const scoredDate = scoring?.[0]?.date ?? null;

    const status = (
      at: Date | null,
      ms: number,
      o?: "failed" | "warming" | null,
    ) => componentStatus(at, ms, now, o);
    const iso = (d: Date | null) => (d ? d.toISOString() : null);

    return [
      {
        id: "pcn",
        name: "PCN",
        delivers: "Live-Wartezeiten bis 12 Stunden",
        role: pcnServed ? "served" : "shadow",
        lastRunAt: iso(pcnAt),
        expected: "alle 15 Min. (rund um die Uhr)",
        status: status(pcnAt, LATE_AFTER_MS.pcn),
        note: pcnServed
          ? versionNote(pcn?.[0]?.version)
          : `nicht ausgeliefert (SERVE_PCN_INTRADAY aus)${pcn?.[0]?.version ? ` · Version ${pcn[0].version}` : ""}`,
      },
      {
        id: "tft",
        name: "TFT",
        delivers: "Kalender und Tagesniveau, 1 bis 60 Tage",
        role: "served",
        lastRunAt: iso(tftAt),
        expected: "täglich 03:00 UTC",
        status: status(tftAt, LATE_AFTER_MS.tft, tftFailed ? "failed" : null),
        note: tftNote,
      },
      {
        id: "catboost",
        name: "CatBoost",
        delivers: "Stundenwerte jenseits 12 Stunden, Tagesniveau ab Tag 61",
        role: "served",
        lastRunAt: iso(cbAt),
        expected: "täglich 06:00 UTC",
        status: status(
          cbAt,
          LATE_AFTER_MS.catboost,
          cbFailed ? "failed" : null,
        ),
        note: cbNote,
      },
      {
        id: "chronos",
        name: "Chronos-2",
        // PAR-911: with SERVE_CHRONOS_INTRADAY on, the live 30–120 min slots
        // are served; the planner variant stays a shadow either way.
        delivers: chronosServed
          ? "Live 30–120 Min. (ausgeliefert), Tagesplaner 0–7 Tage (Schatten)"
          : "Live 30–120 Min., Tagesplaner 0–7 Tage (nur Schatten)",
        role: chronosServed ? "served" : "shadow",
        lastRunAt: iso(chronosAt),
        expected: chronosServed
          ? "stündlich zur Minute :05 UTC (je offenem Park 10–18 Uhr Ortszeit)"
          : "stündlich zur Minute :05 UTC (je offenem Park alle 2 Std.)",
        status: status(chronosAt, LATE_AFTER_MS.chronos, chronos.override),
        note: chronosServed
          ? ["Live ausgeliefert (SERVE_CHRONOS_INTRADAY an)", chronos.note]
              .filter(Boolean)
              .join(" · ")
          : chronos.note,
      },
      {
        id: "h5",
        name: "H5-Profile",
        delivers: "15-Minuten-Verlauf für den Tagesplaner (nur Schatten)",
        role: "shadow",
        lastRunAt: iso(surfaceAt(ARCHIVE_SURFACES.plan_day_shadow_h5)),
        expected: "stündlich zur Minute :05 UTC (je Park einmal am Tag)",
        status: status(
          surfaceAt(ARCHIVE_SURFACES.plan_day_shadow_h5),
          LATE_AFTER_MS.h5,
        ),
        note: null,
      },
      {
        id: "archive",
        name: "Forward-Archiv",
        delivers: "speichert alle Prognosen für den Vergleich",
        role: "measurement",
        lastRunAt: iso(archiveAt),
        expected: "stündlich zur Minute :05 UTC",
        status: status(archiveAt, LATE_AFTER_MS.archive),
        note: null,
      },
      {
        id: "scoring",
        name: "Bewertung",
        delivers: "rechnet die Matrix gegen echte Wartezeiten",
        role: "measurement",
        lastRunAt: iso(scoringAt),
        expected: "täglich 12:00 UTC",
        status: status(scoringAt, LATE_AFTER_MS.scoring),
        note: scoredDate
          ? `zuletzt bewertet: Tag ${dayMonthDe(scoredDate)}`
          : null,
      },
    ];
  }
}

/** A training run that ended in `failed` after the newest output we have. */
export function trainingFailedAfter(
  st: TrainStatus | null,
  lastRunAt: Date | null,
): boolean {
  if (!st || st.status !== "failed") return false;
  if (!st.finished_at) return !lastRunAt;
  const finished = new Date(st.finished_at);
  if (Number.isNaN(finished.getTime())) return false;
  return !lastRunAt || finished > lastRunAt;
}

/** "Letztes Training fehlgeschlagen: … · Version v…", "trainiert gerade · Version v…", "Version v…". */
export function trainingNote(
  st: TrainStatus | null,
  failed: boolean,
  version: string | null | undefined,
): string | null {
  const parts: string[] = [];
  if (failed) {
    parts.push(
      `Letztes Training fehlgeschlagen${st?.error ? `: ${short(st.error)}` : ""}`,
    );
  } else if (st?.is_training) {
    parts.push("trainiert gerade");
  }
  const v = versionNote(version);
  if (v) parts.push(v);
  return parts.length ? parts.join(" · ") : null;
}

function short(s: string): string {
  return s.length > 80 ? `${s.slice(0, 77)}…` : s;
}

/**
 * Chronos-2's override and note from the archive's last run summary and
 * nf-service's `/chronos/status`. A summary older than the late threshold
 * says nothing about now and is ignored.
 */
export function chronosState(
  run: ChronosRunSummary | null,
  nf: TrainStatus | null,
  lastRunAt: Date | null,
  now: Date,
  apiEnabled: boolean,
): { override: "failed" | "warming" | null; note: string | null } {
  if (!apiEnabled) {
    return {
      override: null,
      note: "abgeschaltet (CHRONOS_SHADOW_ENABLED=false)",
    };
  }
  if (nf && nf.enabled === false) {
    return { override: null, note: "in nf-service abgeschaltet" };
  }
  const fresh =
    run && now.getTime() - new Date(run.at).getTime() <= LATE_AFTER_MS.chronos;
  if (fresh && run) {
    if (run.calls > 0 && run.ok === 0 && run.failures > 0) {
      return {
        override: "failed",
        note: `letzter Lauf: ${run.failures} von ${run.calls} Abrufen fehlgeschlagen`,
      };
    }
    if (run.warming > 0) {
      return {
        override: "warming",
        note: `lädt Historie (${run.ok} von ${run.calls} Parks liefern schon)`,
      };
    }
  }
  if (!lastRunAt && !run) {
    return {
      override: null,
      note: nf?.loaded === false ? "Modell noch nicht geladen" : null,
    };
  }
  return { override: null, note: null };
}
