import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectDataSource } from "@nestjs/typeorm";
import { DataSource } from "typeorm";
import { Redis } from "ioredis";
import * as os from "os";
import * as fs from "fs";
import axios from "axios";
import { REDIS_CLIENT } from "../common/redis/redis.module";

const ML_URL = process.env.ML_SERVICE_URL || "http://ml-service:8000";
const NF_URL = process.env.NF_SERVICE_URL || "http://nf-service:8000";
const GB = 1024 ** 3;

/**
 * Aggregates everything a system-health / ML-monitoring dashboard needs:
 * host (CPU/RAM/disk), Postgres, Redis, and both ML services (CatBoost = ml-service,
 * TFT = nf-service) — their training progress/quality + the TFT-vs-CatBoost scoreboard.
 *
 * Every source is fetched independently with its own try/catch so one slow/down
 * component degrades to an {error} field instead of failing the whole endpoint.
 */
@Injectable()
export class SystemHealthService {
  private readonly logger = new Logger(SystemHealthService.name);

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  async getHealth(): Promise<Record<string, unknown>> {
    const [host, gpu, postgres, redis, freshness, catboost, tft, comparison] =
      await Promise.all([
        this.safe("host", () => this.host()),
        this.safe("gpu", () => this.gpu()),
        this.safe("postgres", () => this.postgres()),
        this.safe("redis", () => this.redisStats()),
        this.safe("freshness", () => this.freshness()),
        this.safe("catboost", () => this.catboost()),
        this.safe("tft", () => this.tft()),
        this.safe("comparison", () => this.comparison()),
      ]);
    return {
      timestamp: new Date().toISOString(),
      host,
      gpu,
      postgres,
      redis,
      freshness,
      ml: { catboost, tft, comparison },
    };
  }

  /** Data-ingestion freshness: how long since the newest wait-time / weather row.
   * A stalled cron (once ran 83 days dead) is otherwise invisible. */
  private async freshness() {
    const rows = await this.dataSource.query(`
      SELECT
        (SELECT MAX(timestamp) FROM queue_data) AS latest_queue,
        (SELECT MAX(date) FROM weather_data) AS latest_weather,
        (SELECT COUNT(*) FROM queue_data
           WHERE timestamp > NOW() - INTERVAL '1 hour') AS queue_rows_last_hour
    `);
    const r = rows?.[0] ?? {};
    const ageMin = (d: unknown): number | null =>
      d ? Math.round((Date.now() - new Date(d as string).getTime()) / 60000) : null;
    const weatherMin = ageMin(r.latest_weather);
    return {
      latestQueueTime: r.latest_queue ?? null,
      queueStaleMinutes: ageMin(r.latest_queue),
      queueRowsLastHour: Number(r.queue_rows_last_hour ?? 0),
      latestWeatherDate: r.latest_weather ?? null,
      weatherStaleHours: weatherMin != null ? +(weatherMin / 60).toFixed(1) : null,
    };
  }

  /** GPU telemetry (temp/util/VRAM/power) from nf-service's /gpu (nvidia-smi).
   * On a CPU-only host nf-service returns {available:false}; pass it through. */
  private async gpu() {
    return axios
      .get(`${NF_URL}/gpu`, { timeout: 4000 })
      .then((r) => r.data)
      .catch((e) => ({ available: false, error: String(e?.message ?? e) }));
  }

  private async safe<T>(name: string, fn: () => Promise<T>) {
    try {
      return await fn();
    } catch (e: any) {
      this.logger.warn(`system-health: ${name} failed: ${e?.message ?? e}`);
      return { error: String(e?.message ?? e) };
    }
  }

  /** CPU package temperature (°C) from Linux hwmon, thermal_zone fallback.
   * null on non-Linux or when no sensor is exposed (e.g. in some containers). */
  private async cpuTemp(): Promise<number | null> {
    // Prefer hwmon: coretemp (Intel) / k10temp / zenpower (AMD) expose the real
    // package/Tctl temp. temp*_input is in millidegrees C.
    try {
      const base = "/sys/class/hwmon";
      const dirs = await fs.promises.readdir(base);
      const preferred = ["coretemp", "k10temp", "zenpower", "cpu_thermal"];
      let best: { rank: number; temp: number } | null = null;
      for (const d of dirs) {
        const dir = `${base}/${d}`;
        let name = "";
        try {
          name = (await fs.promises.readFile(`${dir}/name`, "utf8")).trim();
        } catch {
          continue;
        }
        const rank = preferred.indexOf(name);
        if (rank === -1) continue;
        const files = await fs.promises.readdir(dir);
        let maxC = 0;
        for (const f of files) {
          const m = /^temp(\d+)_input$/.exec(f);
          if (!m) continue;
          const raw = Number(
            (await fs.promises.readFile(`${dir}/${f}`, "utf8")).trim(),
          );
          if (raw > 0) maxC = Math.max(maxC, raw / 1000);
        }
        if (maxC > 0 && (!best || rank < best.rank)) {
          best = { rank, temp: maxC };
        }
      }
      if (best) return +best.temp.toFixed(1);
    } catch {
      /* hwmon unavailable — fall through to thermal_zone */
    }
    // Fallback: thermal_zone with a CPU-ish type.
    try {
      const base = "/sys/class/thermal";
      const zones = (await fs.promises.readdir(base)).filter((z) =>
        z.startsWith("thermal_zone"),
      );
      let fallback: number | null = null;
      for (const z of zones) {
        let type = "";
        try {
          type = (await fs.promises.readFile(`${base}/${z}/type`, "utf8")).trim();
        } catch {
          continue;
        }
        const raw = Number(
          (await fs.promises.readFile(`${base}/${z}/temp`, "utf8")).trim(),
        );
        if (!(raw > 0)) continue;
        const c = +(raw / 1000).toFixed(1);
        if (/pkg|core|cpu|k10|tctl|tdie/i.test(type)) return c;
        if (fallback == null) fallback = c;
      }
      return fallback;
    } catch {
      return null;
    }
  }

  /** Host CPU/RAM/disk. In a container /proc still reports the host, so os.* is host-wide. */
  private async host() {
    const cores = os.cpus()?.length ?? 0;
    const cpuTempC = await this.cpuTemp();
    const [l1, l5, l15] = os.loadavg();
    const totalMem = os.totalmem();
    const freeMem = os.freemem();
    let disk: Record<string, unknown> = {};
    try {
      const s: any = await fs.promises.statfs("/");
      const total = s.blocks * s.bsize;
      const free = s.bavail * s.bsize;
      disk = {
        totalGB: +(total / GB).toFixed(1),
        freeGB: +(free / GB).toFixed(1),
        usedPct: +(((total - free) / total) * 100).toFixed(1),
      };
    } catch {
      disk = { error: "statfs unavailable" };
    }
    // Swap from /proc/meminfo (Linux). os.* has no swap; swap pressure was the root
    // cause of a real incident, so surface it. null on non-Linux or no swap.
    let swap: { totalGB: number; usedGB: number; usedPct: number } | null = null;
    try {
      const mi = await fs.promises.readFile("/proc/meminfo", "utf8");
      const kb = (k: string) =>
        Number(new RegExp(`^${k}:\\s+(\\d+)`, "m").exec(mi)?.[1] ?? 0);
      const totalKb = kb("SwapTotal");
      if (totalKb > 0) {
        const usedKb = totalKb - kb("SwapFree");
        swap = {
          totalGB: +(totalKb / 1024 / 1024).toFixed(1),
          usedGB: +(usedKb / 1024 / 1024).toFixed(1),
          usedPct: +((usedKb / totalKb) * 100).toFixed(1),
        };
      }
    } catch {
      swap = null;
    }
    return {
      cpu: {
        cores,
        model: os.cpus()?.[0]?.model?.trim(),
        load: {
          "1m": +l1.toFixed(2),
          "5m": +l5.toFixed(2),
          "15m": +l15.toFixed(2),
        },
        loadPct: cores ? +((l1 / cores) * 100).toFixed(0) : null,
        temperatureC: cpuTempC,
      },
      memory: {
        totalGB: +(totalMem / GB).toFixed(1),
        usedGB: +((totalMem - freeMem) / GB).toFixed(1),
        usedPct: +(((totalMem - freeMem) / totalMem) * 100).toFixed(1),
      },
      swap,
      disk,
      uptimeHours: +(os.uptime() / 3600).toFixed(1),
    };
  }

  private async postgres() {
    const rows = await this.dataSource.query(`
      SELECT
        (SELECT count(*) FROM pg_stat_activity) AS connections,
        (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') AS active,
        (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') AS max_connections,
        pg_database_size(current_database()) AS db_size_bytes,
        (SELECT round(100.0 * sum(blks_hit) / NULLIF(sum(blks_hit) + sum(blks_read), 0), 1)
           FROM pg_stat_database) AS cache_hit_pct
    `);
    const r = rows?.[0] ?? {};
    return {
      status: "connected",
      connections: Number(r.connections),
      activeQueries: Number(r.active),
      maxConnections: Number(r.max_connections),
      connectionsPct: r.max_connections
        ? +((Number(r.connections) / Number(r.max_connections)) * 100).toFixed(
            1,
          )
        : null,
      dbSizeGB: +(Number(r.db_size_bytes) / GB).toFixed(2),
      cacheHitPct: r.cache_hit_pct != null ? Number(r.cache_hit_pct) : null,
    };
  }

  private async redisStats() {
    const info = await this.redis.info();
    const map: Record<string, string> = {};
    for (const line of info.split("\n")) {
      const i = line.indexOf(":");
      if (i > 0 && !line.startsWith("#")) {
        map[line.slice(0, i).trim()] = line.slice(i + 1).trim();
      }
    }
    const hits = Number(map["keyspace_hits"] ?? 0);
    const misses = Number(map["keyspace_misses"] ?? 0);
    let keys = 0;
    for (const [k, v] of Object.entries(map)) {
      if (k.startsWith("db")) {
        const m = /keys=(\d+)/.exec(v);
        if (m) keys += Number(m[1]);
      }
    }
    return {
      status: "connected",
      usedMemoryMB: +(Number(map["used_memory"] ?? 0) / 1024 / 1024).toFixed(1),
      maxMemoryMB:
        +(Number(map["maxmemory"] ?? 0) / 1024 / 1024).toFixed(1) || null,
      connectedClients: Number(map["connected_clients"] ?? 0),
      keys,
      hitRatePct:
        hits + misses > 0 ? +((hits / (hits + misses)) * 100).toFixed(1) : null,
      uptimeHours: +(Number(map["uptime_in_seconds"] ?? 0) / 3600).toFixed(1),
    };
  }

  /** CatBoost = ml-service: live status + active model quality. */
  private async catboost() {
    const [statusRes, modelRows] = await Promise.all([
      axios
        .get(`${ML_URL}/train/status`, { timeout: 4000 })
        .then((r) => r.data)
        .catch((e) => ({ error: String(e?.message ?? e) })),
      this.dataSource
        .query(
          `SELECT version, mae, rmse, mape, "r2Score" AS r2, "trainSamples" AS train_samples,
                  "trainedAt"
             FROM ml_models WHERE "isActive" = true
             ORDER BY "trainedAt" DESC LIMIT 1`,
        )
        .catch(() => []),
    ]);
    const m = modelRows?.[0];
    return {
      service: ML_URL,
      training: statusRes,
      activeModel: m
        ? {
            version: m.version,
            mae: m.mae != null ? +Number(m.mae).toFixed(3) : null,
            rmse: m.rmse != null ? +Number(m.rmse).toFixed(3) : null,
            mape: m.mape != null ? +Number(m.mape).toFixed(3) : null,
            r2: m.r2 != null ? +Number(m.r2).toFixed(4) : null,
            trainSamples: m.train_samples,
            trainedAt: m.trainedAt,
          }
        : null,
    };
  }

  /** TFT = nf-service: live status aligned with CatBoost shape. */
  private async tft() {
    const [status, health] = await Promise.all([
      axios
        .get(`${NF_URL}/train/status`, { timeout: 4000 })
        .then((r) => r.data)
        .catch((e) => ({ error: String(e?.message ?? e) })),
      axios
        .get(`${NF_URL}/health`, { timeout: 4000 })
        .then((r) => r.data)
        .catch((e) => ({ error: String(e?.message ?? e) })),
    ]);
    const modelTrained = health?.model_trained === true;
    const version = status?.version ?? null;
    const finishedAt = status?.finished_at ?? null;
    return {
      service: NF_URL,
      training: status,
      // activeModel mirrors CatBoost shape so dashboards treat both uniformly.
      // TFT has no MAE in DB (no ml_models row); quality lives in model_comparisons.
      activeModel: modelTrained && version
        ? { version, trainedAt: finishedAt, horizon: health?.horizon ?? null, parkScope: health?.park_scope ?? null }
        : null,
      // health kept for backward-compat with frontend (tft.health.status badge)
      health: health ?? null,
    };
  }

  /** TFT-vs-CatBoost forward scoreboard (latest scored target days). */
  private async comparison() {
    const exists = await this.dataSource.query(
      `SELECT to_regclass('public.model_comparisons') AS t`,
    );
    if (!exists?.[0]?.t) return { rows: [], note: "no scoreboard yet" };
    const rows = await this.dataSource.query(
      `SELECT "targetDate", model, n, round(mae::numeric,1) AS mae,
              round(bias::numeric,1) AS bias, "avgLeadDays"
         FROM model_comparisons
         ORDER BY "targetDate" DESC, model LIMIT 30`,
    );
    return { rows, count: rows.length };
  }
}
