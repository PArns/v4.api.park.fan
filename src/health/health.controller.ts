import {
  Controller,
  Get,
  Logger,
  ServiceUnavailableException,
} from "@nestjs/common";
import { getMlServiceUrl, getNfServiceUrl } from "../config/ml-services.config";
import { SkipThrottle } from "@nestjs/throttler";
import { ApiTags, ApiOperation, ApiResponse } from "@nestjs/swagger";
import { InjectConnection, InjectRepository } from "@nestjs/typeorm";
import { Connection, Repository, MoreThanOrEqual } from "typeorm";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { MLModel } from "../ml/entities/ml-model.entity";
import { Redis } from "ioredis";
import { Inject } from "@nestjs/common";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import * as packageJson from "../../package.json";
import axios from "axios";

interface HealthStatus {
  status: "ok" | "degraded" | "error";
  timestamp: string;
  uptime: number;
  version: string;
  services: {
    database: {
      status: "connected" | "disconnected";
      type: string;
    };
    redis: {
      status: "connected" | "disconnected";
    };
    ml: {
      status: "ready" | "not_ready" | "unhealthy";
      active_model?: {
        version: string;
        trained_at: string;
        metrics?: { mae: number; rmse: number };
      };
      predictions_24h?: number;
      service_url?: string;
    };
    tft?: {
      status: "ready" | "not_ready" | "unhealthy";
      active_model?: { version: string; trained_at: string };
      service_url?: string;
    };
  };
  data: {
    parks: number;
    attractions: number;
    wait_times_24h: number;
    last_sync: {
      wait_times?: string;
      parks?: string;
    };
    data_age_minutes?: number;
  };
}

/** How long one /health answer is served before it is built again. */
const HEALTH_MEMO_TTL_MS = 60 * 1000;

/**
 * How long the two 24-hour row counts are shared through Redis (PAR-872).
 *
 * The in-process memo above is per instance and lasts a minute, and the probes
 * arrive faster than that: production ran `COUNT(*) FROM wait_time_predictions`
 * 2 620 times a day at 106 ms (≈280 s of database time a day, counting 1.6 M
 * rows each time) and the queue_data 24 h count as often at 13 ms. Both are
 * informational figures on a status page, not a liveness signal — the probe
 * that decides "up" is /health/ready — so ten minutes of staleness costs
 * nothing, and Redis makes the one count serve every instance.
 */
const HEALTH_COUNT_TTL_SECONDS = 10 * 60;
export const HEALTH_COUNT_KEYS = {
  waitTimes24h: "health:count:queue_data_24h",
  predictions24h: "health:count:wait_time_predictions_24h",
} as const;

@ApiTags("health")
@Controller("health")
@SkipThrottle() // monitoring/uptime probes must never be rate limited
export class HealthController {
  private readonly logger = new Logger(HealthController.name);
  private healthMemo: {
    payload: Promise<HealthStatus>;
    expiresAt: number;
  } | null = null;

  constructor(
    @InjectConnection() private connection: Connection,
    @InjectRepository(Park)
    private parkRepository: Repository<Park>,
    @InjectRepository(Attraction)
    private attractionRepository: Repository<Attraction>,
    @InjectRepository(QueueData)
    private queueDataRepository: Repository<QueueData>,
    @InjectRepository(MLModel)
    private mlModelRepository: Repository<MLModel>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  @Get()
  @ApiOperation({
    summary: "System health check",
    description:
      "Returns detailed health status of the API, database, Redis, and ML services.",
  })
  @ApiResponse({
    status: 200,
    description: "System health status retrieved successfully",
    schema: {
      type: "object",
      properties: {
        status: { type: "string", example: "ok" },
        timestamp: { type: "string", format: "date-time" },
        uptime: { type: "number" },
        services: { type: "object" },
        data: { type: "object" },
      },
    },
  })
  @ApiResponse({ status: 503, description: "System is unhealthy" })
  getHealth(): Promise<HealthStatus> {
    // Ten reads per call — two of them count a day of queue_data and of
    // wait_time_predictions (those two are shared through Redis for ten
    // minutes, see HEALTH_COUNT_TTL_SECONDS), two more are HTTP calls to
    // nf-service. It used to back the container healthcheck every 30 s; that
    // now hits /health/ready. The memo keeps an external monitor from paying
    // for the same answer more than once a minute, and shares one in-flight
    // build between concurrent callers. A failed build is not kept.
    const now = Date.now();
    if (this.healthMemo && this.healthMemo.expiresAt > now) {
      return this.healthMemo.payload;
    }
    const payload = this.buildHealth();
    this.healthMemo = { payload, expiresAt: now + HEALTH_MEMO_TTL_MS };
    payload.catch(() => {
      if (this.healthMemo?.payload === payload) this.healthMemo = null;
    });
    return payload;
  }

  /**
   * Readiness for the container healthcheck: can this process reach Postgres
   * and Redis? Two round-trips, no table scanned. Answers 503 when either is
   * down, as the full /health did through its uncaught count queries.
   */
  @Get("ready")
  @ApiOperation({
    summary: "Readiness probe",
    description:
      "Checks the database and Redis connections only. Used by the container healthcheck.",
  })
  @ApiResponse({ status: 200, description: "Database and Redis reachable" })
  @ApiResponse({ status: 503, description: "Database or Redis unreachable" })
  async ready(): Promise<{ status: "ok" }> {
    try {
      await Promise.all([this.connection.query("SELECT 1"), this.redis.ping()]);
    } catch (error) {
      throw new ServiceUnavailableException(
        `Not ready: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    return { status: "ok" };
  }

  private async buildHealth(): Promise<HealthStatus> {
    const dbStatus = this.connection.isInitialized
      ? "connected"
      : "disconnected";

    // Run all queries in parallel for speed
    const [
      parksCount,
      attractionsCount,
      waitTimes24h,
      predictions24h,
      latestWaitTime,
      latestParkUpdate,
      activeModel,
      redisStatus,
      tftStatus,
    ] = await Promise.all([
      this.parkRepository.count(),
      this.attractionRepository.count(),
      this.getWaitTimesCount24h(),
      this.getPredictionsCount24h(),
      this.getLatestWaitTime(),
      this.getLatestParkUpdate(),
      this.getActiveMLModel(),
      this.checkRedisConnection(),
      this.getTftStatus(),
    ]);

    // Calculate data age
    let dataAgeMinutes: number | undefined;
    if (latestWaitTime) {
      const ageMs = Date.now() - latestWaitTime.getTime();
      dataAgeMinutes = Math.round(ageMs / 60000); // Convert to minutes
    }

    // Build ML status
    const mlStatus = {
      status: activeModel ? ("ready" as const) : ("not_ready" as const),
      ...(activeModel && {
        active_model: {
          version: activeModel.version,
          trained_at: activeModel.trainedAt?.toISOString() || "",
          ...(activeModel.mae !== undefined &&
            activeModel.rmse !== undefined && {
              metrics: {
                mae: activeModel.mae,
                rmse: activeModel.rmse,
              },
            }),
        },
      }),
      ...(predictions24h > 0 && { predictions_24h: predictions24h }),
      service_url: getMlServiceUrl(),
    };

    return {
      status: dbStatus === "connected" && redisStatus ? "ok" : "degraded",
      timestamp: new Date().toISOString(),
      uptime: Math.floor(process.uptime()),
      version: packageJson.version,
      services: {
        database: {
          status: dbStatus,
          type: "PostgreSQL + TimescaleDB",
        },
        redis: {
          status: redisStatus ? "connected" : "disconnected",
        },
        ml: mlStatus,
        ...(tftStatus && { tft: tftStatus }),
      },
      data: {
        parks: parksCount,
        attractions: attractionsCount,
        wait_times_24h: waitTimes24h,
        last_sync: {
          ...(latestWaitTime && {
            wait_times: latestWaitTime.toISOString(),
          }),
          ...(latestParkUpdate && {
            parks: latestParkUpdate.toISOString(),
          }),
        },
        ...(dataAgeMinutes !== undefined && {
          data_age_minutes: dataAgeMinutes,
        }),
      },
    };
  }

  @Get("ping")
  ping(): { message: string; timestamp: string } {
    return {
      message: "pong",
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Read-through over Redis for a count that does not need to be exact to the
   * minute. A failed count is never cached (it answers 0, as before), and an
   * unreachable Redis falls through to the database rather than failing.
   */
  private async cachedCount(
    key: string,
    count: () => Promise<number | null>,
  ): Promise<number> {
    try {
      const hit = await this.redis.get(key);
      if (hit !== null) {
        const n = Number.parseInt(hit, 10);
        if (Number.isFinite(n)) return n;
      }
    } catch {
      // Redis down: count from the database below.
    }
    const n = await count();
    if (n === null) return 0;
    try {
      await this.redis.set(key, String(n), "EX", HEALTH_COUNT_TTL_SECONDS);
    } catch {
      // Not cached this time; the next probe counts again.
    }
    return n;
  }

  private getWaitTimesCount24h(): Promise<number> {
    return this.cachedCount(HEALTH_COUNT_KEYS.waitTimes24h, async () => {
      try {
        const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000);
        return await this.queueDataRepository.count({
          where: {
            timestamp: MoreThanOrEqual(yesterday),
          },
        });
      } catch (error) {
        this.logger.error("Error fetching wait times count:", error);
        return null;
      }
    });
  }

  private async getLatestWaitTime(): Promise<Date | null> {
    try {
      const since = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
      const latest = await this.queueDataRepository
        .createQueryBuilder("qd")
        .select("qd.timestamp")
        .where("qd.timestamp >= :since", { since })
        .orderBy("qd.timestamp", "DESC")
        .limit(1)
        .getOne();
      return latest?.timestamp || null;
    } catch {
      return null;
    }
  }

  private async getLatestParkUpdate(): Promise<Date | null> {
    try {
      const latest = await this.parkRepository
        .createQueryBuilder("park")
        .select("park.updatedAt")
        .orderBy("park.updatedAt", "DESC")
        .limit(1)
        .getOne();
      return latest?.updatedAt || null;
    } catch {
      return null;
    }
  }

  private async getActiveMLModel(): Promise<{
    version: string;
    trainedAt?: Date;
    mae?: number;
    rmse?: number;
  } | null> {
    try {
      const activeModel = await this.mlModelRepository.findOne({
        where: { isActive: true },
        select: ["version", "trainedAt", "mae", "rmse"],
      });
      return activeModel || null;
    } catch {
      return null;
    }
  }

  private getPredictionsCount24h(): Promise<number> {
    return this.cachedCount(HEALTH_COUNT_KEYS.predictions24h, async () => {
      try {
        const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000);
        // Served index-only by IDX_daef (predictionType, createdAt) through
        // PG18 skip-scan, ~90 ms for 1.6 M rows — cheap once, not per probe.
        const count = await this.connection.query(
          `SELECT COUNT(*) as count FROM wait_time_predictions 
           WHERE "createdAt" >= $1`,
          [yesterday],
        );
        return parseInt(count[0]?.count || "0", 10);
      } catch (error) {
        // If table doesn't exist yet (first deployment), return 0
        if (
          error instanceof Error &&
          error.message?.includes("does not exist")
        ) {
          return null;
        }
        this.logger.error("Error fetching predictions count:", error);
        return null;
      }
    });
  }

  private async checkRedisConnection(): Promise<boolean> {
    try {
      await this.redis.ping();
      return true;
    } catch {
      return false;
    }
  }

  private async getTftStatus(): Promise<{
    status: "ready" | "not_ready" | "unhealthy";
    active_model?: { version: string; trained_at: string };
    service_url: string;
  } | null> {
    const nfUrl = getNfServiceUrl();
    try {
      const [healthRes, statusRes] = await Promise.all([
        axios.get(`${nfUrl}/health`, { timeout: 3000 }).then((r) => r.data),
        axios
          .get(`${nfUrl}/train/status`, { timeout: 3000 })
          .then((r) => r.data),
      ]);
      const modelTrained = healthRes?.model_trained === true;
      const version = statusRes?.version ?? null;
      const finishedAt = statusRes?.finished_at ?? null;
      return {
        status: modelTrained ? "ready" : "not_ready",
        ...(modelTrained &&
          version && {
            active_model: {
              version,
              trained_at: finishedAt ?? new Date(0).toISOString(),
            },
          }),
        service_url: nfUrl,
      };
    } catch {
      return { status: "unhealthy", service_url: nfUrl };
    }
  }
}
