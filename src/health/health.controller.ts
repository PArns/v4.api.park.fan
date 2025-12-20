import { Controller, Get } from "@nestjs/common";
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
        metrics?: {
          mae: number;
          rmse: number;
        };
      };
      predictions_24h?: number;
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

@ApiTags("health")
@Controller("health")
export class HealthController {
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
  async getHealth(): Promise<HealthStatus> {
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
    ] = await Promise.all([
      this.parkRepository.count(),
      this.attractionRepository.count(),
      this.getWaitTimesCount24h(),
      this.getPredictionsCount24h(),
      this.getLatestWaitTime(),
      this.getLatestParkUpdate(),
      this.getActiveMLModel(),
      this.checkRedisConnection(),
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
      service_url: process.env.ML_SERVICE_URL || "http://ml-service:8000",
    };

    return {
      status: dbStatus === "connected" && redisStatus ? "ok" : "degraded",
      timestamp: new Date().toISOString(),
      uptime: Math.floor(process.uptime()),
      version: "4.0.0",
      services: {
        database: {
          status: dbStatus,
          type: "PostgreSQL + TimescaleDB",
        },
        redis: {
          status: redisStatus ? "connected" : "disconnected",
        },
        ml: mlStatus,
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

  private async getWaitTimesCount24h(): Promise<number> {
    try {
      const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000);
      const count = await this.queueDataRepository.count({
        where: {
          timestamp: MoreThanOrEqual(yesterday),
        },
      });
      return count;
    } catch (error) {
      console.error("Error fetching wait times count:", error);
      return 0;
    }
  }

  private async getLatestWaitTime(): Promise<Date | null> {
    try {
      const latest = await this.queueDataRepository
        .createQueryBuilder("qd")
        .select("qd.timestamp")
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

  private async getPredictionsCount24h(): Promise<number> {
    try {
      const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000);
      // Query WaitTimePrediction table for predictions generated in last 24h
      const count = await this.connection.query(
        `SELECT COUNT(*) as count FROM wait_time_predictions 
         WHERE "createdAt" >= $1`,
        [yesterday],
      );
      return parseInt(count[0]?.count || "0", 10);
    } catch (error) {
      // If table doesn't exist yet (first deployment), return 0
      if (error instanceof Error && error.message?.includes("does not exist")) {
        return 0;
      }
      console.error("Error fetching predictions count:", error);
      return 0;
    }
  }

  private async checkRedisConnection(): Promise<boolean> {
    try {
      await this.redis.ping();
      return true;
    } catch {
      return false;
    }
  }
}
