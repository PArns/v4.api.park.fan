import { Controller, Get } from "@nestjs/common";
import { InjectConnection, InjectRepository } from "@nestjs/typeorm";
import { Connection, Repository } from "typeorm";
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
    ml?: {
      status: "ready" | "not_ready";
      version?: string;
      trained_at?: string;
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
  ) { }

  @Get()
  async getHealth(): Promise<HealthStatus> {
    const dbStatus = this.connection.isInitialized
      ? "connected"
      : "disconnected";

    // Run all queries in parallel for speed
    const [
      parksCount,
      attractionsCount,
      waitTimes24h,
      latestWaitTime,
      latestParkUpdate,
      activeModel,
      redisStatus,
    ] = await Promise.all([
      this.parkRepository.count(),
      this.attractionRepository.count(),
      this.getWaitTimesCount24h(),
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

    return {
      status: dbStatus === "connected" && redisStatus ? "ok" : "degraded",
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      version: "4.0.0",
      services: {
        database: {
          status: dbStatus,
          type: "PostgreSQL + TimescaleDB",
        },
        redis: {
          status: redisStatus ? "connected" : "disconnected",
        },
        ...(activeModel && {
          ml: {
            status: "ready",
            version: activeModel.version,
            trained_at: activeModel.trainedAt?.toISOString(),
          },
        }),
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
        ...(dataAgeMinutes !== undefined && { data_age_minutes: dataAgeMinutes }),
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
          timestamp: { $gte: yesterday } as any,
        },
      });
      return count;
    } catch {
      return 0;
    }
  }

  private async getLatestWaitTime(): Promise<Date | null> {
    try {
      const latest = await this.queueDataRepository.findOne({
        order: { timestamp: "DESC" },
        select: ["timestamp"],
      });
      return latest?.timestamp || null;
    } catch {
      return null;
    }
  }

  private async getLatestParkUpdate(): Promise<Date | null> {
    try {
      const latest = await this.parkRepository.findOne({
        order: { updatedAt: "DESC" },
        select: ["updatedAt"],
      });
      return latest?.updatedAt || null;
    } catch {
      return null;
    }
  }

  private async getActiveMLModel(): Promise<{
    version: string;
    trainedAt?: Date;
  } | null> {
    try {
      const activeModel = await this.mlModelRepository.findOne({
        where: { isActive: true },
        select: ["version", "trainedAt"],
      });
      return activeModel || null;
    } catch {
      return null;
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

