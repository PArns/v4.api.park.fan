import { Controller, Get } from "@nestjs/common";
import { InjectConnection } from "@nestjs/typeorm";
import { InjectQueue } from "@nestjs/bull";
import { Connection } from "typeorm";
import { Queue } from "bull";

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
    queues: {
      [key: string]: {
        waiting: number;
        active: number;
        completed: number;
        failed: number;
      };
    };
  };
}

@Controller("health")
export class HealthController {
  constructor(
    @InjectConnection() private connection: Connection,
    @InjectQueue("wait-times") private waitTimesQueue: Queue,
    @InjectQueue("park-metadata") private parkMetadataQueue: Queue,
    @InjectQueue("attractions-metadata") private attractionsQueue: Queue,
  ) {}

  @Get()
  async getHealth(): Promise<HealthStatus> {
    const dbStatus = this.connection.isInitialized
      ? "connected"
      : "disconnected";

    // Get queue stats
    const queues = {
      "wait-times": await this.getQueueStats(this.waitTimesQueue),
      "park-metadata": await this.getQueueStats(this.parkMetadataQueue),
      "attractions-metadata": await this.getQueueStats(this.attractionsQueue),
    };

    const redisStatus = await this.checkRedisConnection();

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
        queues,
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

  private async getQueueStats(queue: Queue): Promise<{
    waiting: number;
    active: number;
    completed: number;
    failed: number;
  }> {
    try {
      const [waiting, active, completed, failed] = await Promise.all([
        queue.getWaitingCount(),
        queue.getActiveCount(),
        queue.getCompletedCount(),
        queue.getFailedCount(),
      ]);

      return { waiting, active, completed, failed };
    } catch {
      return { waiting: 0, active: 0, completed: 0, failed: 0 };
    }
  }

  private async checkRedisConnection(): Promise<boolean> {
    try {
      await this.waitTimesQueue.client.ping();
      return true;
    } catch {
      return false;
    }
  }
}
