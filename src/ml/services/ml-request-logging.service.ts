import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Between, LessThan } from "typeorm";
import { MLPredictionRequestLog } from "../entities/ml-prediction-request-log.entity";

/**
 * ML Request Logging Service
 *
 * Logs prediction requests for analytics and monitoring.
 * Tracks request volume, latency, and usage patterns.
 */
@Injectable()
export class MLRequestLoggingService {
  private readonly logger = new Logger(MLRequestLoggingService.name);

  constructor(
    @InjectRepository(MLPredictionRequestLog)
    private requestLogRepository: Repository<MLPredictionRequestLog>,
  ) {}

  /**
   * Log a prediction request
   */
  async logRequest(data: {
    parkId?: string | null;
    attractionCount: number;
    parkCount: number;
    predictionType: "hourly" | "daily";
    modelVersion: string;
    durationMs: number;
    predictionCount?: number | null;
    requestMetadata?: Record<string, unknown> | null;
  }): Promise<MLPredictionRequestLog> {
    const log = new MLPredictionRequestLog();
    log.parkId = data.parkId || null;
    log.attractionCount = data.attractionCount;
    log.parkCount = data.parkCount;
    log.predictionType = data.predictionType;
    log.modelVersion = data.modelVersion;
    log.durationMs = data.durationMs;
    log.predictionCount = data.predictionCount || null;
    log.requestMetadata = data.requestMetadata || null;

    return await this.requestLogRepository.save(log);
  }

  /**
   * Get request statistics
   */
  async getRequestStats(days: number = 7): Promise<{
    totalRequests: number;
    avgDuration: number;
    requestsByType: { hourly: number; daily: number };
    requestsByPark: Array<{ parkId: string; count: number }>;
    peakHours: Array<{ hour: number; count: number }>;
    avgAttractionCount: number;
    avgParkCount: number;
  }> {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    const logs = await this.requestLogRepository.find({
      where: {
        createdAt: Between(startDate, new Date()),
      },
    });

    if (logs.length === 0) {
      return {
        totalRequests: 0,
        avgDuration: 0,
        requestsByType: { hourly: 0, daily: 0 },
        requestsByPark: [],
        peakHours: [],
        avgAttractionCount: 0,
        avgParkCount: 0,
      };
    }

    // Calculate statistics
    const totalRequests = logs.length;
    const avgDuration =
      logs.reduce((sum, log) => sum + log.durationMs, 0) / totalRequests;

    const requestsByType = {
      hourly: logs.filter((l) => l.predictionType === "hourly").length,
      daily: logs.filter((l) => l.predictionType === "daily").length,
    };

    // Requests by park
    const parkCounts = new Map<string, number>();
    for (const log of logs) {
      if (log.parkId) {
        parkCounts.set(log.parkId, (parkCounts.get(log.parkId) || 0) + 1);
      }
    }
    const requestsByPark = Array.from(parkCounts.entries())
      .map(([parkId, count]) => ({ parkId, count }))
      .sort((a, b) => b.count - a.count);

    // Peak hours
    const hourCounts = new Map<number, number>();
    for (const log of logs) {
      const hour = log.createdAt.getUTCHours();
      hourCounts.set(hour, (hourCounts.get(hour) || 0) + 1);
    }
    const peakHours = Array.from(hourCounts.entries())
      .map(([hour, count]) => ({ hour, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 10);

    const avgAttractionCount =
      logs.reduce((sum, log) => sum + log.attractionCount, 0) / totalRequests;
    const avgParkCount =
      logs.reduce((sum, log) => sum + log.parkCount, 0) / totalRequests;

    return {
      totalRequests,
      avgDuration: Math.round(avgDuration),
      requestsByType,
      requestsByPark,
      peakHours,
      avgAttractionCount: Math.round(avgAttractionCount * 10) / 10,
      avgParkCount: Math.round(avgParkCount * 10) / 10,
    };
  }

  /**
   * Get request trends over time
   */
  async getRequestTrends(days: number = 30): Promise<
    Array<{
      date: string;
      requestCount: number;
      avgDuration: number;
      hourlyCount: number;
      dailyCount: number;
    }>
  > {
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    const logs = await this.requestLogRepository
      .createQueryBuilder("log")
      .select("DATE(log.createdAt)", "date")
      .addSelect("COUNT(*)", "requestCount")
      .addSelect("AVG(log.durationMs)", "avgDuration")
      .addSelect(
        "SUM(CASE WHEN log.predictionType = 'hourly' THEN 1 ELSE 0 END)",
        "hourlyCount",
      )
      .addSelect(
        "SUM(CASE WHEN log.predictionType = 'daily' THEN 1 ELSE 0 END)",
        "dailyCount",
      )
      .where("log.createdAt >= :startDate", { startDate })
      .groupBy("DATE(log.createdAt)")
      .orderBy("DATE(log.createdAt)", "ASC")
      .getRawMany();

    return logs.map((row) => ({
      date: row.date,
      requestCount: parseInt(row.requestCount, 10),
      avgDuration: Math.round(parseFloat(row.avgDuration)),
      hourlyCount: parseInt(row.hourlyCount, 10),
      dailyCount: parseInt(row.dailyCount, 10),
    }));
  }

  /**
   * Cleanup old logs (older than 90 days)
   */
  async cleanupOldLogs(): Promise<number> {
    const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000);
    const result = await this.requestLogRepository.delete({
      createdAt: LessThan(ninetyDaysAgo),
    });
    return result.affected || 0;
  }
}
