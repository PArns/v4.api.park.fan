import { TypeOrmModuleAsyncOptions } from "@nestjs/typeorm";
import { ConfigModule, ConfigService } from "@nestjs/config";
import { getDatabaseConfig } from "./database.config";
import { SlowQueryFileLogger } from "../common/utils/typeorm-slow-query-logger";

export const typeOrmConfig: TypeOrmModuleAsyncOptions = {
  imports: [ConfigModule],
  inject: [ConfigService],
  useFactory: () => {
    const dbConfig = getDatabaseConfig();
    const isBuildTime = process.env.NODE_ENV === "build";

    return {
      type: "postgres" as const,
      host: dbConfig.host,
      port: dbConfig.port,
      username: dbConfig.username,
      password: dbConfig.password,
      database: dbConfig.database,
      entities: [__dirname + "/../**/*.entity{.ts,.js}"],
      synchronize: dbConfig.synchronize, // Auto-sync schema (dev only!)
      logging: dbConfig.logging,
      logger: new SlowQueryFileLogger(),
      maxQueryExecutionTime: 500, // Triggers logQuerySlow → logs/slow-queries.log
      timezone: "UTC", // Always use UTC
      extra: {
        // Connection pool size. Raised 30 -> 50 after measuring that a third
        // of all logged "slow query" time was queueing for a connection, not
        // executing: bursts of exactly 30 queries finishing within a few ms of
        // each other, a primary-key lookup among them at 4.2 s that postgres
        // itself ran in 0.05 ms. Throttling the background jobs
        // (DB_JOB_CONCURRENCY) removed 71 % of that; the rest is read-side
        // work that cannot be deferred, so it needs slots. Safe because
        // work_mem is now 16 MB (worst case ~4.8 GB) and max_connections is
        // 150 — count the python services' own pools before raising further.
        max: parseInt(process.env.DB_POOL_SIZE ?? "50", 10),
        // Reap idle clients so an off-peak pool doesn't pin connections.
        idleTimeoutMillis: 30000,
        // During build, use very short timeout to fail fast
        connectionTimeoutMillis: isBuildTime ? 100 : 15000,
      },
      // During build, don't retry connections
      retryAttempts: isBuildTime ? 0 : 3,
      retryDelay: isBuildTime ? 0 : 3000,
    };
  },
};
