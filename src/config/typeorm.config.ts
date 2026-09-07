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
        // work_mem is now 16 MB and max_connections is 200 — count the python
        // services' own pools before raising further.
        //
        // Raised again 50 -> 80 on 2026-09-05. Bounding the calendar fan-out
        // removed the single largest stall (43 identical queries in one
        // second, gone), but the pool still ran at 50/50: what fills it now is
        // many small unbounded fan-outs at once — park statistics, schedule
        // and attraction reads, a per-attraction downtime query — rather than
        // one culprit worth chasing. Postgres never logs these as slow, so the
        // time is queueing, and the machine is idle while it happens (load
        // 0.61 on 24 cores, 12 GB RAM free). Worst case at 80: 80 + 31
        // (ml-service) + 24 (pcn peak) + 10 = 145 of 197 usable connections.
        max: parseInt(process.env.DB_POOL_SIZE ?? "80", 10),
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
