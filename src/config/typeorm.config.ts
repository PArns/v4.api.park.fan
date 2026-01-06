import { TypeOrmModuleAsyncOptions } from "@nestjs/typeorm";
import { ConfigModule, ConfigService } from "@nestjs/config";
import { getDatabaseConfig } from "./database.config";

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
      timezone: "UTC", // Always use UTC
      extra: {
        max: 20, // Connection pool size
        // During build, use very short timeout to fail fast
        connectionTimeoutMillis: isBuildTime ? 100 : 2000,
      },
      // During build, don't retry connections
      retryAttempts: isBuildTime ? 0 : 3,
      retryDelay: isBuildTime ? 0 : 3000,
    };
  },
};
