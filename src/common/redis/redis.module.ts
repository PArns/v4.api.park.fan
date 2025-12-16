import { Module, Global } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import Redis from "ioredis";

export const REDIS_CLIENT = "REDIS_CLIENT";

@Global()
@Module({
  providers: [
    {
      provide: REDIS_CLIENT,
      useFactory: (configService: ConfigService) => {
        return new Redis({
          host: configService.get<string>("REDIS_HOST") || "localhost",
          port: parseInt(configService.get<string>("REDIS_PORT") || "6379", 10),
          password: configService.get<string>("REDIS_PASSWORD") || undefined,
          // Performance optimizations
          enableReadyCheck: true,
          maxRetriesPerRequest: 3,
          enableOfflineQueue: false, // Fail fast if Redis is down
          lazyConnect: false, // Connect immediately on startup
          connectTimeout: 10000, // 10 seconds
          retryStrategy: (times: number) => {
            // Exponential backoff: 50ms, 100ms, 200ms, ..., max 2s
            return Math.min(times * 50, 2000);
          },
        });
      },
      inject: [ConfigService],
    },
  ],
  exports: [REDIS_CLIENT],
})
export class RedisModule {}
