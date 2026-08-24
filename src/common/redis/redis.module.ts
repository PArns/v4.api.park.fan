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
        if (process.env.SKIP_REDIS === "true") {
          // A stub that answers "nothing cached" to everything, rather than a
          // partial one that throws on the first command nobody thought of.
          // It used to carry five methods, which was enough for the read paths
          // that existed when it was written and a TypeError for the admin
          // session store, whose whole job is del/sadd/srem/smembers/expire —
          // so a build with SKIP_REDIS could boot and then fail every login.
          // Everything here returns the empty answer for its type: no key, no
          // members, no TTL. Callers already treat a miss as normal.
          return {
            on: () => {},
            get: () => Promise.resolve(null),
            mget: (...keys: string[]) => Promise.resolve(keys.map(() => null)),
            set: () => Promise.resolve("OK"),
            setex: () => Promise.resolve("OK"),
            del: () => Promise.resolve(0),
            keys: () => Promise.resolve([]),
            incr: () => Promise.resolve(1),
            expire: () => Promise.resolve(0),
            ttl: () => Promise.resolve(-2),
            sadd: () => Promise.resolve(0),
            srem: () => Promise.resolve(0),
            smembers: () => Promise.resolve([]),
            pipeline: () => ({
              set: function () {
                return this;
              },
              del: function () {
                return this;
              },
              exec: () => Promise.resolve([]),
            }),
            quit: () => Promise.resolve("OK"),
            disconnect: () => Promise.resolve("OK"),
            readonly: true,
          };
        }
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
