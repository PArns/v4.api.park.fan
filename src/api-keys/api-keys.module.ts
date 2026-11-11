import { Global, Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { RedisModule } from "../common/redis/redis.module";
import { AdminApiKeysController } from "./admin-api-keys.controller";
import { ApiKeyService } from "./api-key.service";
import { ApiKey } from "./entities/api-key.entity";
import { ForecastAccessGuard } from "./forecast-access.guard";
import { ForecastFieldsInterceptor } from "./forecast-fields.interceptor";
import { ApiKeysController } from "./api-keys.controller";

/**
 * Data API keys: the table, the admin endpoints and the guard every forecast
 * route carries. Global so a controller anywhere can put `@RequiresApiKey()`
 * on a route without importing this module.
 */
@Global()
@Module({
  imports: [TypeOrmModule.forFeature([ApiKey]), RedisModule],
  controllers: [AdminApiKeysController, ApiKeysController],
  providers: [ApiKeyService, ForecastAccessGuard, ForecastFieldsInterceptor],
  exports: [ApiKeyService, ForecastAccessGuard, ForecastFieldsInterceptor],
})
export class ApiKeysModule {}
