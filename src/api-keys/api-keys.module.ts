import { Global, Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { RedisModule } from "../common/redis/redis.module";
import { AdminApiKeysController } from "./admin-api-keys.controller";
import { ApiKeyService } from "./api-key.service";
import { ApiKey } from "./entities/api-key.entity";
import { ApiKeyRequest } from "./entities/api-key-request.entity";
import { ApiKeyRequestService } from "./api-key-request.service";
import { AdminApiKeyRequestsController } from "./admin-api-key-requests.controller";
import { DeveloperController } from "./developer.controller";
import { ForecastAccessGuard } from "./forecast-access.guard";
import { ForecastFieldsInterceptor } from "./forecast-fields.interceptor";
import { PermissionFieldsInterceptor } from "./permission-fields.interceptor";
import { ApiKeysController } from "./api-keys.controller";

/**
 * Data API keys: the table, the admin endpoints and the guard every public data
 * route carries. Global so a controller anywhere can put `@ApiPermission(…)`
 * on a route without importing this module.
 */
@Global()
@Module({
  imports: [TypeOrmModule.forFeature([ApiKey, ApiKeyRequest]), RedisModule],
  controllers: [
    AdminApiKeysController,
    AdminApiKeyRequestsController,
    ApiKeysController,
    DeveloperController,
  ],
  providers: [
    ApiKeyService,
    ApiKeyRequestService,
    ForecastAccessGuard,
    ForecastFieldsInterceptor,
    PermissionFieldsInterceptor,
  ],
  exports: [
    ApiKeyService,
    ForecastAccessGuard,
    ForecastFieldsInterceptor,
    PermissionFieldsInterceptor,
  ],
})
export class ApiKeysModule {}
