import { applyDecorators, UseGuards, UseInterceptors } from "@nestjs/common";
import { ApiResponse, ApiSecurity } from "@nestjs/swagger";
import { DATA_TERMS_URL } from "./api-key.constants";
import { ForecastAccessGuard } from "./forecast-access.guard";
import { PrivateCacheInterceptor } from "./private-cache.interceptor";

/**
 * Puts a route behind a data key: the guard, the private `Cache-Control`, and
 * the Swagger security entry. Every route that returns a forecast carries it.
 */
export const RequiresApiKey = (): MethodDecorator & ClassDecorator =>
  applyDecorators(
    UseGuards(ForecastAccessGuard),
    UseInterceptors(PrivateCacheInterceptor),
    ApiSecurity("api-key"),
    ApiResponse({
      status: 401,
      description: `\`api-key-required\` or \`api-key-invalid\`. Forecasts need a data key in \`X-API-Key\`; terms: ${DATA_TERMS_URL}`,
    }),
  );
