import { applyDecorators, UseGuards } from "@nestjs/common";
import { ApiResponse, ApiSecurity } from "@nestjs/swagger";
import { DATA_TERMS_URL } from "./api-key.constants";
import { ForecastAccessGuard } from "./forecast-access.guard";

/**
 * Puts a route behind a data key: the guard (which also keeps the answer out of
 * shared caches) and the Swagger security entry. Every route whose answer is a
 * forecast carries it; a route that only has forecast fields uses `ForecastFields`.
 */
export const RequiresApiKey = (): MethodDecorator & ClassDecorator =>
  applyDecorators(
    UseGuards(ForecastAccessGuard),
    ApiSecurity("api-key"),
    ApiResponse({
      status: 403,
      description:
        "`api-key-park-not-allowed` or `api-key-park-required`: the key is limited to other parks.",
    }),
    ApiResponse({
      status: 401,
      description: `\`api-key-required\` or \`api-key-invalid\`. Forecasts need a data key in \`X-API-Key\`; terms: ${DATA_TERMS_URL}`,
    }),
  );
