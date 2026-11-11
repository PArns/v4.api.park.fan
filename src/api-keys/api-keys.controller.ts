import { Controller, Get, Req } from "@nestjs/common";
import { ApiOperation, ApiQuery, ApiResponse, ApiTags } from "@nestjs/swagger";
import type { ForecastRequest } from "./forecast-access.guard";
import { RequiresApiKey } from "./requires-api-key.decorator";

/**
 * Lets a holder, or park.fan's own MCP server on a holder's behalf, check a
 * data key without fetching a forecast for it.
 */
@ApiTags("api-keys")
@Controller("api-keys")
export class ApiKeysController {
  @Get("verify")
  @RequiresApiKey({ anyPark: true })
  @ApiOperation({
    summary: "Check a data API key",
    description:
      "200 with the key's usage and parks when the key in `X-API-Key` is active, 401 otherwise. With `park`, 403 when the key does not cover that park.",
  })
  @ApiQuery({
    name: "park",
    required: false,
    description:
      "A park as `continent/country/city/park`, e.g. `europe/germany/rust/europa-park`.",
  })
  @ApiResponse({
    status: 200,
    description: "`{ valid: true, usage, allowedParkIds }` (null: every park)",
  })
  verify(@Req() request: ForecastRequest) {
    return {
      valid: true,
      usage: request.apiKey?.usage ?? null,
      allowedParkIds: request.apiKey?.allowedParkIds ?? null,
    };
  }
}
