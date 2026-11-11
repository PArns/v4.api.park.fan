import { Controller, Get, Req } from "@nestjs/common";
import { ApiOperation, ApiResponse, ApiTags } from "@nestjs/swagger";
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
  @RequiresApiKey()
  @ApiOperation({
    summary: "Check a data API key",
    description:
      "200 with the key's usage when the key in `X-API-Key` is active, 401 otherwise.",
  })
  @ApiResponse({ status: 200, description: "`{ valid: true, usage }`" })
  verify(@Req() request: ForecastRequest) {
    return { valid: true, usage: request.apiKey?.usage ?? null };
  }
}
