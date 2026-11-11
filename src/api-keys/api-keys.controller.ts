import {
  Controller,
  Get,
  Req,
  Res,
  UnauthorizedException,
} from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import {
  ApiOperation,
  ApiQuery,
  ApiResponse,
  ApiSecurity,
  ApiTags,
} from "@nestjs/swagger";
import type { Response } from "express";
import { DATA_TERMS_URL } from "./api-key.constants";
import { ApiKeyService } from "./api-key.service";
import {
  checkAllowedPark,
  parkIdResolver,
  readPresentedApiKey,
  type ForecastRequest,
} from "./forecast-access.guard";
import { keepResponsePrivate } from "./private-cache";

/**
 * Lets a holder, or park.fan's own MCP server on a holder's behalf, check a
 * data key without fetching a forecast for it.
 *
 * Not behind `ForecastAccessGuard`: that guard lets park.fan's own frontend
 * through on its bypass key, and here that would confirm any value at all. It
 * checks exactly the key presented, and counts nothing against its allowance.
 */
@ApiTags("api-keys")
@Controller("api-keys")
export class ApiKeysController {
  constructor(
    private readonly apiKeys: ApiKeyService,
    private readonly moduleRef: ModuleRef,
  ) {}

  @Get("verify")
  @ApiSecurity("api-key")
  @ApiOperation({
    summary: "Check a data API key",
    description:
      "200 when the key in `X-API-Key` is active, 401 otherwise. With `park`, 403 when the key does not cover that park. `allParks` says whether the key covers every park; otherwise `allowedParkIds` lists them.",
  })
  @ApiQuery({
    name: "park",
    required: false,
    description:
      "A park as `continent/country/city/park`, e.g. `europe/germany/rust/europa-park`.",
  })
  @ApiResponse({
    status: 200,
    description: "`{ valid: true, usage, allParks, allowedParkIds }`",
  })
  async verify(
    @Req() request: ForecastRequest,
    @Res({ passthrough: true }) response: Response,
  ) {
    keepResponsePrivate(response);
    const presented = readPresentedApiKey(request.headers);
    const key = presented ? await this.apiKeys.resolve(presented) : null;
    if (!key) {
      throw new UnauthorizedException({
        error: presented ? "api-key-invalid" : "api-key-required",
        message: presented
          ? "This API key is unknown or revoked."
          : "Send the key to check in the X-API-Key header.",
        terms: DATA_TERMS_URL,
      });
    }
    await checkAllowedPark(key, request, {
      verify: true,
      resolveParkId: parkIdResolver(this.moduleRef),
    });
    // Explicit rather than null for "every park": the API strips null fields.
    return {
      valid: true,
      usage: key.usage,
      allParks: key.allowedParkIds === null,
      allowedParkIds: key.allowedParkIds ?? [],
    };
  }
}
