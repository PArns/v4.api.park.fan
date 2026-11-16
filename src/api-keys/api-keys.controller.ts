import { Controller, Get, Req, Res, UseInterceptors } from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import {
  ApiOperation,
  ApiQuery,
  ApiResponse,
  ApiSecurity,
  ApiTags,
} from "@nestjs/swagger";
import type { Response } from "express";
import { ApiKeyService } from "./api-key.service";
import {
  checkAllowedPark,
  keyRefusal,
  parkIdResolver,
  readPresentedApiKey,
  type ForecastRequest,
} from "./forecast-access.guard";
import { keepResponsePrivate } from "./private-cache";
import { OpenRoute } from "./api-permission.decorator";
import { HttpCacheInterceptor } from "../common/interceptors/cache.interceptor";
import {
  API_PERMISSION_DESCRIPTIONS,
  API_PERMISSION_GROUPS,
  KEYLESS_PERMISSIONS,
  effectivePermissions,
} from "./permissions";

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
  @OpenRoute("checks exactly the key it is sent")
  @ApiSecurity("api-key")
  @ApiOperation({
    summary: "Check a data API key",
    description:
      "200 when the key in `X-API-Key` is active, 401 otherwise (`api-key-expired` past its end date). With `park`, 403 when the key does not cover that park. `allParks` says whether the key covers every park; otherwise `allowedParkIds` lists them. `permissions` lists what the key may read (`GET /v1/api-keys/permissions`). `expiresAt` is the end date, absent for a key that does not expire.",
  })
  @ApiQuery({
    name: "park",
    required: false,
    description:
      "A park as `continent/country/city/park`, e.g. `europe/germany/rust/europa-park`.",
  })
  @ApiResponse({
    status: 200,
    description:
      "`{ valid: true, usage, allParks, allowedParkIds, permissions, expiresAt? }`",
  })
  async verify(
    @Req() request: ForecastRequest,
    @Res({ passthrough: true }) response: Response,
  ) {
    keepResponsePrivate(response);
    const presented = readPresentedApiKey(request.headers);
    const found = presented ? await this.apiKeys.lookup(presented) : null;
    if (found?.status !== "active") {
      throw keyRefusal(found, "Send the key to check in the X-API-Key header.");
    }
    const key = found.key;
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
      permissions: [...effectivePermissions(key)].filter(
        (permission) => permission !== "internal",
      ),
      ...(key.expiresAt && { expiresAt: key.expiresAt }),
    };
  }

  @Get("permissions")
  @OpenRoute("the catalog of permissions a key can carry")
  @UseInterceptors(new HttpCacheInterceptor(3600))
  @ApiOperation({
    summary: "What a data key can be allowed to read",
    description:
      "The permissions in their groups, what each opens, and `keyless`: what a caller without a key may read. Request a key with the permissions a project needs at https://park.fan/en/developers.",
  })
  permissions() {
    return {
      keyless: KEYLESS_PERMISSIONS,
      groups: API_PERMISSION_GROUPS.map((group) => ({
        id: group.id,
        label: group.label,
        permissions: group.permissions.map((id) => ({
          id,
          description: API_PERMISSION_DESCRIPTIONS[id],
        })),
      })),
    };
  }
}
