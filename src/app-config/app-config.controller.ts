import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Header,
  Param,
  Put,
  Query,
} from "@nestjs/common";
import { ApiOperation, ApiTags } from "@nestjs/swagger";
import { OpenRoute } from "../api-keys/api-permission.decorator";
import { AdminApi } from "../admin/auth/admin-api.decorator";
import {
  AdminMinRole,
  CurrentAdmin,
} from "../admin/auth/admin-auth.decorators";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import {
  AppConfigService,
  PLATFORMS,
  type Platform,
  type AppConfigAdminView,
  type AppPublicConfig,
} from "./app-config.service";

/**
 * What the app asks before it starts. Open on purpose: an old app whose data key
 * was revoked or never sent must still learn that it has to update.
 */
@ApiTags("app")
@OpenRoute(
  "The app's start check; an old app with a revoked key must still read it",
)
@Controller("app")
export class AppConfigController {
  constructor(private readonly config: AppConfigService) {}

  @Get("config")
  @Header("Cache-Control", "public, max-age=300")
  @ApiOperation({
    summary:
      "The minimum app version per platform (`ios`, `android`) and where to update",
    description:
      "Per platform block: `minVersion`/`minBuild` (iOS build, Android versionCode) null means no minimum; the app is too old when its version is " +
      "below `minVersion`, or equal with a build below `minBuild`. Never a 5xx for missing settings.",
  })
  get(): Promise<AppPublicConfig> {
    return this.config.publicConfig();
  }
}

function sample(version?: string, build?: string) {
  const n = build ? Number(build) : undefined;
  return {
    version: version?.match(/^\d+(\.\d+){0,2}$/) ? version : undefined,
    build: n !== undefined && Number.isInteger(n) && n > 0 ? n : undefined,
  };
}

/** "App-Version" in the admin: reading for every account, writing for owners. */
@AdminApi("admin/app-config")
export class AdminAppConfigController {
  constructor(private readonly config: AppConfigService) {}

  @Get()
  @ApiOperation({
    summary:
      "Every platform's minimum version; `samples` says what an app of `?version=&build=` would meet",
  })
  get(
    @Query("version") version?: string,
    @Query("build") build?: string,
  ): Promise<AppConfigAdminView> {
    return this.config.view(sample(version, build));
  }

  @Put(":platform")
  @AdminMinRole("owner")
  @ApiOperation({
    summary:
      "Replace one platform's minimum version, update link and messages (null clears)",
    description:
      "`:platform` is `ios` or `android`. Everyone on an older version is locked out until they update. " +
      "Body: `{ minVersion, minBuild, updateUrl, message }`, all four present.",
  })
  set(
    @Param("platform") platform: string,
    @Body() body: unknown,
    @CurrentAdmin() admin: AdminPrincipal,
    @Query("version") version?: string,
    @Query("build") build?: string,
  ): Promise<AppConfigAdminView> {
    if (!(PLATFORMS as readonly string[]).includes(platform)) {
      throw new BadRequestException(
        `platform must be one of ${PLATFORMS.join(", ")}`,
      );
    }
    return this.config.update(
      platform as Platform,
      body,
      admin.email,
      sample(version, build),
    );
  }
}
