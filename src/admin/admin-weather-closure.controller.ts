import {
  Controller,
  Get,
  Post,
  Query,
  UseGuards,
  UseInterceptors,
} from "@nestjs/common";
import { InjectQueue } from "@nestjs/bull";
import { ApiOperation, ApiQuery, ApiSecurity, ApiTags } from "@nestjs/swagger";
import type { Queue } from "bull";
import { AdminAuditInterceptor } from "./auth/admin-audit.interceptor";
import { AdminAuthGuard } from "./auth/admin-auth.guard";
import { AdminMinRole } from "./auth/admin-auth.decorators";
import { NoCdnCacheInterceptor } from "../common/interceptors/no-cdn-cache.interceptor";
import {
  WeatherClosureOverview,
  WeatherClosureService,
} from "../parks/services/weather-closure.service";

/**
 * The per-ride weather-closure profiles in the admin: which rides cold or
 * snow shuts, how close each is to the 90 % bar the public page needs, and
 * the rebuild that is otherwise Monday's cron. docs/frontend/weather-closure.md.
 */
@ApiTags("admin")
@ApiSecurity("admin-auth")
@Controller("admin/weather-closure")
@UseGuards(AdminAuthGuard)
@UseInterceptors(AdminAuditInterceptor, new NoCdnCacheInterceptor())
export class AdminWeatherClosureController {
  constructor(
    private readonly weatherClosure: WeatherClosureService,
    @InjectQueue("weather-closure") private readonly queue: Queue,
  ) {}

  @Get()
  @ApiOperation({
    summary: "Which rides cold or snow shuts, and how sure we are",
    description:
      "Every stored profile with its ride and park, best probability first. " +
      "`publishable` rides are the ones the public page names; `totals.near` " +
      "counts the rest at 0.75 or more. Read-only.",
  })
  @ApiQuery({
    name: "minProbability",
    required: false,
    description:
      "Leave out rides whose best profile is below this (0–1). Default 0.5.",
    example: 0.5,
  })
  async overview(
    @Query("minProbability") minProbability?: string,
  ): Promise<WeatherClosureOverview> {
    const min = Number(minProbability);
    return this.weatherClosure.listProfiles(
      Number.isFinite(min) && min >= 0 && min <= 1 ? min : undefined,
    );
  }

  @Post("rebuild")
  @AdminMinRole("editor")
  @ApiOperation({
    summary: "Relearn every park's weather-closure profiles now",
    description:
      "Normally Mondays 05:45. Reads a year of each park's open hours and " +
      "the Open-Meteo archive; a few minutes for all parks.",
  })
  async rebuild(): Promise<{ message: string; queue: string }> {
    // One attempt: a retry would ask the archive for every park again.
    await this.queue.add(
      "rebuild-weather-closure-profiles",
      {},
      { attempts: 1 },
    );
    return {
      message: "Weather-closure rebuild queued",
      queue: "weather-closure",
    };
  }
}
