import {
  ConflictException,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Inject,
  Post,
  Query,
  UseGuards,
  UseInterceptors,
} from "@nestjs/common";
import { InjectQueue } from "@nestjs/bull";
import {
  ApiOperation,
  ApiResponse,
  ApiSecurity,
  ApiTags,
} from "@nestjs/swagger";
import type { Queue } from "bull";
import type Redis from "ioredis";
import { AdminAuditInterceptor } from "../admin/auth/admin-audit.interceptor";
import { AdminMinRole } from "../admin/auth/admin-auth.decorators";
import { AdminAuthGuard } from "../admin/auth/admin-auth.guard";
import { NoCdnCacheInterceptor } from "../common/interceptors/no-cdn-cache.interceptor";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { getSearchConsoleSetup } from "../config/search-console.config";
import {
  SearchConsolePagesDto,
  SearchConsolePagesQueryDto,
  SearchConsoleQueriesDto,
  SearchConsoleQueriesQueryDto,
  SearchConsoleSeriesDto,
  SearchConsoleSeriesQueryDto,
  SearchConsoleStatusDto,
} from "./dto/search-console.dto";
import {
  IMPORT_REQUEST_COOLDOWN_SECONDS,
  IMPORT_REQUEST_KEY,
} from "./search-console.constants";
import { SearchConsoleService } from "./search-console.service";

/**
 * Google Search Console in the admin: how the site is found in Google's
 * results (queries, pages, a series), joined with the site statistic's own
 * numbers, and the import that fills it. Every report takes the site
 * statistic's window (`days`, or `from` and `to`) and `locale`; they answer
 * `private, no-store` like all admin routes. docs/frontend/search-console.md.
 */
@ApiTags("admin")
@ApiSecurity("admin-auth")
@Controller("admin/search-console")
@UseGuards(AdminAuthGuard)
@UseInterceptors(AdminAuditInterceptor, new NoCdnCacheInterceptor())
export class AdminSearchConsoleController {
  constructor(
    private readonly reports: SearchConsoleService,
    @InjectQueue("search-console") private readonly queue: Queue,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  @Get("status")
  @ApiOperation({
    summary: "Whether Search Console is connected, and what the import has",
    description:
      "`configured` is false without `GSC_SERVICE_ACCOUNT_JSON` and `GSC_SITE_URL` " +
      "(or with ones that do not parse; `lastError` says which). `coverage` is the " +
      "first and last day with data, `lastImportAt` the end of the last successful run.",
  })
  @ApiResponse({ status: 200, type: SearchConsoleStatusDto })
  status(): Promise<SearchConsoleStatusDto> {
    return this.reports.status();
  }

  @Post("import")
  @AdminMinRole("editor")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Import from Search Console now",
    description:
      "Queues the nightly import: the last days again and any day never fetched. " +
      "Returns at once; the status shows the outcome. Audited. A second request " +
      "within a minute queues nothing more.",
  })
  @ApiResponse({ status: 202, description: "`{ queued: true }`" })
  @ApiResponse({
    status: 409,
    description: "Search Console is not configured.",
  })
  async importNow(): Promise<{ queued: true }> {
    if (!getSearchConsoleSetup().configured) {
      throw new ConflictException(
        "Search Console is not configured: set GSC_SERVICE_ACCOUNT_JSON and GSC_SITE_URL",
      );
    }
    let fresh = true;
    try {
      fresh =
        (await this.redis.set(
          IMPORT_REQUEST_KEY,
          "1",
          "EX",
          IMPORT_REQUEST_COOLDOWN_SECONDS,
          "NX",
        )) === "OK";
    } catch {
      // Redis away: queue it; the import's own lock keeps two runs apart.
    }
    if (fresh) {
      await this.queue.add(
        "import",
        { trigger: "manual" },
        { removeOnComplete: true, removeOnFail: 20 },
      );
    }
    return { queued: true };
  }

  @Get("queries")
  @ApiOperation({
    summary: "The queries the site was found with",
    description:
      "Most clicks first, with the previous window's change. `page`, `country` " +
      "(alpha-3) and `device` narrow it. Google leaves queries it anonymises out, " +
      "so the totals are those of the queries listed, below the pages' and the series'.",
  })
  @ApiResponse({ status: 200, type: SearchConsoleQueriesDto })
  queries(
    @Query() query: SearchConsoleQueriesQueryDto,
  ): Promise<SearchConsoleQueriesDto> {
    return this.reports.queries(query);
  }

  @Get("pages")
  @ApiOperation({
    summary: "The pages the site was found on, with the site's own numbers",
    description:
      "Most clicks first. `site` is the site statistic's visits from the `search` " +
      "channel that entered on the page in the same window, with bounce rate and " +
      "average visit. `query` shows the pages one query brought.",
  })
  @ApiResponse({ status: 200, type: SearchConsolePagesDto })
  pages(
    @Query() query: SearchConsolePagesQueryDto,
  ): Promise<SearchConsolePagesDto> {
    return this.reports.pages(query);
  }

  @Get("series")
  @ApiOperation({
    summary: "Clicks, impressions, click-through rate and position over time",
    description:
      "Per `interval` (day, week, month) from the window's first day to the last " +
      "with data. `page`, `query`, `country` and `device` narrow it.",
  })
  @ApiResponse({ status: 200, type: SearchConsoleSeriesDto })
  series(
    @Query() query: SearchConsoleSeriesQueryDto,
  ): Promise<SearchConsoleSeriesDto> {
    return this.reports.series(query);
  }
}
