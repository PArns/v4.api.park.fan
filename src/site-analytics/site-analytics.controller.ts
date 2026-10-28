import {
  BadRequestException,
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  PayloadTooLargeException,
  Post,
  Query,
  Req,
  UseGuards,
  UseInterceptors,
} from "@nestjs/common";
import {
  ApiBody,
  ApiConsumes,
  ApiOperation,
  ApiParam,
  ApiResponse,
  ApiSecurity,
  ApiTags,
} from "@nestjs/swagger";
import { Request } from "express";
import { AdminAuthGuard } from "../admin/auth/admin-auth.guard";
import {
  DEFAULT_PRODUCTION_CORS_ORIGINS,
  isAllowedCorsOrigin,
} from "../common/cors/cors.config";
import { NoCdnCacheInterceptor } from "../common/interceptors/no-cdn-cache.interceptor";
import { getRateLimitIp } from "../common/utils/request.util";
import { SITE_EVENT_NAMES } from "./site-analytics.schema";
import { parseCollectBody } from "./site-analytics.hits";
import { SiteAnalyticsCollectService } from "./site-analytics-collect.service";
import { SiteAnalyticsService } from "./site-analytics.service";
import { TEXT_BODY_TOO_LARGE } from "./text-body.middleware";
import {
  CollectBodyDto,
  SiteAnalyticsBreakdownQueryDto,
  SiteAnalyticsQueryDto,
  SiteBreakdownDto,
  SiteEventDetailDto,
  SiteEventsDto,
  SiteOverviewDto,
  SiteSearchDto,
} from "./dto/site-analytics.dto";

/**
 * Whether a hit comes from the site itself. In production that is a request
 * whose `Origin` (or, should a browser leave it out, whose `Referer`) is
 * park.fan: a preview, a local build or a script without one is not counted.
 * Every browser sends `Origin` on a POST, `sendBeacon` included.
 */
export function isFromSite(request: Request, isProduction: boolean): boolean {
  if (!isProduction) return true;
  const origin = request.get("origin");
  if (origin)
    return isAllowedCorsOrigin(origin, DEFAULT_PRODUCTION_CORS_ORIGINS);
  const referer = request.get("referer") ?? "";
  return DEFAULT_PRODUCTION_CORS_ORIGINS.some((site) =>
    referer.startsWith(`${site}/`),
  );
}

/**
 * The browser's write. Sent straight to api.park.fan, never through the
 * frontend's relay, so the address and User-Agent are the reader's own; both
 * are used for the cap, the visit and the country, and neither is stored.
 *
 * 204 whenever the body parsed, counted or not: a caller that could tell a
 * counted hit from a dropped one could probe the schema, the cap and the bot
 * rule, and the beacon does nothing with the answer anyway.
 */
@ApiTags("site-analytics")
@Controller("analytics")
export class SiteAnalyticsCollectController {
  constructor(private readonly collector: SiteAnalyticsCollectService) {}

  @Post("collect")
  @HttpCode(HttpStatus.NO_CONTENT)
  @UseInterceptors(new NoCdnCacheInterceptor())
  @ApiConsumes("text/plain", "application/json")
  @ApiBody({ type: CollectBodyDto })
  @ApiOperation({
    summary: "Count page views and events of the site statistic",
    description:
      "JSON as `text/plain` (what `navigator.sendBeacon` sends, so no preflight) " +
      "or `application/json`: `{ v: 1, hits: [...] }` with 1–20 hits. Each hit is " +
      "checked against the closed schema and dropped alone if it does not fit. " +
      "Bots, `DNT: 1` and `Sec-GPC: 1` are not counted; one address counts at " +
      "most 3000 hits a UTC day. Always 204 once the body parsed.",
  })
  @ApiResponse({ status: 204 })
  @ApiResponse({
    status: 400,
    description: "Not JSON, or not `{ v: 1, hits: [1..20] }`.",
  })
  @ApiResponse({ status: 413, description: "A body over 16 KiB." })
  async collect(@Body() body: unknown, @Req() request: Request): Promise<void> {
    if (
      (request as Request & { [TEXT_BODY_TOO_LARGE]?: true })[
        TEXT_BODY_TOO_LARGE
      ]
    ) {
      throw new PayloadTooLargeException();
    }
    const hits = parseCollectBody(body);
    if (!hits) {
      throw new BadRequestException(
        "Expected JSON { v: 1, hits: [...] } with 1 to 20 hits",
      );
    }
    if (request.get("dnt") === "1" || request.get("sec-gpc") === "1") return;
    if (!isFromSite(request, process.env.NODE_ENV === "production")) return;
    await this.collector.collect({
      hits,
      ip: getRateLimitIp(request),
      userAgent: request.get("user-agent") ?? null,
    });
  }
}

/**
 * The admin's site statistic: traffic, the dimensions behind it, events and
 * the search report. UTC days, today included.
 */
@ApiTags("admin")
@ApiSecurity("admin-auth")
@Controller("admin/analytics")
@UseGuards(AdminAuthGuard)
export class AdminSiteAnalyticsController {
  constructor(private readonly analytics: SiteAnalyticsService) {}

  @Get("overview")
  @ApiOperation({
    summary:
      "Page views, visits, visitors and events, with the window before and realtime",
  })
  @ApiResponse({ status: 200, type: SiteOverviewDto })
  overview(@Query() query: SiteAnalyticsQueryDto): Promise<SiteOverviewDto> {
    return this.analytics.overview(query);
  }

  @Get("breakdown")
  @ApiOperation({
    summary: "Views and visits per value of one dimension",
    description:
      "`dim`: path, page_type, entry, referrer, viewport, browser, os, country, " +
      "locale. For entry and referrer a view is a visit started.",
  })
  @ApiResponse({ status: 200, type: SiteBreakdownDto })
  breakdown(
    @Query() query: SiteAnalyticsBreakdownQueryDto,
  ): Promise<SiteBreakdownDto> {
    return this.analytics.breakdown(query);
  }

  @Get("events")
  @ApiOperation({
    summary:
      "Every event of the schema with its count, the previous window's and a series",
  })
  @ApiResponse({ status: 200, type: SiteEventsDto })
  events(@Query() query: SiteAnalyticsQueryDto): Promise<SiteEventsDto> {
    return this.analytics.events(query);
  }

  @Get("events/:name")
  @ApiParam({ name: "name", enum: SITE_EVENT_NAMES })
  @ApiOperation({
    summary: "One event by property, page type, viewport, entity and day",
  })
  @ApiResponse({ status: 200, type: SiteEventDetailDto })
  @ApiResponse({ status: 404, description: "No such event in the schema." })
  event(
    @Param("name") name: string,
    @Query() query: SiteAnalyticsQueryDto,
  ): Promise<SiteEventDetailDto> {
    return this.analytics.event(name, query);
  }

  @Get("search")
  @ApiOperation({
    summary: "Which search is used, from where, and how well",
  })
  @ApiResponse({ status: 200, type: SiteSearchDto })
  search(@Query() query: SiteAnalyticsQueryDto): Promise<SiteSearchDto> {
    return this.analytics.search(query);
  }
}
