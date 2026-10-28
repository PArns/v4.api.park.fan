import {
  BadRequestException,
  Body,
  Controller,
  Delete,
  Get,
  Header,
  HttpCode,
  HttpStatus,
  Param,
  PayloadTooLargeException,
  Post,
  Put,
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
import { AdminAuditInterceptor } from "../admin/auth/admin-audit.interceptor";
import {
  AdminMinRole,
  CurrentAdmin,
} from "../admin/auth/admin-auth.decorators";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import { NoCdnCacheInterceptor } from "../common/interceptors/no-cdn-cache.interceptor";
import { getRateLimitIp } from "../common/utils/request.util";
import { EVENT_NAME_PATTERN, parseCollectBody } from "./site-analytics.hits";
import { SiteAnalyticsCollectService } from "./site-analytics-collect.service";
import { SiteAnalyticsService } from "./site-analytics.service";
import { SiteAnalyticsSeoService } from "./site-analytics-seo.service";
import { SiteAnalyticsBlocklist } from "./site-analytics-blocklist.service";
import { SiteAnalyticsLiveService } from "./site-analytics-live.service";
import { BODY_TOO_LARGE } from "./collect-body.middleware";
import { BEACON_RESOURCE_POLICY, isFromSite } from "./from-site.util";
import {
  CollectBodyDto,
  SiteAnalyticsBreakdownQueryDto,
  SiteAnalyticsQueryDto,
  SiteBlockedEventsDto,
  SiteBreakdownDto,
  SiteCrosstabDto,
  SiteCrosstabQueryDto,
  SiteEventDetailDto,
  SiteEventsDto,
  SiteLandingPagesDto,
  SiteLandingPagesQueryDto,
  SiteLiveDto,
  SiteLiveQueryDto,
  SiteNotFoundDto,
  SiteNotFoundQueryDto,
  SiteOverviewDto,
  SiteSearchDto,
  SiteTrafficCrosstabDto,
  SiteTrafficCrosstabQueryDto,
  SiteTrendsDto,
  SiteTrendsQueryDto,
  SiteUncoveredDto,
  SiteUncoveredQueryDto,
} from "./dto/site-analytics.dto";

/** What every report answers when it is bounded; the body's `reason` says which. */
const REPORT_BUSY =
  "A statement ran past its time limit (`reason: timeout`), or too many reports are running at once (`reason: busy`); the message says what to try.";

/** An event name from the path, or 400. */
function eventName(name: string): string {
  if (!EVENT_NAME_PATTERN.test(name)) {
    throw new BadRequestException("Not an event name");
  }
  return name;
}

/**
 * The browser's write. Sent straight to api.park.fan, never through the
 * frontend's relay, so the address and User-Agent are the reader's own; both
 * are used for the cap, the visit and the country, and neither is stored.
 *
 * 204 whenever the body parsed, counted or not: a caller that could tell a
 * counted hit from a dropped one could probe the guards and the bot rule, and
 * the beacon does nothing with the answer anyway.
 */
@ApiTags("site-analytics")
@Controller("analytics")
export class SiteAnalyticsCollectController {
  constructor(private readonly collector: SiteAnalyticsCollectService) {}

  @Post("collect")
  @HttpCode(HttpStatus.NO_CONTENT)
  @Header("Cross-Origin-Resource-Policy", BEACON_RESOURCE_POLICY)
  @UseInterceptors(new NoCdnCacheInterceptor())
  @ApiConsumes("text/plain", "application/json")
  @ApiBody({ type: CollectBodyDto })
  @ApiOperation({
    summary: "Count page views and events of the site statistic",
    description:
      "JSON as `text/plain` (what `navigator.sendBeacon` sends, so no preflight) " +
      "or `application/json`: `{ v: 1, hits: [...] }` with 1–20 page views, " +
      "events and engagements (foreground seconds of a page view). Each hit is " +
      "checked field by field and dropped alone if it does not fit. Bots, " +
      "`DNT: 1` and `Sec-GPC: 1` are not counted; one address counts at most " +
      "3000 hits a UTC day. Always 204 once the body parsed.",
  })
  @ApiResponse({ status: 204 })
  @ApiResponse({
    status: 400,
    description: "Not JSON, or not `{ v: 1, hits: [1..20] }`.",
  })
  @ApiResponse({ status: 413, description: "A body over 16 KiB." })
  async collect(@Body() body: unknown, @Req() request: Request): Promise<void> {
    if ((request as Request & { [BODY_TOO_LARGE]?: true })[BODY_TOO_LARGE]) {
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
      acceptLanguage: request.get("accept-language") ?? null,
    });
  }
}

/**
 * The admin's site statistic: traffic and its dimensions, events, the search
 * report, the SEO view, the live view and the event block list. Every report
 * takes a window of UTC days (`days`, or `from` and `to`), `interval`,
 * `locale` and `filter=<dim>:<value>`.
 */
@ApiTags("admin")
@ApiSecurity("admin-auth")
@Controller("admin/analytics")
@UseGuards(AdminAuthGuard)
@UseInterceptors(AdminAuditInterceptor)
export class AdminSiteAnalyticsController {
  constructor(
    private readonly analytics: SiteAnalyticsService,
    private readonly seo: SiteAnalyticsSeoService,
    private readonly blocklist: SiteAnalyticsBlocklist,
    private readonly liveView: SiteAnalyticsLiveService,
  ) {}

  @Get("overview")
  @ApiOperation({
    summary:
      "Page views, visits, visitors, events, bounce rate and time on site, the window before, a series and realtime",
  })
  @ApiResponse({ status: 200, type: SiteOverviewDto })
  @ApiResponse({ status: 503, description: REPORT_BUSY })
  overview(@Query() query: SiteAnalyticsQueryDto): Promise<SiteOverviewDto> {
    return this.analytics.overview(query);
  }

  @Get("breakdown")
  @ApiOperation({
    summary: "Views and visits per value of one dimension",
    description:
      "For entry, referrer and channel a view is a visit started, " +
      "with a bounce rate and the average visit; for exit a visit that ended " +
      "on the page. A page dimension has the average time on the page, path " +
      "also exits and exit rate.",
  })
  @ApiResponse({ status: 200, type: SiteBreakdownDto })
  @ApiResponse({ status: 503, description: REPORT_BUSY })
  breakdown(
    @Query() query: SiteAnalyticsBreakdownQueryDto,
  ): Promise<SiteBreakdownDto> {
    return this.analytics.breakdown(query);
  }

  @Get("events")
  @ApiOperation({
    summary:
      "Every event seen, and every blocked one, with its count, the window before and a series",
  })
  @ApiResponse({ status: 200, type: SiteEventsDto })
  @ApiResponse({ status: 503, description: REPORT_BUSY })
  events(@Query() query: SiteAnalyticsQueryDto): Promise<SiteEventsDto> {
    return this.analytics.events(query);
  }

  @Get("events/:name")
  @ApiParam({ name: "name", example: "search_result_clicked" })
  @ApiOperation({
    summary:
      "One event by property, combination, page type, device, entity and day",
  })
  @ApiResponse({ status: 200, type: SiteEventDetailDto })
  @ApiResponse({ status: 503, description: REPORT_BUSY })
  event(
    @Param("name") name: string,
    @Query() query: SiteAnalyticsQueryDto,
  ): Promise<SiteEventDetailDto> {
    return this.analytics.event(eventName(name), query);
  }

  @Get("events/:name/crosstab")
  @ApiParam({ name: "name", example: "search_opened" })
  @ApiOperation({
    summary: "One event counted by two sides at once",
    description:
      "`row` and `col`: page_type, device, locale, day, path, country, " +
      "browser, os or prop:<key>. 30 values per side by count, the rest " +
      "folded into `__rest__`.",
  })
  @ApiResponse({ status: 200, type: SiteCrosstabDto })
  @ApiResponse({ status: 503, description: REPORT_BUSY })
  crosstab(
    @Param("name") name: string,
    @Query() query: SiteCrosstabQueryDto,
  ): Promise<SiteCrosstabDto> {
    return this.analytics.crosstab(eventName(name), query);
  }

  @Get("crosstab")
  @ApiOperation({
    summary: "Page views or visits counted by two dimensions at once",
    description:
      "metric=pageviews over the page dimensions and the day; metric=visits " +
      "also over entry, referrer, channel and exit. 30 values per " +
      "side by count, the rest folded into `__rest__`.",
  })
  @ApiResponse({ status: 200, type: SiteTrafficCrosstabDto })
  @ApiResponse({ status: 503, description: REPORT_BUSY })
  trafficCrosstab(
    @Query() query: SiteTrafficCrosstabQueryDto,
  ): Promise<SiteTrafficCrosstabDto> {
    return this.analytics.trafficCrosstab(query);
  }

  @Get("live")
  @UseInterceptors(new NoCdnCacheInterceptor())
  @ApiOperation({
    summary: "Who is on the site now, minute by minute",
    description:
      "From Redis only, for polling every few seconds, never cached: " +
      "active visitors, the " +
      "last 30 minutes per minute, and the pages, countries, channels, " +
      "referrers, devices and events of the last 5 or 30 minutes.",
  })
  @ApiResponse({ status: 200, type: SiteLiveDto })
  @ApiResponse({ status: 503, description: "Redis did not answer." })
  live(@Query() query: SiteLiveQueryDto): Promise<SiteLiveDto> {
    return this.liveView.live(query.locale ?? null);
  }

  @Get("search")
  @ApiOperation({
    summary: "Which search is used, from where, and how well",
  })
  @ApiResponse({ status: 200, type: SiteSearchDto })
  @ApiResponse({ status: 503, description: REPORT_BUSY })
  search(@Query() query: SiteAnalyticsQueryDto): Promise<SiteSearchDto> {
    return this.analytics.search(query);
  }

  @Get("seo/landing-pages")
  @ApiOperation({
    summary:
      "Pages visits from one channel (search by default) entered on, with change, bounce and sources",
  })
  @ApiResponse({ status: 200, type: SiteLandingPagesDto })
  @ApiResponse({ status: 503, description: REPORT_BUSY })
  landingPages(
    @Query() query: SiteLandingPagesQueryDto,
  ): Promise<SiteLandingPagesDto> {
    return this.seo.landingPages(query);
  }

  @Get("seo/trends")
  @ApiOperation({
    summary:
      "Landing pages of one channel rising and falling against the window before",
  })
  @ApiResponse({ status: 200, type: SiteTrendsDto })
  @ApiResponse({ status: 503, description: REPORT_BUSY })
  trends(@Query() query: SiteTrendsQueryDto): Promise<SiteTrendsDto> {
    return this.seo.trends(query);
  }

  @Get("seo/not-found")
  @ApiOperation({ summary: "Paths that showed the not-found page" })
  @ApiResponse({ status: 200, type: SiteNotFoundDto })
  @ApiResponse({ status: 503, description: REPORT_BUSY })
  notFound(@Query() query: SiteNotFoundQueryDto): Promise<SiteNotFoundDto> {
    return this.seo.notFound(query);
  }

  @Get("seo/uncovered")
  @ApiOperation({
    summary: "Parks or rides no search visit entered on in the window",
  })
  @ApiResponse({ status: 200, type: SiteUncoveredDto })
  @ApiResponse({ status: 503, description: REPORT_BUSY })
  uncovered(@Query() query: SiteUncoveredQueryDto): Promise<SiteUncoveredDto> {
    return this.seo.uncovered(query);
  }

  @Get("blocked-events")
  @ApiOperation({ summary: "Event names that are no longer recorded" })
  @ApiResponse({ status: 200, type: SiteBlockedEventsDto })
  async blockedEvents(): Promise<SiteBlockedEventsDto> {
    return { items: await this.blocklist.list() };
  }

  @Put("blocked-events/:name")
  @AdminMinRole("editor")
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({
    summary: "Stop recording an event name",
    description: "Every instance stops within a minute. Audited.",
  })
  @ApiResponse({ status: 204 })
  async block(
    @Param("name") name: string,
    @CurrentAdmin() admin: AdminPrincipal,
  ): Promise<void> {
    await this.blocklist.block(eventName(name), admin.email);
  }

  @Delete("blocked-events/:name")
  @AdminMinRole("editor")
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({ summary: "Record an event name again. Audited." })
  @ApiResponse({ status: 204 })
  async unblock(@Param("name") name: string): Promise<void> {
    await this.blocklist.unblock(eventName(name));
  }
}
