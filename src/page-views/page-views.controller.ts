import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Post,
  Query,
  Req,
  UseGuards,
  UseInterceptors,
} from "@nestjs/common";
import {
  ApiOperation,
  ApiResponse,
  ApiSecurity,
  ApiTags,
} from "@nestjs/swagger";
import { Request } from "express";
import { getRateLimitIp } from "../common/utils/request.util";
import { NoCdnCacheInterceptor } from "../common/interceptors/no-cdn-cache.interceptor";
import { AdminAuthGuard } from "../admin/auth/admin-auth.guard";
import { AdminAuditInterceptor } from "../admin/auth/admin-audit.interceptor";
import { PageViewsService } from "./page-views.service";
import {
  AdminPageViewsQueryDto,
  AdminPageViewsResponseDto,
  RecordPageViewDto,
  TopPageViewsQueryDto,
  TopPageViewsResponseDto,
} from "./dto/page-view.dto";

/**
 * The public half: the beacon's write and the rankings the frontend renders.
 *
 * The browser sends the write straight here, never through the frontend's
 * relay, so its own address and User-Agent are the request's. The write
 * answers 204 whether or not the view was counted: a caller that could tell a
 * counted view from a refused one could probe the cap and the bot rule, and
 * nothing the beacon does depends on the answer.
 */
@ApiTags("page-views")
@Controller("page-views")
export class PageViewsController {
  constructor(private readonly pageViews: PageViewsService) {}

  @Post()
  @HttpCode(HttpStatus.NO_CONTENT)
  @UseInterceptors(new NoCdnCacheInterceptor())
  @ApiOperation({
    summary: "Count one page view",
    description:
      "Sent by the frontend once a reader has stayed on a post, park or ride " +
      "page for a few seconds. Bots are not counted, and one address counts " +
      "the same page at most three times a day. Always 204.",
  })
  @ApiResponse({ status: 204 })
  @ApiResponse({
    status: 400,
    description: "Unknown kind or locale, or a malformed key.",
  })
  async record(
    @Body() body: RecordPageViewDto,
    @Req() request: Request,
  ): Promise<void> {
    await this.pageViews.record({
      kind: body.kind,
      key: body.key,
      locale: body.locale,
      ip: getRateLimitIp(request),
      userAgent: request.get("user-agent") ?? null,
    });
  }

  @Get("top")
  @ApiOperation({
    summary: "Most viewed pages of one kind in one language",
    description:
      "Summed over the last `days` UTC days, today included. Keys only: the " +
      "frontend resolves a post's key against its manifest and drops the " +
      "ones it does not know.",
  })
  @ApiResponse({ status: 200, type: TopPageViewsResponseDto })
  async top(
    @Query() query: TopPageViewsQueryDto,
  ): Promise<TopPageViewsResponseDto> {
    const now = new Date();
    return {
      kind: query.kind,
      locale: query.locale,
      days: query.days,
      from: this.pageViews.windowStart(query.days, now),
      items: await this.pageViews.top(
        query.kind,
        query.locale,
        query.days,
        query.limit,
        now,
      ),
    };
  }
}

/** The admin's popularity overview: what is read, and in which language. */
@ApiTags("admin")
@ApiSecurity("admin-auth")
@Controller("admin/page-views")
@UseGuards(AdminAuthGuard)
@UseInterceptors(AdminAuditInterceptor)
export class AdminPageViewsController {
  constructor(private readonly pageViews: PageViewsService) {}

  @Get()
  @ApiOperation({
    summary: "Page views by kind, page and language",
    description:
      "Per kind: the total, the split by language and the most viewed " +
      "pages with their own split; plus the views per day. Parks and " +
      "attractions come with name and link, posts with their key only.",
  })
  @ApiResponse({ status: 200, type: AdminPageViewsResponseDto })
  overview(
    @Query() query: AdminPageViewsQueryDto,
  ): Promise<AdminPageViewsResponseDto> {
    return this.pageViews.adminOverview(query.days, query.limit);
  }
}
