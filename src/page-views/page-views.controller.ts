import { Controller, Get, Query, UseGuards } from "@nestjs/common";
import {
  ApiOperation,
  ApiResponse,
  ApiSecurity,
  ApiTags,
} from "@nestjs/swagger";
import { AdminAuthGuard } from "../admin/auth/admin-auth.guard";
import { PageViewsService } from "./page-views.service";
import {
  AdminPageViewsQueryDto,
  AdminPageViewsResponseDto,
  TopPageViewsQueryDto,
  TopPageViewsResponseDto,
} from "./dto/page-view.dto";

/**
 * The public half: the rankings the frontend renders. A read is counted as a
 * `page_read` sent to `POST /v1/analytics/collect`; the old beacon's
 * `POST /v1/page-views` is gone (PAR-1158).
 */
@ApiTags("page-views")
@Controller("page-views")
export class PageViewsController {
  constructor(private readonly pageViews: PageViewsService) {}

  @Get("top")
  @ApiOperation({
    summary: "Most viewed pages of one kind in one language",
    description:
      "Summed over the last `days` UTC days, today included. Keys only: the " +
      "frontend resolves a post's key against its manifest and drops the " +
      "ones it does not know.",
  })
  @ApiResponse({ status: 200, type: TopPageViewsResponseDto })
  @ApiResponse({
    status: 400,
    description:
      "Unknown kind or locale, `days` not 7, 30 or 90, `limit` over 50.",
  })
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
    return this.pageViews.adminOverview(query.days, query.limit, query.locale);
  }
}
