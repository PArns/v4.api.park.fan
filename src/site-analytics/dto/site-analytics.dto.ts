import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger";
import { Transform, Type } from "class-transformer";
import {
  ArrayMaxSize,
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  Matches,
  Max,
  Min,
} from "class-validator";
import {
  BREAKDOWN_DIMS,
  CHANNELS,
  FILTER_DIMS,
  MAX_ADMIN_WINDOW_DAYS,
  SITE_LOCALES,
  SITE_VIEWPORTS,
  type BreakdownDim,
  type Channel,
  type SiteLocale,
} from "../site-analytics.constants";
import { CROSSTAB_AXIS_PATTERN, FILTER_PATTERN } from "../site-analytics.query";

/** One hit of `POST /v1/analytics/collect`, for Swagger only. */
export class CollectHitDto {
  @ApiProperty({ enum: ["pageview", "event"] })
  t: "pageview" | "event";

  @ApiProperty({ enum: SITE_LOCALES })
  locale: SiteLocale;

  @ApiProperty({
    example: "/de/parks/europe/germany/bruehl/phantasialand",
    description:
      "`location.pathname`, lowercased, without query, hash or trailing slash.",
  })
  path: string;

  @ApiProperty({ example: "park", pattern: "^[a-z][a-z0-9_]{0,31}$" })
  pageType: string;

  @ApiProperty({ enum: SITE_VIEWPORTS })
  viewport: string;

  @ApiPropertyOptional({
    example: "google.com",
    description:
      "Page views, first of a page load: the external referrer's host or `utm_source`.",
  })
  referrer?: string;

  @ApiPropertyOptional({
    example: "summer-2026",
    description: "Page views, first of a page load: `utm_campaign`.",
  })
  campaign?: string;

  @ApiPropertyOptional({
    example: "search_opened",
    pattern: "^[a-z][a-z0-9_]{1,47}$",
    description: "Events only.",
  })
  name?: string;

  @ApiPropertyOptional({
    type: "object",
    additionalProperties: {
      oneOf: [{ type: "string" }, { type: "number" }, { type: "boolean" }],
    },
    example: { surface: "hero" },
    description:
      "Events only: up to 8 properties. Never free text: no search query, no name somebody typed.",
  })
  props?: Record<string, string | number | boolean>;

  @ApiPropertyOptional({
    description:
      "Events only: the lowercase UUID of the park, ride, show or restaurant the event is about.",
  })
  entity?: string;
}

export class CollectBodyDto {
  @ApiProperty({ enum: [1] })
  v: 1;

  @ApiProperty({ type: [CollectHitDto], minItems: 1, maxItems: 20 })
  hits: CollectHitDto[];
}

/** `days`, `locale` and `filter`, shared by every admin report. */
export class SiteAnalyticsQueryDto {
  @ApiPropertyOptional({
    minimum: 1,
    maximum: MAX_ADMIN_WINDOW_DAYS,
    default: 30,
    description: "UTC days, today included. 1 gives hourly series.",
  })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(MAX_ADMIN_WINDOW_DAYS)
  days: number = 30;

  @ApiPropertyOptional({
    enum: SITE_LOCALES,
    description: "One language; without it, all six.",
  })
  @IsOptional()
  @IsIn(SITE_LOCALES)
  locale?: SiteLocale;

  @ApiPropertyOptional({
    type: [String],
    example: ["channel:search", "country:nl"],
    description: `\`<dim>:<value>\`, repeatable, all must match. dim: ${FILTER_DIMS.join(", ")}.`,
  })
  @IsOptional()
  @Transform(({ value }) =>
    value === undefined ? undefined : Array.isArray(value) ? value : [value],
  )
  @ArrayMaxSize(FILTER_DIMS.length)
  @IsString({ each: true })
  @Matches(FILTER_PATTERN, { each: true })
  filter?: string[];
}

export class SiteAnalyticsBreakdownQueryDto extends SiteAnalyticsQueryDto {
  @ApiProperty({ enum: BREAKDOWN_DIMS })
  @IsIn(BREAKDOWN_DIMS)
  dim: BreakdownDim;

  @ApiPropertyOptional({ minimum: 1, maximum: 200, default: 50 })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(200)
  limit: number = 50;
}

export class SiteCrosstabQueryDto extends SiteAnalyticsQueryDto {
  @ApiProperty({
    example: "prop:surface",
    description:
      "`page_type`, `viewport`, `locale`, `day`, a filter dimension or `prop:<key>`.",
  })
  @Matches(CROSSTAB_AXIS_PATTERN)
  row: string;

  @ApiProperty({ example: "page_type" })
  @Matches(CROSSTAB_AXIS_PATTERN)
  col: string;
}

export class SiteChannelQueryDto extends SiteAnalyticsQueryDto {
  @ApiPropertyOptional({ enum: CHANNELS, default: "search" })
  @IsOptional()
  @IsIn(CHANNELS)
  channel?: Channel;
}

export class SiteLandingPagesQueryDto extends SiteChannelQueryDto {
  @ApiPropertyOptional({ minimum: 1, maximum: 200, default: 50 })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(200)
  limit: number = 50;
}

export class SiteNotFoundQueryDto extends SiteAnalyticsQueryDto {
  @ApiPropertyOptional({ minimum: 1, maximum: 200, default: 50 })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(200)
  limit: number = 50;
}

export class SiteUncoveredQueryDto extends SiteAnalyticsQueryDto {
  @ApiProperty({ enum: ["park", "attraction"] })
  @IsIn(["park", "attraction"])
  kind: "park" | "attraction";

  @ApiPropertyOptional({ minimum: 1, maximum: 1000, default: 200 })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(1000)
  limit: number = 200;
}

/** What every report echoes. */
class WindowDto {
  @ApiProperty({ example: "2026-09-29", description: "First UTC day." })
  from: string;

  @ApiProperty({ example: "2026-10-28", description: "Last UTC day, today." })
  to: string;

  @ApiProperty({ example: 30 })
  days: number;

  @ApiProperty({ enum: SITE_LOCALES, nullable: true, type: String })
  locale: SiteLocale | null;

  @ApiProperty({ type: [String], example: ["channel:search"] })
  filters: string[];
}

export class SiteTotalsDto {
  @ApiProperty() pageviews: number;
  @ApiProperty({ description: "Visits started." }) visits: number;
  @ApiProperty({
    nullable: true,
    type: Number,
    description:
      "Sum of the daily unique visitors: a reader who comes back on another day counts again. Null with a filter, which visitors cannot be split by.",
  })
  visitors: number | null;
  @ApiProperty() events: number;
  @ApiProperty({ description: "Page views per visit, a float." })
  viewsPerVisit: number;
  @ApiProperty({ description: "1 − visits with a second page view / visits." })
  bounceRate: number;
}

export class SiteSeriesPointDto {
  @ApiProperty({
    example: "2026-10-28",
    description:
      "The UTC day, or the hour (`2026-10-28T07:00:00Z`) when days=1.",
  })
  day: string;
  @ApiProperty() pageviews: number;
  @ApiProperty() visits: number;
  @ApiProperty({ nullable: true, type: Number }) visitors: number | null;
  @ApiProperty() events: number;
}

export class SiteMinuteDto {
  @ApiProperty({ example: "2026-10-28T07:12:00Z" }) minute: string;
  @ApiProperty() pageviews: number;
}

export class SiteRealtimeDto {
  @ApiProperty({
    description: "Distinct visitors with a hit in the last 5 minutes.",
  })
  activeVisitors: number;

  @ApiProperty({
    type: [SiteMinuteDto],
    description: "The last 30 minutes, oldest first, the current one included.",
  })
  perMinute: SiteMinuteDto[];
}

export class SiteOverviewDto extends WindowDto {
  @ApiProperty({ type: SiteTotalsDto }) totals: SiteTotalsDto;

  @ApiProperty({
    type: SiteTotalsDto,
    description: "The same number of days right before `from`.",
  })
  previous: SiteTotalsDto;

  @ApiProperty({
    type: [SiteSeriesPointDto],
    description:
      "Every day (every hour so far today when days=1), zeros included.",
  })
  series: SiteSeriesPointDto[];

  @ApiProperty({ type: SiteRealtimeDto }) realtime: SiteRealtimeDto;
}

export class SiteViewsVisitsDto {
  @ApiProperty() views: number;
  @ApiProperty() visits: number;
}

export class SiteBreakdownItemDto extends SiteViewsVisitsDto {
  @ApiProperty({ example: "google" }) value: string;
  @ApiProperty({ description: "Of `total.views`, 0..1." }) share: number;
  @ApiPropertyOptional({
    description: "entry, referrer, channel and campaign only.",
  })
  bounceRate?: number;
}

export class SiteBreakdownDto extends WindowDto {
  @ApiProperty({ enum: BREAKDOWN_DIMS }) dim: BreakdownDim;

  @ApiProperty({
    type: SiteViewsVisitsDto,
    description: "Over every value of the dimension, not only the listed ones.",
  })
  total: SiteViewsVisitsDto;

  @ApiProperty({
    type: [SiteBreakdownItemDto],
    description: "Most viewed first (most visits for a visit dimension).",
  })
  items: SiteBreakdownItemDto[];
}

export class SiteDayCountDto {
  @ApiProperty({ example: "2026-10-28" }) day: string;
  @ApiProperty() count: number;
}

export class SiteEventSummaryDto {
  @ApiProperty({ example: "search_opened" }) name: string;
  @ApiProperty() count: number;
  @ApiProperty({ description: "The same number of days right before `from`." })
  previous: number;
  @ApiProperty() blocked: boolean;
  @ApiProperty({ type: [SiteDayCountDto] }) series: SiteDayCountDto[];
}

export class SiteEventsDto extends WindowDto {
  @ApiProperty({
    type: [SiteEventSummaryDto],
    description:
      "Every name seen in the window or the one before, and every blocked one; most counted first.",
  })
  items: SiteEventSummaryDto[];
}

export class SiteValueCountDto {
  @ApiProperty() value: string;
  @ApiProperty() count: number;
}

export class SitePropDto {
  @ApiProperty({ example: "surface" }) prop: string;

  @ApiProperty({
    type: [SiteValueCountDto],
    description: "The 50 most counted values.",
  })
  values: SiteValueCountDto[];

  @ApiProperty({ description: "Events with any other value." })
  otherCount: number;
}

export class SiteCombinationDto {
  @ApiProperty({
    type: "object",
    additionalProperties: { type: "string" },
    example: { result: "park", surface: "hero" },
  })
  props: Record<string, string>;
  @ApiProperty() count: number;
}

export class SiteEventEntityDto {
  @ApiProperty() id: string;
  @ApiProperty({
    enum: ["park", "attraction", "show", "restaurant"],
    nullable: true,
    type: String,
    description: "Null for an id that resolves to nothing any more.",
  })
  kind: "park" | "attraction" | "show" | "restaurant" | null;
  @ApiProperty({ nullable: true, type: String }) name: string | null;
  @ApiProperty({
    nullable: true,
    type: String,
    example: "/parks/europe/germany/bruehl/phantasialand",
    description: "Frontend path without a locale.",
  })
  url: string | null;
  @ApiProperty() count: number;
}

export class SiteEventDetailDto extends WindowDto {
  @ApiProperty({ example: "search_result_clicked" }) name: string;
  @ApiProperty() blocked: boolean;
  @ApiProperty() total: number;
  @ApiProperty({
    type: [SitePropDto],
    description: "Every property key seen, the most counted first.",
  })
  props: SitePropDto[];
  @ApiProperty({ type: [SiteCombinationDto], description: "Top 50." })
  combinations: SiteCombinationDto[];
  @ApiProperty({ type: [SiteValueCountDto] }) byPageType: SiteValueCountDto[];
  @ApiProperty({ type: [SiteValueCountDto] }) byViewport: SiteValueCountDto[];
  @ApiProperty({
    type: [SiteEventEntityDto],
    description: "Top 50; a merged-away entity counts under its survivor.",
  })
  entities: SiteEventEntityDto[];
  @ApiProperty({ type: [SiteDayCountDto] }) series: SiteDayCountDto[];
}

export class SiteCrosstabCellDto {
  @ApiProperty() row: string;
  @ApiProperty() col: string;
  @ApiProperty() count: number;
}

export class SiteCrosstabDto extends WindowDto {
  @ApiProperty() name: string;
  @ApiProperty({ example: "prop:surface" }) row: string;
  @ApiProperty({ example: "page_type" }) col: string;
  @ApiProperty({
    type: [String],
    description: "At most 30 by count, then `other`; days in order.",
  })
  rows: string[];
  @ApiProperty({ type: [String] }) cols: string[];
  @ApiProperty({ type: [SiteCrosstabCellDto], description: "Non-zero cells." })
  cells: SiteCrosstabCellDto[];
  @ApiProperty() total: number;
}

export class SiteSearchSurfaceDto {
  @ApiProperty({ example: "hero" }) surface: string;
  @ApiProperty() opened: number;
  @ApiProperty() typed: number;
  @ApiProperty() resultClicks: number;
  @ApiProperty() browseClicks: number;
  @ApiProperty() noResults: number;
  @ApiProperty() viewAll: number;
  @ApiProperty({
    description:
      "(resultClicks + browseClicks) / opened; 0 without an opening.",
  })
  clickRate: number;
  @ApiProperty({ description: "noResults / typed; 0 without a typed query." })
  noResultRate: number;
}

export class SiteSearchPageTypeDto {
  @ApiProperty() surface: string;
  @ApiProperty() pageType: string;
  @ApiProperty() opened: number;
}

export class SiteSearchViewportDto {
  @ApiProperty() surface: string;
  @ApiProperty() viewport: string;
  @ApiProperty() opened: number;
}

export class SiteSearchResultTypeDto {
  @ApiProperty() surface: string;
  @ApiProperty() result: string;
  @ApiProperty() clicks: number;
}

export class SiteSearchPositionDto {
  @ApiProperty() position: string;
  @ApiProperty() clicks: number;
}

export class SiteSearchBrowseItemDto {
  @ApiProperty() surface: string;
  @ApiProperty() item: string;
  @ApiProperty() clicks: number;
}

export class SiteSearchDayDto {
  @ApiProperty() day: string;
  @ApiProperty() surface: string;
  @ApiProperty() opened: number;
}

export class SiteSearchDto extends WindowDto {
  @ApiProperty({
    type: [SiteSearchSurfaceDto],
    description: "Every surface seen, the most opened first.",
  })
  surfaces: SiteSearchSurfaceDto[];
  @ApiProperty({ type: [SiteSearchPageTypeDto] })
  bySurfaceAndPageType: SiteSearchPageTypeDto[];
  @ApiProperty({ type: [SiteSearchViewportDto] })
  bySurfaceAndViewport: SiteSearchViewportDto[];
  @ApiProperty({ type: [SiteSearchResultTypeDto] })
  resultTypes: SiteSearchResultTypeDto[];
  @ApiProperty({
    type: [SiteSearchPositionDto],
    description: "Result clicks by position.",
  })
  positions: SiteSearchPositionDto[];
  @ApiProperty({ type: [SiteSearchBrowseItemDto] })
  browseItems: SiteSearchBrowseItemDto[];
  @ApiProperty({
    type: [SiteSearchDayDto],
    description: "Every day × every surface.",
  })
  series: SiteSearchDayDto[];
}

export class SiteBlockedEventDto {
  @ApiProperty() name: string;
  @ApiProperty({ example: "2026-10-28T07:12:00.000Z" }) blockedAt: string;
  @ApiProperty({ description: "The admin's e-mail." }) blockedBy: string;
}

export class SiteBlockedEventsDto {
  @ApiProperty({ type: [SiteBlockedEventDto] }) items: SiteBlockedEventDto[];
}

export class SiteSourceDto {
  @ApiProperty({ example: "google" }) value: string;
  @ApiProperty() visits: number;
}

export class SiteLandingPageDto {
  @ApiProperty() path: string;
  @ApiProperty() pageType: string;
  @ApiProperty() visits: number;
  @ApiProperty() previous: number;
  @ApiProperty({ description: "visits − previous." }) change: number;
  @ApiProperty() bounceRate: number;
  @ApiProperty({ type: [SiteSourceDto] }) sources: SiteSourceDto[];
}

export class SiteLandingPagesDto extends WindowDto {
  @ApiProperty({ enum: CHANNELS }) channel: Channel;
  @ApiProperty({ type: [SiteLandingPageDto] }) items: SiteLandingPageDto[];
}

export class SiteTrendDto {
  @ApiProperty() path: string;
  @ApiProperty() pageType: string;
  @ApiProperty() visits: number;
  @ApiProperty() previous: number;
  @ApiProperty() change: number;
}

export class SiteTrendsDto extends WindowDto {
  @ApiProperty({ enum: CHANNELS }) channel: Channel;
  @ApiProperty({ type: [SiteTrendDto] }) rising: SiteTrendDto[];
  @ApiProperty({ type: [SiteTrendDto] }) falling: SiteTrendDto[];
}

export class SiteNotFoundReferrerDto {
  @ApiProperty() value: string;
  @ApiProperty() views: number;
}

export class SiteNotFoundItemDto {
  @ApiProperty() path: string;
  @ApiProperty() views: number;
  @ApiProperty({ type: [SiteNotFoundReferrerDto] })
  referrers: SiteNotFoundReferrerDto[];
}

export class SiteNotFoundDto extends WindowDto {
  @ApiProperty({ type: [SiteNotFoundItemDto] }) items: SiteNotFoundItemDto[];
}

export class SiteUncoveredItemDto {
  @ApiProperty() id: string;
  @ApiProperty({ enum: ["park", "attraction"] }) kind: "park" | "attraction";
  @ApiProperty() name: string;
  @ApiPropertyOptional({ description: "A ride's park." }) park?: string;
  @ApiProperty({ example: "/parks/europe/germany/bruehl/phantasialand" })
  url: string;
}

export class SiteUncoveredDto extends WindowDto {
  @ApiProperty({ enum: ["park", "attraction"] }) kind: "park" | "attraction";
  @ApiProperty({ description: "Parks or rides open on the site." })
  total: number;
  @ApiProperty({ description: "How many of them no search visit entered on." })
  uncovered: number;
  @ApiProperty({ type: [SiteUncoveredItemDto], description: "By name." })
  items: SiteUncoveredItemDto[];
}
