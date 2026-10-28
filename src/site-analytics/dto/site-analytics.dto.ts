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
  HOURLY_RETENTION_DAYS,
  INTERVALS,
  MAX_ADMIN_WINDOW_DAYS,
  MAX_COMPARABLE_DAYS,
  MAX_ENGAGEMENT_SECONDS,
  MAX_HOURLY_DAYS,
  MAX_RANGE_DAYS,
  PAGE_DIMS,
  SITE_LOCALES,
  SITE_DEVICES,
  VISIT_DIMS,
  type BreakdownDim,
  type Channel,
  type SiteInterval,
  type SiteLocale,
} from "../site-analytics.constants";
import {
  CROSSTAB_AXIS_PATTERN,
  DAY_PATTERN,
  FILTER_PATTERN,
} from "../site-analytics.query";

/** The sides a traffic crosstab takes, over both metrics. */
export const TRAFFIC_CROSSTAB_SIDES = [
  ...PAGE_DIMS,
  ...VISIT_DIMS,
  "exit",
  "day",
] as const;

/** One hit of `POST /v1/analytics/collect`, for Swagger only. */
export class CollectHitDto {
  @ApiProperty({ enum: ["pageview", "event", "engagement"] })
  t: "pageview" | "event" | "engagement";

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

  @ApiPropertyOptional({
    example: "google.com",
    description:
      "Page views, first of a page load: the host of the external referrer, nothing else. " +
      "Query parameters of the landing URL are not read.",
  })
  referrer?: string;

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

  @ApiPropertyOptional({
    minimum: 1,
    maximum: MAX_ENGAGEMENT_SECONDS,
    description:
      "Engagement only: whole seconds the page was in the foreground since the page view's last report; summed.",
  })
  seconds?: number;
}

export class CollectBodyDto {
  @ApiProperty({ enum: [1] })
  v: 1;

  @ApiProperty({ type: [CollectHitDto], minItems: 1, maxItems: 20 })
  hits: CollectHitDto[];
}

/**
 * The window (`days`, or `from` and `to`), `interval`, `locale` and `filter`,
 * shared by every admin report.
 */
export class SiteAnalyticsQueryDto {
  @ApiPropertyOptional({
    minimum: 1,
    maximum: MAX_ADMIN_WINDOW_DAYS,
    default: 30,
    description:
      "The last UTC days, today included. Not with `from` and `to`; without either, 30.",
  })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(MAX_ADMIN_WINDOW_DAYS)
  days?: number;

  @ApiPropertyOptional({
    example: "2026-09-01",
    description: `First UTC day, with \`to\`: up to ${MAX_RANGE_DAYS} days, both included.`,
  })
  @IsOptional()
  @Matches(DAY_PATTERN)
  from?: string;

  @ApiPropertyOptional({
    example: "2026-09-30",
    description: "Last UTC day, with `from`: today at the latest.",
  })
  @IsOptional()
  @Matches(DAY_PATTERN)
  to?: string;

  @ApiPropertyOptional({
    enum: INTERVALS,
    description:
      `The series step. \`hour\` takes at most ${MAX_HOURLY_DAYS} days. Without it: hour for one ` +
      "day, day up to 92 days, week (ISO, from Monday, UTC) up to 183, month beyond.",
  })
  @IsOptional()
  @IsIn(INTERVALS)
  interval?: SiteInterval;

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
    description:
      `\`<dim>:<value>\`, repeatable, all must match. dim: ${FILTER_DIMS.join(", ")}. ` +
      "entry, referrer and channel select visits by how they began: visit figures " +
      "follow them, page views and events are null, and the reports that count page views or " +
      "events (events, event, crosstab, search, not-found, traffic crosstab with " +
      "metric=pageviews) answer 400. Events carry no browser_language either.",
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
      "`page_type`, `device`, `locale`, `day`, `path`, `country`, `browser`, `os` or " +
      "`prop:<key>`. `day` steps by the window's interval.",
  })
  @Matches(CROSSTAB_AXIS_PATTERN)
  row: string;

  @ApiProperty({ example: "page_type" })
  @Matches(CROSSTAB_AXIS_PATTERN)
  col: string;
}

export class SiteTrafficCrosstabQueryDto extends SiteAnalyticsQueryDto {
  @ApiProperty({
    enum: TRAFFIC_CROSSTAB_SIDES,
    example: "os",
    description:
      "metric=pageviews: a page dimension or `day`. metric=visits also entry, referrer, " +
      "channel and exit; the page dimensions are then the first page's, or the " +
      "last page's with `exit` on a side. `day` steps by the window's interval.",
  })
  @IsIn(TRAFFIC_CROSSTAB_SIDES)
  row: string;

  @ApiProperty({ enum: TRAFFIC_CROSSTAB_SIDES, example: "device" })
  @IsIn(TRAFFIC_CROSSTAB_SIDES)
  col: string;

  @ApiPropertyOptional({ enum: ["pageviews", "visits"], default: "pageviews" })
  @IsOptional()
  @IsIn(["pageviews", "visits"])
  metric: "pageviews" | "visits" = "pageviews";
}

/** The live view takes one language, or none for all six. */
export class SiteLiveQueryDto {
  @ApiPropertyOptional({
    enum: SITE_LOCALES,
    description: "One language; without it, all six.",
  })
  @IsOptional()
  @IsIn(SITE_LOCALES)
  locale?: SiteLocale;
}

export class SiteChannelQueryDto extends SiteAnalyticsQueryDto {
  @ApiPropertyOptional({ enum: CHANNELS, default: "search" })
  @IsOptional()
  @IsIn(CHANNELS)
  channel?: Channel;
}

/** Trends compare with the window before, which longer windows lack. */
export class SiteTrendsQueryDto extends SiteChannelQueryDto {
  @ApiPropertyOptional({
    minimum: 1,
    maximum: MAX_COMPARABLE_DAYS,
    default: 30,
    description: `As everywhere, but at most ${MAX_COMPARABLE_DAYS} days, also from \`from\` to \`to\`.`,
  })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(MAX_COMPARABLE_DAYS)
  days?: number;
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

  @ApiProperty({ example: "2026-10-28", description: "Last UTC day." })
  to: string;

  @ApiProperty({ example: 30, description: "UTC days from `from` to `to`." })
  days: number;

  @ApiProperty({ enum: INTERVALS, description: "The series step." })
  interval: SiteInterval;

  @ApiProperty({ enum: SITE_LOCALES, nullable: true, type: String })
  locale: SiteLocale | null;

  @ApiProperty({ type: [String], example: ["channel:search"] })
  filters: string[];

  @ApiProperty({
    nullable: true,
    type: String,
    example: null,
    description:
      `The hourly rows are kept ${HOURLY_RETENTION_DAYS} days, in whole weeks, so up to a week more; the ` +
      "daily roll-ups are kept longer. A statement no daily roll-up can serve (a filter on the " +
      "path or the entry page, three filters, an hourly series) reads the hourly rows, and for " +
      "older days it finds nothing. When one did, this is the first UTC day its figures are " +
      "complete from, and the report answers with what it has: the figures that read hourly rows " +
      "miss the days before it, in the window, in `previous` (the window before), or in both. " +
      "Null when every figure covers the days it should.",
  })
  hourlyFrom: string | null;
}

export class SiteTotalsDto {
  @ApiProperty({
    nullable: true,
    type: Number,
    description: "Null under a filter on a visit dimension.",
  })
  pageviews: number | null;
  @ApiProperty({ description: "Visits started." }) visits: number;
  @ApiProperty({
    nullable: true,
    type: Number,
    description:
      "Sum of the daily unique visitors: a reader who comes back on another day counts again. Null with a filter, which visitors cannot be split by.",
  })
  visitors: number | null;
  @ApiProperty({
    nullable: true,
    type: Number,
    description: "Null under a filter on a visit dimension.",
  })
  events: number | null;
  @ApiProperty({
    nullable: true,
    type: Number,
    description:
      "Page views per visit, a float. Null under a filter on a visit dimension.",
  })
  viewsPerVisit: number | null;
  @ApiProperty({ description: "1 − visits with a second page view / visits." })
  bounceRate: number;
  @ApiProperty({
    nullable: true,
    type: Number,
    description:
      "Measured foreground seconds per visit, whole; null without a visit.",
  })
  avgVisitSeconds: number | null;
  @ApiProperty({
    nullable: true,
    type: Number,
    description:
      "Measured foreground seconds per page view, whole; null without a page view and under a filter on a visit dimension.",
  })
  avgTimeOnPageSeconds: number | null;
}

/** A series point's start, and whether the window holds all of it. */
class SitePeriodDto {
  @ApiProperty({
    example: "2026-10-28",
    description:
      "The period's start: the UTC day, the hour (`2026-10-28T07:00:00Z`), the Monday of the ISO week or the first of the month.",
  })
  day: string;
  @ApiPropertyOptional({
    enum: [true],
    description:
      "Present on a period that reaches past the window or is still under way.",
  })
  partial?: true;
}

export class SiteSeriesPointDto extends SiteTotalsDto {
  @ApiProperty({
    example: "2026-10-28",
    description:
      "The period's start: the UTC day, the hour (`2026-10-28T07:00:00Z`), the Monday of the ISO week or the first of the month.",
  })
  day: string;
  @ApiPropertyOptional({
    enum: [true],
    description:
      "Present on a period that reaches past the window or is still under way.",
  })
  partial?: true;
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
    nullable: true,
    description: `The same number of days right before \`from\`, its last day up to the current hour. Null when \`days\` is over ${MAX_COMPARABLE_DAYS}.`,
  })
  previous: SiteTotalsDto | null;

  @ApiProperty({
    type: [SiteSeriesPointDto],
    description:
      "Every period of the interval, zeros included; hours up to the current one. An hour's visitors are live for today and the nightly snapshot's for a finished day, null only for a day from before the snapshot kept hours.",
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
  @ApiPropertyOptional({ description: "measure=visits only." })
  bounceRate?: number;
  @ApiPropertyOptional({
    nullable: true,
    type: Number,
    description: "measure=visits only: foreground seconds per visit.",
  })
  avgVisitSeconds?: number | null;
  @ApiPropertyOptional({
    nullable: true,
    type: Number,
    description: "measure=views only: foreground seconds per page view.",
  })
  avgTimeOnPageSeconds?: number | null;
  @ApiPropertyOptional({
    description: "dim=path, measure=views: visits that ended on the page.",
  })
  exits?: number;
  @ApiPropertyOptional({
    description: "dim=path, measure=views: exits / views, at most 1.",
  })
  exitRate?: number;
}

export class SiteBreakdownDto extends WindowDto {
  @ApiProperty({ enum: BREAKDOWN_DIMS }) dim: BreakdownDim;

  @ApiProperty({
    enum: ["views", "visits", "exits"],
    description:
      "What `views` count: page views, visits started (for a visit dimension, and under a filter on one), or for dim=exit the visits that ended on the page.",
  })
  measure: "views" | "visits" | "exits";

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

export class SiteDayCountDto extends SitePeriodDto {
  @ApiProperty() count: number;
}

export class SiteEventSummaryDto {
  @ApiProperty({ example: "search_opened" }) name: string;
  @ApiProperty() count: number;
  @ApiProperty({
    nullable: true,
    type: Number,
    description: `The same number of days right before \`from\`; null when \`days\` is over ${MAX_COMPARABLE_DAYS}.`,
  })
  previous: number | null;
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
  @ApiProperty({ type: [SiteValueCountDto] }) byDevice: SiteValueCountDto[];
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
    description:
      "At most 30 by count, then `__rest__` for all the others; days in order. `(none)` is an empty value.",
  })
  rows: string[];
  @ApiProperty({ type: [String], description: "As `rows`." }) cols: string[];
  @ApiProperty({ type: [SiteCrosstabCellDto], description: "Non-zero cells." })
  cells: SiteCrosstabCellDto[];
  @ApiProperty() total: number;
}

export class SiteTrafficCrosstabDto extends WindowDto {
  @ApiProperty({ enum: ["pageviews", "visits"] })
  metric: "pageviews" | "visits";
  @ApiProperty({ example: "os" }) row: string;
  @ApiProperty({ example: "device" }) col: string;
  @ApiProperty({
    type: [String],
    description:
      "At most 30 by count, then `__rest__` for all the others; periods in order. `(none)` is an empty value.",
  })
  rows: string[];
  @ApiProperty({ type: [String], description: "As `rows`." }) cols: string[];
  @ApiProperty({
    type: [SiteCrosstabCellDto],
    description:
      "Non-zero cells: page views, visits started, or with `exit` visits ended.",
  })
  cells: SiteCrosstabCellDto[];
  @ApiProperty() total: number;
}

export class SiteLiveMinuteDto {
  @ApiProperty({ example: "2026-10-28T12:05:00Z" }) minute: string;
  @ApiProperty() pageviews: number;
  @ApiProperty() events: number;
  @ApiProperty({ description: "Distinct visitors with a hit in the minute." })
  visitors: number;
}

export class SiteLivePageDto {
  @ApiProperty({ example: "/de/parks/europe/germany/bruehl/phantasialand" })
  path: string;
  @ApiProperty({ example: "park" }) pageType: string;
  @ApiProperty() views: number;
}

export class SiteLiveCountryDto {
  @ApiProperty({
    example: "DE",
    description: "ISO code, upper case; `ZZ` unknown.",
  })
  country: string;
  @ApiProperty() views: number;
}

export class SiteLiveChannelDto {
  @ApiProperty({ enum: CHANNELS }) channel: string;
  @ApiProperty({ description: "Visits started." }) visits: number;
}

export class SiteLiveReferrerDto {
  @ApiProperty({ example: "google" }) referrer: string;
  @ApiProperty({ description: "Visits started." }) visits: number;
}

export class SiteLiveDeviceDto {
  @ApiProperty({ enum: SITE_DEVICES }) device: string;
  @ApiProperty() views: number;
}

export class SiteLiveEventDto {
  @ApiProperty({ example: "search_opened" }) name: string;
  @ApiProperty() count: number;
}

export class SiteLiveDto {
  @ApiProperty({
    example: "2026-10-28T12:34:56Z",
    description: "When the counts were read.",
  })
  at: string;
  @ApiProperty({
    description: "Distinct visitors with a hit in the last 5 minutes.",
  })
  activeVisitors: number;
  @ApiProperty({
    type: [SiteLiveMinuteDto],
    description: "The last 30 whole minutes and the current one, oldest first.",
  })
  perMinute: SiteLiveMinuteDto[];
  @ApiProperty({
    type: [SiteLivePageDto],
    description: "Page views of the last 5 minutes, top 10.",
  })
  pages: SiteLivePageDto[];
  @ApiProperty({
    type: [SiteLiveCountryDto],
    description: "Page views of the last 5 minutes, every country.",
  })
  countries: SiteLiveCountryDto[];
  @ApiProperty({
    type: [SiteLiveChannelDto],
    description: "Visits started in the last 30 minutes.",
  })
  channels: SiteLiveChannelDto[];
  @ApiProperty({
    type: [SiteLiveReferrerDto],
    description: "Visits started in the last 30 minutes, top 10.",
  })
  referrers: SiteLiveReferrerDto[];
  @ApiProperty({
    type: [SiteLiveDeviceDto],
    description: "Page views of the last 5 minutes.",
  })
  devices: SiteLiveDeviceDto[];
  @ApiProperty({
    type: [SiteLiveEventDto],
    description: "Events of the last 30 minutes, top 15.",
  })
  events: SiteLiveEventDto[];
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
      "(resultClicks + browseClicks) / opened, at most 1; 0 without an opening.",
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

export class SiteSearchDeviceDto {
  @ApiProperty() surface: string;
  @ApiProperty() device: string;
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

export class SiteSearchDayDto extends SitePeriodDto {
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
  @ApiProperty({ type: [SiteSearchDeviceDto] })
  bySurfaceAndDevice: SiteSearchDeviceDto[];
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
    description: "Every period × every surface.",
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
  @ApiProperty({
    nullable: true,
    type: Number,
    description: `Null when \`days\` is over ${MAX_COMPARABLE_DAYS}.`,
  })
  previous: number | null;
  @ApiProperty({
    nullable: true,
    type: Number,
    description: "visits − previous; null without previous.",
  })
  change: number | null;
  @ApiProperty() bounceRate: number;
  @ApiProperty({
    nullable: true,
    type: Number,
    description: "Foreground seconds per visit, whole.",
  })
  avgVisitSeconds: number | null;
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
  @ApiProperty({
    nullable: true,
    type: Number,
    description: "In the window: foreground seconds per visit, whole.",
  })
  avgVisitSeconds: number | null;
}

export class SiteTrendsDto extends WindowDto {
  @ApiProperty({ enum: CHANNELS }) channel: Channel;
  @ApiProperty({ type: [SiteTrendDto] }) rising: SiteTrendDto[];
  @ApiProperty({ type: [SiteTrendDto] }) falling: SiteTrendDto[];
}

export class SiteNotFoundReferrerDto {
  @ApiProperty({ example: "google" }) value: string;
  @ApiProperty({ description: "Visits that entered on the path from there." })
  visits: number;
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
