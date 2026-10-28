import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger";
import { Type } from "class-transformer";
import { IsIn, IsInt, IsOptional, Max, Min } from "class-validator";
import {
  MAX_ADMIN_WINDOW_DAYS,
  TRAFFIC_DIMS,
  type TrafficDim,
} from "../site-analytics.constants";
import {
  SEARCH_POSITIONS,
  SEARCH_SURFACES,
  SITE_ENTITY_KINDS,
  SITE_EVENT_NAMES,
  SITE_LOCALES,
  SITE_PAGE_TYPES,
  SITE_VIEWPORTS,
  type SiteEntityKind,
  type SiteLocale,
} from "../site-analytics.schema";

/** The body of `POST /v1/analytics/collect`, for Swagger only. */
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

  @ApiProperty({ enum: SITE_PAGE_TYPES })
  pageType: string;

  @ApiProperty({ enum: SITE_VIEWPORTS })
  viewport: string;

  @ApiPropertyOptional({
    example: "google.com",
    description:
      "Page views only, on a page load's first: the external referrer's host or `utm_source`.",
  })
  referrer?: string;

  @ApiPropertyOptional({ enum: SITE_EVENT_NAMES, description: "Events only." })
  name?: string;

  @ApiPropertyOptional({
    type: "object",
    additionalProperties: { type: "string" },
    example: { surface: "hero" },
    description: "Events only: exactly the properties the event declares.",
  })
  props?: Record<string, string>;

  @ApiPropertyOptional({
    description:
      "Events only: the lowercase UUID of the park, ride or show the event is about.",
  })
  entity?: string;
}

export class CollectBodyDto {
  @ApiProperty({ enum: [1] })
  v: 1;

  @ApiProperty({ type: [CollectHitDto], minItems: 1, maxItems: 20 })
  hits: CollectHitDto[];
}

/** `days` and `locale`, shared by every admin report. */
export class SiteAnalyticsQueryDto {
  @ApiPropertyOptional({
    minimum: 1,
    maximum: MAX_ADMIN_WINDOW_DAYS,
    default: 30,
    description: "UTC days, today included.",
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
}

export class SiteAnalyticsBreakdownQueryDto extends SiteAnalyticsQueryDto {
  @ApiProperty({ enum: TRAFFIC_DIMS })
  @IsIn(TRAFFIC_DIMS)
  dim: TrafficDim;

  @ApiPropertyOptional({ minimum: 1, maximum: 200, default: 50 })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(200)
  limit: number = 50;
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
}

export class SiteTotalsDto {
  @ApiProperty() pageviews: number;
  @ApiProperty() visits: number;
  @ApiProperty({
    description:
      "Sum of the daily unique visitors: a reader who comes back on another day counts again.",
  })
  visitors: number;
  @ApiProperty() events: number;
  @ApiProperty({ description: "Page views per visit, a float." })
  viewsPerVisit: number;
}

export class SiteDayDto {
  @ApiProperty({ example: "2026-10-28" }) day: string;
  @ApiProperty() pageviews: number;
  @ApiProperty() visits: number;
  @ApiProperty() visitors: number;
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
    type: [SiteDayDto],
    description: "Every day, zeros included.",
  })
  series: SiteDayDto[];

  @ApiProperty({ type: SiteRealtimeDto }) realtime: SiteRealtimeDto;
}

export class SiteViewsVisitsDto {
  @ApiProperty() views: number;
  @ApiProperty() visits: number;
}

export class SiteBreakdownItemDto extends SiteViewsVisitsDto {
  @ApiProperty({ example: "google.com" }) value: string;
  @ApiProperty({ description: "Of `total.views`, 0..1." }) share: number;
}

export class SiteBreakdownDto extends WindowDto {
  @ApiProperty({ enum: TRAFFIC_DIMS }) dim: TrafficDim;

  @ApiProperty({
    type: SiteViewsVisitsDto,
    description: "Over every value of the dimension, not only the listed ones.",
  })
  total: SiteViewsVisitsDto;

  @ApiProperty({
    type: [SiteBreakdownItemDto],
    description: "Most viewed first.",
  })
  items: SiteBreakdownItemDto[];
}

export class SiteDayCountDto {
  @ApiProperty({ example: "2026-10-28" }) day: string;
  @ApiProperty() count: number;
}

export class SiteEventSummaryDto {
  @ApiProperty({ enum: SITE_EVENT_NAMES }) name: string;
  @ApiProperty() count: number;
  @ApiProperty({ description: "The same number of days right before `from`." })
  previous: number;
  @ApiProperty({ type: [SiteDayCountDto] }) series: SiteDayCountDto[];
}

export class SiteEventsDto extends WindowDto {
  @ApiProperty({
    type: [SiteEventSummaryDto],
    description:
      "Every event of the schema, zeros included, most counted first.",
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
    description:
      "Most counted first; every value of a closed list, zeros included.",
  })
  values: SiteValueCountDto[];
}

export class SiteEventEntityDto {
  @ApiProperty() id: string;
  @ApiProperty({ enum: SITE_ENTITY_KINDS }) kind: SiteEntityKind;
  @ApiProperty({
    nullable: true,
    type: String,
    description: "Null for an entity that is gone.",
  })
  name: string | null;
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
  @ApiProperty({ enum: SITE_EVENT_NAMES }) name: string;
  @ApiProperty() total: number;
  @ApiProperty({
    type: [SitePropDto],
    description: "In the event's property order.",
  })
  props: SitePropDto[];
  @ApiProperty({ type: [SiteValueCountDto] }) byPageType: SiteValueCountDto[];
  @ApiProperty({ type: [SiteValueCountDto] }) byViewport: SiteValueCountDto[];
  @ApiProperty({ type: [SiteEventEntityDto], description: "Top 50." })
  entities: SiteEventEntityDto[];
  @ApiProperty({ type: [SiteDayCountDto] }) series: SiteDayCountDto[];
}

export class SiteSearchSurfaceDto {
  @ApiProperty({ enum: SEARCH_SURFACES }) surface: string;
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
  @ApiProperty({ enum: SEARCH_POSITIONS }) position: string;
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
    description: "All seven surfaces.",
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
