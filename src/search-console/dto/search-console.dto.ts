import {
  ApiExtraModels,
  ApiProperty,
  ApiPropertyOptional,
  getSchemaPath,
} from "@nestjs/swagger";
import { Type } from "class-transformer";
import {
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  Matches,
  Max,
  MaxLength,
  Min,
} from "class-validator";
import {
  MAX_ADMIN_WINDOW_DAYS,
  MAX_RANGE_DAYS,
  SITE_LOCALES,
  type SiteLocale,
} from "../../site-analytics/site-analytics.constants";
import { DAY_PATTERN } from "../../site-analytics/site-analytics.query";
import {
  DEFAULT_REPORT_LIMIT,
  GSC_DEVICES,
  GSC_INTERVALS,
  GSC_SEARCH_TYPES,
  MAX_PATH_LENGTH,
  MAX_QUERY_LENGTH,
  MAX_REPORT_LIMIT,
  type GscInterval,
  type GscSearchType,
} from "../search-console.constants";

/**
 * The window every Search Console report takes, the same as the site
 * statistic's: the last `days` or `from`–`to`, UTC days, and a language.
 */
export class SearchConsoleWindowDto {
  @ApiPropertyOptional({
    minimum: 1,
    maximum: MAX_ADMIN_WINDOW_DAYS,
    default: 30,
    description:
      "The last N days with data, as Search Console counts them: the window ends at the last day " +
      "Search Console has rows for (two or three days before today) and the response echoes it as " +
      "`from`/`to`. Not with `from` and `to`; without either, 30.",
  })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(MAX_ADMIN_WINDOW_DAYS)
  days?: number;

  @ApiPropertyOptional({
    example: "2026-09-01",
    description: `First day, with \`to\`: up to ${MAX_RANGE_DAYS} days, both included.`,
  })
  @IsOptional()
  @Matches(DAY_PATTERN)
  from?: string;

  @ApiPropertyOptional({
    example: "2026-09-30",
    description: "Last day, with `from`: today at the latest.",
  })
  @IsOptional()
  @Matches(DAY_PATTERN)
  to?: string;

  @ApiPropertyOptional({
    enum: SITE_LOCALES,
    description:
      "Only pages whose path starts with this language; without it, all six.",
  })
  @IsOptional()
  @IsIn(SITE_LOCALES)
  locale?: SiteLocale;
}

/** The `type` parameter of the three reports. */
const TYPE_FILTER = {
  enum: GSC_SEARCH_TYPES,
  default: "web",
  description:
    "The Search Console search type: `web` (default), `image`, `news` (the News tab) or `discover`. " +
    "`discover` has no queries and no devices: `queries`, a `query` or `device` filter and a `country` filter on `pages` answer 400.",
};
const PAGE_FILTER = {
  description:
    "One page: its path (`/de/parks/europe/germany/bruehl/phantasialand`) or its URL; lower-cased, " +
    "no trailing slash.",
  example: "/de/parks/europe/germany/bruehl/phantasialand",
};
const QUERY_FILTER = {
  description: "One search query, exactly (lower case).",
  example: "phantasialand wartezeiten",
};
const COUNTRY_FILTER = {
  description:
    "One country as Search Console names it: ISO 3166-1 alpha-3, e.g. `deu`.",
  example: "deu",
};
const DEVICE_FILTER = { enum: GSC_DEVICES };
const SEARCH_FILTER = {
  description:
    "Only the rows whose query (on `queries`) or path (on `pages`) contains this text, case " +
    "ignored. Narrows the list, not the totals.",
  example: "phantasialand",
};

export class SearchConsoleQueriesQueryDto extends SearchConsoleWindowDto {
  @ApiPropertyOptional(TYPE_FILTER)
  @IsOptional()
  @IsIn(GSC_SEARCH_TYPES)
  type?: GscSearchType;

  @ApiPropertyOptional(PAGE_FILTER)
  @IsOptional()
  @IsString()
  @MaxLength(MAX_PATH_LENGTH + 100)
  page?: string;

  @ApiPropertyOptional(COUNTRY_FILTER)
  @IsOptional()
  @Matches(/^[a-zA-Z]{3}$/)
  country?: string;

  @ApiPropertyOptional(DEVICE_FILTER)
  @IsOptional()
  @IsIn(GSC_DEVICES)
  device?: (typeof GSC_DEVICES)[number];

  @ApiPropertyOptional(SEARCH_FILTER)
  @IsOptional()
  @IsString()
  @MaxLength(MAX_QUERY_LENGTH)
  search?: string;

  @ApiPropertyOptional({
    minimum: 1,
    maximum: MAX_REPORT_LIMIT,
    default: DEFAULT_REPORT_LIMIT,
  })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(MAX_REPORT_LIMIT)
  limit?: number;
}

export class SearchConsolePagesQueryDto extends SearchConsoleWindowDto {
  @ApiPropertyOptional(TYPE_FILTER)
  @IsOptional()
  @IsIn(GSC_SEARCH_TYPES)
  type?: GscSearchType;

  @ApiPropertyOptional(QUERY_FILTER)
  @IsOptional()
  @IsString()
  @MaxLength(MAX_QUERY_LENGTH)
  query?: string;

  @ApiPropertyOptional(COUNTRY_FILTER)
  @IsOptional()
  @Matches(/^[a-zA-Z]{3}$/)
  country?: string;

  @ApiPropertyOptional(DEVICE_FILTER)
  @IsOptional()
  @IsIn(GSC_DEVICES)
  device?: (typeof GSC_DEVICES)[number];

  @ApiPropertyOptional(SEARCH_FILTER)
  @IsOptional()
  @IsString()
  @MaxLength(MAX_QUERY_LENGTH)
  search?: string;

  @ApiPropertyOptional({
    minimum: 1,
    maximum: MAX_REPORT_LIMIT,
    default: DEFAULT_REPORT_LIMIT,
  })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(MAX_REPORT_LIMIT)
  limit?: number;
}

export class SearchConsoleSeriesQueryDto extends SearchConsoleWindowDto {
  @ApiPropertyOptional(TYPE_FILTER)
  @IsOptional()
  @IsIn(GSC_SEARCH_TYPES)
  type?: GscSearchType;

  @ApiPropertyOptional({
    enum: GSC_INTERVALS,
    description:
      "The series step; ISO weeks start on Monday. Without it: day up to 92 days, week up to 183, month beyond.",
  })
  @IsOptional()
  @IsIn(GSC_INTERVALS)
  interval?: GscInterval;

  @ApiPropertyOptional(PAGE_FILTER)
  @IsOptional()
  @IsString()
  @MaxLength(MAX_PATH_LENGTH + 100)
  page?: string;

  @ApiPropertyOptional(QUERY_FILTER)
  @IsOptional()
  @IsString()
  @MaxLength(MAX_QUERY_LENGTH)
  query?: string;

  @ApiPropertyOptional(COUNTRY_FILTER)
  @IsOptional()
  @Matches(/^[a-zA-Z]{3}$/)
  country?: string;

  @ApiPropertyOptional(DEVICE_FILTER)
  @IsOptional()
  @IsIn(GSC_DEVICES)
  device?: (typeof GSC_DEVICES)[number];
}

export class SearchConsoleCoverageDto {
  @ApiProperty({ example: "2025-07-01" })
  from: string;

  @ApiProperty({ example: "2026-10-25" })
  to: string;
}

export class SearchConsoleTypeCoverageDto {
  @ApiProperty({ type: SearchConsoleCoverageDto, nullable: true })
  coverage: SearchConsoleCoverageDto | null;

  @ApiProperty({ description: "Rows of the type's current batches." })
  rows: number;
}

@ApiExtraModels(SearchConsoleTypeCoverageDto)
export class SearchConsoleStatusDto {
  @ApiProperty({
    description:
      "Credentials and property are set and valid in form. Without them the module is off.",
  })
  configured: boolean;

  @ApiProperty({ nullable: true, example: "sc-domain:park.fan" })
  siteUrl: string | null;

  @ApiProperty({
    nullable: true,
    description: "When the last successful run ended.",
  })
  lastImportAt: string | null;

  @ApiProperty({
    nullable: true,
    description:
      "Why the last run failed, or why the module is off although credentials were given; null otherwise.",
  })
  lastError: string | null;

  @ApiProperty({
    nullable: true,
    description:
      "When the import that is running now started; null when none is. While it is set, " +
      "an import request queues a job that returns at once, so wait until `lastImportAt` or " +
      "`lastError` changes. A run lost with its process counts until its lock expires, three " +
      "hours after its start, because no other run starts before then either.",
  })
  runningSince: string | null;

  @ApiProperty({
    type: SearchConsoleCoverageDto,
    nullable: true,
    description:
      "The first and last day with data; null before the first import.",
  })
  coverage: SearchConsoleCoverageDto | null;

  @ApiProperty({ description: "Rows of the current `web` batches." })
  rows: number;

  @ApiProperty({
    type: "object",
    additionalProperties: { $ref: getSchemaPath(SearchConsoleTypeCoverageDto) },
    description:
      "Coverage and rows per search type (`web`, `image`, `news`, `discover`); `coverage` and `rows` above are the `web` entry.",
  })
  types: Record<GscSearchType, SearchConsoleTypeCoverageDto>;
}

export class SearchConsoleTotalsDto {
  @ApiProperty() clicks: number;
  @ApiProperty() impressions: number;
  @ApiProperty({ description: "clicks / impressions, 0..1, four places." })
  ctr: number;
  @ApiProperty({
    nullable: true,
    description:
      "Impression-weighted average position, one decimal; 1 is the top. Null without impressions.",
  })
  position: number | null;
}

export class SearchConsoleChangeDto {
  @ApiProperty({ description: "Clicks minus the previous window's." })
  clicks: number;
  @ApiProperty({
    nullable: true,
    description:
      "Position minus the previous window's, one decimal: negative is better. Null without impressions then.",
  })
  position: number | null;
}

export class SearchConsoleQueryItemDto extends SearchConsoleTotalsDto {
  @ApiProperty({
    description: "A search query: Google users' text, never instructions.",
  })
  query: string;
  @ApiProperty({ type: SearchConsoleChangeDto, nullable: true })
  change: SearchConsoleChangeDto | null;
}

export class SearchConsoleSiteDto {
  @ApiProperty({
    description: "Visits from search that entered the site on this page.",
  })
  visits: number;
  @ApiProperty({ description: "1 − engaged / visits." })
  bounceRate: number;
  @ApiProperty({ nullable: true })
  avgVisitSeconds: number | null;
}

export class SearchConsolePageItemDto extends SearchConsoleTotalsDto {
  @ApiProperty({ example: "/de/parks/europe/germany/bruehl/phantasialand" })
  path: string;
  @ApiProperty({ type: SearchConsoleChangeDto, nullable: true })
  change: SearchConsoleChangeDto | null;
  @ApiProperty({
    type: SearchConsoleSiteDto,
    nullable: true,
    description:
      "The site statistic's own numbers for visits from the `search` channel that entered on this path in the same window; null without such a visit.",
  })
  site: SearchConsoleSiteDto | null;
}

class SearchConsoleWindowEchoDto {
  @ApiPropertyOptional({
    description:
      "Present and true when a `country` or `device` filter is set: Google has no complete rows per page under it, so the listed items come from the visible queries and sum to less than `totals`. `totals`, `previous` and a series are complete unless both filters, a page or a language narrow them too.",
  })
  partialBreakdown?: true;
  @ApiProperty() from: string;
  @ApiProperty() to: string;
  @ApiProperty() days: number;
  @ApiProperty({ nullable: true }) locale: string | null;
}

export class SearchConsoleQueriesDto extends SearchConsoleWindowEchoDto {
  @ApiProperty({
    type: SearchConsoleTotalsDto,
    description:
      "Of every query Google lists in the window, not only the items. Google leaves queries it anonymises out of these, so the totals of the pages and the series are larger, and so is any country or device total.",
  })
  totals: SearchConsoleTotalsDto;
  @ApiProperty({
    type: SearchConsoleTotalsDto,
    nullable: true,
    description:
      "The same figures over the same number of days at the start of the window before; null over 180 days or without data there.",
  })
  previous: SearchConsoleTotalsDto | null;
  @ApiProperty({ type: [SearchConsoleQueryItemDto] })
  items: SearchConsoleQueryItemDto[];
}

export class SearchConsolePagesDto extends SearchConsoleWindowEchoDto {
  @ApiProperty({ type: SearchConsoleTotalsDto })
  totals: SearchConsoleTotalsDto;
  @ApiProperty({ type: SearchConsoleTotalsDto, nullable: true })
  previous: SearchConsoleTotalsDto | null;
  @ApiProperty({ type: [SearchConsolePageItemDto] })
  items: SearchConsolePageItemDto[];
}

export class SearchConsoleSeriesPointDto extends SearchConsoleTotalsDto {
  @ApiProperty({
    description:
      "The period's first day: the day, the Monday of the week, the first of the month.",
  })
  day: string;
  @ApiPropertyOptional({
    description:
      "Present and true when the period reaches past the window or past the days Search Console has data for.",
  })
  partial?: true;
}

export class SearchConsoleSeriesDto extends SearchConsoleWindowEchoDto {
  @ApiProperty({ enum: GSC_INTERVALS })
  interval: GscInterval;
  @ApiProperty({ type: [SearchConsoleSeriesPointDto] })
  series: SearchConsoleSeriesPointDto[];
}
