import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger";
import { Type } from "class-transformer";
import {
  IsIn,
  IsInt,
  IsOptional,
  Max,
  Min,
  Validate,
  ValidatorConstraint,
  type ValidationArguments,
  type ValidatorConstraintInterface,
} from "class-validator";
import {
  MAX_ADMIN_WINDOW_DAYS,
  MAX_TOP_LIMIT,
  PAGE_VIEW_KINDS,
  PAGE_VIEW_LOCALES,
  TOP_WINDOW_DAYS,
  isPageKeyFor,
  type PageViewKind,
  type PageViewLocale,
} from "../page-view.constants";

/** The key has to fit the kind beside it, see `isPageKeyFor`. */
@ValidatorConstraint({ name: "pageKeyForKind" })
class PageKeyForKind implements ValidatorConstraintInterface {
  validate(key: unknown, args: ValidationArguments): boolean {
    return isPageKeyFor((args.object as RecordPageViewDto).kind, key);
  }

  defaultMessage(): string {
    return "key must be a translationKey, or a UUID for a park or an attraction";
  }
}

/** One view, as the frontend's beacon sends it. */
export class RecordPageViewDto {
  @ApiProperty({ enum: PAGE_VIEW_KINDS, example: "news" })
  @IsIn(PAGE_VIEW_KINDS)
  kind: PageViewKind;

  @ApiProperty({
    example: "europa-park-voltron-nevera-2026",
    description:
      "The post's `translationKey`, or the park's or attraction's UUID.",
  })
  @Validate(PageKeyForKind)
  key: string;

  @ApiProperty({ enum: PAGE_VIEW_LOCALES, example: "de" })
  @IsIn(PAGE_VIEW_LOCALES)
  locale: PageViewLocale;
}

/** `GET /v1/page-views/top`. */
export class TopPageViewsQueryDto {
  @ApiProperty({ enum: PAGE_VIEW_KINDS })
  @IsIn(PAGE_VIEW_KINDS)
  kind: PageViewKind;

  @ApiProperty({ enum: PAGE_VIEW_LOCALES })
  @IsIn(PAGE_VIEW_LOCALES)
  locale: PageViewLocale;

  @ApiPropertyOptional({
    enum: TOP_WINDOW_DAYS,
    default: 30,
    description: "UTC days, today included.",
  })
  @IsOptional()
  @Type(() => Number)
  @IsIn(TOP_WINDOW_DAYS)
  days: number = 30;

  @ApiPropertyOptional({ minimum: 1, maximum: MAX_TOP_LIMIT, default: 10 })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(MAX_TOP_LIMIT)
  limit: number = 10;
}

export class TopPageViewDto {
  @ApiProperty({ example: "europa-park-voltron-nevera-2026" })
  key: string;

  @ApiProperty({ example: 412 })
  views: number;
}

export class TopPageViewsResponseDto {
  @ApiProperty({ enum: PAGE_VIEW_KINDS })
  kind: PageViewKind;

  @ApiProperty({ enum: PAGE_VIEW_LOCALES })
  locale: PageViewLocale;

  @ApiProperty({ example: 7 })
  days: number;

  @ApiProperty({ example: "2026-10-21", description: "First UTC day counted." })
  from: string;

  @ApiProperty({ type: [TopPageViewDto], description: "Most viewed first." })
  items: TopPageViewDto[];
}

/** `GET /v1/admin/page-views`. */
export class AdminPageViewsQueryDto {
  @ApiPropertyOptional({
    minimum: 1,
    maximum: MAX_ADMIN_WINDOW_DAYS,
    default: 30,
  })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(MAX_ADMIN_WINDOW_DAYS)
  days: number = 30;

  @ApiPropertyOptional({
    enum: PAGE_VIEW_LOCALES,
    description:
      "Rank by the views in this language; without it, by all languages. " +
      "Every entry still carries its split by language.",
  })
  @IsOptional()
  @IsIn(PAGE_VIEW_LOCALES)
  locale?: PageViewLocale;

  @ApiPropertyOptional({ minimum: 1, maximum: 100, default: 25 })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(100)
  limit: number = 25;
}

export class AdminPageViewEntryDto {
  @ApiProperty()
  key: string;

  @ApiProperty({ description: "All languages together." })
  total: number;

  @ApiProperty({
    type: "object",
    additionalProperties: { type: "number" },
    example: { de: 310, en: 64, nl: 38 },
    description: "Views per language; a language without one is absent.",
  })
  byLocale: Partial<Record<PageViewLocale, number>>;

  @ApiPropertyOptional({
    description:
      "Name of the park or attraction. Absent for posts, whose titles live " +
      "in the frontend's manifest, and for an id that no longer resolves.",
  })
  name?: string;

  @ApiPropertyOptional({
    description: "Park of an attraction, or city and country of a park.",
  })
  context?: string;

  @ApiPropertyOptional({
    description: "Frontend path of a park or attraction.",
  })
  url?: string;
}

export class AdminPageViewKindDto {
  @ApiProperty({ enum: PAGE_VIEW_KINDS })
  kind: PageViewKind;

  @ApiProperty()
  total: number;

  @ApiProperty({ type: "object", additionalProperties: { type: "number" } })
  byLocale: Partial<Record<PageViewLocale, number>>;

  @ApiProperty({
    type: [AdminPageViewEntryDto],
    description:
      "Most viewed first, across all languages or in the language asked for.",
  })
  top: AdminPageViewEntryDto[];
}

export class AdminPageViewDayDto {
  @ApiProperty({ example: "2026-10-27" })
  day: string;

  @ApiProperty({
    type: "object",
    additionalProperties: { type: "number" },
    description: "Views that day per kind.",
  })
  byKind: Partial<Record<PageViewKind, number>>;
}

export class AdminPageViewsResponseDto {
  @ApiProperty()
  days: number;

  @ApiProperty({ description: "First UTC day counted." })
  from: string;

  @ApiPropertyOptional({
    enum: PAGE_VIEW_LOCALES,
    description:
      "The language the top lists are ranked by, when one was asked for.",
  })
  locale?: PageViewLocale;

  @ApiProperty({ type: [AdminPageViewKindDto] })
  kinds: AdminPageViewKindDto[];

  @ApiProperty({
    type: [AdminPageViewDayDto],
    description:
      "Every day of the window, oldest first; a day without views is empty.",
  })
  daily: AdminPageViewDayDto[];
}
