import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger";
import {
  Allow,
  ArrayMaxSize,
  IsArray,
  IsOptional,
  IsString,
  MaxLength,
} from "class-validator";
import {
  TripTicketAllowanceDto,
  TripTicketCapDto,
  TripTicketHopSetDto,
  TripTicketLinkDto,
  TripTicketOnSaleDto,
  TripTicketProductDto,
  TripTicketWindowDto,
} from "../../../trip-regions/dto/trip-tickets.dto";

/**
 * The ticket catalog's admin shapes: a product as the editor reads and writes
 * it, the problems a write is refused with, and the dry run's verdict.
 * Contract: docs/frontend/trip-regions.md#editing-the-catalog.
 *
 * The write body's fields are only whitelisted here (`@Allow`): every check,
 * nested ones included, is `validateTicketProduct`'s, which names each
 * problem by its path so the form can put it beside the field.
 */

/** The research behind a product. Admin only. */
export class TicketSourceDto {
  @ApiProperty({ example: "https://disneyworld.disney.go.com/faq/tickets/" })
  url: string;
  @ApiPropertyOptional({ example: "Ticket Expiration Timeline" })
  title?: string;
  @ApiPropertyOptional({ example: "2026-11-13" }) accessed?: string;
  @ApiPropertyOptional() note?: string;
}

/** A product as the admin reads it: the public one and what only the editor sees. */
export class AdminTicketProductDto extends TripTicketProductDto {
  @ApiProperty({
    example: "wdw-date-based",
    description: "The product's id in the contract (`id`); fixed after create.",
  })
  slug: string;
  @ApiProperty({ example: "orlando" }) region: string;
  @ApiProperty({
    description: "False once archived: off the catalog route, never suggested.",
  })
  active: boolean;
  @ApiProperty({ description: "The admin's order; the catalog lists by it." })
  sortOrder: number;
  @ApiProperty({
    nullable: true,
    type: Number,
    description:
      "The order plan first tries a resort's products in, lower first; null: never suggested.",
  })
  suggestRank: number | null;
  @ApiProperty({ type: () => [TicketSourceDto] }) sources: TicketSourceDto[];
  @ApiProperty() createdAt: string;
  @ApiProperty() updatedAt: string;
  @ApiPropertyOptional({ description: "The admin who wrote it last." })
  updatedBy?: string;
}

export class AdminTicketCatalogDto {
  @ApiProperty({ example: "orlando" }) region: string;
  @ApiProperty({
    description: "The catalog's version, as the public route gives it.",
  })
  version: string;
  @ApiProperty({ type: () => [AdminTicketProductDto] })
  tickets: AdminTicketProductDto[];
}

/**
 * A product to create, or the fields of a PATCH (all optional there; the
 * merged product is checked whole). `id`, `shopUrl` and the stamps are not
 * written: `shopUrl` is the official link's url.
 */
export class TicketProductWriteDto {
  @ApiPropertyOptional({
    example: "wdw-date-based",
    description:
      "Required on create, kebab-case, at most 60; a PATCH may not change it.",
  })
  @Allow()
  slug?: unknown;
  @ApiPropertyOptional({
    example: "Walt Disney World date-based theme park ticket",
  })
  @Allow()
  name?: unknown;
  @ApiPropertyOptional() @Allow() description?: unknown;
  @ApiPropertyOptional({ example: "walt-disney-world" })
  @Allow()
  resort?: unknown;
  @ApiPropertyOptional({
    type: [String],
    description:
      "Codes of the region's `ticketMarkets` (Orlando: us, uk, intl).",
  })
  @Allow()
  markets?: unknown;
  @ApiPropertyOptional({ type: [String] }) @Allow() parks?: unknown;
  @ApiPropertyOptional({
    oneOf: [
      { $ref: "#/components/schemas/TripTicketDaysRangeDto" },
      { type: "string", enum: ["unlimited"] },
    ],
  })
  @Allow()
  days?: unknown;
  @ApiPropertyOptional({ type: () => [TripTicketWindowDto] })
  @Allow()
  windowTable?: unknown;
  @ApiPropertyOptional() @Allow() windowDays?: unknown;
  @ApiPropertyOptional({ enum: ["start", "first_use"] })
  @Allow()
  anchor?: unknown;
  @ApiPropertyOptional({ enum: ["none", "optional", "included"] })
  @Allow()
  hopping?: unknown;
  @ApiPropertyOptional({ type: () => [TripTicketHopSetDto] })
  @Allow()
  hopSets?: unknown;
  @ApiPropertyOptional({ type: () => [TripTicketCapDto] })
  @Allow()
  caps?: unknown;
  @ApiPropertyOptional({ type: () => TripTicketAllowanceDto })
  @Allow()
  allowance?: unknown;
  @ApiPropertyOptional({ type: [String] }) @Allow() reservedDayParks?: unknown;
  @ApiPropertyOptional({ type: () => [TripTicketOnSaleDto] })
  @Allow()
  onSale?: unknown;
  @ApiPropertyOptional({ example: "2027-12-31" }) @Allow() until?: unknown;
  @ApiPropertyOptional() @Allow() pass?: unknown;
  @ApiPropertyOptional() @Allow() blockouts?: unknown;
  @ApiPropertyOptional() @Allow() blockoutUrl?: unknown;
  @ApiPropertyOptional({ example: "15:00" }) @Allow() entryFrom?: unknown;
  @ApiPropertyOptional({
    type: [String],
    enum: ["weekday", "weekend", "holiday"],
  })
  @Allow()
  validDays?: unknown;
  @ApiPropertyOptional() @Allow() reservationUrl?: unknown;
  @ApiPropertyOptional({ enum: ["high", "medium", "low"] })
  @Allow()
  confidence?: unknown;
  @ApiPropertyOptional() @Allow() notes?: unknown;
  @ApiPropertyOptional({ type: () => [TripTicketLinkDto] })
  @Allow()
  links?: unknown;
  @ApiPropertyOptional({ type: () => [TicketSourceDto] })
  @Allow()
  sources?: unknown;
  @ApiPropertyOptional() @Allow() active?: unknown;
  @ApiPropertyOptional() @Allow() sortOrder?: unknown;
  @ApiPropertyOptional({ nullable: true, type: Number })
  @Allow()
  suggestRank?: unknown;
}

export class TicketArchiveDto {
  @ApiPropertyOptional({ description: "Why, for the audit log." })
  @IsOptional()
  @IsString()
  @MaxLength(500)
  reason?: string;
}

export class TicketOrderDto {
  @ApiProperty({
    type: [String],
    description: "Every product slug of the region, in the new order.",
  })
  @IsArray()
  @ArrayMaxSize(500)
  @IsString({ each: true })
  slugs: string[];
}

/** One reason a product was refused, by the path of the field it is about. */
export class TicketProblemDto {
  @ApiProperty({ example: "windowTable[2].window" }) path: string;
  @ApiProperty({ example: "must be at least its days (3)" }) message: string;
}

export class TicketDryRunDto {
  @ApiProperty({ enum: ["ok", "failed", "skipped"] })
  owned: "ok" | "failed" | "skipped";
  @ApiProperty({ enum: ["ok", "failed", "skipped"] })
  planFirst: "ok" | "failed" | "skipped";
  @ApiPropertyOptional({ description: "Why a run was skipped." }) note?: string;
}

/** `POST …/tickets/validate`: the checks and the dry run, nothing saved. */
export class TicketValidationDto {
  @ApiProperty() ok: boolean;
  @ApiProperty({ type: () => [TicketProblemDto] }) problems: TicketProblemDto[];
  @ApiPropertyOptional({
    type: () => TicketDryRunDto,
    description:
      "Absent while the checks fail: a dry run needs a valid product.",
  })
  dryRun?: TicketDryRunDto;
}
