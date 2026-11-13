import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger";

/**
 * The ticket catalog on `GET /v1/trip-regions/:region` and the ticket fields of
 * the plan, for Swagger. The contract and every rule behind a field:
 * docs/frontend/trip-regions.md#tickets.
 */

export class TripTicketDaysRangeDto {
  @ApiProperty({ example: 1 }) min: number;
  @ApiProperty({ example: 10 }) max: number;
}

export class TripTicketWindowDto {
  @ApiProperty({ description: "Park days of the ticket.", example: 4 })
  days: number;
  @ApiProperty({
    description:
      "Calendar days its use may spread over, from the start date (the start is day 1).",
    example: 7,
  })
  window: number;
}

export class TripTicketHopSetDto {
  @ApiProperty({
    type: [String],
    description: "Parks one hop day or hop evening may combine.",
  })
  parks: string[];
  @ApiPropertyOptional({
    type: () => TripTicketDaysRangeDto,
    description: "The ticket lengths the set applies to; absent: every length.",
  })
  ticketDays?: TripTicketDaysRangeDto;
}

export class TripTicketCapDto {
  @ApiProperty({ type: [String] }) parks: string[];
  @ApiProperty({
    description:
      "At most this many days on which any of `parks` is visited (a hop day between two is one). " +
      "0: the length does not cover them.",
  })
  max: number;
  @ApiPropertyOptional({ type: () => TripTicketDaysRangeDto })
  ticketDays?: TripTicketDaysRangeDto;
}

export class TripTicketAllowanceDto {
  @ApiProperty({ type: [String] }) parks: string[];
  @ApiProperty({
    description:
      'Visits on the allowance: a number, or "days" (as many as the ticket\'s days).',
    oneOf: [{ type: "number" }, { type: "string", enum: ["days"] }],
  })
  visits: number | "days";
}

export class TripTicketOnSaleDto {
  @ApiPropertyOptional({ example: "2027-01-03" }) from?: string;
  @ApiPropertyOptional({ example: "2027-05-22" }) to?: string;
}

export class TripTicketProductDto {
  @ApiProperty({ example: "wdw-date-based" }) id: string;
  @ApiProperty({
    example: "Walt Disney World date-based ticket",
    description: "The operator's product name, not translated.",
  })
  name: string;
  @ApiProperty({
    example: "walt-disney-world",
    description: "The region group whose operator sells it.",
  })
  resort: string;
  @ApiProperty({
    type: [String],
    enum: ["us", "uk", "intl"],
    description:
      "us: sold to anyone; intl: by sellers abroad; uk: UK and Ireland residents only.",
  })
  markets: Array<"us" | "uk" | "intl">;
  @ApiProperty({ type: [String], description: "Parks a ticket day covers." })
  parks: string[];
  @ApiProperty({
    description: 'The lengths on sale, or "unlimited" for a pass.',
    oneOf: [
      { $ref: "#/components/schemas/TripTicketDaysRangeDto" },
      { type: "string", enum: ["unlimited"] },
    ],
  })
  days: TripTicketDaysRangeDto | "unlimited";
  @ApiPropertyOptional({ type: () => [TripTicketWindowDto] })
  windowTable?: TripTicketWindowDto[];
  @ApiPropertyOptional({
    description:
      "The window of a product without a day count (SeaWorld's Four Parks: 14).",
  })
  windowDays?: number;
  @ApiProperty({ enum: ["start", "first_use"] }) anchor: "start" | "first_use";
  @ApiProperty({ enum: ["none", "optional", "included"] })
  hopping: "none" | "optional" | "included";
  @ApiPropertyOptional({ type: () => [TripTicketHopSetDto] })
  hopSets?: TripTicketHopSetDto[];
  @ApiPropertyOptional({ type: () => [TripTicketCapDto] })
  caps?: TripTicketCapDto[];
  @ApiPropertyOptional({
    type: () => TripTicketAllowanceDto,
    description: "Visits on a separate allowance that use no ticket day.",
  })
  allowance?: TripTicketAllowanceDto;
  @ApiPropertyOptional({
    type: [String],
    description:
      "Parks used only on the start date (Discovery Cove's reserved day).",
  })
  reservedDayParks?: string[];
  @ApiPropertyOptional({
    type: () => [TripTicketOnSaleDto],
    description: "The start dates it is sold for; absent: no limit known.",
  })
  onSale?: TripTicketOnSaleDto[];
  @ApiPropertyOptional({
    description: "The last day a pass or Fun Card is valid.",
  })
  until?: string;
  @ApiPropertyOptional({
    description: "An annual pass or Fun Card: unlimited days, no window.",
  })
  pass?: true;
  @ApiPropertyOptional({
    description: "Blockout dates apply; the plan does not check them.",
  })
  blockouts?: true;
  @ApiPropertyOptional({
    description: "The operator's page with the blockout calendar.",
  })
  blockoutUrl?: string;
  @ApiPropertyOptional({
    description: "Where passholders reserve their park (Disney).",
  })
  reservationUrl?: string;
  @ApiProperty({ enum: ["high", "medium", "low"] })
  confidence: "high" | "medium" | "low";
  @ApiProperty() shopUrl: string;
  @ApiPropertyOptional() notes?: string;
}

export class TripTicketHopDto {
  @ApiProperty({ example: "2027-02-11" }) date: string;
  @ApiProperty({
    type: [String],
    description:
      "A hop day's two parks; an evening's day park, then the evening's.",
  })
  parks: string[];
}

export class TripTicketHopperDto {
  @ApiProperty() recommended: boolean;
  @ApiProperty({
    description: "Headliner-wait sum saved over the visits both plans place.",
  })
  savedMinutes: number;
  @ApiProperty({ description: "The same per headliner ride." })
  savedMinutesPerRide: number;
  @ApiProperty() hopDays: number;
  @ApiProperty() eveningHops: number;
  @ApiProperty() fitsMoreParks: number;
  @ApiProperty({ type: () => [TripTicketHopDto] })
  hopDayList: TripTicketHopDto[];
  @ApiProperty({ type: () => [TripTicketHopDto] })
  eveningHopList: TripTicketHopDto[];
  @ApiProperty({
    type: [String],
    description: "The parks of the visits only the hopper plan places.",
  })
  fitsOnlyWithHopper: string[];
  @ApiProperty({
    type: [String],
    enum: [
      "evening_hops",
      "hop_days",
      "fits_more_parks",
      "saves_minutes",
      "no_gain",
    ],
  })
  why: string[];
}

/** `meta.tickets[]` of a plan: an owned ticket as the plan used it. */
export class TripPlanTicketDto {
  @ApiProperty({ example: "t1", description: "t1 … t6 in the order given." })
  id: string;
  @ApiProperty({ example: "wdw-uk-14-day-magic" }) product: string;
  @ApiPropertyOptional({ description: "Absent for a pass." }) days?: number;
  @ApiProperty({
    oneOf: [{ type: "boolean" }, { type: "string", enum: ["unknown"] }],
    description:
      '"unknown" for `.h?`: planned without hopping, with `hopperVerdict`.',
  })
  hopper: boolean | "unknown";
  @ApiPropertyOptional({
    type: () => TripTicketHopperDto,
    description: 'What the hopper would add, for `hopper: "unknown"`.',
  })
  hopperVerdict?: TripTicketHopperDto;
  @ApiPropertyOptional({
    description:
      "The window's first day: given, or with `startChosen` picked by the plan. Absent for a pass.",
  })
  start?: string;
  @ApiPropertyOptional({ description: "The plan picked `start`." })
  startChosen?: true;
  @ApiPropertyOptional({ description: "The window's last day." }) end?: string;
  @ApiProperty() usedDays: number;
  @ApiPropertyOptional() unusedDays?: number;
  @ApiPropertyOptional({
    enum: [
      "window_outside_trip",
      "no_open_day",
      "rest_days_kept",
      "parks_fewer_than_days",
    ],
  })
  unusedReason?: string;
  @ApiPropertyOptional({ description: "Visits on the allowance." })
  allowanceVisits?: number;
  @ApiProperty({
    type: [String],
    enum: [
      "blockouts_unchecked",
      "park_reservation_needed",
      "covers_no_planned_park",
    ],
  })
  warnings: string[];
}

export class TripTicketAlternativeDto {
  @ApiProperty({ example: "wdw-uk-14-day-magic" }) product: string;
  @ApiProperty({ example: 14 }) days: number;
  @ApiProperty({ type: [String], example: ["uk"] }) markets: string[];
  @ApiProperty({ enum: ["long_stay", "promotion"] })
  reason: "long_stay" | "promotion";
}

/** `meta.ticketSuggestion[]`: plan first's ticket for a resort. */
export class TripTicketSuggestionDto {
  @ApiProperty({ example: "suggested:walt-disney-world" }) id: string;
  @ApiProperty({ example: "walt-disney-world" }) resort: string;
  @ApiProperty({ example: "wdw-date-based" }) product: string;
  @ApiProperty({ example: 4 }) days: number;
  @ApiProperty({ example: "2027-02-02" }) start: string;
  @ApiProperty({ example: "2027-02-08", description: "The window's last day." })
  end: string;
  @ApiProperty({ type: [String], description: "The planned parks it covers." })
  parks: string[];
  @ApiPropertyOptional({
    description:
      "Days beyond the park days, for a window that reaches the last one.",
  })
  extraDays?: number;
  @ApiPropertyOptional({
    type: () => TripTicketHopperDto,
    description:
      "What the hopper option adds; products with an optional hopper only.",
  })
  hopper?: TripTicketHopperDto;
  @ApiProperty() shopUrl: string;
  @ApiPropertyOptional({ type: () => [TripTicketAlternativeDto] })
  alternatives?: TripTicketAlternativeDto[];
  @ApiPropertyOptional({
    type: [String],
    description:
      "Owned tickets of the resort that do not cover these parks (Epic Universe with a pass).",
  })
  notCoveredBy?: string[];
}
