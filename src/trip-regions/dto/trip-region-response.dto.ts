import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger";
import { TripTicketProductDto } from "./trip-tickets.dto";

/**
 * The three trip-region answers, for Swagger. The contract and every rule
 * behind a field: docs/frontend/trip-regions.md.
 */

export class TripRegionGroupDto {
  @ApiProperty({ example: "walt-disney-world" }) key: string;
  @ApiProperty({ example: "Walt Disney World Resort" }) name: string;
  @ApiProperty({ enum: ["resort", "day_trip"] }) kind: "resort" | "day_trip";
}

export class TripRegionParkDto {
  @ApiProperty() id: string;
  @ApiProperty({ example: "magic-kingdom-park" }) slug: string;
  @ApiProperty({ example: "Magic Kingdom Park" }) name: string;
  @ApiProperty({
    example: "/parks/north-america/united-states/orlando/magic-kingdom-park",
  })
  path: string;
  @ApiProperty({ example: "walt-disney-world" }) group: string;
  @ApiProperty({ enum: ["theme_park", "water_park", "reservation_park"] })
  type: string;
  @ApiProperty({ description: "One of the region's default picks." })
  headliner: boolean;
  @ApiProperty() latitude: number;
  @ApiProperty() longitude: number;
  @ApiProperty({ example: "America/New_York" }) timezone: string;
}

export class TripHotelAreaDto {
  @ApiProperty({ example: "disney" }) key: string;
  @ApiProperty() name: string;
  @ApiProperty() latitude: number;
  @ApiProperty() longitude: number;
  @ApiProperty({
    description: "The perks of the hotel a stay in this area usually means.",
    example: ["early_entry"],
  })
  perks: string[];
}

export class TripVenueHoursDto {
  @ApiProperty({ example: "10:00" }) opens: string;
  @ApiProperty({ example: "23:00" }) closes: string;
}

export class TripVenueDto {
  @ApiProperty({ example: "disney-springs" }) key: string;
  @ApiProperty({ example: "Disney Springs" }) name: string;
  @ApiProperty({ example: "walt-disney-world" }) group: string;
  @ApiProperty({ enum: ["district", "viewpoint", "resort_area"] })
  kind: "district" | "viewpoint" | "resort_area";
  @ApiProperty() latitude: number;
  @ApiProperty() longitude: number;
  @ApiPropertyOptional({
    type: () => TripVenueHoursDto,
    description:
      "Typical park-local hours, not the day's: the venue's own page has those.",
  })
  typicalHours?: TripVenueHoursDto;
  @ApiProperty({ description: "No admission to walk in." }) free: boolean;
  @ApiProperty() url: string;
  @ApiPropertyOptional() notes?: string;
  @ApiPropertyOptional({
    description: "A fireworks view: the park whose fireworks it looks at.",
    example: "epcot",
  })
  viewOf?: string;
}

export class TripTransportAlternativeDto {
  @ApiProperty({
    enum: ["monorail", "skyliner", "boat", "bus", "walk", "train"],
  })
  mode: string;
  @ApiPropertyOptional({
    description: "A curated estimate, waiting included; absent for the buses.",
  })
  minutes?: number;
  @ApiPropertyOptional({ example: "ttc" }) via?: string;
  @ApiPropertyOptional({ example: "Hogwarts Express" }) name?: string;
  @ApiPropertyOptional({ enum: ["park_to_park"] }) ticket?: string;
}

export class TripLegDto {
  @ApiProperty({ example: "magic-kingdom-park" }) from: string;
  @ApiProperty({ example: "epcot" }) to: string;
  @ApiPropertyOptional({ example: 6.5 }) km?: number;
  @ApiProperty({ example: 10 }) minutes: number;
  @ApiProperty({ enum: ["drive", "walk", "bus"] }) mode: string;
  @ApiPropertyOptional({
    description:
      "Where a drive ends short of the gate (`ttc`, `garage_shuttle`).",
  })
  via?: string;
  @ApiPropertyOptional({
    description: "The curated minutes from that car park to the gate.",
  })
  transferMinutes?: number;
  @ApiPropertyOptional({ type: () => [TripTransportAlternativeDto] })
  alternatives?: TripTransportAlternativeDto[];
}

export class TripTravelDto {
  @ApiProperty({ enum: ["osrm"] }) source: string;
  @ApiProperty({ example: "2026-11-12" }) computedOn: string;
  @ApiProperty({ example: "© OpenStreetMap contributors" }) attribution: string;
  @ApiProperty({ enum: ["free_flow"] }) basis: string;
  @ApiProperty({ type: () => [TripLegDto] }) legs: TripLegDto[];
}

export class TripRegionDto {
  @ApiProperty({ example: "orlando" }) slug: string;
  @ApiProperty({ example: "Orlando" }) name: string;
  @ApiProperty({ example: "America/New_York" }) timezone: string;
  @ApiProperty({ type: () => [TripRegionGroupDto] })
  groups: TripRegionGroupDto[];
  @ApiProperty({ type: () => [TripRegionParkDto] }) parks: TripRegionParkDto[];
  @ApiProperty({ type: () => [TripHotelAreaDto] })
  hotelAreas: TripHotelAreaDto[];
  @ApiProperty({
    type: () => [TripVenueDto],
    description:
      "Places for an evening outside the parks; travel legs to them are `venue:<key>`.",
  })
  venues: TripVenueDto[];
  @ApiProperty({ type: () => TripTravelDto }) travel: TripTravelDto;
  @ApiProperty({
    type: () => [TripTicketProductDto],
    description:
      "The ticket catalog: what the operators sell, the window, hopping and caps of each " +
      "(docs/frontend/trip-regions.md#tickets). No prices.",
  })
  tickets: TripTicketProductDto[];
}

export class TripSlotDto {
  @ApiProperty({
    enum: [
      "early_entry",
      "early_park_admission",
      "extended_evening",
      "ticketed_event",
    ],
  })
  kind: string;
  @ApiProperty({ example: "2026-11-29T19:00:00-05:00" }) opensAt: string;
  @ApiProperty({ example: "2026-11-30T00:00:00-05:00" }) closesAt: string;
  @ApiPropertyOptional({
    enum: ["season"],
    description:
      "The feed lost this evening's row; a curated season supplies it.",
  })
  source?: string;
}

export class TripParkDayDto {
  @ApiProperty({ enum: ["OPERATING", "CLOSED", "UNKNOWN"] }) status: string;
  @ApiProperty() hoursPublished: boolean;
  @ApiPropertyOptional() opensAt?: string;
  @ApiPropertyOptional({ description: "The day guests' close." })
  closesAt?: string;
  @ApiPropertyOptional({ type: () => TripSlotDto }) early?: TripSlotDto;
  @ApiPropertyOptional({ type: () => TripSlotDto }) evening?: TripSlotDto;
  @ApiProperty({ example: "low" }) crowdLevel: string;
  @ApiPropertyOptional({
    description:
      "Mean expected standby of the park's headliners, rounded to 5.",
  })
  headlinerWait?: number;
  @ApiProperty({ enum: ["forecast", "long_range", "last_year", "none"] })
  basis: string;
  @ApiPropertyOptional({ type: [String] }) seasons?: string[];
  @ApiPropertyOptional({
    description: "The curated closure season behind a CLOSED day.",
  })
  closedBy?: string;
  @ApiPropertyOptional({
    description:
      "The day guests leave at least two hours before the park's median close in the window.",
  })
  closesEarly?: true;
  @ApiPropertyOptional({
    description: "The day guests' close on such a day (park offset).",
  })
  dayGuestCloseAt?: string;
}

export class TripCalendarDayDto {
  @ApiProperty({ example: "2026-11-29" }) date: string;
  @ApiProperty() isHoliday: boolean;
  @ApiProperty() isSchoolVacation: boolean;
  @ApiPropertyOptional() schoolVacationUnknown?: boolean;
  @ApiPropertyOptional({ type: [String] }) holidays?: string[];
  @ApiPropertyOptional({
    description:
      "Per resort group, the park of the day by the plan's day cost (the unrounded headliner " +
      "wait, dearer where day guests leave early). Absent on last-year days.",
    type: "object",
    additionalProperties: { type: "string" },
  })
  best?: Record<string, string>;
  @ApiPropertyOptional({
    description:
      "Per group in `best`: `shortest_wait`, or `closes_early_elsewhere` when the lowest raw " +
      "wait was a park that closes early.",
    type: "object",
    additionalProperties: {
      type: "string",
      enum: ["shortest_wait", "closes_early_elsewhere"],
    },
  })
  bestWhy?: Record<string, "shortest_wait" | "closes_early_elsewhere">;
  @ApiPropertyOptional({
    description:
      "A pick in `best` is on a day whose hours are not published yet: chosen on its forecast, " +
      "priced as the park's usual day.",
  })
  bestHoursUnpublished?: true;
  @ApiProperty({
    description: "One entry per region park, keyed by slug.",
    type: "object",
    additionalProperties: { $ref: "#/components/schemas/TripParkDayDto" },
  })
  parks: Record<string, TripParkDayDto>;
}

export class TripCalendarMetaDto {
  @ApiProperty() region: string;
  @ApiProperty() timezone: string;
  @ApiProperty() from: string;
  @ApiProperty() to: string;
  @ApiProperty() computedAt: string;
  @ApiPropertyOptional({
    description: "The last day any of the region's parks carries a forecast.",
  })
  forecastUntil?: string;
  @ApiProperty({
    description: "Per park, its last published operating day (null: none).",
    type: "object",
    additionalProperties: { type: "string", nullable: true },
  })
  publishedHoursUntil: Record<string, string | null>;
}

export class TripCalendarDto {
  @ApiProperty({ type: () => TripCalendarMetaDto }) meta: TripCalendarMetaDto;
  @ApiProperty({ type: [String] }) parks: string[];
  @ApiProperty({ type: () => [TripCalendarDayDto] }) days: TripCalendarDayDto[];
  @ApiProperty({
    description: "The curated seasons overlapping the window (park_seasons).",
    type: "array",
    items: { type: "object" },
  })
  seasons: Array<Record<string, unknown>>;
}

export class TripPlanDto {
  @ApiProperty({
    description:
      "region, from, to, computedAt, objective, exact, relaxed, split, ticket, tickets " +
      "(TripPlanTicketDto[]), ticketSuggestion (TripTicketSuggestionDto[]), warnings",
    type: "object",
    additionalProperties: true,
  })
  meta: Record<string, unknown>;
  @ApiProperty({
    description:
      "One entry per trip day: arrival | park | hop | rest | departure. A park day that " +
      "ends early carries `evening` (a hop park or a venue, with up to two alternatives). " +
      "A park or hop day carries `ticket` (an owned ticket's id or a suggestion's) and " +
      "`ticketDay`, or `ticketAllowance` on an allowance visit.",
    type: "array",
    items: { type: "object" },
  })
  days: Array<Record<string, unknown>>;
  @ApiProperty({ type: "array", items: { type: "object" } })
  parks: Array<Record<string, unknown>>;
  @ApiProperty({ type: "object", additionalProperties: true })
  totals: Record<string, unknown>;
  @ApiProperty({ type: "array", items: { type: "object" } })
  unplaced: Array<Record<string, unknown>>;
  @ApiPropertyOptional({ type: "array", items: { type: "object" } })
  ignoredLocks?: Array<Record<string, unknown>>;
}
