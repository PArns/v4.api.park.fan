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
  @ApiPropertyOptional({
    example: "DL",
    description:
      "A short curated code for tight views, unique in the region (Tokyo Disneyland DL, Tokyo DisneySea DS); absent: derive one.",
  })
  code?: string;
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

/** Every transport mode a leg or an alternative names. */
export const TRANSPORT_MODES = [
  "monorail",
  "skyliner",
  "boat",
  "bus",
  "walk",
  "train",
  "shinkansen",
  "rail",
  "metro",
  "highway_bus",
  "shuttle",
];

export class TripTransportAlternativeDto {
  @ApiProperty({ enum: TRANSPORT_MODES })
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
  @ApiProperty({ enum: ["drive", ...TRANSPORT_MODES] }) mode: string;
  @ApiPropertyOptional({
    description: "A line or service, where a rail leg has one.",
    example: "Keiyo Line",
  })
  name?: string;
  @ApiPropertyOptional({
    enum: ["curated"],
    description:
      "A leg whose figures are not the region's `travel.source`: curated in an OSRM region.",
  })
  source?: string;
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
  @ApiProperty({
    enum: ["osrm", "curated"],
    description:
      "osrm: a drive without traffic from OpenStreetMap; curated: legs from timetables (a region travelled by train).",
  })
  source: string;
  @ApiProperty({ example: "2026-11-12" }) computedOn: string;
  @ApiPropertyOptional({ example: "© OpenStreetMap contributors" })
  attribution?: string;
  @ApiProperty({ enum: ["free_flow", "timetable"] }) basis: string;
  @ApiProperty({ type: () => [TripLegDto] }) legs: TripLegDto[];
}

export class TripHotelKindDto {
  @ApiProperty({ example: "disney" }) key: string;
  @ApiProperty({
    example: ["early_entry"],
    description:
      "The perks a stay at such a hotel carries: the codes a plan day's `perks` and `reasons` name.",
  })
  perks: string[];
}

export class TripTicketMarketsDto {
  @ApiProperty({
    example: "us",
    description:
      "Sold to anyone at the operator's own shop: what plan first suggests from.",
  })
  home: string;
  @ApiProperty({ example: ["us", "uk", "intl"] }) codes: string[];
}

export class TripStormSeasonDto {
  @ApiProperty({ enum: ["hurricane", "typhoon"] }) kind: string;
  @ApiProperty({
    example: "06-01",
    description: "The season's first day, MM-DD.",
  })
  from: string;
  @ApiProperty({ example: "11-30", description: "Its last day, MM-DD." })
  to: string;
  @ApiProperty({ example: { from: "08-15", to: "10-15" } })
  peak: { from: string; to: string };
}

export class TripSeasonNoteDto {
  @ApiProperty({ enum: ["rainy_season"] }) kind: string;
  @ApiProperty({ example: "06-01" }) from: string;
  @ApiProperty({ example: "07-20" }) to: string;
}

export class TripRegionWeatherDto {
  @ApiPropertyOptional({ type: () => TripStormSeasonDto })
  storm?: TripStormSeasonDto;
  @ApiPropertyOptional({ type: () => [TripSeasonNoteDto] })
  notes?: TripSeasonNoteDto[];
  @ApiProperty({
    example: "magic-kingdom-park",
    description:
      "The park whose climate normals stand for the region's months.",
  })
  climatePark: string;
}

export class TripGatewayDto {
  @ApiProperty({ example: "HND", description: "IATA code where there is one." })
  code: string;
  @ApiProperty({ example: "Haneda" }) name: string;
  @ApiProperty({ enum: ["airport", "station"] }) kind: string;
  @ApiPropertyOptional({ description: "Typical minutes to the base." })
  minutes?: number;
  @ApiProperty({
    type: [String],
    example: ["disneyland-paris", "parc-asterix", "paris"],
    description:
      "Every base that lists this gateway. Map it to startBase or endBase only where this is one base; otherwise send neither and the plan picks.",
  })
  bases: string[];
}

export class TripBaseReachDto {
  @ApiProperty({ example: "tokyo-disneyland" }) slug: string;
  @ApiProperty({ example: 20, description: "Door to door, one way, curated." })
  minutes: number;
  @ApiProperty({ example: "rail" }) mode: string;
}

export class TripBaseDto {
  @ApiProperty({ example: "tokyo" }) key: string;
  @ApiProperty({ example: "Tokyo" }) name: string;
  @ApiPropertyOptional({
    description:
      "Its place on the route line; stays follow it one way or the other. Absent: any order.",
  })
  order?: number;
  @ApiProperty({ type: [String] }) hotelAreas: string[];
  @ApiPropertyOptional() defaultArea?: string;
  @ApiPropertyOptional({ type: () => [TripGatewayDto] })
  gateways?: TripGatewayDto[];
  @ApiPropertyOptional({ description: "Where to stay, curated, English." })
  stayNote?: string;
  @ApiProperty({ type: () => [TripBaseReachDto] }) parks: TripBaseReachDto[];
}

export class TripBaseLegDto {
  @ApiProperty({ example: "tokyo" }) from: string;
  @ApiProperty({ example: "nagoya" }) to: string;
  @ApiProperty({ example: 150, description: "Door to door, curated." })
  minutes: number;
  @ApiProperty({ example: "shinkansen" }) mode: string;
  @ApiPropertyOptional({ example: "Tokaido Shinkansen" }) name?: string;
  @ApiProperty({ description: "A park fits the day of this move." })
  parkDay: boolean;
  @ApiPropertyOptional() changes?: number;
  @ApiPropertyOptional({ example: "Shinjuku" }) fromStop?: string;
  @ApiPropertyOptional({ example: "Kawaguchiko" }) toStop?: string;
  @ApiPropertyOptional({
    type: [String],
    enum: ["jr_pass", "reserve_seat", "forward_luggage"],
  })
  notes?: string[];
}

export class TripStayRulesDto {
  @ApiProperty({ example: 45 }) dayTripFreeMinutes: number;
  @ApiProperty({ example: 60 }) moveOverheadMinutes: number;
  @ApiProperty({ example: 150 }) parkDayMaxLegMinutes: number;
  @ApiProperty({ example: 20 }) moveCost: number;
  @ApiProperty({ example: "08:00" }) moveStartsAt: string;
  @ApiProperty({ example: "23:00" }) lastArrivalAt: string;
  @ApiProperty({
    example: [30, 60, 90, 120],
    description:
      "The buffers a visitor may choose for a hotel change (`moveBuffer=`); `moveOverheadMinutes` is the default.",
  })
  moveBufferOptions: number[];
}

export class TripRegionSummaryDto {
  @ApiProperty({ example: "orlando" }) slug: string;
  @ApiProperty({ example: "Orlando" }) name: string;
  @ApiProperty({ example: "America/New_York" }) timezone: string;
  @ApiProperty({
    example: 16,
    description: "The parks the planner places: the region route's `parks`.",
  })
  parkCount: number;
  @ApiProperty({
    example: 21,
    description: "The longest trip it plans, in days.",
  })
  maxTripDays: number;
  @ApiProperty({ type: () => [TripRegionGroupDto] })
  groups: TripRegionGroupDto[];
}

export class TripRegionListDto {
  @ApiProperty({ type: () => [TripRegionSummaryDto] })
  regions: TripRegionSummaryDto[];
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
    type: () => [TripHotelKindDto],
    description:
      "The `hotel=` values the plan takes in this region, with the perks each carries.",
  })
  hotels: TripHotelKindDto[];
  @ApiProperty({ type: () => TripTicketMarketsDto })
  ticketMarkets: TripTicketMarketsDto;
  @ApiProperty({
    type: () => TripRegionWeatherDto,
    description: "What the weather does to the region's days.",
  })
  weather: TripRegionWeatherDto;
  @ApiPropertyOptional({
    type: () => [TripBaseDto],
    description:
      "A region whose parks lie in several cities: the bases a trip stays in.",
  })
  bases?: TripBaseDto[];
  @ApiPropertyOptional({ type: () => [TripBaseLegDto] })
  baseLegs?: TripBaseLegDto[];
  @ApiPropertyOptional({ type: () => TripStayRulesDto })
  stayRules?: TripStayRulesDto;
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
      "region, from, to, computedAt, objective, exact, relaxed, split, ticket, hopper, tickets " +
      "(TripPlanTicketDto[]), mixing ({ used, savedMinutesPerRide? }), ticketSuggestion " +
      "(TripTicketSuggestionDto[]), warnings",
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
  @ApiProperty({
    type: "array",
    items: { type: "object" },
    description:
      "The chosen parks the plan has no visit of, in the region's order: { park, reason } with " +
      "reason no_day_left (the trip's park days went to the other parks), no_open_day, " +
      "no_ticket_day or no_base. Empty when every chosen park has a day.",
    example: [
      { park: "epcot", reason: "no_day_left" },
      { park: "universal-epic-universe", reason: "no_day_left" },
    ],
  })
  parksLeftOut: Array<{ park: string; reason: string }>;
  @ApiPropertyOptional({ type: "array", items: { type: "object" } })
  ignoredLocks?: Array<Record<string, unknown>>;
}
