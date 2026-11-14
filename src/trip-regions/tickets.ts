/**
 * A trip region's ticket products and the helpers that read their rules:
 * which parks a ticket day covers, the calendar days a ticket's use may spread
 * over, its hopping and its caps. No prices.
 *
 * The catalog is data, edited in the admin (`ticket_products`,
 * `TicketCatalogService`), and reaches every reader on the region object
 * (`RegionConfig.tickets`). The list below is only its **seed**: the products
 * as researched for late 2026 and 2027 (PAR-1298), written to the table once
 * for a region that has no row, and the catalog of a reader without a
 * database (the dev shim, the specs). Where a rule could not be settled, an
 * entry keeps the side that cannot plan a day the ticket does not allow and
 * says so in `notes`. Products that differ only in price are one entry. The
 * contract and how the plan reads each field: docs/frontend/trip-regions.md#tickets.
 */
import { TICKET_SEED_LINKS, type SeedLinkEntry } from "./ticket-seed-links";
import { JAPAN_TICKETS } from "./tickets.japan";

/**
 * Whom a product or a link is sold to: a code of the region's
 * `ticketMarkets` (Orlando: `us` sold to anyone, `uk` UK and Ireland residents
 * only, `intl` sellers abroad).
 */
export type TicketMarket = string;

/** The kinds of day a ticket may be used on (`validDays`). */
export const TICKET_DAY_KINDS = ["weekday", "weekend", "holiday"] as const;
/**
 * `weekday`: Monday to Friday that is no public holiday; `weekend`: Saturday
 * and Sunday; `holiday`: a public holiday of the region's calendar.
 */
export type TicketDayKind = (typeof TICKET_DAY_KINDS)[number];

export interface TicketDaysRange {
  min: number;
  max: number;
}

export interface TicketWindowRow {
  days: number;
  /** Calendar days from the start date (the start is day 1). */
  window: number;
}

/** A rule that holds for some lengths of a product only; absent `ticketDays`: every length. */
interface ByLength {
  ticketDays?: TicketDaysRange;
}

export interface TicketHopSet extends ByLength {
  parks: string[];
}

export interface TicketCap extends ByLength {
  parks: string[];
  /** Days on which any of `parks` is visited; 0: the length does not cover them. */
  max: number;
}

/** Where a ticket is bought; exactly one link of a product is the operator's own shop. */
export interface TicketLink {
  /** The seller's own name, not translated. */
  seller: string;
  url: string;
  official?: true;
  /** An affiliate or paid link. */
  sponsored: boolean;
  /** Whom it is for; absent: everyone. */
  markets?: TicketMarket[];
  /** The site languages it is for; absent: every one. */
  locales?: string[];
}

/** The fields of a product the optimizer reads: `ticket_products.rules`. */
export const TICKET_RULE_KEYS = [
  "markets",
  "parks",
  "days",
  "windowTable",
  "windowDays",
  "anchor",
  "hopping",
  "hopSets",
  "caps",
  "allowance",
  "reservedDayParks",
  "onSale",
  "until",
  "pass",
  "blockouts",
  "entryFrom",
  "validDays",
] as const;

export type TicketRules = Pick<
  TicketProduct,
  (typeof TICKET_RULE_KEYS)[number]
>;

export interface TicketProduct {
  id: string;
  /** The operator's product name, not translated. */
  name: string;
  /** The region group whose operator sells it. */
  resort: string;
  markets: TicketMarket[];
  /** Parks a ticket day covers. */
  parks: string[];
  days: TicketDaysRange | "unlimited";
  windowTable?: TicketWindowRow[];
  /** The window of a product without a day count. */
  windowDays?: number;
  anchor: "start" | "first_use";
  hopping: "none" | "optional" | "included";
  hopSets?: TicketHopSet[];
  caps?: TicketCap[];
  /** Visits that use no ticket day: the UK Magic Tickets' and the Plus option's water parks. */
  allowance?: { parks: string[]; visits: number | "days" };
  /** Parks used only on the start date (Discovery Cove's reserved day). */
  reservedDayParks?: string[];
  /** Start dates it is sold for; absent: no limit known. */
  onSale?: Array<{ from?: string; to?: string }>;
  /** The last day a pass or Fun Card is valid. */
  until?: string;
  /** An annual pass or Fun Card: unlimited days, no window. */
  pass?: true;
  /** Blockout dates apply and are not checked. */
  blockouts?: true;
  /**
   * The ticket admits from this park-local time only (`HH:MM`): an evening
   * ticket, Tokyo Disney Resort's Early Evening Passport (15:00) or Weeknight
   * Passport (17:00). A day on it is the evening of the day. Never suggested
   * by plan first, which plans whole days.
   */
  entryFrom?: string;
  /** The kinds of day it may be used on; absent: every day. */
  validDays?: TicketDayKind[];
  /** The operator's page with the blockout calendar. */
  blockoutUrl?: string;
  /** Where passholders reserve their park. */
  reservationUrl?: string;
  confidence: "high" | "medium" | "low";
  /** The official link's url. */
  shopUrl: string;
  /** Where to buy it, in order; the seed has the official shop only. */
  links?: TicketLink[];
  /** A sentence about the product, in English. */
  description?: string;
  notes?: string;
  /**
   * Plan first: the order a resort's products are tried in when the plan
   * suggests one (lower first, among equally short ones). Absent: never
   * suggested. Not served.
   */
  suggest?: number;
  /**
   * Archived in the admin: off the catalog route and never suggested, but an
   * owned ticket may still name it.
   */
  archived?: true;
}

const MK = "magic-kingdom-park";
const EPCOT = "epcot";
const HS = "disneys-hollywood-studios";
const AK = "disneys-animal-kingdom-theme-park";
const TYPHOON_LAGOON = "disneys-typhoon-lagoon-water-park";
const BLIZZARD_BEACH = "disneys-blizzard-beach-water-park";
const USF = "universal-studios-florida";
const IOA = "universal-islands-of-adventure";
const EPIC = "universal-epic-universe";
const VOLCANO_BAY = "universal-volcano-bay";
const SEAWORLD = "seaworld-orlando";
const AQUATICA = "aquatica-orlando";
const DISCOVERY_COVE = "discovery-cove-orlando";
const LEGOLAND = "legoland-florida";
const BUSCH_GARDENS = "busch-gardens-tampa";
const ADVENTURE_ISLAND = "adventure-island-tampa";

const DISNEY = [MK, EPCOT, HS, AK];
const DISNEY_WATER = [TYPHOON_LAGOON, BLIZZARD_BEACH];

/**
 * Disney's ticket expiration timeline, the same for base and Park Hopper
 * (Disney's FAQ, quoted word for word in planDisney answer 610024; WDW Prep
 * School and Disney Park Nerds, Oct 2026): 1 day in 1, 2 in 4, 3 in 5, 4 in 7,
 * 5 in 8, 6 in 9, 7 in 10, 8 in 12, 9 in 13, 10 in 14.
 */
const DISNEY_WINDOWS: TicketWindowRow[] = [
  1, 4, 5, 7, 8, 9, 10, 12, 13, 14,
].map((window, i) => ({ days: i + 1, window }));

/** Universal's US tickets: N days in a run that starts on the selected date. */
const UNIVERSAL_WINDOWS: TicketWindowRow[] = [
  { days: 1, window: 1 },
  { days: 2, window: 4 },
  { days: 3, window: 5 },
  { days: 4, window: 6 },
  { days: 5, window: 7 },
];

const DISNEY_SHOP = "https://disneyworld.disney.go.com/admission/tickets/";
const DISNEY_UK_SHOP =
  "https://www.disneyholidays.co.uk/walt-disney-world/uk-ireland-exclusive-tickets/";
const UNIVERSAL_SHOP =
  "https://www.universalorlando.com/web/en/us/tickets-packages/park-tickets";
const UNIVERSAL_UK_SHOP = "https://ukstore.universalorlando.com/park-tickets";
const SEAWORLD_SHOP = "https://seaworld.com/orlando/tickets/";
const BUSCH_GARDENS_SHOP = "https://buschgardens.com/tampa/tickets/";
const DISCOVERY_COVE_SHOP = "https://discoverycove.com/orlando/pricing/";
const LEGOLAND_SHOP =
  "https://www.legoland.com/florida/tickets-passes/tickets/";

const fixed = (days: number, window: number) => ({
  days: { min: days, max: days },
  windowTable: [{ days, window }],
});

export const ORLANDO_TICKETS: TicketProduct[] = [
  // ── Walt Disney World ──────────────────────────────────────────────────
  {
    id: "wdw-date-based",
    name: "Walt Disney World date-based theme park ticket",
    resort: "walt-disney-world",
    markets: ["us", "uk", "intl"],
    parks: DISNEY,
    days: { min: 1, max: 10 },
    windowTable: DISNEY_WINDOWS,
    anchor: "start",
    hopping: "optional",
    hopSets: [{ parks: DISNEY }],
    confidence: "high",
    shopUrl: DISNEY_SHOP,
    notes:
      "The start date chosen at purchase is day 1 whether or not the guest goes in; unused days " +
      "are lost. A 1-day ticket is bought for one park. Park Hopper (.h): another park at any time " +
      "after entering the first; a hop day counts as one day. No park reservation since 2024-01-09.",
    suggest: 10,
  },
  {
    id: "wdw-date-based-plus",
    name: "Walt Disney World date-based ticket with Water Park and Sports (Park Hopper Plus with .h)",
    resort: "walt-disney-world",
    markets: ["us", "uk", "intl"],
    parks: DISNEY,
    days: { min: 2, max: 10 },
    windowTable: DISNEY_WINDOWS.filter((row) => row.days >= 2),
    anchor: "start",
    hopping: "optional",
    hopSets: [{ parks: DISNEY }],
    allowance: { parks: DISNEY_WATER, visits: "days" },
    confidence: "medium",
    shopUrl: DISNEY_SHOP,
    notes:
      "As many Plus visits (Typhoon Lagoon, Blizzard Beach, mini golf) as the ticket has days, " +
      "apart from the theme-park days. Disney's FAQ says the option adds a day to the window; a " +
      "planDisney panelist saw none on the ticket calendar, so the plan keeps the base window. " +
      "The water parks close in rotation; usually one is open.",
    suggest: 20,
  },
  {
    id: "wdw-4-park-magic",
    name: "Disney 4-Park Magic Ticket",
    resort: "walt-disney-world",
    markets: ["us", "uk"],
    parks: DISNEY,
    ...fixed(4, 6),
    anchor: "start",
    hopping: "none",
    caps: DISNEY.map((park) => ({ parks: [park], max: 1 })),
    onSale: [{ from: "2027-01-03", to: "2027-05-22" }],
    confidence: "medium",
    shopUrl:
      "https://disneyworld.disney.go.com/admission/tickets/four-park-magic-ticket-offer/",
    notes:
      "One visit to each of the four parks, one park a day, within 6 days, for arrivals " +
      "2027-01-03 to 2027-05-22. Terms from an authorized UK reseller; eligibility not stated.",
  },
  {
    id: "wdw-uk-14-day-magic",
    name: "Disney 14-Day Magic Ticket",
    resort: "walt-disney-world",
    markets: ["uk"],
    parks: DISNEY,
    ...fixed(14, 18),
    anchor: "start",
    hopping: "included",
    hopSets: [{ parks: DISNEY }],
    allowance: { parks: DISNEY_WATER, visits: 14 },
    onSale: [{ to: "2027-12-31" }],
    confidence: "high",
    shopUrl: DISNEY_UK_SHOP,
    notes:
      "UK and Ireland residents, bought before travel. 14 theme-park days within 18 days of the " +
      "chosen start; expires in full on day 18. Park hopping, the open water park, mini golf and " +
      "Memory Maker included; a water-park day uses no theme-park day.",
  },
  {
    id: "wdw-uk-7-day-magic",
    name: "Disney 7-Day Magic Ticket",
    resort: "walt-disney-world",
    markets: ["uk"],
    parks: DISNEY,
    ...fixed(7, 15),
    anchor: "start",
    hopping: "included",
    hopSets: [{ parks: DISNEY }],
    allowance: { parks: DISNEY_WATER, visits: 7 },
    confidence: "medium",
    shopUrl: DISNEY_UK_SHOP,
    notes:
      "UK and Ireland residents. 7 days within 15 days of the chosen start, hopping and the water " +
      "parks included. The number of water-park visits is not stated; the plan assumes 7.",
  },
  {
    id: "wdw-annual-pass",
    name: "Walt Disney World Annual Pass (Incredi-Pass, Sorcerer, Pirate, Pixie Dust)",
    resort: "walt-disney-world",
    markets: ["us"],
    parks: DISNEY,
    days: "unlimited",
    anchor: "first_use",
    hopping: "included",
    hopSets: [{ parks: DISNEY }],
    pass: true,
    blockouts: true,
    blockoutUrl: "https://disneyworld.disney.go.com/passholder-program/",
    reservationUrl:
      "https://disneyworld.disney.go.com/experience-updates/park-reservations/",
    confidence: "medium",
    shopUrl: "https://disneyworld.disney.go.com/passholder-program/",
    notes:
      "Blockout dates by pass (Incredi-Pass none). Passholders reserve their first park of the " +
      "day, except on Good-to-Go days and for entry after 2 p.m. Water parks are an add-on.",
  },

  // ── Universal Orlando ──────────────────────────────────────────────────
  {
    id: "uor-date-based",
    name: "Universal Orlando date-based ticket (park-to-park with .h)",
    resort: "universal-orlando",
    markets: ["us", "intl"],
    parks: [USF, IOA, EPIC],
    days: { min: 1, max: 5 },
    windowTable: UNIVERSAL_WINDOWS,
    anchor: "start",
    hopping: "optional",
    hopSets: [
      { parks: [USF, IOA], ticketDays: { min: 1, max: 2 } },
      { parks: [USF, IOA, EPIC], ticketDays: { min: 3, max: 5 } },
    ],
    caps: [{ parks: [EPIC], max: 0, ticketDays: { min: 1, max: 2 } }],
    confidence: "medium",
    shopUrl: UNIVERSAL_SHOP,
    notes:
      "N days within a run of consecutive days that starts on the selected date. Since 2026 the " +
      "3-day and longer tickets include Epic Universe every day (park-to-park hops to it too); 1- " +
      "and 2-day tickets do not. Windows from authorized resellers quoting Universal's terms; no " +
      "terms were found for 4- and 5-day base tickets, which take the park-to-park windows.",
    suggest: 10,
  },
  {
    id: "uor-date-based-volcano-bay",
    name: "Universal Orlando 3-, 4- or 5-Day Park-to-Park ticket with Volcano Bay",
    resort: "universal-orlando",
    markets: ["us", "intl"],
    parks: [USF, IOA, EPIC, VOLCANO_BAY],
    days: { min: 3, max: 5 },
    windowTable: UNIVERSAL_WINDOWS.filter((row) => row.days >= 3),
    anchor: "start",
    hopping: "included",
    hopSets: [{ parks: [USF, IOA, EPIC] }],
    confidence: "medium",
    shopUrl: UNIVERSAL_SHOP,
    notes:
      "Park-to-park between Universal Studios, Islands of Adventure and Epic Universe, and Volcano " +
      "Bay, every ticket day. Volcano Bay is closed from 2026-10-26 to a reopening on or before " +
      "2027-04-01.",
    suggest: 30,
  },
  {
    id: "uor-1-day-plus-epic",
    name: "Universal Orlando 1-Day ticket plus 1-Day Epic Universe",
    resort: "universal-orlando",
    markets: ["us", "intl"],
    parks: [USF, IOA, EPIC],
    ...fixed(2, 4),
    anchor: "start",
    hopping: "optional",
    hopSets: [{ parks: [USF, IOA] }],
    caps: [
      { parks: [EPIC], max: 1 },
      { parks: [USF, IOA], max: 1 },
    ],
    confidence: "medium",
    shopUrl: UNIVERSAL_SHOP,
    notes:
      "Epic Universe on one day, Universal Studios or Islands of Adventure (both with " +
      "park-to-park, never into Epic) on another, within 4 days. Sold for 2026 visits; 2027 not " +
      "separately confirmed.",
    suggest: 20,
  },
  {
    id: "uor-2-day-p2p-plus-epic",
    name: "Universal 2-Day Park-to-Park + 1 Day Epic Universe (promotion)",
    resort: "universal-orlando",
    markets: ["us", "uk", "intl"],
    parks: [USF, IOA, EPIC],
    ...fixed(3, 7),
    anchor: "start",
    hopping: "included",
    hopSets: [{ parks: [USF, IOA] }],
    caps: [
      { parks: [USF, IOA], max: 2 },
      { parks: [EPIC], max: 1 },
    ],
    onSale: [{ from: "2026-09-03", to: "2027-04-18" }],
    blockouts: true,
    blockoutUrl:
      "https://www.universalorlando.com/web/en/us/tickets-packages/offers/ticketoffer",
    confidence: "medium",
    shopUrl:
      "https://www.universalorlando.com/web/en/us/tickets-packages/offers/ticketoffer",
    notes:
      "Two park-to-park days at Universal Studios and Islands of Adventure and one Epic Universe " +
      "day within 7 days; no hopping into Epic. Blockout dates apply (e.g. 2026-12-24 to " +
      "2027-01-03). Window and sale dates from a reseller quoting Universal's terms.",
  },
  {
    id: "uor-epic-1-day",
    name: "Universal Epic Universe 1-Day ticket",
    resort: "universal-orlando",
    markets: ["us", "intl"],
    parks: [EPIC],
    ...fixed(1, 1),
    anchor: "start",
    hopping: "none",
    confidence: "high",
    shopUrl: UNIVERSAL_SHOP,
    notes:
      "Valid on the selected date only. Annual passholders buy it as a dated add-on.",
    suggest: 40,
  },
  {
    id: "uor-volcano-bay-1-day",
    name: "Universal Volcano Bay 1-Day ticket",
    resort: "universal-orlando",
    markets: ["us", "intl"],
    parks: [VOLCANO_BAY],
    ...fixed(1, 1),
    anchor: "start",
    hopping: "none",
    confidence: "high",
    shopUrl: UNIVERSAL_SHOP,
    notes:
      "Valid on the selected date only. Volcano Bay is closed from 2026-10-26 to a reopening on " +
      "or before 2027-04-01.",
    suggest: 40,
  },
  {
    id: "uor-uk-all-parks",
    name: "Universal Orlando All Parks Ticket",
    resort: "universal-orlando",
    markets: ["uk", "intl"],
    parks: [USF, IOA, EPIC, VOLCANO_BAY],
    ...fixed(14, 14),
    anchor: "start",
    hopping: "included",
    hopSets: [{ parks: [USF, IOA, EPIC] }],
    onSale: [{ to: "2026-10-19" }, { from: "2027-03-25", to: "2027-12-31" }],
    confidence: "high",
    shopUrl: UNIVERSAL_UK_SHOP,
    notes:
      "Every day of 14 consecutive days from the selected start, with unlimited park-to-park " +
      "including Epic Universe and Volcano Bay. Not sold while Volcano Bay is closed (the UK " +
      "store lists arrivals from 2027-03-25, other pages 2027-04-01).",
  },
  {
    id: "uor-uk-three-park-adventure",
    name: "Universal Orlando Three Park Adventure Ticket",
    resort: "universal-orlando",
    markets: ["uk", "intl"],
    parks: [USF, IOA, EPIC],
    ...fixed(14, 14),
    anchor: "start",
    hopping: "included",
    hopSets: [{ parks: [USF, IOA, EPIC] }],
    onSale: [{ from: "2026-10-20", to: "2027-03-31" }],
    confidence: "high",
    shopUrl: UNIVERSAL_UK_SHOP,
    notes:
      "The All Parks Ticket without Volcano Bay, for arrivals while it is closed: every day of 14 " +
      "consecutive days, unlimited park-to-park including Epic Universe.",
  },
  {
    id: "uor-annual-pass",
    name: "Universal Orlando 2-Park Annual Pass (Seasonal, Power, Preferred, Premier)",
    resort: "universal-orlando",
    markets: ["us", "intl"],
    parks: [USF, IOA],
    days: "unlimited",
    anchor: "first_use",
    hopping: "optional",
    hopSets: [{ parks: [USF, IOA] }],
    pass: true,
    blockouts: true,
    blockoutUrl:
      "https://www.universalorlando.com/web/en/us/tickets-packages/annual-passes/blockout-dates",
    confidence: "medium",
    shopUrl:
      "https://www.universalorlando.com/web/en/us/tickets-packages/annual-passes",
    notes:
      "Epic Universe is in no pass; passholders buy dated Epic day tickets. Seasonal, Power and " +
      "Preferred have blockout dates, Premier none. Park-to-park (.h) is stated officially for " +
      "Premier only.",
  },
  {
    id: "uor-annual-pass-3-park",
    name: "Universal Orlando 3-Park Annual Pass (with Volcano Bay)",
    resort: "universal-orlando",
    markets: ["us", "intl"],
    parks: [USF, IOA, VOLCANO_BAY],
    days: "unlimited",
    anchor: "first_use",
    hopping: "optional",
    hopSets: [{ parks: [USF, IOA] }],
    pass: true,
    blockouts: true,
    blockoutUrl:
      "https://www.universalorlando.com/web/en/us/tickets-packages/annual-passes/blockout-dates",
    confidence: "medium",
    shopUrl:
      "https://www.universalorlando.com/web/en/us/tickets-packages/annual-passes",
    notes:
      "The 2-Park Annual Pass with Volcano Bay. Epic Universe is in no pass. Park-to-park (.h) is " +
      "stated officially for Premier only.",
  },

  // ── SeaWorld Parks (SeaWorld, Aquatica, Discovery Cove, Busch Gardens, Adventure Island) ──
  {
    id: "seaworld-1-day",
    name: "SeaWorld Orlando Single-Day Ticket",
    resort: "seaworld-orlando",
    markets: ["us", "intl"],
    parks: [SEAWORLD],
    ...fixed(1, 1),
    anchor: "start",
    hopping: "none",
    confidence: "high",
    shopUrl: SEAWORLD_SHOP,
    notes:
      "Valid on the selected date. The Any Day ticket is one visit within 6 months of purchase.",
    suggest: 10,
  },
  {
    id: "seaworld-two-park",
    name: "SeaWorld & Aquatica or SeaWorld & Busch Gardens Two Park Ticket",
    resort: "seaworld-orlando",
    markets: ["us", "intl"],
    parks: [SEAWORLD, AQUATICA, BUSCH_GARDENS],
    ...fixed(2, 14),
    anchor: "first_use",
    hopping: "none",
    caps: [
      { parks: [SEAWORLD], max: 1 },
      { parks: [AQUATICA, BUSCH_GARDENS], max: 1 },
    ],
    confidence: "high",
    shopUrl: SEAWORLD_SHOP,
    notes:
      "One visit to SeaWorld and one to Aquatica or Busch Gardens (by version); the second within " +
      "14 days of the first, counted here with the first day.",
    suggest: 20,
  },
  {
    id: "seaworld-three-park-3-day",
    name: "SeaWorld Parks Three-Day, Three Park Ticket",
    resort: "seaworld-orlando",
    markets: ["us", "intl"],
    parks: [SEAWORLD, AQUATICA, BUSCH_GARDENS, ADVENTURE_ISLAND],
    ...fixed(3, 14),
    anchor: "first_use",
    hopping: "none",
    confidence: "high",
    shopUrl: SEAWORLD_SHOP,
    notes:
      "Three visits to any of SeaWorld, Aquatica, Busch Gardens and Adventure Island (seasonal); " +
      "the second and third within 14 days of the first, counted here with the first day.",
    suggest: 30,
  },
  {
    id: "seaworld-four-parks-14-day",
    name: "Four Parks: Unlimited Visits + Free Parking",
    resort: "seaworld-orlando",
    markets: ["us", "intl"],
    parks: [SEAWORLD, AQUATICA, BUSCH_GARDENS, ADVENTURE_ISLAND],
    days: "unlimited",
    windowDays: 14,
    anchor: "first_use",
    hopping: "none",
    confidence: "high",
    shopUrl: SEAWORLD_SHOP,
    notes:
      "Unlimited visits to the four parks for 14 consecutive days from the first visit. Same-day " +
      "visits to two parks are not addressed.",
    suggest: 40,
  },
  {
    id: "seaworld-uk-3-visit",
    name: "SeaWorld Parks 3-Visit Ticket + Eat Free",
    resort: "seaworld-orlando",
    markets: ["uk", "intl"],
    parks: [SEAWORLD, AQUATICA, BUSCH_GARDENS],
    ...fixed(3, 14),
    anchor: "first_use",
    hopping: "none",
    confidence: "high",
    shopUrl: SEAWORLD_SHOP,
    notes:
      "Open-dated to the end of the calendar year chosen at booking, then 14 consecutive days from " +
      "the first visit; three visits in any combination, the same park included.",
  },
  {
    id: "seaworld-uk-ultimate",
    name: "SeaWorld Parks Ultimate Ticket",
    resort: "seaworld-orlando",
    markets: ["uk", "intl"],
    parks: [SEAWORLD, AQUATICA, BUSCH_GARDENS],
    days: "unlimited",
    windowDays: 14,
    anchor: "first_use",
    hopping: "none",
    confidence: "medium",
    shopUrl: SEAWORLD_SHOP,
    notes:
      "Unlimited visits for 14 days from the first, parking and three all-day dining days " +
      "included. One seller's page.",
  },
  {
    id: "seaworld-fun-card",
    name: "SeaWorld Orlando 2027 Fun Card",
    resort: "seaworld-orlando",
    markets: ["us", "intl"],
    parks: [SEAWORLD],
    days: "unlimited",
    anchor: "first_use",
    hopping: "none",
    pass: true,
    blockouts: true,
    blockoutUrl: "https://seaworld.com/orlando/tickets/",
    until: "2027-12-31",
    confidence: "medium",
    shopUrl: SEAWORLD_SHOP,
    notes: "Unlimited visits through 2027 outside blockout dates.",
  },
  {
    id: "seaworld-fun-card-aquatica",
    name: "SeaWorld Orlando + Aquatica 2027 Fun Card",
    resort: "seaworld-orlando",
    markets: ["us", "intl"],
    parks: [SEAWORLD, AQUATICA],
    days: "unlimited",
    anchor: "first_use",
    hopping: "none",
    pass: true,
    blockouts: true,
    blockoutUrl: "https://seaworld.com/orlando/tickets/",
    until: "2027-12-31",
    confidence: "medium",
    shopUrl: SEAWORLD_SHOP,
    notes: "Unlimited visits through 2027 outside blockout dates.",
  },
  {
    id: "seaworld-annual-pass",
    name: "SeaWorld Orlando Annual Pass",
    resort: "seaworld-orlando",
    markets: ["us", "intl"],
    parks: [SEAWORLD],
    days: "unlimited",
    anchor: "first_use",
    hopping: "none",
    pass: true,
    blockouts: true,
    blockoutUrl: "https://seaworld.com/orlando/annual-pass/",
    confidence: "medium",
    shopUrl: "https://seaworld.com/orlando/annual-pass/",
    notes:
      "Bronze has blockout dates; Silver, Gold and Platinum none. Discovery Cove is never included.",
  },
  {
    id: "seaworld-annual-pass-florida",
    name: "SeaWorld Annual Pass, Florida parks (SeaWorld, Aquatica, Busch Gardens, Adventure Island)",
    resort: "seaworld-orlando",
    markets: ["us", "intl"],
    parks: [SEAWORLD, AQUATICA, BUSCH_GARDENS, ADVENTURE_ISLAND],
    days: "unlimited",
    anchor: "first_use",
    hopping: "none",
    pass: true,
    blockouts: true,
    blockoutUrl: "https://seaworld.com/orlando/annual-pass/",
    confidence: "medium",
    shopUrl: "https://seaworld.com/orlando/annual-pass/",
    notes:
      "The Florida Parks membership and Platinum. Bronze has blockout dates. Discovery Cove is " +
      "never included.",
  },
  {
    id: "discovery-cove",
    name: "Discovery Cove day package",
    resort: "seaworld-orlando",
    markets: ["us", "uk", "intl"],
    parks: [DISCOVERY_COVE],
    ...fixed(1, 1),
    anchor: "start",
    hopping: "none",
    reservedDayParks: [DISCOVERY_COVE],
    confidence: "high",
    shopUrl: DISCOVERY_COVE_SHOP,
    notes: "A reserved, capacity-limited date chosen at booking.",
    suggest: 10,
  },
  {
    id: "discovery-cove-plus-2",
    name: "Discovery Cove day package + 2 Parks (SeaWorld, Aquatica)",
    resort: "seaworld-orlando",
    markets: ["us", "uk", "intl"],
    parks: [DISCOVERY_COVE, SEAWORLD, AQUATICA],
    days: "unlimited",
    windowDays: 14,
    anchor: "first_use",
    hopping: "none",
    caps: [{ parks: [DISCOVERY_COVE], max: 1 }],
    reservedDayParks: [DISCOVERY_COVE],
    confidence: "high",
    shopUrl: DISCOVERY_COVE_SHOP,
    notes:
      "The reserved Discovery Cove day, and SeaWorld and Aquatica unlimited for up to 14 " +
      "consecutive days around it; the plan keeps every day in one 14-day span that holds the " +
      "reserved day.",
    suggest: 20,
  },
  {
    id: "discovery-cove-plus-3",
    name: "Discovery Cove day package + 3 Parks (SeaWorld, Aquatica, Busch Gardens)",
    resort: "seaworld-orlando",
    markets: ["us", "uk", "intl"],
    parks: [DISCOVERY_COVE, SEAWORLD, AQUATICA, BUSCH_GARDENS],
    days: "unlimited",
    windowDays: 14,
    anchor: "first_use",
    hopping: "none",
    caps: [{ parks: [DISCOVERY_COVE], max: 1 }],
    reservedDayParks: [DISCOVERY_COVE],
    confidence: "high",
    shopUrl: DISCOVERY_COVE_SHOP,
    notes:
      "+2 Parks with Busch Gardens and parking; every day in one 14-day span that holds the " +
      "reserved day.",
    suggest: 30,
  },
  {
    id: "busch-gardens-1-day",
    name: "Busch Gardens Tampa Bay Single-Day Ticket",
    resort: "seaworld-orlando",
    markets: ["us", "intl"],
    parks: [BUSCH_GARDENS],
    ...fixed(1, 1),
    anchor: "start",
    hopping: "none",
    confidence: "high",
    shopUrl: BUSCH_GARDENS_SHOP,
    notes:
      "Valid on the selected date. Sold by SeaWorld Parks, like the multi-park tickets.",
    suggest: 10,
  },
  {
    id: "busch-gardens-fun-card",
    name: "Busch Gardens Tampa Bay 2027 Fun Card",
    resort: "seaworld-orlando",
    markets: ["us", "intl"],
    parks: [BUSCH_GARDENS],
    days: "unlimited",
    anchor: "first_use",
    hopping: "none",
    pass: true,
    blockouts: true,
    blockoutUrl: "https://buschgardens.com/tampa/tickets/",
    until: "2027-12-31",
    confidence: "medium",
    shopUrl: BUSCH_GARDENS_SHOP,
    notes:
      "Unlimited visits through 2027 outside blockout dates. The version with Adventure Island is " +
      "not listed (its Adventure Island part ends 2027-10-24).",
  },

  // ── LEGOLAND Florida ───────────────────────────────────────────────────
  {
    id: "legoland-1-day",
    name: "LEGOLAND Florida 1-Day ticket",
    resort: "day-trips",
    markets: ["us", "intl"],
    parks: [LEGOLAND],
    ...fixed(1, 1),
    anchor: "start",
    hopping: "none",
    confidence: "medium",
    shopUrl: LEGOLAND_SHOP,
    notes: "Valid on the selected date.",
    suggest: 10,
  },
  {
    id: "legoland-2-day",
    name: "LEGOLAND Florida 2-Day ticket",
    resort: "day-trips",
    markets: ["us", "intl"],
    parks: [LEGOLAND],
    ...fixed(2, 4),
    anchor: "start",
    hopping: "none",
    confidence: "medium",
    shopUrl: LEGOLAND_SHOP,
    notes:
      "The first visit on the selected date, the second within 3 days of the first scan; how the " +
      "3 days count is not stated, so the plan keeps both in 4 days from the start.",
    suggest: 20,
  },
  {
    id: "legoland-merlin-7-attractions",
    name: "7 Florida Attractions Ticket (Merlin Florida combo)",
    resort: "day-trips",
    markets: ["us", "uk", "intl"],
    parks: [LEGOLAND],
    ...fixed(1, 14),
    anchor: "first_use",
    hopping: "none",
    caps: [{ parks: [LEGOLAND], max: 1 }],
    confidence: "high",
    shopUrl: LEGOLAND_SHOP,
    notes:
      "One visit per attraction within 14 days of the first (or a year of purchase): LEGOLAND " +
      "with its water park and SEA LIFE on one day, plus the Orlando attractions.",
  },
  {
    id: "legoland-annual-pass",
    name: "LEGOLAND Florida Annual Pass",
    resort: "day-trips",
    markets: ["us", "intl"],
    parks: [LEGOLAND],
    days: "unlimited",
    anchor: "first_use",
    hopping: "none",
    pass: true,
    blockouts: true,
    blockoutUrl:
      "https://www.legoland.com/florida/tickets-passes/passholder-hub/annual-pass-blockout-dates/",
    confidence: "medium",
    shopUrl: "https://www.legoland.com/florida/tickets-passes/",
    notes: "The Standard pass has blockout dates and excludes the water park.",
  },
];

/** Each region's seed, under its slug; a region without one starts with an empty catalog. */
export const TICKET_CATALOGS: Record<string, TicketProduct[]> = {
  orlando: ORLANDO_TICKETS,
  japan: JAPAN_TICKETS,
};

/** A region's seed catalog: the products the table starts from; empty without one. */
export function seedTickets(region: { slug: string }): TicketProduct[] {
  return Object.prototype.hasOwnProperty.call(TICKET_CATALOGS, region.slug)
    ? TICKET_CATALOGS[region.slug]
    : [];
}

/**
 * A region's catalog: the one loaded from the admin's table and carried on
 * the region (`TicketCatalogService.withTickets`), else the seed, which is
 * what a reader without a database plans with.
 */
export function ticketCatalog(region: {
  slug: string;
  tickets?: TicketProduct[];
}): TicketProduct[] {
  return region.tickets ?? seedTickets(region);
}

/** The seller an Attraction Tickets seed link names. */
export const ATTRACTION_TICKETS_SELLER = "Attraction Tickets";

/**
 * A seed product's links from its entry in `ticket-seed-links.ts`, in display
 * order: the cheapest authorized seller, then Attraction Tickets' pages (each
 * for its locale, or for all), then the operator's own shop (`official`). A
 * link that repeats one before it (the cheapest seller is Attraction Tickets,
 * or the operator itself) is left out. Never sponsored. Without an entry, the
 * product's `shopUrl` is the official link, named by its host.
 */
export function seedLinksFrom(
  entry: SeedLinkEntry | undefined,
  product: Pick<TicketProduct, "shopUrl">,
): TicketLink[] {
  const official: TicketLink = {
    seller: entry?.official.seller ?? new URL(product.shopUrl).host,
    url: entry?.official.url ?? product.shopUrl,
    official: true,
    sponsored: false,
  };
  const links: TicketLink[] = [];
  if (entry?.cheapest) {
    links.push({
      seller: entry.cheapest.seller,
      url: entry.cheapest.url,
      sponsored: false,
    });
  }
  for (const page of entry?.attractionTickets ?? []) {
    links.push({
      seller: ATTRACTION_TICKETS_SELLER,
      url: page.url,
      sponsored: false,
      ...(page.locale && { locales: [page.locale] }),
    });
  }
  const seen = new Set<string>([official.url]);
  const unique = links.filter((l) => {
    if (seen.has(l.url)) return false;
    seen.add(l.url);
    return true;
  });
  return [...unique, official];
}

/** A seed product's links (`seedLinksFrom` on its entry). */
export function seedLinksOf(product: TicketProduct): TicketLink[] {
  return seedLinksFrom(
    TICKET_SEED_LINKS.find((e) => e.productId === product.id),
    product,
  );
}

/** A product's links: its own (the admin's), else the seed's. */
export function linksOf(product: TicketProduct): TicketLink[] {
  return product.links && product.links.length > 0
    ? product.links
    : seedLinksOf(product);
}

/** The url of a product's official link: its `shopUrl` in every answer. */
export function shopUrlOf(product: TicketProduct): string {
  return linksOf(product).find((l) => l.official)?.url ?? product.shopUrl;
}

/**
 * The catalog as the routes serve it: the active products in order, the
 * internal `suggest` and `archived` left out, `links` always there.
 */
export function publicTicketCatalog(region: {
  slug: string;
  tickets?: TicketProduct[];
}): Array<
  Omit<TicketProduct, "suggest" | "archived"> & { links: TicketLink[] }
> {
  return ticketCatalog(region)
    .filter((p) => !p.archived)
    .map(({ suggest: _suggest, archived: _archived, ...product }) => ({
      ...product,
      shopUrl: shopUrlOf(product),
      links: linksOf(product),
    }));
}

const appliesTo = (rule: ByLength, days: number): boolean =>
  !rule.ticketDays ||
  (days >= rule.ticketDays.min && days <= rule.ticketDays.max);

/** Whether a product is sold in that length. */
export function sellsDays(product: TicketProduct, days: number): boolean {
  if (product.days === "unlimited") return false;
  return days >= product.days.min && days <= product.days.max;
}

/**
 * The calendar days a ticket of that length may be used in, from its start;
 * Infinity for a pass (no window). Undefined for a length not on sale.
 */
export function windowOf(
  product: TicketProduct,
  days: number | "unlimited",
): number | undefined {
  if (product.days === "unlimited") return product.windowDays ?? Infinity;
  if (days === "unlimited") return undefined;
  return product.windowTable?.find((row) => row.days === days)?.window;
}

/** The caps that hold for that length, `max: 0` ones included. */
export function capsOf(
  product: TicketProduct,
  days: number | "unlimited",
): TicketCap[] {
  const n = days === "unlimited" ? Infinity : days;
  return (product.caps ?? []).filter((c) => appliesTo(c, n));
}

/** The parks a ticket day of that length covers: `parks` without those capped at 0. */
export function parksOf(
  product: TicketProduct,
  days: number | "unlimited",
): string[] {
  const excluded = new Set(
    capsOf(product, days)
      .filter((c) => c.max === 0)
      .flatMap((c) => c.parks),
  );
  return product.parks.filter((p) => !excluded.has(p));
}

/** The hop sets of a ticket of that length; none without hopping. */
export function hopSetsOf(
  product: TicketProduct,
  days: number | "unlimited",
  hopper: boolean,
): string[][] {
  if (product.hopping === "none") return [];
  if (product.hopping === "optional" && !hopper) return [];
  const n = days === "unlimited" ? Infinity : days;
  return (product.hopSets ?? [])
    .filter((s) => appliesTo(s, n))
    .map((s) => s.parks);
}

/** The allowance's parks and visits for that length; null without one. */
export function allowanceOf(
  product: TicketProduct,
  days: number | "unlimited",
): { parks: string[]; visits: number } | null {
  if (!product.allowance) return null;
  const visits =
    product.allowance.visits === "days"
      ? days === "unlimited"
        ? Infinity
        : days
      : product.allowance.visits;
  return { parks: product.allowance.parks, visits };
}

/**
 * An every-day product: a window of several days, each of them usable (the
 * UK 14-day Universal tickets, SeaWorld's Four Parks), where a day reads as
 * the window's day ("day 10 of 14"), not as one counted off. Not a pass.
 */
export function everyDay(
  product: TicketProduct,
  days: number | "unlimited",
): boolean {
  if (product.pass) return false;
  const window = windowOf(product, days);
  return (
    window !== undefined &&
    Number.isFinite(window) &&
    window > 1 &&
    (days === "unlimited" || window === days)
  );
}

/** `HH:MM` as minutes after midnight; undefined for anything else. */
export function minuteOfClock(value: string | undefined): number | undefined {
  const m = value ? /^([01]\d|2[0-3]):([0-5]\d)$/.exec(value) : null;
  return m ? Number(m[1]) * 60 + Number(m[2]) : undefined;
}

/**
 * The kinds of day a date is (`validDays`): `weekend` on Saturday and Sunday,
 * `holiday` on a public holiday, `weekday` on any other day.
 */
export function dayKindsOf(day: {
  weekday: number;
  isHoliday: boolean;
}): TicketDayKind[] {
  const kinds: TicketDayKind[] = [];
  const weekend = day.weekday === 0 || day.weekday === 6;
  if (weekend) kinds.push("weekend");
  if (day.isHoliday) kinds.push("holiday");
  if (!weekend && !day.isHoliday) kinds.push("weekday");
  return kinds;
}

/** Whether a product is sold for a start date (`onSale`; no limit known: always). */
export function onSaleAt(product: TicketProduct, date: string): boolean {
  if (!product.onSale) return true;
  return product.onSale.some(
    (r) => (!r.from || date >= r.from) && (!r.to || date <= r.to),
  );
}
