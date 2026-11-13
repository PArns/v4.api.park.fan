import type { RegionConfig } from "../regions";
import type { TicketProduct } from "../tickets";

/**
 * Two small regions for the specs of what a region beyond Orlando brings,
 * before the real ones exist: a Tokyo-like region (trains, typhoons, the rainy
 * season, its own hotel kinds and early-slot name, its own ticket markets and
 * an evening ticket) and a Paris-like one (no storm season, cooler heat
 * thresholds). Their slugs are no region's, so nothing reads them but the
 * specs that import them.
 */

const JP = { continent: "asia", country: "japan" } as const;
const FR = { continent: "europe", country: "france" } as const;

const SHOP = "https://www.tokyodisneyresort.jp/en/tdr/ticket/";

/** Tokyo Disney Resort's two parks and a day trip, by train. */
export const TEST_TOKYO: RegionConfig = {
  slug: "test-tokyo",
  name: "Test Tokyo",
  timezone: "Asia/Tokyo",
  groups: [
    {
      key: "tokyo-disney-resort",
      name: "Tokyo Disney Resort",
      kind: "resort",
      earlyKind: "happy_entry",
    },
    {
      key: "day-trips",
      name: "Day trips",
      kind: "day_trip",
      earlyKind: "early_entry",
    },
  ],
  parks: [
    {
      slug: "test-land",
      ...JP,
      city: "urayasu",
      group: "tokyo-disney-resort",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 1,
    },
    {
      slug: "test-sea",
      ...JP,
      city: "urayasu",
      group: "tokyo-disney-resort",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 2,
    },
    {
      slug: "test-fuji",
      ...JP,
      city: "fujiyoshida",
      group: "day-trips",
      type: "theme_park",
      headliner: false,
    },
  ],
  hotelAreas: [
    {
      key: "maihama",
      name: "Maihama",
      latitude: 35.6329,
      longitude: 139.8804,
      hotel: "tdr_hotel",
      note: "the resort's own hotels",
    },
    {
      key: "tokyo-station",
      name: "Tokyo Station",
      latitude: 35.6812,
      longitude: 139.7671,
      hotel: "offsite",
      note: "central Tokyo",
    },
  ],
  perks: {
    tdr_hotel: [
      {
        perk: "happy_entry",
        parks: ["test-land", "test-sea"],
        needs: "early",
        share: 0.05,
      },
    ],
    offsite: [],
  },
  perkNames: { happy_entry: "Happy Entry" },
  hotelNames: {
    tdr_hotel: "a Tokyo Disney Resort hotel",
    offsite: "any other hotel",
  },
  legs: [
    {
      from: "test-land",
      to: "test-sea",
      primary: { mode: "monorail", minutes: 15, name: "Disney Resort Line" },
      alternatives: [{ mode: "walk", minutes: 20 }],
    },
    {
      from: "hotel:tokyo-station",
      to: "test-land",
      primary: { mode: "rail", minutes: 25, name: "Keiyo Line" },
      alternatives: [],
    },
    {
      from: "hotel:tokyo-station",
      to: "test-fuji",
      primary: { mode: "highway_bus", minutes: 120 },
      alternatives: [],
    },
  ],
  venues: [],
  hopperSets: [],
  nightShows: {},
  weather: {
    heat: {
      forecastFromC: 35,
      normalFromC: 31,
      perDegree: 0.08,
      normalWeight: 0.5,
      cap: 1.4,
    },
    storm: {
      kind: "typhoon",
      weights: [
        [8, 0.5, 0.8],
        [9, 1, 1],
        [10, 0.6, 0.3],
      ],
      peak: { from: "09-01", to: "09-30" },
      peakMinutes: 6,
    },
    notes: [{ kind: "rainy_season", from: "06-05", to: "07-19" }],
    climatePark: "test-land",
  },
  travel: {
    source: "curated",
    computedOn: "2026-11-13",
    basis: "timetable",
  },
  ticketMarkets: { home: "jp", codes: ["jp", "intl"] },
};

/** Tokyo Disney Resort's 1-Day Passport and an evening ticket, for the specs. */
export const TEST_TOKYO_TICKETS: TicketProduct[] = [
  {
    id: "test-1-day",
    name: "1-Day Passport",
    resort: "tokyo-disney-resort",
    markets: ["jp", "intl"],
    parks: ["test-land", "test-sea"],
    days: { min: 1, max: 1 },
    windowTable: [{ days: 1, window: 1 }],
    anchor: "start",
    hopping: "none",
    confidence: "high",
    shopUrl: SHOP,
    links: [
      {
        seller: "Tokyo Disney Resort",
        url: SHOP,
        official: true,
        sponsored: false,
      },
    ],
    suggest: 1,
  },
  {
    id: "test-early-evening",
    name: "Early Evening Passport",
    resort: "tokyo-disney-resort",
    markets: ["jp"],
    parks: ["test-land", "test-sea"],
    days: { min: 1, max: 1 },
    windowTable: [{ days: 1, window: 1 }],
    anchor: "start",
    hopping: "none",
    entryFrom: "15:00",
    validDays: ["weekend", "holiday"],
    confidence: "high",
    shopUrl: SHOP,
    links: [
      {
        seller: "Tokyo Disney Resort",
        url: SHOP,
        official: true,
        sponsored: false,
      },
    ],
  },
];

/** Two parks by Paris: no storm season, a cooler summer. */
export const TEST_PARIS: RegionConfig = {
  slug: "test-paris",
  name: "Test Paris",
  timezone: "Europe/Paris",
  groups: [
    {
      key: "disneyland-paris",
      name: "Disneyland Paris",
      kind: "resort",
      earlyKind: "extra_magic_time",
    },
  ],
  parks: [
    {
      slug: "test-park",
      ...FR,
      city: "chessy",
      group: "disneyland-paris",
      type: "theme_park",
      headliner: true,
    },
    {
      slug: "test-studios",
      ...FR,
      city: "chessy",
      group: "disneyland-paris",
      type: "theme_park",
      headliner: true,
    },
  ],
  hotelAreas: [],
  perks: {
    disney_hotel: [
      {
        perk: "extra_magic_time",
        parks: ["test-park", "test-studios"],
        needs: "early",
        share: 0.05,
      },
    ],
    offsite: [],
  },
  perkNames: { extra_magic_time: "Extra Magic Time" },
  hotelNames: {
    disney_hotel: "a Disneyland Paris hotel",
    offsite: "any other hotel",
  },
  legs: [
    {
      from: "test-park",
      to: "test-studios",
      primary: { mode: "walk", minutes: 5 },
      alternatives: [],
    },
  ],
  venues: [],
  hopperSets: [["test-park", "test-studios"]],
  nightShows: {},
  weather: {
    heat: {
      forecastFromC: 32,
      normalFromC: 26,
      perDegree: 0.08,
      normalWeight: 0.5,
      cap: 1.4,
    },
    climatePark: "test-park",
  },
  travel: {
    source: "curated",
    computedOn: "2026-11-13",
    basis: "timetable",
  },
  ticketMarkets: { home: "eu", codes: ["eu", "uk"] },
};
