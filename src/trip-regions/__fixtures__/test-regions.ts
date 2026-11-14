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

const JP_PARK = {
  continent: "asia",
  country: "japan",
  type: "theme_park",
} as const;

/**
 * Five parks in four cities on one line, by train: Tokyo's two, Fuji (a long
 * day trip from Tokyo or a stay of its own), Nagoya's and Osaka's.
 */
export const TEST_JAPAN: RegionConfig = {
  slug: "test-japan",
  name: "Test Japan",
  timezone: "Asia/Tokyo",
  groups: [
    {
      key: "tdr",
      name: "Tokyo Disney Resort",
      kind: "resort",
      earlyKind: "happy_entry",
    },
    {
      key: "usj",
      name: "Universal Studios Japan",
      kind: "resort",
      earlyKind: "early_entry",
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
      slug: "j-land",
      ...JP_PARK,
      city: "tokyo",
      group: "tdr",
      headliner: true,
      code: "DL",
    },
    {
      slug: "j-sea",
      ...JP_PARK,
      city: "tokyo",
      group: "tdr",
      headliner: true,
      code: "DS",
    },
    {
      slug: "j-fuji",
      ...JP_PARK,
      city: "fujikawaguchiko",
      group: "day-trips",
      headliner: false,
      code: "FQ",
    },
    {
      slug: "j-lego",
      ...JP_PARK,
      city: "nagoya",
      group: "day-trips",
      headliner: false,
      code: "LJ",
    },
    {
      slug: "j-usj",
      ...JP_PARK,
      city: "osaka",
      group: "usj",
      headliner: true,
      code: "US",
    },
  ],
  hotelAreas: [
    {
      key: "maihama",
      name: "Maihama",
      latitude: 35.63,
      longitude: 139.88,
      hotel: "tdr_hotel",
      note: "",
    },
    {
      key: "shinjuku",
      name: "Shinjuku",
      latitude: 35.69,
      longitude: 139.7,
      hotel: "offsite",
      note: "",
    },
    {
      key: "kawaguchiko",
      name: "Kawaguchiko",
      latitude: 35.5,
      longitude: 138.77,
      hotel: "offsite",
      note: "",
    },
    {
      key: "nagoya_station",
      name: "Nagoya Station",
      latitude: 35.17,
      longitude: 136.88,
      hotel: "offsite",
      note: "",
    },
    {
      key: "universal_city",
      name: "Universal City",
      latitude: 34.67,
      longitude: 135.44,
      hotel: "offsite",
      note: "",
    },
  ],
  perks: {
    tdr_hotel: [
      {
        perk: "happy_entry",
        parks: ["j-land", "j-sea"],
        needs: "after_first_night",
        share: 0.025,
      },
    ],
    offsite: [],
  },
  perkNames: { happy_entry: "Happy Entry" },
  hotelNames: { tdr_hotel: "a Disney Hotel", offsite: "any other hotel" },
  legs: [
    {
      from: "hotel:maihama",
      to: "j-land",
      primary: { mode: "walk", minutes: 10 },
      alternatives: [],
    },
    {
      from: "hotel:maihama",
      to: "j-sea",
      primary: { mode: "monorail", minutes: 15 },
      alternatives: [],
    },
    {
      from: "hotel:shinjuku",
      to: "j-fuji",
      primary: { mode: "highway_bus", minutes: 110 },
      alternatives: [],
    },
    {
      from: "hotel:universal_city",
      to: "j-usj",
      primary: { mode: "walk", minutes: 5 },
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
    climatePark: "j-land",
  },
  travel: { source: "curated", computedOn: "2026-11-14", basis: "timetable" },
  ticketMarkets: { home: "jp", codes: ["jp", "intl"] },
  bases: [
    {
      key: "tokyo",
      name: "Tokyo",
      order: 1,
      hotelAreas: ["maihama", "shinjuku"],
      defaultArea: "maihama",
      gateways: [
        { code: "HND", name: "Haneda", kind: "airport" },
        { code: "NRT", name: "Narita", kind: "airport" },
      ],
      parks: [
        { slug: "j-land", minutes: 20, mode: "rail" },
        { slug: "j-sea", minutes: 25, mode: "rail" },
        { slug: "j-fuji", minutes: 110, mode: "highway_bus" },
      ],
    },
    {
      key: "fuji",
      name: "Fujikawaguchiko",
      order: 2,
      hotelAreas: ["kawaguchiko"],
      parks: [{ slug: "j-fuji", minutes: 15, mode: "rail" }],
    },
    {
      key: "nagoya",
      name: "Nagoya",
      order: 3,
      hotelAreas: ["nagoya_station"],
      gateways: [{ code: "NGO", name: "Chubu Centrair", kind: "airport" }],
      parks: [{ slug: "j-lego", minutes: 40, mode: "rail" }],
    },
    {
      key: "osaka",
      name: "Osaka",
      order: 4,
      hotelAreas: ["universal_city"],
      gateways: [{ code: "KIX", name: "Kansai", kind: "airport" }],
      parks: [
        { slug: "j-usj", minutes: 25, mode: "rail" },
        { slug: "j-lego", minutes: 105, mode: "shinkansen" },
      ],
    },
  ],
  baseLegs: [
    { from: "tokyo", to: "fuji", minutes: 130, mode: "highway_bus" },
    {
      from: "fuji",
      to: "nagoya",
      minutes: 225,
      mode: "highway_bus",
      parkDay: false,
    },
    {
      from: "tokyo",
      to: "nagoya",
      minutes: 150,
      mode: "shinkansen",
      name: "Tokaido Shinkansen",
    },
    {
      from: "nagoya",
      to: "osaka",
      minutes: 90,
      mode: "shinkansen",
      name: "Tokaido Shinkansen",
    },
    {
      from: "tokyo",
      to: "osaka",
      minutes: 210,
      mode: "shinkansen",
      parkDay: true,
    },
  ],
};
