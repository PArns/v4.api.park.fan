/**
 * The curated trip regions: which parks a vacation in a region covers, how they
 * group into resorts, where the hotels are, which perks a hotel's guests get and
 * how the resorts' own transport links the parks.
 *
 * A region is a planning decision, not a fact any feed carries: LEGOLAND Florida
 * sits in Winter Haven and Busch Gardens in Tampa, yet both are day trips from an
 * Orlando hotel. So it lives in code and is changed by a pull request, like the
 * rest of `docs/frontend/trip-regions.md`.
 *
 * This file imports nothing at run time on purpose: `scripts/trip-regions/build-travel-matrix.mjs`
 * imports it straight from Node (type stripping, which erases the one type import) to
 * read the coordinates, so the matrix and the region can never disagree about where a
 * car park is.
 */
import type { TicketProduct } from "./tickets";

/** What kind of day a park is: queues worth optimizing, or a flat-cost day. */
export type TripParkType = "theme_park" | "water_park" | "reservation_park";

/**
 * `hotel=` on the plan: which hotel's perks the visitor gets. A region's own
 * vocabulary, the keys of its `perks` (Orlando: `disney`, `disney_deluxe`,
 * `universal`, `universal_premier`, `offsite`); `offsite` (no perks) in every
 * region. Validated per region ({@link hotelKinds}).
 */
export type HotelKind = string;

/**
 * A perk a hotel's guests get, as the plan's `perks` and `reasons` name it: a
 * region's own vocabulary, the keys of its `perkNames` (Orlando's
 * `early_entry`, `early_park_admission`, `extended_evening`,
 * `express_included`; Tokyo Disney Resort's Happy Entry would be
 * `happy_entry`).
 */
export type PerkKind = string;

/**
 * What a resort calls its before-opening slot (`early.kind` on the calendar):
 * the same shape everywhere, a slot before the day guests' opening that some
 * guests get. Orlando's `early_entry` (Disney) and `early_park_admission`
 * (Universal); Disneyland Resort's Early Entry, Tokyo Disney Resort's Happy
 * Entry and Disneyland Paris' Extra Magic Time are the same shape under their
 * own names. Set per group (`RegionGroupConfig.earlyKind`).
 */
export type EarlyKind = string;

/**
 * How a guest gets between two places other than by car: a resort's own
 * transport, and the rail and coach a city-to-city trip takes.
 */
export type TransportMode =
  | "monorail"
  | "skyliner"
  | "boat"
  | "bus"
  | "walk"
  | "train"
  | "shinkansen"
  | "rail"
  | "metro"
  | "highway_bus"
  | "shuttle";

export interface LatLng {
  latitude: number;
  longitude: number;
}

export interface RegionGroupConfig {
  key: string;
  /** Shown when the group's parks share no destination row (`destinations.name`). */
  name: string;
  kind: "resort" | "day_trip";
  /** What the feed's before-opening slot is called at this resort. */
  earlyKind: EarlyKind;
}

export interface RegionParkConfig {
  /** `parks.slug`; with `city` it resolves through `findByGeographicPath`. */
  slug: string;
  continent: string;
  country: string;
  city: string;
  group: string;
  type: TripParkType;
  /** One of the region's headliner parks: picked by default in the planner. */
  headliner: boolean;
  /**
   * With `split=auto`, the order in which parks listed without a count get a
   * second visit when the trip has days to spare (1 first). A curated planning
   * call: the parks with the most to do and the longest queues. Absent: never.
   */
  secondVisitRank?: number;
  /**
   * Where a drive to this park ends, which is not always its gate: Magic
   * Kingdom's car park is the Transportation and Ticket Center across the Seven
   * Seas Lagoon, Volcano Bay has none of its own. Picked so OSRM snaps it to the
   * road a guest actually arrives on; the park's own `latitude`/`longitude`
   * (its centroid) snapped to service roads behind the parks. Required where
   * the region's travel comes from OSRM (`travel.source`), unused where it is
   * curated.
   */
  carPark?: LatLng & { note: string };
  /**
   * The drive ends short of the gate and a transfer follows, e.g. the monorail
   * or ferry from the TTC. `minutes` is a curated estimate, waiting included.
   */
  transfer?: { via: string; minutes: number };
}

export interface HotelAreaConfig extends LatLng {
  key: string;
  name: string;
  /** The `hotel=` value a stay in this area usually means; its perks are the area's. */
  hotel: HotelKind;
  note: string;
}

/**
 * A perk and where it applies. `needs` says what the day must carry for it to
 * count: the feed's before-opening slot, an extended evening, or nothing (the
 * perk holds every open day, like Express at the premier hotels). `share` is
 * what it takes off the park's cost that day: fixed shares, not measurements
 * (an early slot is worth a ride or two before the day guests arrive, an
 * extended evening more, Express Unlimited most of the queue).
 */
export interface PerkRule {
  perk: PerkKind;
  parks: string[];
  needs: "early" | "extended_evening" | "always";
  share: number;
}

/**
 * A resort's own transport between two places. `minutes` is a curated
 * estimate rounded to five, waiting included; it is left out where the wait
 * dominates and varies (the buses).
 */
export interface TransportAlternative {
  mode: TransportMode;
  minutes?: number;
  via?: string;
  /** What the guest needs beyond a one-park ticket. */
  ticket?: "park_to_park";
  name?: string;
}

/** Curated alternatives for one pair, valid both ways. `to`/`from` take `hotel:<key>`. */
export interface CuratedLeg {
  from: string;
  to: string;
  alternatives: TransportAlternative[];
  /**
   * The leg itself, where no drive stands for it: in an OSRM region a pair
   * that shares a car park (OSRM answers 0 km; the walk or shuttle between the
   * two gates is the leg) or a pair a guest makes by rail; in a curated region
   * (`travel.source: "curated"`) every leg. It is served with
   * `source: "curated"` where the region's own source is OSRM.
   */
  primary?: {
    mode: TransportMode;
    minutes: number;
    km?: number;
    via?: string;
    name?: string;
  };
}

/** A place for an evening outside the parks. */
export interface VenueConfig extends LatLng {
  key: string;
  name: string;
  /** The resort group whose evenings it serves. */
  group: string;
  kind: "district" | "viewpoint" | "resort_area";
  /** Typical, not published per day: the venue's own page has the day's. */
  typicalHours?: { opens: string; closes: string };
  /** No admission: walking in costs nothing. */
  free: boolean;
  url: string;
  notes?: string;
  /** A viewpoint: the park whose fireworks it looks at. */
  viewOf?: string;
  highlights?: Array<"dining" | "shopping">;
}

/**
 * A nighttime spectacular of a park, by its `shows.slug`. Curated from the
 * operator's pages; its time on a day comes from the shows data
 * (`PlanDayService.showsOn`), never from here.
 */
export interface NightShowConfig {
  slug: string;
  name: string;
  kind: "fireworks" | "night_show";
}

/**
 * Extreme heat at a theme park: the day's cost rises by `perDegree` for each
 * degree of the day's high above the threshold, up to `cap`. A forecast high
 * counts from `forecastFromC`, a day clearly hotter than the region's usual
 * summer day; past the forecast the climate normal stands in from
 * `normalFromC` (the top fifth of the summer's daily normals, "typically very
 * hot") at `normalWeight`: a normal is a 70-day mean, flatter than any one
 * day. How Orlando's were set: {@link ORLANDO_HEAT}; contract:
 * docs/frontend/trip-regions.md#how-a-day-is-priced.
 */
export interface HeatModelConfig {
  forecastFromC: number;
  normalFromC: number;
  perDegree: number;
  normalWeight: number;
  cap: number;
}

/**
 * A storm season (Orlando's Atlantic hurricanes, Japan's typhoons) as a cost:
 * `peakMinutes` × the date's weight added to every park's day. Weights by
 * half month, `[month, firstHalf, secondHalf]`, the halves split after the
 * 15th unless `firstHalfEnds` names another day for the month; the peak is
 * `peak.from` … `peak.to` (`MM-DD`, inclusive). Curated relative to the
 * season's climatology, never a forecast. Its codes are the kind's own:
 * `hurricane_season`/`hurricane_peak`, `typhoon_season`/`typhoon_peak`.
 */
export interface StormSeasonConfig {
  kind: "hurricane" | "typhoon";
  weights: ReadonlyArray<readonly [number, number, number]>;
  firstHalfEnds?: Readonly<Record<number, number>>;
  peak: { from: string; to: string };
  peakMinutes: number;
}

/**
 * A season the answer names but does not price: Japan's rainy season
 * (tsuyu). `from` … `to` as `MM-DD`, inclusive; the plan's days and `/when`'s
 * windows that touch it warn `rainy_season`, and `/when`'s months say
 * `rainySeason`.
 */
export interface SeasonNoteConfig {
  kind: "rainy_season";
  from: string;
  to: string;
}

/** What the weather does to a region's days. */
export interface RegionWeatherConfig {
  heat: HeatModelConfig;
  /** Absent: the region has no storm season worth a cost (California, Paris). */
  storm?: StormSeasonConfig;
  notes?: SeasonNoteConfig[];
  /** The park whose climate normals stand for the region's months (`/when`). */
  climatePark: string;
}

/**
 * Where a region's travel times come from. `osrm`: a drive with no traffic
 * between car parks, read once from OpenStreetMap
 * (`travel-matrix.generated.ts`), with curated legs laid over it. `curated`:
 * every leg is curated in the region's `legs` (`primary`), from timetables
 * and the operators' own pages, as for a region travelled by train.
 */
export type RegionTravelConfig =
  | { source: "osrm" }
  | {
      source: "curated";
      /** The day the curated legs were last checked. */
      computedOn: string;
      /** What a leg's minutes are: a timetable's door to door, waits included. */
      basis: "timetable";
      attribution?: string;
    };

/**
 * The markets a region's tickets are sold in (`TicketProduct.markets`, a
 * link's `markets`). `home` is sold to anyone at the operator's own shop: what
 * plan first suggests from. Orlando: `us` home, `uk` (UK and Ireland residents
 * only), `intl` (sellers abroad).
 */
export interface TicketMarketsConfig {
  home: string;
  codes: readonly string[];
}

/** A city a visitor sleeps in, in a region whose parks lie in several (Japan). */
export interface RegionBaseConfig {
  key: string;
  name: string;
  /** Its hotel areas (`hotelAreas` keys); each area belongs to one base. */
  hotelAreas: string[];
  /** The parks a day from here reaches: door to door, one way, curated. */
  parks: Array<{ slug: string; minutes: number; mode: TransportMode }>;
}

/** A move between two bases, both ways, door to door between their main hotel areas. */
export interface BaseLegConfig {
  from: string;
  to: string;
  minutes: number;
  mode: TransportMode;
  name?: string;
}

export interface RegionConfig {
  slug: string;
  name: string;
  timezone: string;
  groups: RegionGroupConfig[];
  parks: RegionParkConfig[];
  hotelAreas: HotelAreaConfig[];
  /** Per `hotel=` value the perks a stay there carries; the keys are the region's hotel kinds. */
  perks: Record<HotelKind, PerkRule[]>;
  /** Every perk the region's rules name, by its code: its English name (Parky). */
  perkNames: Record<PerkKind, string>;
  /** Every hotel kind, by its `hotel=` value: whose hotel it is, in English (Parky). */
  hotelNames: Record<HotelKind, string>;
  legs: CuratedLeg[];
  venues: VenueConfig[];
  /**
   * The parks one hopper ticket lets a guest move between on a day. Disney's
   * Park Hopper: the four theme parks. Universal's park-to-park: Universal
   * Studios and Islands of Adventure (the official store's "2-Park 1-Day
   * Park-to-Park Ticket"); Epic Universe sells as a one-park ticket and is
   * never hopped to or from here (whether a multi-day ticket allows it is not
   * verified). Water parks need their own add-on and are left out.
   */
  hopperSets: string[][];
  nightShows: Record<string, NightShowConfig[]>;
  weather: RegionWeatherConfig;
  travel: RegionTravelConfig;
  ticketMarkets: TicketMarketsConfig;
  /**
   * The cities a trip may stay in, where the parks lie too far apart for one
   * hotel (docs/frontend/trip-regions.md#bases-a-trip-across-several-cities).
   * Absent: one base, the region as a whole.
   */
  bases?: RegionBaseConfig[];
  baseLegs?: BaseLegConfig[];
  /**
   * The ticket catalog as loaded from the admin's table, archived products
   * included (`TicketCatalogService.withTickets`); absent: the seed in
   * `tickets.ts`. Never set in this file.
   */
  tickets?: TicketProduct[];
  /** The catalog's version: plans and `/when` are cached by it. */
  ticketsVersion?: string;
  /** The newest edit of the catalog. */
  ticketsUpdatedAt?: string;
}

/**
 * Orlando's heat thresholds. Magic Kingdom's 2015–2024 daily normals (ERA5) top
 * out at 32.1 °C in late June and run 30.9 °C at the June–October median
 * (measured 2026-11-12): a forecast from 35 °C is clearly hotter than a usual
 * summer day, a normal from 31.5 °C is the top fifth of those normals. How to
 * set another region's: the same two measurements at its climate park.
 */
export const ORLANDO_HEAT: HeatModelConfig = {
  forecastFromC: 35,
  normalFromC: 31.5,
  perDegree: 0.08,
  normalWeight: 0.5,
  cap: 1.4,
};

/**
 * The Atlantic hurricane season, 1 June to 30 November, weighted by half
 * month to the shape of the National Hurricane Center's climatology
 * (nhc.noaa.gov/climo, 1991–2020): the first named storm usually forms in
 * mid to late June, the first hurricane in early to mid August; the peak is
 * 10 September, with most activity between mid-August and mid-October. The
 * weights are curated relative to that peak, not NHC figures, and never a
 * forecast. August splits after the 14th, so the peak starts on 15 August.
 * Eight minutes at the peak is about a fifth of a typical headliner figure.
 */
export const ATLANTIC_HURRICANES: StormSeasonConfig = {
  kind: "hurricane",
  weights: [
    [6, 0.05, 0.1],
    [7, 0.15, 0.2],
    [8, 0.4, 0.8],
    [9, 1, 1],
    [10, 0.8, 0.4],
    [11, 0.15, 0.05],
  ],
  firstHalfEnds: { 8: 14 },
  peak: { from: "08-15", to: "10-15" },
  peakMinutes: 8,
};

/** Every region's `hotel=` values, in the order its `perks` lists them. */
export function hotelKinds(region: Pick<RegionConfig, "perks">): string[] {
  return Object.keys(region.perks);
}

const US = { continent: "north-america", country: "united-states" } as const;

const DISNEY_THEME_PARKS = [
  "magic-kingdom-park",
  "epcot",
  "disneys-hollywood-studios",
  "disneys-animal-kingdom-theme-park",
];
const DISNEY_PARKS = [
  ...DISNEY_THEME_PARKS,
  "disneys-typhoon-lagoon-water-park",
  "disneys-blizzard-beach-water-park",
];
const UNIVERSAL_PARKS = [
  "universal-studios-florida",
  "universal-islands-of-adventure",
  "universal-epic-universe",
  "universal-volcano-bay",
];

const DISNEY_EARLY: PerkRule = {
  perk: "early_entry",
  parks: DISNEY_THEME_PARKS,
  needs: "early",
  share: 0.05,
};
const UNIVERSAL_EARLY: PerkRule = {
  perk: "early_park_admission",
  parks: UNIVERSAL_PARKS,
  needs: "early",
  share: 0.05,
};

/** Resort buses run between every Disney hotel and park, and park to park. */
const bus: TransportAlternative = { mode: "bus" };

function disneyBusLegs(): CuratedLeg[] {
  const legs: CuratedLeg[] = [];
  for (let i = 0; i < DISNEY_PARKS.length; i++) {
    for (let j = i + 1; j < DISNEY_PARKS.length; j++) {
      legs.push({
        from: DISNEY_PARKS[i],
        to: DISNEY_PARKS[j],
        alternatives: [bus],
      });
    }
    legs.push({
      from: "hotel:disney",
      to: DISNEY_PARKS[i],
      alternatives: [bus],
    });
  }
  return legs;
}

/**
 * Merges curated legs for the same pair (either direction) into one, so the
 * generic bus list and the specific monorail or Skyliner entries stack.
 */
function mergeLegs(legs: CuratedLeg[]): CuratedLeg[] {
  const byPair = new Map<string, CuratedLeg>();
  for (const leg of legs) {
    const key = [leg.from, leg.to].sort().join("|");
    const found = byPair.get(key);
    if (!found) {
      byPair.set(key, { ...leg, alternatives: [...leg.alternatives] });
      continue;
    }
    for (const alt of leg.alternatives) {
      if (!found.alternatives.some((a) => a.mode === alt.mode)) {
        found.alternatives.push(alt);
      }
    }
    found.primary ??= leg.primary;
  }
  return [...byPair.values()];
}

export const ORLANDO: RegionConfig = {
  slug: "orlando",
  name: "Orlando",
  timezone: "America/New_York",
  weather: {
    heat: ORLANDO_HEAT,
    storm: ATLANTIC_HURRICANES,
    climatePark: "magic-kingdom-park",
  },
  travel: { source: "osrm" },
  ticketMarkets: { home: "us", codes: ["us", "uk", "intl"] },
  perkNames: {
    early_entry: "Early Entry",
    early_park_admission: "Early Park Admission",
    extended_evening: "Extended Evening Hours",
    express_included: "Express Unlimited",
  },
  hotelNames: {
    disney: "a Walt Disney World resort hotel",
    disney_deluxe: "a Disney Deluxe or Deluxe Villa resort hotel",
    universal: "a Universal Orlando hotel",
    universal_premier:
      "a Universal Premier hotel (Royal Pacific, Hard Rock, Portofino Bay)",
    offsite: "any other hotel",
  },
  groups: [
    {
      key: "walt-disney-world",
      name: "Walt Disney World Resort",
      kind: "resort",
      earlyKind: "early_entry",
    },
    {
      key: "universal-orlando",
      name: "Universal Orlando Resort",
      kind: "resort",
      earlyKind: "early_park_admission",
    },
    {
      key: "seaworld-orlando",
      name: "SeaWorld Orlando",
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
      slug: "magic-kingdom-park",
      ...US,
      city: "orlando",
      group: "walt-disney-world",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 1,
      carPark: {
        latitude: 28.4057,
        longitude: -81.579,
        note: "Transportation and Ticket Center; the park is across the lagoon",
      },
      transfer: { via: "ttc", minutes: 15 },
    },
    {
      slug: "epcot",
      ...US,
      city: "orlando",
      group: "walt-disney-world",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 2,
      carPark: {
        latitude: 28.3786,
        longitude: -81.5487,
        note: "main car park north of the main entrance",
      },
    },
    {
      slug: "disneys-hollywood-studios",
      ...US,
      city: "orlando",
      group: "walt-disney-world",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 5,
      carPark: {
        latitude: 28.3577,
        longitude: -81.5555,
        note: "car park east of the entrance",
      },
    },
    {
      slug: "disneys-animal-kingdom-theme-park",
      ...US,
      city: "orlando",
      group: "walt-disney-world",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 7,
      carPark: {
        latitude: 28.361,
        longitude: -81.588,
        note: "car park north-east of the entrance",
      },
    },
    {
      slug: "disneys-typhoon-lagoon-water-park",
      ...US,
      city: "orlando",
      group: "walt-disney-world",
      type: "water_park",
      headliner: false,
      carPark: {
        latitude: 28.3664,
        longitude: -81.5258,
        note: "car park north-east of the park off Buena Vista Drive",
      },
    },
    {
      slug: "disneys-blizzard-beach-water-park",
      ...US,
      city: "orlando",
      group: "walt-disney-world",
      type: "water_park",
      headliner: false,
      carPark: {
        latitude: 28.3542,
        longitude: -81.5713,
        note: "car park east of the entrance",
      },
    },
    {
      slug: "universal-studios-florida",
      ...US,
      city: "orlando",
      group: "universal-orlando",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 6,
      carPark: {
        latitude: 28.4738,
        longitude: -81.462,
        note: "Universal's parking garages, shared with Islands of Adventure",
      },
    },
    {
      slug: "universal-islands-of-adventure",
      ...US,
      city: "orlando",
      group: "universal-orlando",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 4,
      carPark: {
        latitude: 28.4738,
        longitude: -81.462,
        note: "Universal's parking garages, shared with Universal Studios",
      },
    },
    {
      slug: "universal-epic-universe",
      ...US,
      city: "orlando",
      group: "universal-orlando",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 3,
      carPark: {
        latitude: 28.4425,
        longitude: -81.443,
        note: "Epic Universe's own car park off Universal Boulevard",
      },
    },
    {
      slug: "universal-volcano-bay",
      ...US,
      city: "orlando",
      group: "universal-orlando",
      type: "water_park",
      headliner: false,
      carPark: {
        latitude: 28.4738,
        longitude: -81.462,
        note: "no car park of its own: guests park in Universal's garages and take the shuttle",
      },
      transfer: { via: "garage_shuttle", minutes: 15 },
    },
    {
      slug: "seaworld-orlando",
      ...US,
      city: "orlando",
      group: "seaworld-orlando",
      type: "theme_park",
      headliner: false,
      carPark: {
        latitude: 28.4141,
        longitude: -81.459,
        note: "car park entrance on International Drive",
      },
    },
    {
      slug: "aquatica-orlando",
      ...US,
      city: "orlando",
      group: "seaworld-orlando",
      type: "water_park",
      headliner: false,
      carPark: {
        latitude: 28.4164,
        longitude: -81.4549,
        note: "car park on International Drive, opposite SeaWorld",
      },
    },
    {
      slug: "discovery-cove-orlando",
      ...US,
      city: "orlando",
      group: "seaworld-orlando",
      type: "reservation_park",
      headliner: false,
      carPark: {
        latitude: 28.4048,
        longitude: -81.4625,
        note: "entrance road off Central Florida Parkway",
      },
    },
    {
      slug: "legoland-florida",
      ...US,
      city: "winter-haven",
      group: "day-trips",
      type: "theme_park",
      headliner: false,
      carPark: {
        latitude: 27.9917,
        longitude: -81.688,
        note: "car park north of the entrance, Winter Haven",
      },
    },
    {
      slug: "busch-gardens-tampa",
      ...US,
      city: "tampa",
      group: "day-trips",
      type: "theme_park",
      headliner: false,
      carPark: {
        latitude: 28.0334,
        longitude: -82.4202,
        note: "drop-off on McKinley Drive, Tampa",
      },
    },
    {
      slug: "adventure-island-tampa",
      ...US,
      city: "tampa",
      group: "day-trips",
      type: "water_park",
      headliner: false,
      carPark: {
        latitude: 28.0417,
        longitude: -82.412,
        note: "car park on McKinley Drive, Tampa",
      },
    },
  ],
  venues: [
    {
      key: "disney-springs",
      name: "Disney Springs",
      group: "walt-disney-world",
      kind: "district",
      latitude: 28.3706,
      longitude: -81.5152,
      typicalHours: { opens: "10:00", closes: "23:00" },
      free: true,
      url: "https://disneyworld.disney.go.com/destinations/disney-springs/",
      notes: "shops and restaurants; entry and parking free",
      highlights: ["dining", "shopping"],
    },
    {
      key: "disney-boardwalk",
      name: "Disney's BoardWalk",
      group: "walt-disney-world",
      kind: "resort_area",
      latitude: 28.3672,
      longitude: -81.5552,
      free: true,
      url: "https://disneyworld.disney.go.com/destinations/boardwalk/",
      notes:
        "a lakeside promenade by EPCOT's International Gateway; EPCOT's fireworks are seen from it",
      viewOf: "epcot",
      highlights: ["dining"],
    },
    {
      key: "polynesian-beach",
      name: "Disney's Polynesian Village Resort beach",
      group: "walt-disney-world",
      kind: "viewpoint",
      latitude: 28.4049,
      longitude: -81.5827,
      free: true,
      url: "https://disneyworld.disney.go.com/resorts/polynesian-resort/",
      notes:
        "across the lagoon from Magic Kingdom, on the resort monorail; at busy times Disney admits hotel guests and diners only",
      viewOf: "magic-kingdom-park",
      highlights: ["dining"],
    },
    {
      key: "universal-citywalk",
      name: "Universal CityWalk",
      group: "universal-orlando",
      kind: "district",
      latitude: 28.4738,
      longitude: -81.462,
      typicalHours: { opens: "11:00", closes: "02:00" },
      free: true,
      url: "https://www.universalorlando.com/web/en/us/universal-citywalk",
      notes:
        "between Universal Studios and Islands of Adventure, by the parking garages; entry free, parking paid",
      highlights: ["dining", "shopping"],
    },
    {
      key: "icon-park",
      name: "ICON Park, International Drive",
      group: "seaworld-orlando",
      kind: "district",
      latitude: 28.4433,
      longitude: -81.4686,
      free: true,
      url: "https://iconparkorlando.com/",
      notes: "entry free, attractions paid; hours vary by venue",
      highlights: ["dining", "shopping"],
    },
  ],
  hopperSets: [
    DISNEY_THEME_PARKS,
    ["universal-studios-florida", "universal-islands-of-adventure"],
  ],
  nightShows: {
    "magic-kingdom-park": [
      {
        slug: "happily-ever-after",
        name: "Happily Ever After",
        kind: "fireworks",
      },
      {
        slug: "disney-starlight-dream-the-night-away",
        name: "Disney Starlight: Dream the Night Away",
        kind: "night_show",
      },
    ],
    epcot: [
      {
        slug: "luminous-the-symphony-of-us",
        name: "Luminous The Symphony of Us",
        kind: "fireworks",
      },
    ],
    "disneys-hollywood-studios": [
      { slug: "fantasmic", name: "Fantasmic!", kind: "night_show" },
      {
        slug: "wonderful-world-of-animation",
        name: "Wonderful World of Animation",
        kind: "night_show",
      },
      {
        slug: "disney-movie-magic",
        name: "Disney Movie Magic",
        kind: "night_show",
      },
    ],
  },
  hotelAreas: [
    {
      key: "disney",
      name: "Walt Disney World",
      latitude: 28.3668,
      longitude: -81.544,
      hotel: "disney",
      note: "Disney's Caribbean Beach Resort, the Skyliner hub at the centre of the resort",
    },
    {
      key: "universal",
      name: "Universal Orlando hotels",
      latitude: 28.467,
      longitude: -81.465,
      hotel: "universal",
      note: "Universal Boulevard by the Royal Pacific and Sapphire Falls hotels",
    },
    {
      key: "international_drive",
      name: "International Drive",
      latitude: 28.444,
      longitude: -81.47,
      hotel: "offsite",
      note: "International Drive at ICON Park, the middle of the hotel strip",
    },
    {
      key: "lake_buena_vista",
      name: "Lake Buena Vista",
      latitude: 28.3768,
      longitude: -81.5138,
      hotel: "offsite",
      note: "Hotel Plaza Boulevard by Disney Springs",
    },
    {
      key: "kissimmee",
      name: "Kissimmee (US-192)",
      latitude: 28.3326,
      longitude: -81.512,
      hotel: "offsite",
      note: "US-192 (Irlo Bronson Memorial Highway) east of the Disney gate",
    },
  ],
  perks: {
    disney: [DISNEY_EARLY],
    disney_deluxe: [
      DISNEY_EARLY,
      {
        perk: "extended_evening",
        parks: DISNEY_THEME_PARKS,
        needs: "extended_evening",
        share: 0.1,
      },
    ],
    universal: [UNIVERSAL_EARLY],
    universal_premier: [
      UNIVERSAL_EARLY,
      {
        // Universal Express Unlimited is included with a stay at the Royal
        // Pacific, Hard Rock and Portofino Bay hotels, at these two parks only.
        perk: "express_included",
        parks: ["universal-studios-florida", "universal-islands-of-adventure"],
        needs: "always",
        share: 0.6,
      },
    ],
    offsite: [],
  },
  legs: mergeLegs([
    ...disneyBusLegs(),
    {
      from: "magic-kingdom-park",
      to: "epcot",
      alternatives: [{ mode: "monorail", minutes: 30, via: "ttc" }],
    },
    {
      from: "epcot",
      to: "disneys-hollywood-studios",
      alternatives: [
        { mode: "skyliner", minutes: 20, via: "caribbean_beach" },
        { mode: "boat", minutes: 25, via: "international_gateway" },
        { mode: "walk", minutes: 30, via: "international_gateway" },
      ],
    },
    {
      from: "hotel:disney",
      to: "epcot",
      alternatives: [
        { mode: "skyliner", minutes: 15, via: "international_gateway" },
      ],
    },
    {
      from: "hotel:disney",
      to: "disneys-hollywood-studios",
      alternatives: [{ mode: "skyliner", minutes: 15 }],
    },
    {
      from: "universal-studios-florida",
      to: "universal-islands-of-adventure",
      primary: { mode: "walk", minutes: 10, km: 0.8 },
      alternatives: [
        { mode: "walk", minutes: 10, via: "citywalk" },
        {
          mode: "train",
          minutes: 15,
          name: "Hogwarts Express",
          ticket: "park_to_park",
        },
      ],
    },
    {
      from: "universal-studios-florida",
      to: "universal-volcano-bay",
      primary: { mode: "bus", minutes: 20 },
      alternatives: [{ mode: "bus", via: "garage_shuttle" }],
    },
    {
      from: "universal-islands-of-adventure",
      to: "universal-volcano-bay",
      primary: { mode: "bus", minutes: 20 },
      alternatives: [{ mode: "bus", via: "garage_shuttle" }],
    },
    {
      from: "universal-studios-florida",
      to: "universal-epic-universe",
      alternatives: [{ mode: "bus", name: "Universal shuttle" }],
    },
    {
      from: "universal-islands-of-adventure",
      to: "universal-epic-universe",
      alternatives: [{ mode: "bus", name: "Universal shuttle" }],
    },
    {
      from: "hotel:universal",
      to: "universal-epic-universe",
      alternatives: [{ mode: "bus", name: "Universal shuttle" }],
    },
    {
      from: "hotel:universal",
      to: "universal-volcano-bay",
      alternatives: [{ mode: "bus", name: "Universal shuttle" }],
    },
    {
      from: "venue:universal-citywalk",
      to: "universal-studios-florida",
      primary: { mode: "walk", minutes: 5 },
      alternatives: [{ mode: "walk", minutes: 5 }],
    },
    {
      from: "venue:universal-citywalk",
      to: "universal-islands-of-adventure",
      primary: { mode: "walk", minutes: 5 },
      alternatives: [{ mode: "walk", minutes: 5 }],
    },
    {
      from: "venue:disney-boardwalk",
      to: "epcot",
      alternatives: [
        { mode: "walk", minutes: 10, via: "international_gateway" },
      ],
    },
    {
      from: "venue:polynesian-beach",
      to: "magic-kingdom-park",
      alternatives: [{ mode: "monorail", minutes: 10 }],
    },
    {
      from: "hotel:universal",
      to: "universal-studios-florida",
      alternatives: [{ mode: "walk", minutes: 15, via: "citywalk" }],
    },
    {
      from: "hotel:universal",
      to: "universal-islands-of-adventure",
      alternatives: [{ mode: "walk", minutes: 15, via: "citywalk" }],
    },
  ]),
};

// ── Southern California (PAR-1303) ──────────────────────────────────────────
// Researched 2026-11-13: the parks, car parks, hotels and transport from the
// operators' pages and OpenStreetMap; the night shows and day-guest closes
// from our own data. The sources per value are in the research's region.md.

/**
 * Southern California's heat thresholds, set as Orlando's were at the
 * region's climate park: Disneyland's 2015–2024 daily normals (ERA5, measured
 * 2026-11-14) top out at 32.4 °C on 7 September, and the top fifth of its
 * June–October normals starts at 30.4 °C. A forecast counts from 35 °C (the
 * highest normal plus Orlando's margin of about 3 °C), a normal from 30.5 °C.
 * Every park is read against them with its own temperatures: Six Flags Magic
 * Mountain, inland in the Santa Clarita Valley, runs a June–October median of
 * 32.9 °C and peaks at 35.7 °C, so it is the park the heat prices most; the
 * San Diego parks never reach the normal's threshold.
 */
export const SOUTHERN_CALIFORNIA_HEAT: HeatModelConfig = {
  forecastFromC: 35,
  normalFromC: 30.5,
  perDegree: 0.08,
  normalWeight: 0.5,
  cap: 1.4,
};

const DISNEYLAND_RESORT_PARKS = [
  "disneyland-park",
  "disney-california-adventure-park",
];

/** Mickey & Friends: the drive to either Disneyland park ends there, the tram follows. */
const MICKEY_AND_FRIENDS = {
  latitude: 33.8153,
  longitude: -117.9262,
  note: "Mickey & Friends parking structure off Disneyland Drive (Pixar Pals is beside it); the tram runs to the Esplanade between the two gates",
};

export const SOUTHERN_CALIFORNIA: RegionConfig = {
  slug: "southern-california",
  name: "Southern California",
  timezone: "America/Los_Angeles",
  // No storm season: coastal Southern California has no hurricane season.
  weather: {
    heat: SOUTHERN_CALIFORNIA_HEAT,
    climatePark: "disneyland-park",
  },
  travel: { source: "osrm" },
  ticketMarkets: { home: "us", codes: ["us", "uk", "intl"] },
  // No perk in this region. Disneyland ended Early Entry for its hotel guests
  // on 2026-01-05 (the last day was 2026-01-04); its hotel guests now get one
  // Lightning Lane Multi Pass entry per stay, a single ride's skip that moves
  // no park choice and has no perk kind. Universal owns no hotel here, and the
  // hotels by Universal City run a shuttle without any admission benefit.
  perkNames: {},
  hotelNames: {
    disney:
      "a Disneyland Resort hotel (Disneyland Hotel, Grand Californian, Pixar Place)",
    offsite: "any other hotel",
  },
  groups: [
    {
      key: "disneyland-resort",
      name: "Disneyland Resort",
      kind: "resort",
      earlyKind: "early_entry",
    },
    {
      // A day trip, not a resort: one park, no hotel of Universal's own, and
      // 54 km from the Disneyland gates.
      key: "universal-studios-hollywood",
      name: "Universal Studios Hollywood",
      kind: "day_trip",
      // The feed carries a one-hour slot before opening every day from
      // September 2026, most likely the separately sold SUPER NINTENDO WORLD
      // Early Access (Universal's pages as the search index quotes them; not
      // verified). No hotel guest gets it, so it carries a name of its own
      // rather than Orlando's hotel perk, and a client that does not know the
      // code shows nothing.
      earlyKind: "early_access_ticket",
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
      slug: "disneyland-park",
      ...US,
      city: "anaheim",
      group: "disneyland-resort",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 1,
      carPark: MICKEY_AND_FRIENDS,
      transfer: { via: "tram", minutes: 15 },
    },
    {
      slug: "disney-california-adventure-park",
      ...US,
      city: "anaheim",
      group: "disneyland-resort",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 2,
      carPark: MICKEY_AND_FRIENDS,
      transfer: { via: "tram", minutes: 15 },
    },
    {
      slug: "universal-studios-hollywood",
      ...US,
      city: "los-angeles",
      group: "universal-studios-hollywood",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 3,
      carPark: {
        // Which of Universal's garages is open to day guests on a day is not
        // published where we could read it; the Frankenstein garage is the
        // one at the top of the drive, by CityWalk.
        latitude: 34.1383,
        longitude: -118.3566,
        note: "Universal's garages at the top of Universal Hollywood Drive (Frankenstein garage); the gate is a walk through CityWalk",
      },
    },
    {
      slug: "knotts-berry-farm",
      ...US,
      city: "buena-park",
      group: "day-trips",
      type: "theme_park",
      headliner: false,
      carPark: {
        latitude: 33.8449,
        longitude: -117.9955,
        note: "main car park off Beach Boulevard, Buena Park",
      },
    },
    {
      slug: "six-flags-magic-mountain",
      ...US,
      city: "santa-clarita",
      group: "day-trips",
      type: "theme_park",
      headliner: false,
      carPark: {
        latitude: 34.4289,
        longitude: -118.5961,
        note: "main car park north of the entrance off Magic Mountain Parkway, Valencia",
      },
    },
    {
      slug: "seaworld-san-diego",
      ...US,
      city: "san-diego",
      group: "day-trips",
      type: "theme_park",
      headliner: false,
      carPark: {
        latitude: 32.7629,
        longitude: -117.225,
        note: "main car park off SeaWorld Drive, Mission Bay",
      },
    },
    {
      slug: "legoland-california",
      ...US,
      city: "carlsbad",
      group: "day-trips",
      type: "theme_park",
      headliner: false,
      carPark: {
        latitude: 33.1244,
        longitude: -117.3119,
        note: "car park south of the entrance off LEGOLAND Drive and Armada Drive, Carlsbad",
      },
    },
    {
      slug: "sesame-place-san-diego",
      ...US,
      city: "chula-vista",
      group: "day-trips",
      type: "theme_park",
      headliner: false,
      carPark: {
        latitude: 32.5881,
        longitude: -117.0058,
        note: "car park by the North Island Credit Union Amphitheatre, Entertainment Circle, Chula Vista",
      },
    },
  ],
  venues: [
    {
      key: "downtown-disney",
      name: "Downtown Disney District",
      group: "disneyland-resort",
      kind: "district",
      latitude: 33.8091,
      longitude: -117.9214,
      typicalHours: { opens: "07:00", closes: "01:00" },
      free: true,
      url: "https://disneyland.disney.go.com/destinations/downtown-disney-district/",
      notes:
        "between the Esplanade and the Disneyland Hotel; no ticket needed; open 07:00-01:00, Thursdays to 23:00; parking in the Simba Lot",
      highlights: ["dining", "shopping"],
    },
    {
      key: "universal-citywalk",
      name: "Universal CityWalk Hollywood",
      group: "universal-studios-hollywood",
      kind: "district",
      latitude: 34.1363,
      longitude: -118.3549,
      free: true,
      url: "https://www.universalstudioshollywood.com/web/en/us/things-to-do/citywalk",
      notes:
        "outside the park gate, between the garages and the entrance; entry free, parking paid; venue and cinema hours vary",
      highlights: ["dining", "shopping"],
    },
  ],
  hopperSets: [DISNEYLAND_RESORT_PARKS],
  nightShows: {
    // Fire of the Rising Moons is the castle fireworks seen from Galaxy's
    // Edge, so it is left out: it would count one show twice. On some nights
    // (2026-11-13) our data lists the fireworks under that slug only.
    "disneyland-park": [
      {
        slug: "believein-holiday-magic-fireworks-spectacular",
        name: '"Believe...In Holiday Magic" Fireworks Spectacular',
        kind: "fireworks",
      },
      {
        slug: "halloween-screams-with-fireworks",
        name: "Halloween Screams",
        kind: "fireworks",
      },
      { slug: "fantasmic", name: "Fantasmic!", kind: "night_show" },
      {
        slug: "tapestry-of-happiness",
        name: "Tapestry of Happiness",
        kind: "night_show",
      },
      {
        slug: "shadows-of-memory-a-skywalker-saga",
        name: "Shadows of Memory: A Skywalker Saga",
        kind: "night_show",
      },
    ],
    // World of Color Happiness! was the 70th celebration's version, which
    // ended on 2026-08-09; ONE is the one performing.
    "disney-california-adventure-park": [
      {
        slug: "world-of-color-one",
        name: "World of Color - ONE",
        kind: "night_show",
      },
    ],
    "seaworld-san-diego": [
      {
        slug: "a-christmas-wish-fireworks-show",
        name: "A Christmas Wish Fireworks Show",
        kind: "fireworks",
      },
      {
        slug: "donner-and-blitzens-lazer-light-show",
        name: "Donner and Blitzen's Lazer Light Show",
        kind: "night_show",
      },
      { slug: "fireworks", name: "Fireworks", kind: "fireworks" },
    ],
  },
  hotelAreas: [
    {
      key: "disneyland_resort",
      name: "Disneyland Resort hotels",
      latitude: 33.8078,
      longitude: -117.9248,
      hotel: "disney",
      note: "the Disneyland Hotel, Grand Californian and Pixar Place along Downtown Disney; the point is the middle of the three",
    },
    {
      key: "anaheim_harbor",
      name: "Harbor Boulevard (Anaheim)",
      latitude: 33.8063,
      longitude: -117.9145,
      hotel: "offsite",
      note: "Harbor Boulevard opposite the main entrance, where the walking-distance Good Neighbor hotels are (The Anaheim Hotel, 1700 S Harbor Blvd)",
    },
    {
      key: "buena_park",
      name: "Buena Park",
      latitude: 33.8404,
      longitude: -117.9984,
      hotel: "offsite",
      note: "Knott's Hotel on Crescent Avenue, next to Knott's Berry Farm",
    },
    {
      key: "universal_city",
      name: "Universal City",
      latitude: 34.1369,
      longitude: -118.3583,
      hotel: "offsite",
      note: "Hilton Los Angeles/Universal City and Sheraton Universal below CityWalk; free hotel shuttle to the park, no admission perks",
    },
    {
      key: "hollywood",
      name: "Hollywood",
      latitude: 34.1017,
      longitude: -118.3393,
      hotel: "offsite",
      note: "Hollywood Boulevard at Hollywood/Highland, one Metro B Line stop from Universal City",
    },
    {
      key: "santa_monica",
      name: "Santa Monica",
      latitude: 34.0163,
      longitude: -118.4969,
      hotel: "offsite",
      note: "downtown Santa Monica at the Third Street Promenade, near the pier",
    },
    {
      key: "downtown_la",
      name: "Downtown Los Angeles",
      latitude: 34.0484,
      longitude: -118.253,
      hotel: "offsite",
      note: "Pershing Square, on the Metro B Line to Universal City",
    },
    {
      key: "san_diego",
      name: "San Diego (Downtown)",
      latitude: 32.7115,
      longitude: -117.1601,
      hotel: "offsite",
      note: "Gaslamp Quarter, downtown San Diego; Santa Fe Depot (Pacific Surfliner, COASTER) is 1 km west",
    },
    {
      key: "carlsbad",
      name: "Carlsbad",
      latitude: 33.126,
      longitude: -117.3096,
      hotel: "offsite",
      note: "the LEGOLAND California Hotel at the park; Carlsbad's other hotels are along Palomar Airport Road and in the Village",
    },
  ],
  perks: { disney: [], offsite: [] },
  legs: [
    {
      // The two gates face each other across the Esplanade; both parks share
      // the car park, so OSRM answers no drive.
      from: "disneyland-park",
      to: "disney-california-adventure-park",
      primary: { mode: "walk", minutes: 5, km: 0.1 },
      alternatives: [{ mode: "walk", minutes: 5, via: "esplanade" }],
    },
    {
      from: "hotel:disneyland_resort",
      to: "disneyland-park",
      primary: { mode: "walk", minutes: 15, via: "downtown_disney" },
      alternatives: [{ mode: "walk", minutes: 15, via: "downtown_disney" }],
    },
    {
      from: "hotel:disneyland_resort",
      to: "disney-california-adventure-park",
      primary: { mode: "walk", minutes: 15, via: "downtown_disney" },
      alternatives: [
        { mode: "walk", minutes: 15, via: "downtown_disney" },
        // The Grand Californian's own gate into the park reopened in January
        // 2026 (press, not Disney's page) and may be kept for its guests at
        // busy times, so it is an alternative, never the leg.
        { mode: "walk", minutes: 10, via: "grand_californian_entrance" },
      ],
    },
    {
      from: "hotel:anaheim_harbor",
      to: "disneyland-park",
      primary: { mode: "walk", minutes: 10, via: "harbor_entrance" },
      alternatives: [
        { mode: "walk", minutes: 10, via: "harbor_entrance" },
        { mode: "bus", name: "Anaheim Resort Transportation (ART)" },
      ],
    },
    {
      from: "hotel:anaheim_harbor",
      to: "disney-california-adventure-park",
      primary: { mode: "walk", minutes: 10, via: "harbor_entrance" },
      alternatives: [
        { mode: "walk", minutes: 10, via: "harbor_entrance" },
        { mode: "bus", name: "Anaheim Resort Transportation (ART)" },
      ],
    },
    {
      from: "venue:downtown-disney",
      to: "disneyland-park",
      primary: { mode: "walk", minutes: 10 },
      alternatives: [{ mode: "walk", minutes: 10 }],
    },
    {
      from: "venue:downtown-disney",
      to: "disney-california-adventure-park",
      primary: { mode: "walk", minutes: 10 },
      alternatives: [{ mode: "walk", minutes: 10 }],
    },
    {
      from: "hotel:buena_park",
      to: "knotts-berry-farm",
      primary: { mode: "walk", minutes: 10 },
      alternatives: [{ mode: "walk", minutes: 10 }],
    },
    {
      from: "hotel:universal_city",
      to: "universal-studios-hollywood",
      alternatives: [
        { mode: "shuttle", name: "hotel shuttle" },
        { mode: "walk", minutes: 15, via: "citywalk" },
      ],
    },
    {
      from: "venue:universal-citywalk",
      to: "universal-studios-hollywood",
      primary: { mode: "walk", minutes: 5 },
      alternatives: [{ mode: "walk", minutes: 5 }],
    },
    {
      // Metro to Universal City/Studio City, then Universal's free shuttle up
      // the hill; no minutes, the waits for both vary too much.
      from: "hotel:hollywood",
      to: "universal-studios-hollywood",
      alternatives: [
        {
          mode: "metro",
          name: "Metro B Line",
          via: "universal_city_studio_city",
        },
      ],
    },
    {
      from: "hotel:downtown_la",
      to: "universal-studios-hollywood",
      alternatives: [
        {
          mode: "metro",
          name: "Metro B Line",
          via: "universal_city_studio_city",
        },
      ],
    },
    {
      from: "hotel:carlsbad",
      to: "legoland-california",
      primary: { mode: "walk", minutes: 10 },
      alternatives: [{ mode: "walk", minutes: 10 }],
    },
    {
      // The COASTER to Carlsbad Poinsettia, then 2 km by bus or taxi.
      from: "hotel:san_diego",
      to: "legoland-california",
      alternatives: [
        { mode: "rail", name: "COASTER", via: "carlsbad_poinsettia" },
      ],
    },
  ],
};

export const TRIP_REGIONS: Record<string, RegionConfig> = {
  [ORLANDO.slug]: ORLANDO,
  [SOUTHERN_CALIFORNIA.slug]: SOUTHERN_CALIFORNIA,
};

/** The region a slug names, or null. */
export function findRegion(slug: string): RegionConfig | null {
  return Object.prototype.hasOwnProperty.call(TRIP_REGIONS, slug)
    ? TRIP_REGIONS[slug]
    : null;
}

/** `hotel:<key>` for a hotel area, the form a leg's `from`/`to` takes. */
export function hotelPoint(key: string): string {
  return `hotel:${key}`;
}

/** The geographic path of a park, as `parks` stores it. */
export interface ParkPathLike {
  continentSlug?: string | null;
  countrySlug?: string | null;
  citySlug?: string | null;
  slug?: string | null;
}

/** The region a park belongs to, matched on its full geographic path, or null. */
export function regionOfPark(park: ParkPathLike): RegionConfig | null {
  for (const region of Object.values(TRIP_REGIONS)) {
    const found = region.parks.some(
      (p) =>
        p.slug === park.slug &&
        p.city === park.citySlug &&
        p.country === park.countrySlug &&
        p.continent === park.continentSlug,
    );
    if (found) return region;
  }
  return null;
}

/** `venue:<key>`, the form a leg's `from`/`to` takes for a venue. */
export function venuePoint(key: string): string {
  return `venue:${key}`;
}
