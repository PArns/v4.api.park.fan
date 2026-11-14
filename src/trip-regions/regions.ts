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
  /**
   * A short curated code for tight views (the month grid's cell), unique in
   * the region: Tokyo Disneyland `DL` and Tokyo DisneySea `DS`, where initials
   * would give both `TD`. Absent: the client derives one.
   */
  code?: string;
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
  /**
   * `after_first_night`: every open day the guest woke at the hotel, so not
   * the day they check in (Tokyo Disney Resort's Happy Entry): the trip's
   * arrival day, or in a region with bases the first day of the stay.
   */
  needs: "early" | "extended_evening" | "always" | "after_first_night";
  share: number;
  /** Dates the perk does not hold (a hotel's published blackouts). */
  blackoutDates?: string[];
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

/**
 * Where a trip arrives or leaves: an airport, or a railway station a
 * long-distance train reaches (Marne-la-Vallée–Chessy for the TGV and the
 * Eurostar), and the base it serves.
 */
export interface GatewayConfig {
  /** The IATA code (`HND`, `CDG`; a station's where it has one, `XED`), else a short code of our own. */
  code: string;
  name: string;
  kind: "airport" | "station";
  /** Typical minutes to the base's main hotel area. */
  minutes?: number;
}

/** A city a visitor sleeps in, in a region whose parks lie in several (Japan). */
export interface RegionBaseConfig {
  key: string;
  name: string;
  /**
   * Its place on the route line, where the region's bases lie on one (Japan:
   * Tokyo, Fujikawaguchiko, Nagoya, Osaka): a trip's stays follow the line
   * one way or the other, never doubling back. Absent on every base (Paris,
   * whose three bases all reach each other): the stays may come in any order
   * the base legs connect.
   */
  order?: number;
  /** Its hotel areas (`hotelAreas` keys); each area belongs to one base. */
  hotelAreas: string[];
  /** The area a stay is reckoned from when the visitor names none. */
  defaultArea?: string;
  /** The airports and stations that serve it: the client maps one to `startBase` and `endBase`. */
  gateways?: GatewayConfig[];
  /** Where to stay in it, a curated hint in English. */
  stayNote?: string;
  /**
   * The parks a day from here reaches: door to door from its usual area, one
   * way, curated. A stay's own area refines the minutes where a leg from it
   * (`hotel:<area>`) is curated.
   */
  parks: Array<{
    slug: string;
    minutes: number;
    mode: "drive" | TransportMode;
  }>;
}

/** A move between two bases, both ways, door to door between their main hotel areas. */
export interface BaseLegConfig {
  from: string;
  to: string;
  minutes: number;
  mode: "drive" | TransportMode;
  name?: string;
  /**
   * Whether a park fits the day of this move before or after it: overrides
   * the region's `parkDayMaxLegMinutes` rule (a curated call, e.g. false for
   * Fujikawaguchiko to Nagoya, a full day by bus and train).
   */
  parkDay?: boolean;
  changes?: number;
  /** Where the train or coach leaves and arrives: `Shinjuku`, `Kawaguchiko`. */
  fromStop?: string;
  toStop?: string;
  /** Curated notes the client words: the rail pass, a seat reservation, luggage sent ahead. */
  notes?: Array<"jr_pass" | "reserve_seat" | "forward_luggage">;
}

/**
 * How stays and moves are priced (docs/frontend/trip-regions.md#bases-a-trip-across-several-cities).
 * A region may set any of them; {@link DEFAULT_STAY_RULES} fills the rest.
 */
export interface StayRulesConfig {
  /** Travel to a park up to this, one way, costs a day nothing. */
  dayTripFreeMinutes: number;
  /** Checking out, luggage and checking in on a move. */
  moveOverheadMinutes: number;
  /** A move up to this, door to door, leaves room for a park the same day. */
  parkDayMaxLegMinutes: number;
  /** What a hotel change costs the plan, in minutes of headliner wait. */
  moveCost: number;
  /** When a morning move leaves, park-local `HH:MM`. */
  moveStartsAt: string;
  /** An evening move after a park must arrive by this, park-local `HH:MM`. */
  lastArrivalAt: string;
}

/**
 * The move buffers a visitor may choose (`moveBuffer=`), in minutes on top of the train: 30 when
 * the bags travel ahead by courier, more for a slower pace or bags carried along. A short list,
 * so the plans stay cacheable.
 */
export const MOVE_BUFFER_OPTIONS = [30, 60, 90, 120] as const;

/** The stay rules a region leaves unset (owner-approved starting values, PAR-1302). */
export const DEFAULT_STAY_RULES: StayRulesConfig = {
  dayTripFreeMinutes: 45,
  moveOverheadMinutes: 60,
  parkDayMaxLegMinutes: 150,
  moveCost: 20,
  moveStartsAt: "08:00",
  lastArrivalAt: "23:00",
};

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
  /** How the region prices stays and moves; unset fields take {@link DEFAULT_STAY_RULES}. */
  stayRules?: Partial<StayRulesConfig>;
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

/** A region's stay rules, its own over the defaults. */
export function stayRulesOf(
  region: Pick<RegionConfig, "stayRules">,
): StayRulesConfig {
  return { ...DEFAULT_STAY_RULES, ...region.stayRules };
}

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

// ── Japan ──────────────────────────────────────────────────────────────────
// Researched 2026-11-13 (PAR-1304): JR Central's and Fujikyu's timetables,
// Tokyo Disney Resort's FAQ, the parks' own pages, JMA's normals. Travel is
// curated (no drive matrix): door to door, the wait for the train included.

const JP = { continent: "asia", country: "japan" } as const;
const TDL = "tokyo-disneyland";
const TDS = "tokyo-disneysea";
const FUJI_Q = "fuji-q-highland";
const LEGOLAND_JP = "legoland-japan";
const USJ = "universal-studios-japan";

/** A curated door-to-door leg, the leg itself in a region whose travel is curated. */
const jpLeg = (
  from: string,
  to: string,
  mode: TransportMode,
  minutes: number,
  name?: string,
): CuratedLeg => ({
  from,
  to,
  primary: { mode, minutes, ...(name && { name }) },
  alternatives: [],
});

export const JAPAN: RegionConfig = {
  slug: "japan",
  name: "Japan",
  timezone: "Asia/Tokyo",
  weather: {
    // JMA 1991–2020 mean daily highs: Tokyo 31.3 °C in August, Osaka 33.7,
    // Nagoya 33.2, Kawaguchiko 28.1. A forecast from 35 °C is clearly hotter
    // than a usual summer day in all of them; a normal from 30.5 °C is the top
    // of Tokyo's June–September (and humid heat weighs more than the figure).
    heat: { ...ORLANDO_HEAT, normalFromC: 30.5 },
    // Typhoon approaches by month, curated relative to September from JMA's
    // 1991–2020 normals (Kanto-Koshin Jun 0.2, Jul 0.4, Aug 0.8, Sep 1.2,
    // Oct 0.7; Kinki 0.3, 0.6, 0.7, 1.1, 0.7). Monthly data only: the
    // half-month split is curated, never a forecast.
    storm: {
      kind: "typhoon",
      weights: [
        [6, 0.15, 0.25],
        [7, 0.35, 0.5],
        [8, 0.6, 0.7],
        [9, 1, 1],
        [10, 0.8, 0.45],
        [11, 0.05, 0],
      ],
      peak: { from: "09-01", to: "10-15" },
      peakMinutes: 8,
    },
    // Tsuyu: JMA's normal onset and end, Kanto-Koshin 06-07 … 07-19, Kinki
    // and Tokai 06-06 … 07-19. Named, not priced: rain on most days, not a
    // closure, and the forecast carries the day's own rain.
    notes: [{ kind: "rainy_season", from: "06-06", to: "07-19" }],
    climatePark: TDL,
  },
  travel: { source: "curated", computedOn: "2026-11-13", basis: "timetable" },
  // `jp`: the operators' own shops, which sell to anyone; `intl`: the
  // authorized sellers abroad Tokyo Disney Resort names for its 1-Day Passport.
  ticketMarkets: { home: "jp", codes: ["jp", "intl"] },
  perkNames: {
    happy_entry: "Happy Entry",
    early_entry: "early entry (Highland Resort Hotel & Spa)",
  },
  hotelNames: {
    disney:
      "a Disney Hotel at Tokyo Disneyland (Tokyo Disneyland Hotel, Disney Ambassador Hotel, Toy Story Hotel)",
    disney_deluxe:
      "a Disney Hotel at Tokyo DisneySea (Hotel MiraCosta, Fantasy Springs Hotel)",
    universal: "an official hotel of Universal Studios Japan",
    park_hotel: "a park's own hotel (Highland Resort, LEGOLAND Japan Hotel)",
    offsite: "any other hotel",
  },
  groups: [
    {
      key: "tokyo-disney-resort",
      name: "Tokyo Disney Resort",
      kind: "resort",
      earlyKind: "happy_entry",
    },
    {
      key: "universal-studios-japan",
      name: "Universal Studios Japan",
      kind: "resort",
      earlyKind: "early_entry",
    },
    {
      key: "fuji-q-highland",
      name: "Fuji-Q Highland",
      kind: "day_trip",
      earlyKind: "early_entry",
    },
    {
      key: "legoland-japan",
      name: "LEGOLAND Japan",
      kind: "day_trip",
      earlyKind: "early_entry",
    },
  ],
  parks: [
    {
      slug: TDL,
      code: "DL",
      ...JP,
      city: "tokyo",
      group: "tokyo-disney-resort",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 3,
    },
    {
      slug: TDS,
      code: "DS",
      ...JP,
      city: "tokyo",
      group: "tokyo-disney-resort",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 2,
    },
    {
      slug: FUJI_Q,
      code: "FQ",
      ...JP,
      city: "fujikawaguchiko",
      group: "fuji-q-highland",
      type: "theme_park",
      headliner: false,
    },
    {
      slug: LEGOLAND_JP,
      code: "LJ",
      ...JP,
      city: "nagoya",
      group: "legoland-japan",
      type: "theme_park",
      headliner: false,
    },
    {
      slug: USJ,
      code: "US",
      ...JP,
      city: "osaka",
      group: "universal-studios-japan",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 1,
    },
  ],
  hotelAreas: [
    {
      key: "maihama",
      name: "Maihama (Disney Hotels)",
      latitude: 35.63526,
      longitude: 139.88512,
      hotel: "disney",
      note: "Resort Gateway Station by JR Maihama: Tokyo Disneyland Hotel, Disney Ambassador Hotel, Toy Story Hotel",
    },
    {
      key: "disneysea_hotels",
      name: "Tokyo DisneySea hotels",
      latitude: 35.6274,
      longitude: 139.88952,
      hotel: "disney_deluxe",
      note: "Hotel MiraCosta inside Tokyo DisneySea and the Fantasy Springs Hotel at it, by Tokyo DisneySea Station",
    },
    {
      key: "bayside",
      name: "Maihama Bayside (Official Hotels)",
      latitude: 35.62786,
      longitude: 139.87637,
      hotel: "offsite",
      note: "Bayside Station: the six Official Hotels; no Happy Entry",
    },
    {
      key: "shin_urayasu",
      name: "Shin-Urayasu (Partner Hotels)",
      latitude: 35.64987,
      longitude: 139.91412,
      hotel: "offsite",
      note: "JR Shin-Urayasu, one stop from Maihama; the Partner Hotels' free shuttle to the parks",
    },
    {
      key: "tokyo_station",
      name: "Tokyo Station",
      latitude: 35.68162,
      longitude: 139.76533,
      hotel: "offsite",
      note: "Marunouchi and Yaesu; the JR Keiyo Line to Maihama, the Shinkansen west",
    },
    {
      key: "shinjuku",
      name: "Shinjuku",
      latitude: 35.68837,
      longitude: 139.70036,
      hotel: "offsite",
      note: "Busta Shinjuku for the highway buses to Fuji-Q and the Fuji Excursion train",
    },
    {
      key: "highland_resort",
      name: "Highland Resort Hotel & Spa",
      latitude: 35.48486,
      longitude: 138.77788,
      hotel: "park_hotel",
      note: "Next to Fuji-Q Highland",
    },
    {
      key: "kawaguchiko",
      name: "Kawaguchiko",
      latitude: 35.49834,
      longitude: 138.76878,
      hotel: "offsite",
      note: "Kawaguchiko Station, the town and the lake; one stop from Fuji-Q",
    },
    {
      key: "nagoya_station",
      name: "Nagoya Station",
      latitude: 35.17111,
      longitude: 136.88168,
      hotel: "offsite",
      note: "The Shinkansen station; the Aonami Line to LEGOLAND Japan",
    },
    {
      key: "kinjo_futo",
      name: "Kinjo-futo (LEGOLAND Japan Hotel)",
      latitude: 35.05037,
      longitude: 136.8464,
      hotel: "park_hotel",
      note: "The LEGOLAND Japan Hotel, a walk from the gate; no early entry found",
    },
    {
      key: "universal_city",
      name: "Universal City (official hotels)",
      latitude: 34.66776,
      longitude: 135.4372,
      hotel: "universal",
      note: "Seven official hotels a few minutes' walk from the gate; no early entry since October 2024",
    },
    {
      key: "umeda",
      name: "Umeda (Osaka Station)",
      latitude: 34.70251,
      longitude: 135.49618,
      hotel: "offsite",
      note: "JR to Universal City about 12 minutes",
    },
    {
      key: "namba",
      name: "Namba",
      latitude: 34.66725,
      longitude: 135.49972,
      hotel: "offsite",
      note: "Hanshin Namba Line and JR to Universal City",
    },
    {
      key: "kyoto_station",
      name: "Kyoto Station",
      latitude: 34.98461,
      longitude: 135.75843,
      hotel: "offsite",
      note: "A common base for USJ: 55–70 minutes door to gate",
    },
  ],
  perks: {
    // Happy Entry: 15 minutes at Tokyo Disneyland, 5–15 at Tokyo DisneySea
    // (the plan counts the 5), not on the check-in day (official FAQ). No
    // feed carries the slot, so it holds every morning after the first night.
    // 2027-01-12 is a published blackout for some hotels, applied to the kind.
    disney: [
      {
        perk: "happy_entry",
        parks: [TDL],
        needs: "after_first_night",
        share: 0.025,
        blackoutDates: ["2027-01-12"],
      },
    ],
    disney_deluxe: [
      {
        perk: "happy_entry",
        parks: [TDL],
        needs: "after_first_night",
        share: 0.025,
        blackoutDates: ["2027-01-12"],
      },
      {
        perk: "happy_entry",
        parks: [TDS],
        needs: "after_first_night",
        share: 0.01,
      },
    ],
    universal: [],
    // Highland Resort: 15–30 minutes' priority admission by date, from booking
    // sites only (low confidence); the 15 are counted.
    park_hotel: [
      { perk: "early_entry", parks: [FUJI_Q], needs: "always", share: 0.025 },
    ],
    offsite: [],
  },
  venues: [
    {
      key: "ikspiari",
      name: "IKSPIARI",
      group: "tokyo-disney-resort",
      kind: "district",
      latitude: 35.6343,
      longitude: 139.88657,
      typicalHours: { opens: "10:00", closes: "22:30" },
      free: true,
      url: "https://www.ikspiari.com/en/",
      notes: "about 140 shops, restaurants and a cinema next to JR Maihama",
      highlights: ["dining", "shopping"],
    },
    {
      key: "universal-citywalk-osaka",
      name: "Universal CityWalk Osaka",
      group: "universal-studios-japan",
      kind: "district",
      latitude: 34.66828,
      longitude: 135.43759,
      typicalHours: { opens: "10:00", closes: "23:00" },
      free: true,
      url: "https://www.ucw.jp/",
      notes: "between Universal City Station and the gate",
      highlights: ["dining", "shopping"],
    },
  ],
  // Tokyo Disney Resort sells no full-day multi-park ticket.
  hopperSets: [],
  nightShows: {
    [TDL]: [
      {
        slug: "tokyo-disneyland-electrical-parade-dreamlights",
        name: "Tokyo Disneyland Electrical Parade Dreamlights",
        kind: "night_show",
      },
      {
        slug: "sky-full-of-colors-tokyo-disneyland",
        name: "Sky Full of Colors",
        kind: "fireworks",
      },
      {
        slug: "night-high-halloween-tokyo-disneyland",
        name: "Night High Halloween",
        kind: "fireworks",
      },
    ],
    [TDS]: [
      {
        slug: "believe-sea-of-dreams",
        name: "Believe! Sea of Dreams",
        kind: "night_show",
      },
      {
        slug: "special-image-projection-sparkling-jubilee-night",
        name: "Special Image Projection: Sparkling Jubilee Night",
        kind: "night_show",
      },
      {
        slug: "sky-full-of-colors-tokyo-disneysea",
        name: "Sky Full of Colors",
        kind: "fireworks",
      },
      {
        slug: "night-high-halloween-tokyo-disneysea",
        name: "Night High Halloween",
        kind: "fireworks",
      },
    ],
  },
  // Most travellers send their bags ahead by courier (takkyubin) to the next hotel, so a
  // morning move needs no check-out queue with luggage: half the default overhead, and an
  // earlier start, as the train timetables allow. The owner's decision for Japan.
  stayRules: { moveOverheadMinutes: 30, moveStartsAt: "07:30" },
  bases: [
    {
      key: "tokyo",
      name: "Tokyo",
      order: 1,
      hotelAreas: [
        "maihama",
        "disneysea_hotels",
        "bayside",
        "shin_urayasu",
        "tokyo_station",
        "shinjuku",
      ],
      defaultArea: "maihama",
      gateways: [
        { code: "HND", name: "Haneda Airport", kind: "airport" },
        { code: "NRT", name: "Narita Airport", kind: "airport" },
      ],
      stayNote:
        "Maihama for both Disney parks; Shinjuku for the buses to Fuji-Q",
      // Fuji-Q from where its buses leave, Busta Shinjuku.
      parks: [
        { slug: TDL, minutes: 10, mode: "walk" },
        { slug: TDS, minutes: 15, mode: "monorail" },
        { slug: FUJI_Q, minutes: 110, mode: "highway_bus" },
      ],
    },
    {
      key: "fujikawaguchiko",
      name: "Fujikawaguchiko",
      order: 2,
      hotelAreas: ["highland_resort", "kawaguchiko"],
      defaultArea: "kawaguchiko",
      stayNote: "Kawaguchiko by the lake, or the Highland Resort at the park",
      parks: [{ slug: FUJI_Q, minutes: 15, mode: "rail" }],
    },
    {
      key: "nagoya",
      name: "Nagoya",
      order: 3,
      hotelAreas: ["nagoya_station", "kinjo_futo"],
      defaultArea: "nagoya_station",
      gateways: [
        {
          code: "NGO",
          name: "Chubu Centrair International Airport",
          kind: "airport",
        },
      ],
      stayNote:
        "Nagoya Station for the Shinkansen, or the LEGOLAND Japan Hotel at the gate",
      parks: [{ slug: LEGOLAND_JP, minutes: 40, mode: "rail" }],
    },
    {
      key: "osaka",
      name: "Osaka",
      order: 4,
      hotelAreas: ["universal_city", "umeda", "namba", "kyoto_station"],
      defaultArea: "universal_city",
      gateways: [
        { code: "KIX", name: "Kansai International Airport", kind: "airport" },
        { code: "ITM", name: "Osaka Itami Airport", kind: "airport" },
      ],
      stayNote:
        "Universal City at the gate, Umeda or Namba in town; Kyoto is an hour away",
      // LEGOLAND Japan from Umeda: the Shinkansen to Nagoya and the Aonami Line.
      parks: [
        { slug: USJ, minutes: 5, mode: "walk" },
        { slug: LEGOLAND_JP, minutes: 105, mode: "shinkansen" },
      ],
    },
  ],
  // Door to door between the bases' main areas; the trains from JR Central's
  // timetable (Nozomi Tokyo–Nagoya 96–99 min, Nagoya–Shin-Osaka 48–51,
  // Tokyo–Shin-Osaka 147–150), the buses from Fujikyu's.
  baseLegs: [
    {
      from: "tokyo",
      to: "fujikawaguchiko",
      minutes: 150,
      mode: "highway_bus",
      name: "highway bus Busta Shinjuku–Kawaguchiko",
      fromStop: "Busta Shinjuku",
      toStop: "Kawaguchiko Station",
      notes: ["reserve_seat"],
    },
    {
      from: "tokyo",
      to: "nagoya",
      minutes: 150,
      mode: "shinkansen",
      name: "Tokaido Shinkansen Nozomi",
      fromStop: "Tokyo",
      toStop: "Nagoya",
      notes: ["reserve_seat", "forward_luggage"],
    },
    {
      from: "tokyo",
      to: "osaka",
      minutes: 210,
      mode: "shinkansen",
      name: "Tokaido Shinkansen Nozomi",
      fromStop: "Tokyo",
      toStop: "Shin-Osaka",
      // An 07:30 Nozomi reaches Universal City around 10:30: a short park day.
      parkDay: true,
      notes: ["reserve_seat", "forward_luggage"],
    },
    {
      from: "nagoya",
      to: "osaka",
      minutes: 90,
      mode: "shinkansen",
      name: "Tokaido Shinkansen Nozomi",
      fromStop: "Nagoya",
      toStop: "Shin-Osaka",
      notes: ["reserve_seat", "forward_luggage"],
    },
    {
      from: "fujikawaguchiko",
      to: "nagoya",
      minutes: 225,
      mode: "highway_bus",
      name: "bus to Mishima, then the Tokaido Shinkansen Kodama",
      fromStop: "Fuji-Q Highland",
      toStop: "Nagoya",
      changes: 1,
      // A full travel day by the Mishima route.
      parkDay: false,
    },
    {
      from: "fujikawaguchiko",
      to: "osaka",
      minutes: 300,
      mode: "highway_bus",
      name: "bus to Mishima, the Kodama to Nagoya and the Nozomi to Shin-Osaka",
      fromStop: "Fuji-Q Highland",
      toStop: "Shin-Osaka",
      changes: 2,
      parkDay: false,
    },
  ],
  legs: [
    {
      from: TDL,
      to: TDS,
      primary: { mode: "monorail", minutes: 15, name: "Disney Resort Line" },
      alternatives: [{ mode: "walk", minutes: 20 }],
    },
    jpLeg("hotel:maihama", TDL, "walk", 10),
    jpLeg("hotel:maihama", TDS, "monorail", 15, "Disney Resort Line"),
    // From Maihama to Fuji-Q: about 40 minutes to Shinjuku, then the bus (derived).
    jpLeg("hotel:maihama", FUJI_Q, "highway_bus", 165),
    jpLeg("hotel:disneysea_hotels", TDS, "walk", 5),
    jpLeg("hotel:disneysea_hotels", TDL, "monorail", 15, "Disney Resort Line"),
    jpLeg("hotel:bayside", TDS, "bus", 15, "Disney Resort Cruiser"),
    jpLeg("hotel:bayside", TDL, "bus", 25, "Disney Resort Cruiser"),
    jpLeg("hotel:shin_urayasu", TDL, "bus", 25, "Partner Hotel shuttle"),
    jpLeg("hotel:shin_urayasu", TDS, "bus", 30, "Partner Hotel shuttle"),
    jpLeg("hotel:tokyo_station", TDL, "rail", 35, "JR Keiyo Line"),
    jpLeg("hotel:tokyo_station", TDS, "rail", 45, "JR Keiyo Line"),
    jpLeg("hotel:tokyo_station", FUJI_Q, "highway_bus", 145),
    jpLeg("hotel:shinjuku", TDL, "rail", 55, "JR Chuo and Keiyo Lines"),
    jpLeg("hotel:shinjuku", TDS, "rail", 65, "JR Chuo and Keiyo Lines"),
    jpLeg("hotel:shinjuku", FUJI_Q, "highway_bus", 110),
    jpLeg("hotel:highland_resort", FUJI_Q, "walk", 5),
    jpLeg("hotel:kawaguchiko", FUJI_Q, "rail", 15, "Fujikyu Railway"),
    jpLeg("hotel:nagoya_station", LEGOLAND_JP, "rail", 40, "Aonami Line"),
    jpLeg("hotel:kinjo_futo", LEGOLAND_JP, "walk", 5),
    jpLeg("hotel:universal_city", USJ, "walk", 5),
    jpLeg("hotel:umeda", USJ, "rail", 25, "JR Yumesaki Line"),
    jpLeg(
      "hotel:namba",
      USJ,
      "rail",
      35,
      "Hanshin Namba and JR Yumesaki Lines",
    ),
    jpLeg(
      "hotel:kyoto_station",
      USJ,
      "rail",
      65,
      "JR Kyoto and Yumesaki Lines",
    ),
    jpLeg(
      "hotel:umeda",
      LEGOLAND_JP,
      "shinkansen",
      105,
      "Tokaido Shinkansen and Aonami Line",
    ),
    jpLeg("venue:ikspiari", TDL, "walk", 10),
    jpLeg("venue:ikspiari", TDS, "monorail", 15, "Disney Resort Line"),
    jpLeg("venue:universal-citywalk-osaka", USJ, "walk", 5),
  ],
};

/** Every region, in the order the list route and the navigation show them. */

// ── Paris (PAR-1296) ────────────────────────────────────────────────────────
// Researched 2026-11-13: Disneyland Paris' press site and media PDFs (its main
// site sends scripts to a waiting room, so its rules are read through the
// search index), parcasterix.fr, OpenStreetMap and our own data. Disney
// Adventure World is Walt Disney Studios Park renamed when it reopened on
// 29 March 2026. Parc Astérix is a resort of its own (owner's decision,
// 2026-11-14): a full-size park with its own hotels.

const FRANCE = { continent: "europe", country: "france" } as const;

/**
 * Paris' heat thresholds, set as Orlando's were at the region's climate park:
 * Disneyland Park's 2015–2024 daily normals (ERA5, measured 2026-11-14) top
 * out at 26.8 °C on 21 July, and the top fifth of its June–October normals
 * starts at 24.9 °C. A forecast counts from 30 °C (the highest normal plus
 * Orlando's margin of about 3 °C; Météo-France's Paris-Montsouris station
 * counts 14 days a year at or above it, 1991–2020), a normal from 25 °C.
 */
export const PARIS_HEAT: HeatModelConfig = {
  forecastFromC: 30,
  normalFromC: 25,
  perDegree: 0.08,
  normalWeight: 0.5,
  cap: 1.4,
};

const DISNEYLAND_PARIS_PARKS = ["disneyland-park", "disney-adventure-world"];

/**
 * Extra Magic Time: an hour before opening ("usually 1 extra hour", Disney's
 * FAQ as the search index quotes it) for the guests of every Disney Hotel,
 * Davy Crockett Ranch included. Disney says which park and how long "may
 * vary", so it counts only on a day the feed carries the slot; the feed has it
 * at both parks on every day from 1 September 2026. Disneyland Pass Gold
 * holders and Les Villages Nature package guests get it too: a ticket's or a
 * package's perk, which a hotel kind cannot carry, so it is left out for them.
 */
const EXTRA_MAGIC_TIME: PerkRule = {
  perk: "extra_magic_time",
  parks: DISNEYLAND_PARIS_PARKS,
  needs: "early",
  share: 0.05,
};

/**
 * Parc Astérix's hotels: a stay includes "Early Entry: access to certain park
 * attractions 30 minutes earlier", subject to the weather and never on Peur
 * sur le Parc nocturne days, special days or Christmas-tree events
 * (parcasterix.fr/hotels, read 2026-11-14). Our feed carries no early slot for
 * the park, so it counts on no day until it does; the region route lists it.
 */
const PARC_ASTERIX_EARLY_ENTRY: PerkRule = {
  perk: "early_entry",
  parks: ["parc-asterix"],
  needs: "early",
  share: 0.05,
};

/** The guest car park both Disney parks share; travelators lead past Disney Village to the gates. */
const DISNEYLAND_PARIS_CAR_PARK = {
  latitude: 48.8753,
  longitude: 2.7877,
  note: "Disneyland Paris guest car park off the A4 (exit 14), shared by both parks; travelators lead past Disney Village to the two gates, which face each other",
};

/** Disney's partner hotels run a free shuttle from the Chessy bus station. */
const PARTNER_SHUTTLE: TransportAlternative = {
  mode: "shuttle",
  name: "Disneyland Paris partner-hotel shuttle",
};

/**
 * RER A from Châtelet–Les Halles: "a 40-minute journey" to Marne-la-Vallée–
 * Chessy, "just a 2-minute walk from the entrance" (Disney's press, September
 * 2026); 50 door to door with the walk down to the platforms and the wait for
 * a train (every 10 to 15 minutes).
 */
const RER_A_FROM_PARIS: TransportAlternative = {
  mode: "rail",
  minutes: 50,
  name: "RER A",
  via: "chatelet_les_halles",
};

/** One RER A stop from Val d'Europe; 15 with the wait, the trains run every 10 to 15 minutes. */
const RER_A_FROM_VAL_D_EUROPE: TransportAlternative = {
  mode: "rail",
  minutes: 15,
  name: "RER A",
  via: "val_d_europe_station",
};

/** Paris legs for both Disney parks alike. */
function bothDisneyParks(
  from: string,
  leg: Omit<CuratedLeg, "from" | "to">,
): CuratedLeg[] {
  return DISNEYLAND_PARIS_PARKS.map((to) => ({ from, to, ...leg }));
}

export const PARIS: RegionConfig = {
  slug: "paris",
  name: "Paris",
  timezone: "Europe/Paris",
  // No storm season: summer thunderstorms pause rides for hours, not a season.
  weather: { heat: PARIS_HEAT, climatePark: "disneyland-park" },
  travel: { source: "osrm" },
  // Every product is sold to anyone at the operators' own shops (`eu`, priced
  // in euros); `intl`: by sellers abroad as well.
  ticketMarkets: { home: "eu", codes: ["eu", "intl"] },
  perkNames: {
    extra_magic_time: "Extra Magic Time",
    early_entry: "Early Entry",
  },
  hotelNames: {
    disney:
      "a Disneyland Paris hotel (the Disney Hotels, Davy Crockett Ranch included)",
    parc_asterix:
      "a Parc Astérix hotel (Les Trois Hiboux, La Cité Suspendue, Les Quais de Lutèce)",
    offsite: "any other hotel, Disneyland Paris' partner hotels included",
  },
  groups: [
    {
      key: "disneyland-paris",
      name: "Disneyland Paris",
      kind: "resort",
      earlyKind: "extra_magic_time",
    },
    {
      key: "parc-asterix",
      name: "Parc Astérix",
      kind: "resort",
      earlyKind: "early_entry",
    },
  ],
  parks: [
    {
      // The slug repeats Anaheim's Disneyland Park: the path (city `paris`,
      // where our data files both Disney parks) tells them apart.
      slug: "disneyland-park",
      ...FRANCE,
      city: "paris",
      group: "disneyland-paris",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 1,
      carPark: DISNEYLAND_PARIS_CAR_PARK,
      transfer: { via: "walk", minutes: 15 },
    },
    {
      slug: "disney-adventure-world",
      ...FRANCE,
      city: "paris",
      group: "disneyland-paris",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 2,
      carPark: DISNEYLAND_PARIS_CAR_PARK,
      transfer: { via: "walk", minutes: 15 },
    },
    {
      slug: "parc-asterix",
      ...FRANCE,
      city: "plailly",
      group: "parc-asterix",
      type: "theme_park",
      headliner: true,
      secondVisitRank: 3,
      carPark: {
        latitude: 49.1375,
        longitude: 2.5735,
        note: "visitor car park beside the entrance, A1 exit 'Parc Astérix' between exits 7 and 8, Plailly",
      },
    },
  ],
  venues: [
    {
      key: "disney-village",
      name: "Disney Village",
      group: "disneyland-paris",
      kind: "district",
      latitude: 48.8682,
      longitude: 2.7847,
      free: true,
      url: "https://www.disneylandparis.com/en-usd/destinations/disney-village",
      notes:
        "restaurants, bars, shops and a cinema between the station, the parks and Lake Disney; open daily from about 08:00 until late (no published closing time); entry free except on some dates with a special show; parking paid",
      highlights: ["dining", "shopping"],
    },
  ],
  hopperSets: [DISNEYLAND_PARIS_PARKS],
  // The paid reserved viewing areas our data lists beside each show are
  // seats, not shows. Parc Astérix has no nighttime spectacular outside its
  // ticketed Halloween nights.
  nightShows: {
    "disneyland-park": [
      {
        slug: "disney-tales-of-magic",
        name: "Disney Tales of Magic",
        kind: "fireworks",
      },
    ],
    "disney-adventure-world": [
      {
        slug: "disney-cascade-of-lights",
        name: "Disney Cascade of Lights",
        kind: "night_show",
      },
    ],
  },
  hotelAreas: [
    {
      key: "disney_lake",
      name: "Disney hotels by Lake Disney",
      latitude: 48.8705,
      longitude: 2.7884,
      hotel: "disney",
      note: "Disney Hotel New York – The Art of Marvel; the Disneyland Hotel, Newport Bay Club and Sequoia Lodge are within 5-15 minutes' walk of the parks",
    },
    {
      key: "disney_cheyenne_santa_fe",
      name: "Disney Hotel Cheyenne and Santa Fe",
      latitude: 48.8738,
      longitude: 2.794,
      hotel: "disney",
      note: "between the two hotels; 20 minutes' walk or the free Disney Hotels shuttle",
    },
    {
      key: "disney_davy_crockett",
      name: "Disney Davy Crockett Ranch",
      latitude: 48.8339,
      longitude: 2.8204,
      hotel: "disney",
      note: "a forest cabin resort in Bailly-Romainvilliers; about 15 minutes by car, no shuttle",
    },
    {
      key: "val_d_europe",
      name: "Val d'Europe",
      latitude: 48.8551,
      longitude: 2.7739,
      hotel: "offsite",
      note: "Val d'Europe RER A station in Serris, by Hôtel l'Elysée, Adagio, Relais Spa and Staycity; one RER stop or the partner-hotel shuttle to the parks",
    },
    {
      key: "val_de_france",
      name: "Val de France, Magny-le-Hongre",
      latitude: 48.8745,
      longitude: 2.8075,
      hotel: "offsite",
      note: "Explorers, Dream Castle, Magic Circus, B&B and Campanile Val de France; free partner-hotel shuttle",
    },
    {
      key: "paris_center",
      name: "Central Paris",
      latitude: 48.8616,
      longitude: 2.347,
      hotel: "offsite",
      note: "Châtelet–Les Halles, on RER A to Marne-la-Vallée–Chessy and RER B to the Parc Astérix shuttle at the airport",
    },
    {
      key: "parc_asterix",
      name: "Parc Astérix hotels",
      latitude: 49.1298,
      longitude: 2.571,
      hotel: "parc_asterix",
      note: "Les Trois Hiboux, La Cité Suspendue and Les Quais de Lutèce in the woods south of the park, a walk from the gate; the point is the Trois Hiboux quarter between them. L'Odyssée opens on 26 March 2027",
    },
  ],
  perks: {
    disney: [EXTRA_MAGIC_TIME],
    parc_asterix: [PARC_ASTERIX_EARLY_ENTRY],
    offsite: [],
  },
  legs: [
    {
      // The gates face each other across the entrance plaza; both parks share
      // the car park, so OSRM answers no drive.
      from: "disneyland-park",
      to: "disney-adventure-world",
      primary: { mode: "walk", minutes: 5, km: 0.3 },
      alternatives: [{ mode: "walk", minutes: 5, via: "entrance_plaza" }],
    },
    // Disney gives the lake hotels as 5 to 15 minutes' walk from the parks;
    // the area counts the far end.
    ...bothDisneyParks("hotel:disney_lake", {
      primary: { mode: "walk", minutes: 15 },
      alternatives: [{ mode: "walk", minutes: 15 }],
    }),
    ...bothDisneyParks("hotel:disney_cheyenne_santa_fe", {
      primary: { mode: "walk", minutes: 20 },
      alternatives: [
        { mode: "walk", minutes: 20 },
        { mode: "shuttle", name: "Disney Hotels shuttle" },
      ],
    }),
    ...bothDisneyParks("hotel:val_d_europe", {
      primary: { ...RER_A_FROM_VAL_D_EUROPE, minutes: 15 },
      alternatives: [RER_A_FROM_VAL_D_EUROPE, PARTNER_SHUTTLE],
    }),
    ...bothDisneyParks("hotel:val_de_france", {
      alternatives: [PARTNER_SHUTTLE],
    }),
    ...bothDisneyParks("hotel:paris_center", {
      primary: { ...RER_A_FROM_PARIS, minutes: 50 },
      alternatives: [
        RER_A_FROM_PARIS,
        { mode: "highway_bus", name: "Disneyland Paris Express" },
      ],
    }),
    ...bothDisneyParks("venue:disney-village", {
      primary: { mode: "walk", minutes: 5 },
      alternatives: [{ mode: "walk", minutes: 5 }],
    }),
    {
      // RER B to Aéroport Charles de Gaulle 1, then the park's shuttle from
      // Roissypôle (quai A3), every 15 to 30 minutes on every open day, booked
      // by the day before (parcasterix.fr/offres/navette, read 2026-11-14).
      // The Paris City Vision coach leaves the Pullman Paris Tour Eiffel at
      // 09:00, so its time of day, not its minutes, decides.
      from: "hotel:paris_center",
      to: "parc-asterix",
      alternatives: [
        {
          mode: "rail",
          minutes: 75,
          name: "RER B and the Parc Astérix shuttle",
          via: "roissypole",
        },
        {
          mode: "highway_bus",
          name: "Paris City Vision coach",
          via: "pullman_tour_eiffel",
        },
      ],
    },
    // No coach runs from the Disney parks to Parc Astérix that we could
    // verify; by rail it is the TGV from Marne-la-Vallée–Chessy to Aéroport
    // CDG 2, then the park's shuttle, timetable-bound, so no minutes.
    ...DISNEYLAND_PARIS_PARKS.map((from) => ({
      from,
      to: "parc-asterix",
      alternatives: [
        {
          mode: "rail" as const,
          name: "TGV to Aéroport CDG 2, then the Parc Astérix shuttle",
          via: "roissypole",
        },
      ],
    })),
    {
      // The park says its hotels have "walking access to the park" without a
      // time; 15 minutes stands for the 0.9 km to the entrance, rounded up.
      from: "hotel:parc_asterix",
      to: "parc-asterix",
      primary: { mode: "walk", minutes: 15 },
      alternatives: [{ mode: "walk", minutes: 15 }],
    },
  ],
  // Three bases that all reach each other, so no route line (`order`): the
  // stays may come in any order. A base's reach is door to door from its
  // default area, read off the legs above (OSRM's free-flow drive where no
  // curated leg stands for it); a stay's own area refines it.
  bases: [
    {
      key: "disneyland-paris",
      name: "Disneyland Paris",
      hotelAreas: [
        "disney_lake",
        "disney_cheyenne_santa_fe",
        "disney_davy_crockett",
        "val_d_europe",
        "val_de_france",
      ],
      defaultArea: "disney_lake",
      // The TGV and the Eurostar (with a change at Lille) stop at
      // Marne-la-Vallée–Chessy, 2 minutes' walk from the gates; the TGV
      // reaches it from Aéroport CDG 2 in about 10 minutes.
      gateways: [
        {
          code: "XED",
          name: "Marne-la-Vallée–Chessy",
          kind: "station",
          minutes: 15,
        },
        { code: "CDG", name: "Paris Charles de Gaulle", kind: "airport" },
      ],
      stayNote:
        "The Disney Hotels by Lake Disney are a walk from the gates and get Extra Magic Time; the partner hotels in Val d'Europe and Val de France run a free shuttle.",
      parks: [
        { slug: "disneyland-park", minutes: 15, mode: "walk" },
        { slug: "disney-adventure-world", minutes: 15, mode: "walk" },
        { slug: "parc-asterix", minutes: 50, mode: "drive" },
      ],
    },
    {
      key: "parc-asterix",
      name: "Parc Astérix",
      hotelAreas: ["parc_asterix"],
      defaultArea: "parc_asterix",
      // The park's shuttle runs from Roissypôle at the airport on every open
      // day (parcasterix.fr/offres/navette).
      gateways: [
        { code: "CDG", name: "Paris Charles de Gaulle", kind: "airport" },
      ],
      stayNote:
        "The park's three hotels are a walk from the gate, and a stay includes park tickets and Early Entry to some rides.",
      parks: [
        { slug: "parc-asterix", minutes: 15, mode: "walk" },
        // 56 minutes' drive to the Disney car park, 15 on foot to the gates.
        { slug: "disneyland-park", minutes: 70, mode: "drive" },
        { slug: "disney-adventure-world", minutes: 70, mode: "drive" },
      ],
    },
    {
      key: "paris",
      name: "Paris",
      hotelAreas: ["paris_center"],
      defaultArea: "paris_center",
      gateways: [
        { code: "CDG", name: "Paris Charles de Gaulle", kind: "airport" },
        { code: "ORY", name: "Paris Orly", kind: "airport" },
      ],
      stayNote:
        "Châtelet–Les Halles is on RER A to Disneyland Paris and on RER B to the Parc Astérix shuttle at the airport.",
      parks: [
        { slug: "disneyland-park", minutes: 50, mode: "rail" },
        { slug: "disney-adventure-world", minutes: 50, mode: "rail" },
        // Free flow; RER B and the park's shuttle take about 75.
        { slug: "parc-asterix", minutes: 45, mode: "drive" },
      ],
    },
  ],
  baseLegs: [
    {
      // About an hour by car between the hotels (OSRM's free flow between the
      // two car parks is 54 minutes); no coach runs between the resorts.
      from: "disneyland-paris",
      to: "parc-asterix",
      minutes: 60,
      mode: "drive",
    },
    {
      from: "paris",
      to: "disneyland-paris",
      minutes: 50,
      mode: "rail",
      name: "RER A",
      changes: 0,
      fromStop: "Châtelet–Les Halles",
      toStop: "Marne-la-Vallée–Chessy",
    },
    {
      // OSRM's free flow between Châtelet and the park, 45 minutes.
      from: "paris",
      to: "parc-asterix",
      minutes: 45,
      mode: "drive",
    },
  ],
};

export const TRIP_REGIONS: Record<string, RegionConfig> = {
  [ORLANDO.slug]: ORLANDO,
  [SOUTHERN_CALIFORNIA.slug]: SOUTHERN_CALIFORNIA,
  [JAPAN.slug]: JAPAN,
  [PARIS.slug]: PARIS,
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
