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
 * This file imports nothing on purpose: `scripts/trip-regions/build-travel-matrix.mjs`
 * imports it straight from Node (type stripping) to read the coordinates, so the
 * matrix and the region can never disagree about where a car park is.
 */

/** What kind of day a park is: queues worth optimizing, or a flat-cost day. */
export type TripParkType = "theme_park" | "water_park" | "reservation_park";

/** `hotel=` on the plan: which resort's perks the visitor's hotel carries. */
export const HOTEL_KINDS = [
  "disney",
  "disney_deluxe",
  "universal",
  "universal_premier",
  "offsite",
] as const;
export type HotelKind = (typeof HOTEL_KINDS)[number];

/** A perk a hotel's guests get, as the plan's `perks` and `reasons` name it. */
export type PerkKind =
  | "early_entry"
  | "early_park_admission"
  | "extended_evening"
  | "express_included";

/** How a guest gets between two places other than by car. */
export type TransportMode =
  "monorail" | "skyliner" | "boat" | "bus" | "walk" | "train";

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
  earlyKind: "early_entry" | "early_park_admission";
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
   * (its centroid) snapped to service roads behind the parks.
   */
  carPark: LatLng & { note: string };
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
 * perk holds every open day, like Express at the premier hotels).
 */
export interface PerkRule {
  perk: PerkKind;
  parks: string[];
  needs: "early" | "extended_evening" | "always";
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
   * Replaces the drive for a pair that shares a car park, where OSRM answers
   * 0 km: the walk or shuttle between the two gates is the leg itself.
   */
  replacesDrive?: { mode: TransportMode; minutes: number; km?: number };
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

export interface RegionConfig {
  slug: string;
  name: string;
  timezone: string;
  groups: RegionGroupConfig[];
  parks: RegionParkConfig[];
  hotelAreas: HotelAreaConfig[];
  perks: Record<HotelKind, PerkRule[]>;
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
};
const UNIVERSAL_EARLY: PerkRule = {
  perk: "early_park_admission",
  parks: UNIVERSAL_PARKS,
  needs: "early",
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
    found.replacesDrive ??= leg.replacesDrive;
  }
  return [...byPair.values()];
}

export const ORLANDO: RegionConfig = {
  slug: "orlando",
  name: "Orlando",
  timezone: "America/New_York",
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
      replacesDrive: { mode: "walk", minutes: 10, km: 0.8 },
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
      replacesDrive: { mode: "bus", minutes: 20 },
      alternatives: [{ mode: "bus", via: "garage_shuttle" }],
    },
    {
      from: "universal-islands-of-adventure",
      to: "universal-volcano-bay",
      replacesDrive: { mode: "bus", minutes: 20 },
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
      replacesDrive: { mode: "walk", minutes: 5 },
      alternatives: [{ mode: "walk", minutes: 5 }],
    },
    {
      from: "venue:universal-citywalk",
      to: "universal-islands-of-adventure",
      replacesDrive: { mode: "walk", minutes: 5 },
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

export const TRIP_REGIONS: Record<string, RegionConfig> = {
  [ORLANDO.slug]: ORLANDO,
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
