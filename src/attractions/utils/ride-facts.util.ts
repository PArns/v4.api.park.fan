import { mapRideProfile, type RideProfileDto } from "../dto/ride-profile.dto";
import type { Attraction } from "../entities/attraction.entity";
import type { AttractionRideProfile } from "../entities/attraction-ride-profile.entity";
import { resolveCuratedFacts } from "./curated-attraction-facts.util";
import type { IndoorOutdoor } from "../../common/types/indoor-outdoor.type";
import {
  chooseNameDuplicateWinner,
  nameDuplicateKey,
} from "../../common/utils/name-duplicate.util";

/**
 * The track figures that are launches. park.fan's ride page counts the same
 * two (`LAUNCH_ELEMENT_IDS` in the frontend's `lib/glossary/ride-profile.ts`):
 * a lift hill or a first drop also gets a train going, but it is not a launch,
 * and counting it would make every coaster a multi-launch.
 */
export const LAUNCH_ELEMENT_IDS: ReadonlySet<string> = new Set([
  "launch",
  "swing-launch",
]);

/**
 * A ride's curated profile, flattened for a reader with no use for nulls: a
 * field is there when park.fan knows it and absent when it does not.
 */
export interface RideFacts {
  /** Glossary term ids of the ride type, e.g. `launch-coaster`. */
  types?: string[];
  /** Glossary term ids of the track figures, in ride order. */
  elements?: string[];
  launches?: number;
  inversions?: number;
  manufacturer?: string;
  model?: string;
  /** The year it opened to the public. */
  openedYear?: number;
  topSpeedKmh?: number;
  heightM?: number;
  lengthM?: number;
  durationSeconds?: number;
}

/**
 * The facts of a curated profile, or null for a ride nobody has profiled.
 *
 * Launches are counted off the track figures, and only when figures were
 * curated at all: an empty list means "not written down", so it gives no
 * count rather than a confident zero.
 */
export function rideFacts(
  profile: RideProfileDto | null | undefined,
): RideFacts | null {
  if (!profile) return null;
  const elements = profile.elements ?? [];
  const facts: RideFacts = {
    types: profile.types?.length ? profile.types : undefined,
    elements: elements.length > 0 ? elements : undefined,
    launches:
      elements.length > 0
        ? elements.filter((id) => LAUNCH_ELEMENT_IDS.has(id)).length
        : undefined,
    inversions: profile.inversions ?? undefined,
    manufacturer: profile.manufacturer ?? undefined,
    model: profile.model ?? undefined,
    openedYear: profile.openedYear ?? undefined,
    topSpeedKmh: profile.stats?.topSpeedKmh ?? undefined,
    heightM: profile.stats?.heightM ?? undefined,
    lengthM: profile.stats?.lengthM ?? undefined,
    durationSeconds: profile.stats?.durationSeconds ?? undefined,
  };
  const known = Object.fromEntries(
    Object.entries(facts).filter(([, value]) => value !== undefined),
  ) as RideFacts;
  return Object.keys(known).length > 0 ? known : null;
}

/** What a park's rides can be ranked by. */
export const RIDE_RANKINGS = [
  "topSpeed",
  "height",
  "length",
  "duration",
  "inversions",
  "launches",
  "openedYear",
  "minimumHeight",
] as const;

export type RideRanking = (typeof RIDE_RANKINGS)[number];

/**
 * The ride types a comparison can be narrowed to, read off the profile's glossary type ids:
 * upstream's `attractionType` is empty on all but one ride (`attraction-kind.type.ts`). No
 * "show": three profiles carry `live-show`, and shows are their own records (`get_shows`).
 */
export const RIDE_TYPE_FILTERS = [
  "coaster",
  "dark_ride",
  "water",
  "family",
] as const;

export type RideTypeFilter = (typeof RIDE_TYPE_FILTERS)[number];

/**
 * Where a rain plan can send people: `covered` keeps them dry while they queue (indoors, or a
 * roofed queue for a ride that runs outside), `indoor` keeps them dry on the ride too. The same
 * split as the frontend's „Überdacht" filter (`lib/utils/covered-rides.ts`).
 */
export const SHELTER_FILTERS = ["covered", "indoor"] as const;

export type ShelterFilter = (typeof SHELTER_FILTERS)[number];

/** Whether a ride's hand-decided `indoorOutdoor` meets `shelter`; unknown never does. */
export function isSheltered(
  indoorOutdoor: IndoorOutdoor | null | undefined,
  shelter: ShelterFilter,
): boolean {
  return shelter === "indoor"
    ? indoorOutdoor === "indoor"
    : indoorOutdoor === "indoor" || indoorOutdoor === "covered_queue";
}

/**
 * Under this share of rides with a known `indoorOutdoor`, a sheltered list is whatever somebody
 * happened to check, and a visitor reads a short list as a complete one: the frontend's
 * `COVERED_MIN_KNOWN_SHARE`.
 */
export const SHELTER_MIN_KNOWN_SHARE = 0.5;

/**
 * What a shelter filter could not judge: how many rides have no `indoorOutdoor`, and whether so
 * many that the list is incomplete.
 */
export function shelterCoverage(
  values: ReadonlyArray<IndoorOutdoor | null | undefined>,
): { shelterUnknown: number; shelterIncomplete?: true } {
  const unknown = values.filter((value) => value == null).length;
  const known = values.length - unknown;
  return {
    shelterUnknown: unknown,
    ...(values.length > 0 &&
      known / values.length < SHELTER_MIN_KNOWN_SHARE && {
        shelterIncomplete: true as const,
      }),
  };
}

/**
 * The coaster terms of park.fan's glossary (`category: 'coasters'` in the frontend's
 * `lib/glossary/data.ts`) that do not end in "-coaster"; every "-coaster" id is one too.
 */
const COASTER_TYPE_IDS: ReadonlySet<string> = new Set([
  "boomerang",
  "euro-fighter",
  "mine-train",
  "wild-mouse",
  "out-and-back",
  "slc",
]);

/**
 * The other filters' glossary type ids (`category: 'attractions'` in the same file). A dark ride
 * is any indoor themed ride system, simulators included, because that is what a visitor asking
 * for immersion means; a plain `boat-ride` is not wet (It's a Small World), so it is not water.
 */
const TYPE_IDS: Record<
  Exclude<RideTypeFilter, "coaster">,
  ReadonlySet<string>
> = {
  dark_ride: new Set([
    "dark-ride",
    "suspended-dark-ride",
    "interactive-dark-ride",
    "trackless-ride",
    "omnimover",
    "motion-simulator",
    "flying-theater",
    "4d-cinema",
    "madhouse",
  ]),
  water: new Set([
    "water-ride",
    "log-flume",
    "river-rapids",
    "shoot-the-chute",
    "water-coaster",
  ]),
  family: new Set([
    "family-coaster",
    "carousel",
    "teacups",
    "ferris-wheel",
    "bumper-cars",
    "swing-ride",
    "people-mover",
    "park-railway",
    "monorail",
    "sky-ride",
  ]),
};

/** Whether a ride's glossary type ids make it a ride of `type`. */
export function isRideOfType(
  types: readonly string[],
  type: RideTypeFilter,
): boolean {
  if (type === "coaster") {
    return types.some(
      (id) => id.endsWith("-coaster") || COASTER_TYPE_IDS.has(id),
    );
  }
  return types.some((id) => TYPE_IDS[type].has(id));
}

/** One ride of a park with what is known about it, for {@link compareRides}. */
export interface ComparedRide extends Omit<RideFacts, "elements" | "model"> {
  id: string;
  name: string;
  /** The ride's park, when the attractions came with it (a country's rides). */
  park?: string;
  /** Minimum rider height in cm; absent when park.fan does not know one. */
  minimumHeightCm?: number;
  maximumHeightCm?: number;
  /** "in" where the park signs the height in inches. */
  heightUnit?: "in";
  /** Whether it keeps riders dry, where an editor decided it. */
  indoorOutdoor?: IndoorOutdoor;
}

/**
 * A park's attractions as rows to compare, one per name as the park page
 * serves them (`chooseNameDuplicateWinner`), with the curated heights and the
 * profile's facts. The track figures and the model stay out: a ranking never
 * needs them, and `get_attraction` has them for the one ride asked about.
 * Names are told apart per park, so two parks' "Pirates of the Caribbean" both stay.
 */
export function comparedRides(
  attractions: readonly Attraction[],
  profiles: ReadonlyMap<string, AttractionRideProfile>,
): ComparedRide[] {
  const groups = new Map<
    string,
    Array<{ slug: string; name: string; attraction: Attraction }>
  >();
  for (const attraction of attractions) {
    const name = nameDuplicateKey(resolveCuratedFacts(attraction).name);
    if (!name) continue;
    const row = { slug: attraction.slug, name, attraction };
    const key = `${attraction.parkId}:${name}`;
    groups.set(key, [...(groups.get(key) ?? []), row]);
  }
  return [...groups.values()].map((rows) => {
    const { attraction } = chooseNameDuplicateWinner(rows)!;
    const facts = resolveCuratedFacts(attraction);
    const profile = rideFacts(mapRideProfile(profiles.get(attraction.id)));
    const { elements: _elements, model: _model, ...known } = profile ?? {};
    return {
      id: attraction.id,
      name: facts.name,
      ...(attraction.park?.name && { park: attraction.park.name }),
      ...(facts.minimumHeight !== null && {
        minimumHeightCm: facts.minimumHeight,
      }),
      ...(facts.maximumHeight !== null && {
        maximumHeightCm: facts.maximumHeight,
      }),
      ...(facts.minimumHeightUnit === "in" && { heightUnit: "in" as const }),
      ...(attraction.indoorOutdoor && {
        indoorOutdoor: attraction.indoorOutdoor,
      }),
      ...known,
    };
  });
}

const VALUE: Record<RideRanking, (ride: ComparedRide) => number | undefined> = {
  topSpeed: (ride) => ride.topSpeedKmh,
  height: (ride) => ride.heightM,
  length: (ride) => ride.lengthM,
  duration: (ride) => ride.durationSeconds,
  inversions: (ride) => ride.inversions,
  launches: (ride) => ride.launches,
  openedYear: (ride) => ride.openedYear,
  minimumHeight: (ride) => ride.minimumHeightCm,
};

/** The result of {@link compareRides}. */
export interface RideComparison {
  /** The first `limit` rides, in order. */
  rides: ComparedRide[];
  /** Rides that had the value and passed the height filter. */
  ranked: number;
  /** Rides left out because park.fan does not know the value ranked by. */
  withoutValue: number;
  /** With a rider height: rides left out because their minimum height is unknown. */
  heightUnknown?: number;
  /** The first {@link LEFT_OUT_NAMES_LIMIT} of them, so an answer can say which. */
  heightUnknownRides?: RideRef[];
  /** With a rider height: rides the rider is too small or too tall for. */
  notAllowed?: number;
  /** The first {@link LEFT_OUT_NAMES_LIMIT} of them with their limits, so none is recommended. */
  notAllowedRides?: RideOutOfReach[];
  /** The first {@link LEFT_OUT_NAMES_LIMIT} of the rides without the value ranked by. */
  withoutValueRides?: RideRef[];
  /** With a type: rides left out because park.fan does not know their type. */
  typeUnknown?: number;
  /** The first {@link LEFT_OUT_NAMES_LIMIT} of them. */
  typeUnknownRides?: RideRef[];
  /** With a shelter: rides left out because nobody decided whether they keep you dry. */
  shelterUnknown?: number;
  /** The first {@link LEFT_OUT_NAMES_LIMIT} of them. */
  shelterUnknownRides?: RideRef[];
  /** With a shelter: under half the rides are known, so the list is incomplete. */
  shelterIncomplete?: true;
}

/**
 * A ride a list leaves out, by id as well as name: two rides can share a name, and the id is what
 * the next tool takes.
 */
export interface RideRef {
  id: string;
  name: string;
  /** The ride's park, when the list spans parks. */
  park?: string;
}

/** A ride a rider may not take, with the limit that rules it out. */
export interface RideOutOfReach {
  id: string;
  name: string;
  park?: string;
  minimumHeightCm: number;
  maximumHeightCm?: number;
  heightUnit?: "in";
}

/**
 * How many rides a filter names among those it left out. A count alone read as
 * "the height is unknown" and got the rides recommended to a child too small
 * for them, and a name tells the visitor which ride park.fan cannot judge.
 */
export const LEFT_OUT_NAMES_LIMIT = 20;

/**
 * A park's rides ranked by one fact, highest first unless `order` is "asc".
 *
 * With `riderHeightCm` only the rides a rider that tall may ride are ranked:
 * the minimum height must be known and met, and a maximum, where there is one,
 * not exceeded. A ride with no known minimum is counted apart rather than
 * passed, because an unknown limit is not the same as none. With `type` only
 * the rides of that type are ranked, and those of unknown type counted apart;
 * with `shelter` only the rides that keep you dry, and the unknown ones too.
 */
export function compareRides(
  all: readonly ComparedRide[],
  params: {
    /** Absent: every ride of the filters, by name, for "which coasters haven't we done". */
    by?: RideRanking;
    order?: "asc" | "desc";
    riderHeightCm?: number;
    type?: RideTypeFilter;
    shelter?: ShelterFilter;
    limit: number;
  },
): RideComparison {
  const { type, shelter } = params;
  const untyped = type ? all.filter((ride) => !ride.types?.length) : [];
  const ofType = type
    ? all.filter((ride) => isRideOfType(ride.types ?? [], type))
    : all;
  const rides = shelter
    ? ofType.filter((ride) => isSheltered(ride.indoorOutdoor, shelter))
    : ofType;
  const rider = params.riderHeightCm;
  const heightUnknown: ComparedRide[] = [];
  const notAllowed: ComparedRide[] = [];
  const allowed =
    rider === undefined
      ? rides
      : rides.filter((ride) => {
          if (ride.minimumHeightCm === undefined) {
            heightUnknown.push(ride);
            return false;
          }
          const fits = riderFits(ride, rider);
          if (!fits) notAllowed.push(ride);
          return fits;
        });

  const by = params.by;
  const value = by ? VALUE[by] : () => 0;
  const withValue = allowed.filter((ride) => value(ride) !== undefined);
  const sign = params.order === "asc" ? 1 : -1;
  const ranked = [...withValue].sort(
    (a, b) =>
      sign * ((value(a) as number) - (value(b) as number)) ||
      a.name.localeCompare(b.name),
  );

  const refs = (rides: readonly ComparedRide[]) =>
    rides.slice(0, LEFT_OUT_NAMES_LIMIT).map(rideRefOf);
  const withoutValue = allowed.filter((ride) => value(ride) === undefined);
  return {
    rides: ranked.slice(0, params.limit),
    ranked: ranked.length,
    withoutValue: withoutValue.length,
    ...(withoutValue.length > 0 && { withoutValueRides: refs(withoutValue) }),
    ...(rider !== undefined && {
      heightUnknown: heightUnknown.length,
      heightUnknownRides: refs(heightUnknown),
      notAllowed: notAllowed.length,
      notAllowedRides: notAllowed
        .slice(0, LEFT_OUT_NAMES_LIMIT)
        .map(outOfReach),
    }),
    ...(type && {
      typeUnknown: untyped.length,
      typeUnknownRides: refs(untyped),
    }),
    ...(shelter && {
      ...shelterCoverage(ofType.map((ride) => ride.indoorOutdoor)),
      shelterUnknownRides: refs(
        ofType.filter((ride) => ride.indoorOutdoor === undefined),
      ),
    }),
  };
}

/**
 * Whether a rider of `riderHeightCm` may take a ride: its minimum must be known
 * and met, and a maximum, where there is one, not exceeded.
 */
export function riderFits(
  ride: Pick<ComparedRide, "minimumHeightCm" | "maximumHeightCm">,
  riderHeightCm: number,
): boolean {
  return (
    ride.minimumHeightCm !== undefined &&
    ride.minimumHeightCm <= riderHeightCm &&
    (ride.maximumHeightCm === undefined ||
      ride.maximumHeightCm >= riderHeightCm)
  );
}

/**
 * Whether a party's shortest rider is under a ride's known minimum. Only the
 * minimum counts, as in the trip planner's own flag: the party's height names
 * its shortest rider, and a maximum would flag a kiddie ride for the child it
 * was built for.
 */
export function partyTooShort(
  minimumHeightCm: number | undefined,
  partyHeightCm: number | undefined,
): boolean {
  return (
    minimumHeightCm !== undefined &&
    partyHeightCm !== undefined &&
    partyHeightCm < minimumHeightCm
  );
}

/** A left-out ride as its id, name and, across parks, its park. */
function rideRefOf(ride: ComparedRide): RideRef {
  return {
    id: ride.id,
    name: ride.name,
    ...(ride.park && { park: ride.park }),
  };
}

function outOfReach(ride: ComparedRide): RideOutOfReach {
  return {
    id: ride.id,
    name: ride.name,
    ...(ride.park && { park: ride.park }),
    minimumHeightCm: ride.minimumHeightCm as number,
    ...(ride.maximumHeightCm !== undefined && {
      maximumHeightCm: ride.maximumHeightCm,
    }),
    ...(ride.heightUnit && { heightUnit: ride.heightUnit }),
  };
}
