import {
  compareRides,
  comparedRides,
  isRideOfType,
  rideFacts,
  type ComparedRide,
} from "./ride-facts.util";
import type { RideProfileDto } from "../dto/ride-profile.dto";
import type { Attraction } from "../entities/attraction.entity";
import type { AttractionRideProfile } from "../entities/attraction-ride-profile.entity";

const profile = (over: Partial<RideProfileDto> = {}): RideProfileDto => ({
  elements: [],
  types: [],
  manufacturer: null,
  manufacturerTermId: null,
  model: null,
  openedYear: null,
  inversions: null,
  stats: null,
  ...over,
});

describe("rideFacts", () => {
  it("is null for a ride nobody has profiled, or profiled with nothing", () => {
    expect(rideFacts(null)).toBeNull();
    expect(rideFacts(profile())).toBeNull();
  });

  it("counts the launches off the track figures, lifts and drops not included", () => {
    const taron = rideFacts(
      profile({
        elements: [
          "launch",
          "airtime-hill",
          "launch",
          "first-drop",
          "lifthill",
        ],
        types: ["launch-coaster"],
        manufacturer: "Intamin",
        openedYear: 2016,
        stats: {
          topSpeedKmh: 117,
          heightM: 30,
          lengthM: null,
          durationSeconds: null,
          source: "curated",
          sourceId: null,
          attribution: null,
        },
      }),
    );
    expect(taron).toEqual({
      types: ["launch-coaster"],
      elements: ["launch", "airtime-hill", "launch", "first-drop", "lifthill"],
      launches: 2,
      manufacturer: "Intamin",
      openedYear: 2016,
      topSpeedKmh: 117,
      heightM: 30,
    });
  });

  it("gives no launch count when no figures were written down", () => {
    const facts = rideFacts(profile({ manufacturer: "Mack Rides" }));
    expect(facts).toEqual({ manufacturer: "Mack Rides" });
    expect(facts).not.toHaveProperty("launches");
  });
});

describe("compareRides", () => {
  const rides: ComparedRide[] = [
    { id: "a", name: "Taron", topSpeedKmh: 117, minimumHeightCm: 130 },
    { id: "b", name: "Black Mamba", topSpeedKmh: 80, minimumHeightCm: 140 },
    { id: "c", name: "Colorado Adventure", minimumHeightCm: 100 },
    {
      id: "d",
      name: "Kiddie Coaster",
      topSpeedKmh: 30,
      minimumHeightCm: 90,
      maximumHeightCm: 140,
    },
    { id: "e", name: "Carousel" },
  ];

  it("ranks by the fact asked for and counts the rides without it", () => {
    const fastest = compareRides(rides, { by: "topSpeed", limit: 2 });
    expect(fastest.rides.map((ride) => ride.name)).toEqual([
      "Taron",
      "Black Mamba",
    ]);
    expect(fastest.ranked).toBe(3);
    expect(fastest.withoutValue).toBe(2);
    // Named, so Parky can say which rides park.fan has no figure for, with the id the next tool takes.
    expect(fastest.withoutValueRides).toEqual([
      { id: "c", name: "Colorado Adventure" },
      { id: "e", name: "Carousel" },
    ]);
    expect(fastest).not.toHaveProperty("heightUnknown");
  });

  it("lists every ride of the filters by name without a fact to rank by", () => {
    const all = compareRides(rides, { limit: 10 });
    expect(all.rides.map((ride) => ride.name)).toEqual([
      "Black Mamba",
      "Carousel",
      "Colorado Adventure",
      "Kiddie Coaster",
      "Taron",
    ]);
    expect(all.withoutValue).toBe(0);
  });

  it("lists the rides not ridden yet first without a fact to rank by, and counts them", () => {
    const ridden = new Set(["b", "e"]);
    const left = compareRides(rides, {
      limit: 3,
      first: (ride) => !ridden.has(ride.id),
    });
    expect(left.rides.map((ride) => ride.id)).not.toContain("b");
    expect(left.rides.map((ride) => ride.id)).not.toContain("e");
    expect(left.firstCount).toBe(3);
    // A ranking keeps its order; `first` only sorts the list by name.
    const fastest = compareRides(rides, {
      by: "topSpeed",
      limit: 10,
      first: (ride) => !ridden.has(ride.id),
    });
    expect(fastest).not.toHaveProperty("firstCount");
  });

  it("ranks the other way round with order asc", () => {
    const slowest = compareRides(rides, {
      by: "topSpeed",
      order: "asc",
      limit: 1,
    });
    expect(slowest.rides[0].name).toBe("Kiddie Coaster");
  });

  it("keeps only what a rider may ride, an unknown minimum counted apart", () => {
    const child = compareRides(rides, {
      by: "minimumHeight",
      riderHeightCm: 100,
      limit: 10,
    });
    expect(child.rides.map((ride) => ride.name)).toEqual([
      "Colorado Adventure",
      "Kiddie Coaster",
    ]);
    expect(child.notAllowed).toBe(2);
    expect(child.heightUnknown).toBe(1);
    // Named, so a ride the child is too small for is never taken for one of unknown height.
    expect(child.notAllowedRides).toEqual([
      { id: "a", name: "Taron", minimumHeightCm: 130 },
      { id: "b", name: "Black Mamba", minimumHeightCm: 140 },
    ]);
    expect(child.heightUnknownRides).toEqual([{ id: "e", name: "Carousel" }]);

    const tall = compareRides(rides, {
      by: "minimumHeight",
      riderHeightCm: 150,
      limit: 10,
    });
    expect(tall.rides.map((ride) => ride.name)).not.toContain("Kiddie Coaster");
  });

  it("keeps only the rides that keep you dry with shelter, unknown ones counted apart", () => {
    const rainy: ComparedRide[] = [
      {
        id: "a",
        name: "Taron",
        topSpeedKmh: 117,
        indoorOutdoor: "covered_queue",
      },
      {
        id: "b",
        name: "Winja's Fear",
        topSpeedKmh: 60,
        indoorOutdoor: "indoor",
      },
      { id: "c", name: "Raik", topSpeedKmh: 40, indoorOutdoor: "outdoor" },
      { id: "d", name: "Colorado Adventure", topSpeedKmh: 55 },
    ];
    const covered = compareRides(rainy, {
      by: "topSpeed",
      shelter: "covered",
      limit: 5,
    });
    expect(covered.rides.map((ride) => ride.name)).toEqual([
      "Taron",
      "Winja's Fear",
    ]);
    expect(covered.shelterUnknown).toBe(1);
    expect(covered).not.toHaveProperty("shelterIncomplete");

    const indoor = compareRides(rainy, {
      by: "topSpeed",
      shelter: "indoor",
      limit: 5,
    });
    expect(indoor.rides.map((ride) => ride.name)).toEqual(["Winja's Fear"]);

    // Under half known: whatever somebody happened to check, so the list says it is incomplete.
    const thin = compareRides(
      [
        ...rainy.slice(0, 1),
        { id: "e", name: "Mystery" },
        { id: "f", name: "Chiapas" },
      ],
      { by: "topSpeed", shelter: "covered", limit: 5 },
    );
    expect(thin.shelterIncomplete).toBe(true);
  });

  it("keeps only coasters with type, the rides of unknown type counted apart", () => {
    const typed: ComparedRide[] = [
      { id: "a", name: "Taron", topSpeedKmh: 117, types: ["launch-coaster"] },
      { id: "b", name: "Raik", topSpeedKmh: 40, types: ["boomerang"] },
      { id: "c", name: "River Quest", topSpeedKmh: 30, types: ["rapids"] },
      { id: "d", name: "Mystery", topSpeedKmh: 120 },
    ];
    const slowest = compareRides(typed, {
      by: "topSpeed",
      order: "asc",
      type: "coaster",
      limit: 5,
    });
    expect(slowest.rides.map((ride) => ride.name)).toEqual(["Raik", "Taron"]);
    expect(slowest.typeUnknown).toBe(1);
    expect(
      compareRides(typed, { by: "topSpeed", limit: 5 }),
    ).not.toHaveProperty("typeUnknown");
  });
});

describe("isRideOfType", () => {
  it("reads every -coaster term and the glossary's other coaster terms as a coaster", () => {
    for (const types of [["wooden-coaster"], ["wild-mouse"], ["slc"]]) {
      expect(isRideOfType(types, "coaster")).toBe(true);
    }
    for (const types of [[], ["dark-ride"], ["log-flume"]]) {
      expect(isRideOfType(types, "coaster")).toBe(false);
    }
  });

  it("reads dark rides, water rides and family rides off their glossary ids", () => {
    expect(isRideOfType(["dark-ride", "animatronics"], "dark_ride")).toBe(true);
    expect(isRideOfType(["trackless-ride"], "dark_ride")).toBe(true);
    expect(isRideOfType(["log-flume"], "water")).toBe(true);
    // A boat ride is not wet by itself.
    expect(isRideOfType(["boat-ride", "dark-ride"], "water")).toBe(false);
    expect(isRideOfType(["family-coaster"], "family")).toBe(true);
    expect(isRideOfType(["family-coaster"], "coaster")).toBe(true);
    expect(isRideOfType([], "family")).toBe(false);
  });
});

describe("comparedRides", () => {
  const attraction = (over: Partial<Attraction>): Attraction =>
    ({
      id: "x",
      name: "Ride",
      slug: "ride",
      minimumHeight: null,
      curatedMinimumHeight: null,
      maximumHeight: null,
      curatedMaximumHeight: null,
      minimumHeightUnit: null,
      ...over,
    }) as Attraction;

  it("serves one row per name, the curated height and the profile's facts", () => {
    const rows = comparedRides(
      [
        attraction({
          id: "a",
          name: "Taron",
          slug: "taron",
          minimumHeight: 120,
        }),
        attraction({ id: "b", name: "Taron", slug: "taron-2" }),
        attraction({
          id: "c",
          name: "Steel Vengeance",
          slug: "steel-vengeance",
          minimumHeight: 120,
          curatedMinimumHeight: 132,
          minimumHeightUnit: "in",
        }),
      ],
      new Map([
        [
          "a",
          {
            attractionId: "a",
            elements: ["launch", "launch"],
            types: [],
            manufacturerName: "Intamin",
            model: "Blitz Coaster",
            openedYear: 2016,
            inversions: null,
            stats: null,
            curatedStats: null,
          } as unknown as AttractionRideProfile,
        ],
      ]),
    );
    expect(rows).toEqual([
      {
        id: "a",
        name: "Taron",
        minimumHeightCm: 120,
        launches: 2,
        manufacturer: "Intamin",
        openedYear: 2016,
      },
      {
        id: "c",
        name: "Steel Vengeance",
        minimumHeightCm: 132,
        heightUnit: "in",
      },
    ]);
  });

  it("keeps one name in two parks apart, each with its park", () => {
    const pirates = (id: string, parkId: string, park: string) =>
      attraction({
        id,
        parkId,
        name: "Pirates of the Caribbean",
        park: { name: park } as Attraction["park"],
      });
    const rows = comparedRides(
      [
        pirates("a", "p1", "Magic Kingdom"),
        pirates("b", "p2", "Disneyland Park"),
      ],
      new Map(),
    );
    expect(rows.map((row) => row.park)).toEqual([
      "Magic Kingdom",
      "Disneyland Park",
    ]);
  });
});
