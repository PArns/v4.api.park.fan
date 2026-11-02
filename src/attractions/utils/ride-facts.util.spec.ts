import {
  compareRides,
  comparedRides,
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
    expect(fastest).not.toHaveProperty("heightUnknown");
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

    const tall = compareRides(rides, {
      by: "minimumHeight",
      riderHeightCm: 150,
      limit: 10,
    });
    expect(tall.rides.map((ride) => ride.name)).not.toContain("Kiddie Coaster");
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
});
