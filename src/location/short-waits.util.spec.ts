import type {
  ParkAttractionDto,
  ParkWithAttractionsDto,
} from "../parks/dto/park-with-attractions.dto";
import {
  pickShortWaits,
  SHORT_WAITS_LIMIT,
  SHORT_WAITS_PER_PARK,
  type ShortWaitCandidatePark,
} from "./short-waits.util";

function ride(
  id: string,
  wait: number | null,
  baseline: number | null,
  overrides: Partial<ParkAttractionDto> = {},
): ParkAttractionDto {
  return {
    id,
    name: id,
    slug: id,
    latitude: null,
    longitude: null,
    effectiveStatus: "OPERATING",
    queues: [{ queueType: "STANDBY", waitTime: wait }],
    baseline,
    comparison:
      wait !== null && baseline
        ? wait / baseline <= 0.3
          ? "much_lower"
          : wait / baseline <= 0.6
            ? "lower"
            : "typical"
        : null,
    url: `/europe/germany/bruehl/p/${id}`,
    ...overrides,
  } as ParkAttractionDto;
}

function park(
  id: string,
  attractions: ParkAttractionDto[],
  overrides: Partial<ParkWithAttractionsDto> = {},
  distance = 19_000,
): ShortWaitCandidatePark {
  return {
    integrated: {
      id,
      slug: id,
      status: "OPERATING",
      liveWaitTimes: { available: true, reason: null },
      url: `/europe/germany/bruehl/${id}`,
      attractions,
      ...overrides,
    } as ParkWithAttractionsDto,
    name: id.toUpperCase(),
    distance,
  };
}

describe("pickShortWaits", () => {
  it("lists a ride well below its usual wait, rounded, with its park", () => {
    const [taron] = pickShortWaits([park("p", [ride("taron", 16, 41)])]);

    expect(taron).toEqual({
      id: "taron",
      name: "taron",
      slug: "taron",
      url: "/europe/germany/bruehl/p/taron",
      waitTime: 15,
      typicalWait: 40,
      isHeadliner: false,
      park: {
        id: "p",
        name: "P",
        slug: "p",
        distance: 19_000,
        url: "/europe/germany/bruehl/p",
      },
    });
  });

  it("skips a ride at its usual wait, below 15 minutes usually, or less than 10 minutes below it", () => {
    const result = pickShortWaits([
      park("p", [
        ride("typical", 35, 40),
        ride("short-anyway", 0, 10),
        ride("small-gap", 15, 20, { comparison: "lower" }),
      ]),
    ]);

    expect(result).toEqual([]);
  });

  it("skips a ride that is not running, out of season, or has no baseline", () => {
    const result = pickShortWaits([
      park("p", [
        ride("down", 5, 40, { effectiveStatus: "DOWN" }),
        ride("winter", 5, 40, { isCurrentlyInSeason: false }),
        ride("no-baseline", 5, null),
        ride("no-wait", null, 40, { comparison: "much_lower" }),
      ]),
    ]);

    expect(result).toEqual([]);
  });

  it("skips a closed park and one whose wait times cannot be read", () => {
    const result = pickShortWaits([
      park("closed", [ride("a", 5, 40)], { status: "CLOSED" }),
      park("unreadable", [ride("b", 5, 40)], {
        liveWaitTimes: { available: false, reason: "not_published" },
      }),
    ]);

    expect(result).toEqual([]);
  });

  it("puts the biggest gap first, then a headliner, then the nearer park", () => {
    const result = pickShortWaits([
      park("far", [ride("far-gap", 10, 40)], {}, 90_000),
      park("near", [
        ride("big-gap", 10, 60),
        ride("plain", 10, 40),
        ride("headliner", 10, 40, { isHeadliner: true }),
      ]),
    ]);

    expect(result.map((r) => r.id)).toEqual([
      "big-gap",
      "headliner",
      "plain",
      "far-gap",
    ]);
  });

  it(`takes at most ${SHORT_WAITS_PER_PARK} per park and ${SHORT_WAITS_LIMIT} in all`, () => {
    const many = (prefix: string) =>
      Array.from({ length: 6 }, (_, i) => ride(`${prefix}${i}`, 5, 40 + i * 5));

    const result = pickShortWaits([
      park("a", many("a")),
      park("b", many("b")),
      park("c", many("c")),
    ]);

    expect(result).toHaveLength(SHORT_WAITS_LIMIT);
    for (const id of ["a", "b", "c"]) {
      expect(result.filter((r) => r.park.id === id).length).toBeLessThanOrEqual(
        SHORT_WAITS_PER_PARK,
      );
    }
  });
});
