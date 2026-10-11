import {
  COVERED_MIN_KNOWN_SHARE,
  RAIN_LEAD_MIN,
  RAIN_PUSH_RIDE_LIMIT,
  coveredOfferReady,
  decideRainSoon,
  isCovered,
  rankCoveredRides,
  type CoveredCandidate,
  type RainNowcastInput,
} from "./rain-soon";

const NOW = Date.parse("2026-10-17T12:00:00.000Z");
const minutes = (n: number) => new Date(NOW + n * 60_000).toISOString();

const dry: RainNowcastInput = {
  currentlyRaining: false,
  rainStartsAt: null,
  rainEndsAt: null,
};

describe("decideRainSoon", () => {
  it("does nothing when no rain is forecast", () => {
    expect(decideRainSoon(dry, null, NOW)).toEqual({ action: "none" });
  });

  it("sends when rain starts inside the lead window", () => {
    const nowcast = { ...dry, rainStartsAt: minutes(20) };
    expect(decideRainSoon(nowcast, null, NOW)).toEqual({
      action: "send",
      startsAtMs: NOW + 20 * 60_000,
    });
  });

  it("sends at exactly the lead and not a minute further out", () => {
    const edge = { ...dry, rainStartsAt: minutes(RAIN_LEAD_MIN) };
    const beyond = { ...dry, rainStartsAt: minutes(RAIN_LEAD_MIN + 1) };
    expect(decideRainSoon(edge, null, NOW).action).toBe("send");
    expect(decideRainSoon(beyond, null, NOW).action).toBe("none");
  });

  it("does not warn about rain that is already falling, and remembers it", () => {
    const raining = {
      currentlyRaining: true,
      rainStartsAt: null,
      rainEndsAt: minutes(40),
    };
    expect(decideRainSoon(raining, null, NOW)).toEqual({
      action: "mark-raining",
    });
    expect(decideRainSoon(raining, "warned", NOW)).toEqual({
      action: "mark-raining",
    });
    expect(decideRainSoon(raining, "raining", NOW)).toEqual({ action: "none" });
  });

  it("stays quiet on the next tick after a warning", () => {
    const nowcast = { ...dry, rainStartsAt: minutes(15) };
    expect(decideRainSoon(nowcast, "warned", NOW)).toEqual({ action: "none" });
  });

  it("does not re-arm when a warned forecast merely stops showing rain", () => {
    expect(decideRainSoon(dry, "warned", NOW)).toEqual({ action: "none" });
  });

  it("re-arms once the rain has stopped, so a second front warns again", () => {
    expect(decideRainSoon(dry, "raining", NOW)).toEqual({ action: "rearm" });
    const secondFront = { ...dry, rainStartsAt: minutes(25) };
    expect(decideRainSoon(secondFront, null, NOW).action).toBe("send");
  });

  it("reads a future start as not raining yet, however far off the end is", () => {
    const nowcast = {
      currentlyRaining: true,
      rainStartsAt: minutes(10),
      rainEndsAt: minutes(90),
    };
    expect(decideRainSoon(nowcast, null, NOW).action).toBe("send");
  });
});

describe("covered rides", () => {
  const ride = (
    over: Partial<CoveredCandidate> & { id: string },
  ): CoveredCandidate => ({
    name: over.id,
    indoorOutdoor: "indoor",
    isCurrentlyInSeason: null,
    operating: true,
    waitTime: 10,
    ...over,
  });

  it("counts indoor and covered_queue as covered, and absent as neither", () => {
    expect(isCovered({ indoorOutdoor: "indoor" })).toBe(true);
    expect(isCovered({ indoorOutdoor: "covered_queue" })).toBe(true);
    expect(isCovered({ indoorOutdoor: "outdoor" })).toBe(false);
    expect(isCovered({ indoorOutdoor: null })).toBe(false);
  });

  it("gates on the known share of in-season attractions", () => {
    const rows = [
      ride({ id: "a" }),
      ride({ id: "b", indoorOutdoor: "outdoor" }),
      ride({ id: "c", indoorOutdoor: null }),
      ride({ id: "d", indoorOutdoor: null }),
    ];
    // 2 of 4 known is exactly the threshold.
    expect(COVERED_MIN_KNOWN_SHARE).toBe(0.5);
    expect(coveredOfferReady(rows)).toBe(true);
    expect(
      coveredOfferReady([...rows, ride({ id: "e", indoorOutdoor: null })]),
    ).toBe(false);
  });

  it("needs at least one covered ride, however well the park is known", () => {
    expect(
      coveredOfferReady([
        ride({ id: "a", indoorOutdoor: "outdoor" }),
        ride({ id: "b", indoorOutdoor: "outdoor" }),
      ]),
    ).toBe(false);
  });

  it("leaves out-of-season rides out of both counts, and null season in", () => {
    const rows = [
      ride({ id: "a" }),
      ride({ id: "off", indoorOutdoor: null, isCurrentlyInSeason: false }),
      ride({ id: "off2", indoorOutdoor: null, isCurrentlyInSeason: false }),
    ];
    expect(coveredOfferReady(rows)).toBe(true);
    expect(
      coveredOfferReady([
        ride({ id: "a" }),
        ride({ id: "x", indoorOutdoor: null, isCurrentlyInSeason: null }),
        ride({ id: "y", indoorOutdoor: null, isCurrentlyInSeason: null }),
      ]),
    ).toBe(false);
    expect(coveredOfferReady([])).toBe(false);
  });

  it("ranks the shortest queue first and caps at three", () => {
    const ranked = rankCoveredRides([
      ride({ id: "slow", waitTime: 45 }),
      ride({ id: "fast", waitTime: 5 }),
      ride({ id: "mid", waitTime: 20 }),
      ride({ id: "mid2", waitTime: 25 }),
    ]);
    expect(ranked.map((r) => r.id)).toEqual(["fast", "mid", "mid2"]);
    expect(ranked).toHaveLength(RAIN_PUSH_RIDE_LIMIT);
  });

  it("sorts a ride without a posted wait last, ties by name", () => {
    const ranked = rankCoveredRides([
      ride({ id: "none", waitTime: null }),
      ride({ id: "b", waitTime: 10 }),
      ride({ id: "a", waitTime: 10 }),
    ]);
    expect(ranked.map((r) => r.id)).toEqual(["a", "b", "none"]);
  });

  it("offers only open, in-season, covered rides", () => {
    const ranked = rankCoveredRides([
      ride({ id: "closed", operating: false }),
      ride({ id: "off", isCurrentlyInSeason: false }),
      ride({ id: "outdoor", indoorOutdoor: "outdoor" }),
      ride({ id: "unknown", indoorOutdoor: null }),
      ride({ id: "ok", indoorOutdoor: "covered_queue" }),
    ]);
    expect(ranked.map((r) => r.id)).toEqual(["ok"]);
  });
});
