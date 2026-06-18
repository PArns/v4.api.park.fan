import { determineCrowdLevel, rateOrUnknown } from "./crowd-level.util";

describe("determineCrowdLevel", () => {
  it("maps occupancy percentages to the six tiers", () => {
    expect(determineCrowdLevel(0)).toBe("very_low");
    expect(determineCrowdLevel(60)).toBe("very_low");
    expect(determineCrowdLevel(75)).toBe("low");
    expect(determineCrowdLevel(100)).toBe("moderate");
    expect(determineCrowdLevel(130)).toBe("high");
    expect(determineCrowdLevel(180)).toBe("very_high");
    expect(determineCrowdLevel(250)).toBe("extreme");
  });

  it("never returns 'unknown' — that is reserved for the no-baseline paths", () => {
    for (const pct of [0, 50, 100, 150, 200, 500]) {
      expect(determineCrowdLevel(pct)).not.toBe("unknown");
    }
  });
});

describe("rateOrUnknown", () => {
  it("returns 'unknown' when the baseline is missing/non-positive (park not ratable)", () => {
    expect(rateOrUnknown(40, 0)).toBe("unknown");
    expect(rateOrUnknown(40, -5)).toBe("unknown");
    expect(rateOrUnknown(40, NaN)).toBe("unknown");
  });

  it("rates against the baseline exactly like determineCrowdLevel when ratable", () => {
    // 40 / 40 = 100% → moderate
    expect(rateOrUnknown(40, 40)).toBe("moderate");
    // 80 / 40 = 200% → very_high
    expect(rateOrUnknown(80, 40)).toBe("very_high");
    // 20 / 40 = 50% → very_low
    expect(rateOrUnknown(20, 40)).toBe("very_low");
    // matches determineCrowdLevel for an arbitrary ratio
    expect(rateOrUnknown(50, 40)).toBe(determineCrowdLevel((50 / 40) * 100));
  });
});
