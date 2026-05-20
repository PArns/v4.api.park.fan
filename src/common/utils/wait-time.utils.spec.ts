import { roundToNearest5Minutes } from "./wait-time.utils";

describe("roundToNearest5Minutes", () => {
  it("rounds typical positive numbers to nearest 5", () => {
    expect(roundToNearest5Minutes(7.2)).toBe(5);
    expect(roundToNearest5Minutes(8.9)).toBe(10);
    expect(roundToNearest5Minutes(12.4)).toBe(10);
    expect(roundToNearest5Minutes(34.7)).toBe(35);
  });

  it("rounds boundary cases consistently (x.5 rounds up)", () => {
    expect(roundToNearest5Minutes(2.5)).toBe(5);
    expect(roundToNearest5Minutes(7.5)).toBe(10);
    expect(roundToNearest5Minutes(12.5)).toBe(15);
  });

  it("returns 0 for sub-minimum and negative inputs", () => {
    expect(roundToNearest5Minutes(0)).toBe(0);
    expect(roundToNearest5Minutes(0.5)).toBe(0);
    expect(roundToNearest5Minutes(2.49)).toBe(0);
    expect(roundToNearest5Minutes(-10)).toBe(0);
  });

  // Regression: Postgres NUMERIC columns (e.g. `$x::numeric as avg_wait_today`)
  // are returned by node-postgres as strings. Without coercion the body
  // computes `"45" + 2.5` → `"452.5"` → 90.5 / 5 → 90 → ×5 = 450. This
  // surfaced on api.park.fan as `avgWaitToday: 450` for parks that had
  // P90 = 45 in `park_daily_stats`. See commit 747f712.
  describe("string inputs (Postgres NUMERIC coercion)", () => {
    it("treats numeric strings the same as numbers", () => {
      expect(roundToNearest5Minutes("45" as unknown as number)).toBe(45);
      expect(roundToNearest5Minutes("110" as unknown as number)).toBe(110);
      expect(roundToNearest5Minutes("25.5" as unknown as number)).toBe(25);
    });

    it("does not produce the 10x concatenation bug", () => {
      // The bug: "45" + 2.5 = "452.5" → /5 → 90.5 → floor → 90 → *5 = 450
      expect(roundToNearest5Minutes("45" as unknown as number)).not.toBe(450);
      expect(roundToNearest5Minutes("25" as unknown as number)).not.toBe(250);
      expect(roundToNearest5Minutes("110" as unknown as number)).not.toBe(1100);
    });

    it("returns 0 for non-numeric strings", () => {
      expect(roundToNearest5Minutes("abc" as unknown as number)).toBe(0);
      expect(roundToNearest5Minutes("" as unknown as number)).toBe(0);
    });
  });

  it("returns 0 for nullish-coerced inputs (defensive)", () => {
    expect(roundToNearest5Minutes(null as unknown as number)).toBe(0);
    expect(roundToNearest5Minutes(undefined as unknown as number)).toBe(0);
    expect(roundToNearest5Minutes(NaN)).toBe(0);
  });
});
