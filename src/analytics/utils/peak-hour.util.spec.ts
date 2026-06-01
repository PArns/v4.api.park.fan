import { peakHourConfidence } from "./peak-hour.util";

describe("peakHourConfidence", () => {
  it("ranks an observed peak highest", () => {
    expect(peakHourConfidence("observed_today")).toBe(0.9);
  });

  it("gives a forecast medium confidence", () => {
    expect(peakHourConfidence("prediction")).toBe(0.6);
  });

  it("gives a historical fallback lower confidence", () => {
    expect(peakHourConfidence("historical_fallback")).toBe(0.4);
  });

  it("returns 0 when there is no peak", () => {
    expect(peakHourConfidence(null)).toBe(0);
  });

  it("always stays within 0..1", () => {
    for (const source of [
      "observed_today",
      "prediction",
      "historical_fallback",
      null,
    ] as const) {
      const c = peakHourConfidence(source);
      expect(c).toBeGreaterThanOrEqual(0);
      expect(c).toBeLessThanOrEqual(1);
    }
  });
});
