import {
  HOURLY_CONFIDENCE_MAX_LEAD_MS,
  servedAdjustedConfidence,
  servedHourlyConfidence,
} from "./hourly-confidence.util";

const NOW = Date.parse("2026-10-04T12:00:00.000Z");
const H = 60 * 60 * 1000;
const at = (leadMs: number) => new Date(NOW + leadMs).toISOString();

describe("servedHourlyConfidence", () => {
  it("keeps the value at 23 h lead", () => {
    expect(servedHourlyConfidence(61, at(23 * H), NOW)).toBe(61);
  });

  it("keeps the value at exactly 24 h lead", () => {
    expect(HOURLY_CONFIDENCE_MAX_LEAD_MS).toBe(24 * H);
    expect(servedHourlyConfidence(50, at(24 * H), NOW)).toBe(50);
  });

  it("serves null at 25 h lead", () => {
    expect(servedHourlyConfidence(50, at(25 * H), NOW)).toBeNull();
  });

  it("serves null one millisecond past 24 h", () => {
    expect(servedHourlyConfidence(50, at(24 * H + 1), NOW)).toBeNull();
  });

  it("counts the lead from delivery, not from when the row was written", () => {
    // The same slot, 25 h out at 12:00, is 24 h out at 13:00.
    const slot = at(25 * H);
    expect(servedHourlyConfidence(50, slot, NOW)).toBeNull();
    expect(servedHourlyConfidence(50, slot, NOW + H)).toBe(50);
  });

  it("keeps the value for the current slot, which started in the past", () => {
    expect(servedHourlyConfidence(88, at(-10 * 60 * 1000), NOW)).toBe(88);
  });

  it("accepts a Date as well as an ISO string", () => {
    expect(servedHourlyConfidence(50, new Date(NOW + 25 * H), NOW)).toBeNull();
    expect(servedHourlyConfidence(50, new Date(NOW + 2 * H), NOW)).toBe(50);
  });

  it("keeps a zero, which is a value and not an absence", () => {
    expect(servedHourlyConfidence(0, at(H), NOW)).toBe(0);
  });

  it("keeps a missing confidence missing", () => {
    expect(servedHourlyConfidence(null, at(H), NOW)).toBeNull();
    expect(servedHourlyConfidence(undefined, at(H), NOW)).toBeNull();
  });

  it("keeps the value when the time does not parse", () => {
    expect(servedHourlyConfidence(70, "not a date", NOW)).toBe(70);
  });
});

describe("servedAdjustedConfidence", () => {
  const row = (
    leadMs: number,
    confidence: number | null = 80,
    predictionType: "hourly" | "daily" = "hourly",
  ) => ({ confidence, predictedTime: at(leadMs), predictionType });

  it("halves the served value up to 24 h", () => {
    expect(servedAdjustedConfidence(row(23 * H), NOW)).toBe(40);
    expect(servedAdjustedConfidence(row(24 * H), NOW)).toBe(40);
  });

  it("follows the same null rule past 24 h for hourly", () => {
    expect(servedAdjustedConfidence(row(25 * H), NOW)).toBeNull();
  });

  it("leaves a daily row halved however far ahead it is", () => {
    expect(servedAdjustedConfidence(row(72 * H, 80, "daily"), NOW)).toBe(40);
  });

  it("is null where the confidence is missing", () => {
    expect(servedAdjustedConfidence(row(H, null), NOW)).toBeNull();
  });
});
