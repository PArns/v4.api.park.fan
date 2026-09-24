import { isClosedByOperatingRange } from "./schedule-closed-day.util";

describe("isClosedByOperatingRange", () => {
  const range = { minDate: "2026-04-01", maxDate: "2026-11-01" };

  it("is false without a known operating range", () => {
    expect(
      isClosedByOperatingRange(
        "2026-06-01",
        { minDate: null, maxDate: null },
        true,
      ),
    ).toBe(false);
  });

  it("closes a day strictly inside the range (a gap in the season)", () => {
    expect(isClosedByOperatingRange("2026-06-01", range, false)).toBe(true);
  });

  it("leaves the first and last operating date alone", () => {
    expect(isClosedByOperatingRange("2026-04-01", range, true)).toBe(false);
    expect(isClosedByOperatingRange("2026-11-01", range, true)).toBe(false);
  });

  it("closes days outside the range only for a seasonal park", () => {
    expect(isClosedByOperatingRange("2026-12-15", range, true)).toBe(true);
    expect(isClosedByOperatingRange("2026-03-15", range, true)).toBe(true);
    expect(isClosedByOperatingRange("2026-12-15", range, false)).toBe(false);
    expect(isClosedByOperatingRange("2026-03-15", range, false)).toBe(false);
  });
});
