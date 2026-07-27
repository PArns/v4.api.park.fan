import { normalizeClosingTime } from "./operating-window.util";

/**
 * Sources publish an operating day as (openingTime, closingTime), and some of
 * them get the closing *date* wrong while the time-of-day is right:
 *
 *   - ThemeParks.wiki stamps a past-midnight close with the day's own date, so
 *     "closes at 00:00" lands 12–24 h BEFORE opening (Parque Warner: every day).
 *   - Others overshoot by a day or typo the year (SeaWorld, Busch Gardens),
 *     producing "operating days" of 34 h up to 3 years.
 *
 * A park's operating day is anchored to one calendar date, so the window must
 * be > 0 and ≤ 24 h. When it isn't, the closing time-of-day is trusted and
 * re-anchored to the opening's park-local date.
 */
describe("normalizeClosingTime", () => {
  const MADRID = "Europe/Madrid";
  const NEW_YORK = "America/New_York";
  const LOS_ANGELES = "America/Los_Angeles";

  describe("windows that are already valid", () => {
    it("leaves an ordinary daytime window untouched", () => {
      const opening = new Date("2026-07-27T08:00:00Z"); // 10:00 Madrid
      const closing = new Date("2026-07-27T16:00:00Z"); // 18:00 Madrid
      expect(normalizeClosingTime(opening, closing, MADRID)).toBe(closing);
    });

    it("leaves a legitimate past-midnight window untouched", () => {
      // Already correct: opens 12:00, closes 00:00 the NEXT day.
      const opening = new Date("2026-07-27T10:00:00Z"); // 12:00 Madrid
      const closing = new Date("2026-07-27T22:00:00Z"); // 00:00 Madrid, 28th
      expect(normalizeClosingTime(opening, closing, MADRID)).toBe(closing);
    });

    it("leaves a full 24 h window untouched", () => {
      const opening = new Date("2026-07-27T10:00:00Z");
      const closing = new Date("2026-07-28T10:00:00Z");
      expect(normalizeClosingTime(opening, closing, MADRID)).toBe(closing);
    });
  });

  describe("closing stamped with the wrong date", () => {
    it("rolls a midnight close onto the next day (Parque Warner)", () => {
      // Upstream: open 2026-07-27T12:00+02:00, close 2026-07-27T00:00+02:00.
      const opening = new Date("2026-07-27T10:00:00Z");
      const closing = new Date("2026-07-26T22:00:00Z"); // 00:00 Madrid, 27th
      const fixed = normalizeClosingTime(opening, closing, MADRID);
      // 00:00 Madrid on the 28th.
      expect(fixed.toISOString()).toBe("2026-07-27T22:00:00.000Z");
    });

    it("pulls back a close that overshoots by a day (SeaWorld San Diego)", () => {
      // Opens 10:00 PST on the 13th; upstream closes 19:00 PST on the 14th.
      const opening = new Date("2026-02-13T18:00:00Z");
      const closing = new Date("2026-02-15T03:00:00Z");
      const fixed = normalizeClosingTime(opening, closing, LOS_ANGELES);
      // 19:00 PST on the 13th.
      expect(fixed.toISOString()).toBe("2026-02-14T03:00:00.000Z");
    });

    it("repairs a typo'd year (Busch Gardens Williamsburg)", () => {
      // Opens 10:00 EDT on 2026-03-29; upstream closes 2029-03-30T00:00Z.
      const opening = new Date("2026-03-29T14:00:00Z");
      const closing = new Date("2029-03-30T00:00:00Z"); // 20:00 EDT
      const fixed = normalizeClosingTime(opening, closing, NEW_YORK);
      // 20:00 EDT on 2026-03-29.
      expect(fixed.toISOString()).toBe("2026-03-30T00:00:00.000Z");
    });
  });

  describe("windows that must not be invented", () => {
    it("leaves an equal opening and closing alone", () => {
      // Degenerate input (a source reporting nothing). Rolling it forward would
      // invent a 24 h operating day; the caller decides what to do with it.
      const t = new Date("2026-07-27T10:00:00Z");
      expect(normalizeClosingTime(t, t, MADRID)).toBe(t);
    });

    it("returns the closing time unchanged when either side is missing", () => {
      const closing = new Date("2026-07-27T22:00:00Z");
      expect(normalizeClosingTime(null, closing, MADRID)).toBe(closing);
      expect(normalizeClosingTime(closing, null, MADRID)).toBeNull();
    });

    it("returns the closing time unchanged for an unusable timezone", () => {
      const opening = new Date("2026-07-27T10:00:00Z");
      const closing = new Date("2026-07-26T22:00:00Z");
      expect(normalizeClosingTime(opening, closing, "Not/AZone")).toBe(closing);
    });
  });

  describe("daylight saving time", () => {
    it("keeps the local closing time across a spring-forward night", () => {
      // Madrid moves +1 h at 02:00 on 2026-03-29. Opens 22:00 local on the 28th,
      // upstream stamps the 03:00 close with the 28th (one day early).
      const opening = new Date("2026-03-28T21:00:00Z"); // 22:00 CET
      const closing = new Date("2026-03-27T02:00:00Z"); // 03:00 CET, 27th
      const fixed = normalizeClosingTime(opening, closing, MADRID);
      // 03:00 local on the 29th is CEST (+02:00) → 01:00Z.
      expect(fixed.toISOString()).toBe("2026-03-29T01:00:00.000Z");
      expect(fixed.getTime()).toBeGreaterThan(opening.getTime());
      expect(fixed.getTime() - opening.getTime()).toBeLessThanOrEqual(
        24 * 60 * 60 * 1000,
      );
    });
  });
});
