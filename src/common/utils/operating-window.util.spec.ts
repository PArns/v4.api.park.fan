import { formatInTimeZone } from "date-fns-tz";
import {
  correctTwelveHourClockClose,
  normalizeClosingTime,
} from "./operating-window.util";

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

/**
 * A separate repair, for the rows `normalizeClosingTime` provably cannot fix.
 *
 * Some sources publish a 12-hour clock without saying so, and a midnight close
 * then arrives as `12:00`. Re-anchoring turns Six Flags Qiddiya City's
 * `opens 15:00 / closes 12:00` into a 21-hour operating day — right while the
 * park is open, wrong for the eleven hours after it shuts. The only thing that
 * separates it from a park genuinely closing at noon is that the closing falls
 * before the opening, and that is precisely what the re-anchoring consumes. So
 * this runs on the raw pair, and only for a park somebody has flagged.
 */
describe("correctTwelveHourClockClose", () => {
  const RIYADH = "Asia/Riyadh";
  const NEW_YORK = "America/New_York";
  const MADRID = "Europe/Madrid";
  const SANTIAGO = "America/Santiago";

  describe("the rows it exists for", () => {
    it("reads a flagged park's noon close as midnight (Six Flags Qiddiya City)", () => {
      // Upstream: open 2026-04-17T15:00+03:00, close 2026-04-17T12:00+03:00.
      const opening = new Date("2026-04-17T12:00:00Z"); // 15:00 Riyadh
      const closing = new Date("2026-04-17T09:00:00Z"); // 12:00 Riyadh, same day
      const fixed = correctTwelveHourClockClose(opening, closing, RIYADH);
      // 00:00 Riyadh on the 18th — a nine-hour evening, not a 21-hour day.
      expect(fixed?.toISOString()).toBe("2026-04-17T21:00:00.000Z");
      expect(fixed!.getTime() - opening.getTime()).toBe(9 * 60 * 60 * 1000);
    });

    it("beats the generic repair, which would have produced a 21 h day", () => {
      // The contrast is the whole point: same input, both paths.
      const opening = new Date("2026-04-17T12:00:00Z");
      const closing = new Date("2026-04-17T09:00:00Z");
      const generic = normalizeClosingTime(opening, closing, RIYADH);
      expect(generic.getTime() - opening.getTime()).toBe(21 * 60 * 60 * 1000);
      const corrected = correctTwelveHourClockClose(opening, closing, RIYADH)!;
      expect(corrected.getTime()).toBeLessThan(generic.getTime());
    });
  });

  describe("what it must never touch", () => {
    it("leaves a genuine noon closing alone (water park, Christmas market)", () => {
      // Opens 09:00, closes 12:00 the SAME day: a real half-day, and the whole
      // reason this is a per-park flag rather than a rule about `12:00`.
      const opening = new Date("2026-07-27T07:00:00Z"); // 09:00 Madrid
      const closing = new Date("2026-07-27T10:00:00Z"); // 12:00 Madrid
      expect(correctTwelveHourClockClose(opening, closing, MADRID)).toBeNull();
    });

    it("leaves a past-midnight close alone — that one already works", () => {
      // Kings Dominion's Haunt evenings: open 18:00, close 00:00, misdated onto
      // the day's own date. `normalizeClosingTime` rolls it forward correctly,
      // and re-deciding it here would be a second opinion on a solved case.
      const opening = new Date("2026-09-18T22:00:00Z"); // 18:00 New York
      const closing = new Date("2026-09-18T04:00:00Z"); // 00:00 New York
      expect(
        correctTwelveHourClockClose(opening, closing, NEW_YORK),
      ).toBeNull();
    });

    it("leaves any other misdated hour alone", () => {
      // 19:00 stamped a day early. A 12-hour clock cannot misprint 19:00 — only
      // the noon/midnight pair survives the conversion ambiguously.
      const opening = new Date("2026-07-27T08:00:00Z"); // 10:00 Madrid
      const closing = new Date("2026-07-26T17:00:00Z"); // 19:00 Madrid, 26th
      expect(correctTwelveHourClockClose(opening, closing, MADRID)).toBeNull();
    });

    it("returns null when either side is missing or the zone is unusable", () => {
      const opening = new Date("2026-04-17T12:00:00Z");
      const closing = new Date("2026-04-17T09:00:00Z");
      expect(correctTwelveHourClockClose(null, closing, RIYADH)).toBeNull();
      expect(correctTwelveHourClockClose(opening, null, RIYADH)).toBeNull();
      expect(
        correctTwelveHourClockClose(opening, closing, "Not/AZone"),
      ).toBeNull();
    });
  });

  describe("the next park-local day", () => {
    it("lands on midnight across a spring-forward night", () => {
      // Madrid moves +1 h at 02:00 on 2026-03-29, so the night after the 28th is
      // 23 h long. Midnight itself is unaffected, and the window stays 9 h.
      const opening = new Date("2026-03-28T14:00:00Z"); // 15:00 CET
      const closing = new Date("2026-03-28T11:00:00Z"); // 12:00 CET
      const fixed = correctTwelveHourClockClose(opening, closing, MADRID)!;
      expect(fixed.toISOString()).toBe("2026-03-28T23:00:00.000Z"); // 00:00 CET, 29th
      expect(fixed.getTime() - opening.getTime()).toBe(9 * 60 * 60 * 1000);
    });

    it("lands inside the right day in a zone that shifts at midnight itself", () => {
      // Santiago springs forward AT 00:00 on 2026-09-06, so that local midnight
      // does not exist at all — the clock goes 23:59:59-04:00 → 01:00:00-03:00.
      // Asked for it naively, `fromZonedTime` resolves backwards to 23:00 on the
      // 5th, an hour BEFORE the day it should start. The corrected close has to
      // be the start of the 6th, which is the transition itself.
      const opening = new Date("2026-09-05T19:00:00Z"); // 15:00 -04:00
      const closing = new Date("2026-09-05T16:00:00Z"); // 12:00 -04:00
      const fixed = correctTwelveHourClockClose(opening, closing, SANTIAGO)!;
      expect(fixed.toISOString()).toBe("2026-09-06T04:00:00.000Z");
      expect(formatInTimeZone(fixed, SANTIAGO, "yyyy-MM-dd HH:mm")).toBe(
        "2026-09-06 01:00",
      );
      expect(fixed.getTime()).toBeGreaterThan(opening.getTime());
      expect(fixed.getTime() - opening.getTime()).toBeLessThanOrEqual(
        24 * 60 * 60 * 1000,
      );
    });
  });
});
