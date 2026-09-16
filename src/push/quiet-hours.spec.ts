import {
  QUIET_HOURS_END_HOUR,
  QUIET_HOURS_START_HOUR,
  isWithinQuietHours,
} from "./quiet-hours";

/**
 * The window is 23:00-07:00 and it wraps midnight, so the interesting cases are
 * the two edges, the wrap itself, and the two ways the zone can be useless. The
 * case the module exists for is the last block: one instant, two subscribers,
 * opposite answers.
 */
describe("isWithinQuietHours", () => {
  const at = (iso: string) => Date.parse(iso);

  it("pins the window the product decision named", () => {
    expect(QUIET_HOURS_START_HOUR).toBe(23);
    expect(QUIET_HOURS_END_HOUR).toBe(7);
  });

  describe("the edges, in the subscriber's zone", () => {
    // 2026-07-15 is CEST, so Berlin is UTC+2 on every instant below.
    it("is loud at 22:59 and quiet from 23:00", () => {
      expect(
        isWithinQuietHours("Europe/Berlin", at("2026-07-15T20:59:00.000Z")),
      ).toBe(false);
      expect(
        isWithinQuietHours("Europe/Berlin", at("2026-07-15T21:00:00.000Z")),
      ).toBe(true);
    });

    it("is quiet at 06:59 and loud from 07:00", () => {
      expect(
        isWithinQuietHours("Europe/Berlin", at("2026-07-15T04:59:00.000Z")),
      ).toBe(true);
      expect(
        isWithinQuietHours("Europe/Berlin", at("2026-07-15T05:00:00.000Z")),
      ).toBe(false);
    });

    it("stays quiet across midnight — the window is a union, not a range", () => {
      for (const localHour of [23, 0, 1, 2, 3, 4, 5, 6]) {
        const utcHour = (localHour - 2 + 24) % 24;
        expect(
          isWithinQuietHours(
            "Europe/Berlin",
            at(`2026-07-15T${String(utcHour).padStart(2, "0")}:30:00.000Z`),
          ),
        ).toBe(true);
      }
    });

    it("is loud through the middle of the day", () => {
      expect(
        isWithinQuietHours("Europe/Berlin", at("2026-07-15T12:00:00.000Z")),
      ).toBe(false);
    });
  });

  describe("daylight saving", () => {
    /**
     * The same UTC clock time, six months apart. Berlin is UTC+1 in January and
     * UTC+2 in July, so 05:30 UTC is 06:30 on one side of the year and 07:30 on
     * the other — and the verdict has to follow the clock the subscriber looks
     * at. An offset resolved once, or a fixed UTC window, gets exactly one of
     * these two wrong.
     */
    it("follows the wall clock rather than a fixed offset", () => {
      expect(
        isWithinQuietHours("Europe/Berlin", at("2026-01-15T05:30:00.000Z")),
      ).toBe(true);
      expect(
        isWithinQuietHours("Europe/Berlin", at("2026-07-15T05:30:00.000Z")),
      ).toBe(false);
    });

    /** The transition night itself: Berlin skips 02:00-03:00 on 2026-03-29. */
    it("stays quiet on both sides of the hour that does not exist", () => {
      // 01:30 CET, before the jump.
      expect(
        isWithinQuietHours("Europe/Berlin", at("2026-03-29T00:30:00.000Z")),
      ).toBe(true);
      // 03:30 CEST, after it. The wall clock skipped 02:30 entirely.
      expect(
        isWithinQuietHours("Europe/Berlin", at("2026-03-29T01:30:00.000Z")),
      ).toBe(true);
      // 07:30 CEST. Under the pre-jump offset this instant would read 06:30
      // and still be quiet.
      expect(
        isWithinQuietHours("Europe/Berlin", at("2026-03-29T05:30:00.000Z")),
      ).toBe(false);
    });
  });

  /**
   * The whole reason the column is the subscriber's rather than the park's. A
   * 21:00 block at Magic Kingdom is inside the park's opening hours and 03:00
   * for the person reading the plan in Berlin.
   */
  it("answers per subscriber, not per park, at one and the same instant", () => {
    const blockAt21InOrlando = at("2026-10-17T01:00:00.000Z");
    expect(isWithinQuietHours("America/New_York", blockAt21InOrlando)).toBe(
      false,
    );
    expect(isWithinQuietHours("Europe/Berlin", blockAt21InOrlando)).toBe(true);
  });

  describe("no usable zone means send", () => {
    it("sends when the column is null or empty", () => {
      const nowMs = at("2026-07-15T01:00:00.000Z"); // 03:00 in Berlin.
      expect(isWithinQuietHours(null, nowMs)).toBe(false);
      expect(isWithinQuietHours(undefined, nowMs)).toBe(false);
      expect(isWithinQuietHours("", nowMs)).toBe(false);
    });

    it("sends rather than throwing on a zone this build cannot resolve", () => {
      const nowMs = at("2026-07-15T01:00:00.000Z");
      expect(isWithinQuietHours("Mars/Olympus_Mons", nowMs)).toBe(false);
      expect(isWithinQuietHours("not a zone", nowMs)).toBe(false);
    });
  });
});
