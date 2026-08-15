import {
  isFreeFlowOpen,
  isInSeason,
  freeFlowQueues,
} from "./free-flow-status.util";

/**
 * Playgrounds, splash pads and climbing nets have no queue, so the upstream
 * feed calls them CLOSED all day. Three surfaces override that — the ride
 * list, the attraction detail and the favorites list — and for a while only
 * two of them did: Phantasialand's Mopti's Monkey Depot, Winni Splash and
 * Wavy Battle read CLOSED on the park page while the park was open and the
 * flag was set. The rule lives in one place now; these tests are what keeps
 * it honest.
 */
describe("free-flow status", () => {
  describe("isFreeFlowOpen", () => {
    it("opens a flagged attraction while its park operates", () => {
      expect(
        isFreeFlowOpen({ openWithPark: true, parkStatus: "OPERATING" }),
      ).toBe(true);
    });

    it("leaves an unflagged attraction alone", () => {
      // Every other ride in the park still gets its status from the feed.
      for (const openWithPark of [false, undefined, null]) {
        expect(isFreeFlowOpen({ openWithPark, parkStatus: "OPERATING" })).toBe(
          false,
        );
      }
    });

    it("closes with the park", () => {
      // "Open whenever the park is" cuts both ways — a playground in a closed
      // park is not walk-on, it is behind a gate.
      expect(isFreeFlowOpen({ openWithPark: true, parkStatus: "CLOSED" })).toBe(
        false,
      );
      expect(
        isFreeFlowOpen({ openWithPark: true, parkStatus: undefined }),
      ).toBe(false);
    });

    it("stays shut in a park whose wait times cannot be read", () => {
      // Hansa-Park rule: with no readable source nothing below the park may
      // claim to be running. This override must not undercut it.
      expect(
        isFreeFlowOpen({
          openWithPark: true,
          parkStatus: "OPERATING",
          waitTimesReadable: false,
        }),
      ).toBe(false);
    });

    it("treats readability as true when the caller does not say", () => {
      // The attraction detail and favorites paths handle unreadable parks
      // themselves and omit the field.
      expect(
        isFreeFlowOpen({ openWithPark: true, parkStatus: "OPERATING" }),
      ).toBe(true);
    });

    it("honours the season, so a summer water playground can carry the flag", () => {
      // Europa-Park is open in winter; its water playgrounds are not. Without
      // this gate those areas could not be flagged at all.
      const summerOnly = {
        openWithPark: true,
        parkStatus: "OPERATING",
        seasonMonths: [6, 7, 8],
        parkTimezone: "Europe/Berlin",
      };

      jest.useFakeTimers().setSystemTime(new Date("2026-07-15T10:00:00Z"));
      expect(isFreeFlowOpen(summerOnly)).toBe(true);

      jest.setSystemTime(new Date("2026-01-15T10:00:00Z"));
      expect(isFreeFlowOpen(summerOnly)).toBe(false);

      jest.useRealTimers();
    });
  });

  describe("isInSeason", () => {
    afterEach(() => jest.useRealTimers());

    it("places no restriction when the season is unknown", () => {
      // Most free-flow areas run all year and carry no months at all — they
      // must keep working. A missing fact never invents a restriction.
      expect(isInSeason(null, "Europe/Berlin")).toBe(true);
      expect(isInSeason(undefined, "Europe/Berlin")).toBe(true);
      expect(isInSeason([], "Europe/Berlin")).toBe(true);
    });

    it("reads months as 1-based, the way detect-seasonal writes them", () => {
      // The classic bug is a 0-based getMonth() in here: in August that would
      // match 8 against a stored 9.
      jest.useFakeTimers().setSystemTime(new Date("2026-08-15T12:00:00Z"));

      expect(isInSeason([8], "Europe/Berlin")).toBe(true);
      expect(isInSeason([7], "Europe/Berlin")).toBe(false);
      expect(isInSeason([9], "Europe/Berlin")).toBe(false);
    });

    it("asks the question in the park's timezone, not the server's", () => {
      // 2026-12-01 00:30 UTC is already December in Berlin but still
      // 30 November in Los Angeles. A season of [12] must therefore be open
      // for the Berlin park and shut for the LA one at the same instant —
      // this is the assertion a UTC implementation fails.
      jest.useFakeTimers().setSystemTime(new Date("2026-12-01T00:30:00Z"));

      expect(isInSeason([12], "Europe/Berlin")).toBe(true);
      expect(isInSeason([12], "America/Los_Angeles")).toBe(false);
      expect(isInSeason([11], "America/Los_Angeles")).toBe(true);
    });

    it("falls back to UTC rather than throwing on a missing timezone", () => {
      // Month granularity makes the fallback harmless, and closing a
      // playground over an absent column would not be.
      jest.useFakeTimers().setSystemTime(new Date("2026-08-15T12:00:00Z"));

      expect(isInSeason([8], null)).toBe(true);
      expect(isInSeason([8], undefined)).toBe(true);
    });
  });

  describe("freeFlowQueues", () => {
    it("reports a real zero-minute standby rather than an empty list", () => {
      // An empty `queues` is how this codebase says "no data". A playground
      // has data: you walk on, and that is zero minutes.
      const [q] = freeFlowQueues();

      expect(freeFlowQueues()).toHaveLength(1);
      expect(q.queueType).toBe("STANDBY");
      expect(q.status).toBe("OPERATING");
      expect(q.waitTime).toBe(0);
    });

    it("stamps a fresh timestamp so the row does not read as stale", () => {
      const before = Date.now();
      const [q] = freeFlowQueues();

      expect(new Date(q.lastUpdated).getTime()).toBeGreaterThanOrEqual(before);
    });
  });
});
