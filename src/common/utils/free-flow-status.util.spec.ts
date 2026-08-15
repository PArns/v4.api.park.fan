import { isFreeFlowOpen, freeFlowQueues } from "./free-flow-status.util";

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
      expect(isFreeFlowOpen(true, "OPERATING")).toBe(true);
    });

    it("leaves an unflagged attraction alone", () => {
      // Every other ride in the park still gets its status from the feed.
      expect(isFreeFlowOpen(false, "OPERATING")).toBe(false);
      expect(isFreeFlowOpen(undefined, "OPERATING")).toBe(false);
      expect(isFreeFlowOpen(null, "OPERATING")).toBe(false);
    });

    it("closes with the park", () => {
      // "Open whenever the park is" cuts both ways — a playground in a closed
      // park is not walk-on, it is behind a gate.
      expect(isFreeFlowOpen(true, "CLOSED")).toBe(false);
      expect(isFreeFlowOpen(true, undefined)).toBe(false);
    });

    it("stays shut in a park whose wait times cannot be read", () => {
      // Hansa-Park rule: with no readable source nothing below the park may
      // claim to be running. This override must not undercut it.
      expect(isFreeFlowOpen(true, "OPERATING", false)).toBe(false);
    });

    it("treats readability as true when the caller does not say", () => {
      // The attraction detail and favorites paths handle unreadable parks
      // themselves and call this without the third argument.
      expect(isFreeFlowOpen(true, "OPERATING")).toBe(true);
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
