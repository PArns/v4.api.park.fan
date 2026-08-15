import {
  isSourceAbsent,
  readsUnknownFromAbsentSource,
  RECONCILIATION_SOURCE,
} from "./source-absent-status.util";

const wiki = { dataSource: "themeparks-wiki" };
const recon = { dataSource: RECONCILIATION_SOURCE };

/**
 * On 2026-06-07 ThemeParks.wiki dropped 44 Europa-Park attractions and 18
 * Rulantica ones from its live feed — all of them rides with no Queue-Times
 * mapping to fall back on. Reverse-reconciliation dutifully wrote CLOSED for
 * each, and ten weeks later the park page still showed a Ball Pool and a
 * London Bus as closed, in August. That row says "we stopped receiving data",
 * and it was being served as "the operator closed this ride".
 */
describe("source-absent status", () => {
  describe("isSourceAbsent", () => {
    it("recognises readings that only our own bookkeeping produced", () => {
      expect(isSourceAbsent([recon])).toBe(true);
      expect(isSourceAbsent([recon, recon])).toBe(true);
    });

    it("leaves a ride alone while any real feed still reports it", () => {
      expect(isSourceAbsent([wiki])).toBe(false);
      expect(isSourceAbsent([{ dataSource: "queue-times" }])).toBe(false);
    });

    it("needs every row, not just the newest", () => {
      // A ride still publishing a real STANDBY queue is being observed,
      // whatever else sits beside it.
      expect(isSourceAbsent([recon, wiki])).toBe(false);
    });

    it("does not treat an empty list as absence", () => {
      // "No rows inside the freshness window" is a different question, and the
      // callers answer it on their own terms (the optimistic fallback).
      expect(isSourceAbsent([])).toBe(false);
    });

    it("does not mistake a missing source field for reconciliation", () => {
      expect(isSourceAbsent([{ dataSource: null }])).toBe(false);
      expect(isSourceAbsent([{}])).toBe(false);
    });
  });

  describe("readsUnknownFromAbsentSource", () => {
    it("shows UNKNOWN while the park is open", () => {
      expect(readsUnknownFromAbsentSource([recon], "OPERATING")).toBe(true);
    });

    it("stays quiet in a closed park", () => {
      // Everything is closed there for a reason we can actually state, and
      // UNKNOWN would replace a true answer with a shrug.
      expect(readsUnknownFromAbsentSource([recon], "CLOSED")).toBe(false);
      expect(readsUnknownFromAbsentSource([recon], undefined)).toBe(false);
    });

    it("never fires for a ride a real source is still reporting", () => {
      expect(readsUnknownFromAbsentSource([wiki], "OPERATING")).toBe(false);
    });
  });
});
