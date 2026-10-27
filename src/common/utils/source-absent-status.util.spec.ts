import {
  attractionSourceIsSilent,
  isSourceAbsent,
  isSourceSilent,
  readsUnknownFromAbsentSource,
  readsUnknownFromSilentSource,
  RECONCILIATION_SOURCE,
  SOURCE_SILENT_DAYS,
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

/**
 * The silent half, measured on 2026-10-27: LEGOLAND Billund's feed answered for
 * all 37 of its attractions at 09:15 UTC carrying `lastUpdated = 2026-04-06
 * 13:54:00+00` on every one of them, identical to the second and 203 days old.
 * The park was open. Every ride read CLOSED as a statement of fact, and neither
 * `isSourceAbsent` (the rows carry a real `dataSource`) nor a gate on the
 * current row's feed age could separate that from a water park in October.
 */
describe("silent-source status", () => {
  const NOW = new Date("2026-10-27T09:00:00.000Z");
  const daysBefore = (days: number) =>
    new Date(NOW.getTime() - days * 24 * 3600 * 1000);
  const closed = { status: "CLOSED" };
  const operating = { status: "OPERATING" };
  const speaking = { sourceLastFreshAt: daysBefore(0.01) };
  const silent = { sourceLastFreshAt: daysBefore(203) };
  const facts = (over: Record<string, unknown> = {}) => ({
    sourceLastFreshAt: silent.sourceLastFreshAt,
    retiredAt: null,
    seasonOutSince: null,
    ...over,
  });

  describe("isSourceSilent", () => {
    it("fires once the last observation is past the threshold", () => {
      expect(isSourceSilent(daysBefore(SOURCE_SILENT_DAYS + 1), NOW)).toBe(
        true,
      );
      expect(isSourceSilent(daysBefore(203), NOW)).toBe(true);
    });

    it("leaves the threshold itself alone", () => {
      // Exactly 14 days is not "more than 14 days". The boundary is named here
      // so that moving the constant cannot move it silently.
      expect(isSourceSilent(daysBefore(SOURCE_SILENT_DAYS), NOW)).toBe(false);
      expect(isSourceSilent(daysBefore(SOURCE_SILENT_DAYS - 1), NOW)).toBe(
        false,
      );
    });

    it("reads null as never scanned, not as silence", () => {
      // This is the one that matters on deploy day: the column is null for
      // every row in the database until the job first runs, and the other
      // reading would put all 7,631 of them on UNKNOWN.
      expect(isSourceSilent(null, NOW)).toBe(false);
      expect(isSourceSilent(undefined, NOW)).toBe(false);
    });

    it("reads an unparseable stamp as never scanned too", () => {
      expect(isSourceSilent("not a date", NOW)).toBe(false);
    });

    it("accepts the ISO string a JSON round-trip leaves behind", () => {
      expect(isSourceSilent(daysBefore(203).toISOString(), NOW)).toBe(true);
    });
  });

  describe("readsUnknownFromSilentSource", () => {
    it("shows UNKNOWN for a frozen CLOSED in an open park", () => {
      expect(
        readsUnknownFromSilentSource([closed], facts(), "OPERATING", NOW),
      ).toBe(true);
    });

    it("stays quiet in a closed park", () => {
      expect(
        readsUnknownFromSilentSource([closed], facts(), "CLOSED", NOW),
      ).toBe(false);
      expect(
        readsUnknownFromSilentSource([closed], facts(), undefined, NOW),
      ).toBe(false);
    });

    it("leaves a frozen OPERATING to PAR-759", () => {
      // The wait-time side of the same column has its own ticket. Reaching it
      // here would turn a stale wait time into a shrug on the way past.
      expect(
        readsUnknownFromSilentSource([operating], facts(), "OPERATING", NOW),
      ).toBe(false);
      // Mixed rows count as running, same direction as isSourceAbsent.
      expect(
        readsUnknownFromSilentSource(
          [closed, operating],
          facts(),
          "OPERATING",
          NOW,
        ),
      ).toBe(false);
    });

    it("leaves the hard markers alone", () => {
      // 41 of the 97 frozen readings sorted by cause on 2026-10-24 were out of
      // service or out of season, and CLOSED was right for every one of them.
      expect(
        readsUnknownFromSilentSource(
          [closed],
          facts({ retiredAt: daysBefore(400) }),
          "OPERATING",
          NOW,
        ),
      ).toBe(false);
      expect(
        readsUnknownFromSilentSource(
          [closed],
          facts({ seasonOutSince: "2026-09-07" }),
          "OPERATING",
          NOW,
        ),
      ).toBe(false);
    });

    it("never fires for a ride the source is still measuring", () => {
      // The 50 evening-event readings of that same sort land here: a Halloween
      // maze measured at three in the afternoon carries a twelve-hour-old
      // stamp and a fresh observation from last night.
      expect(
        readsUnknownFromSilentSource(
          [closed],
          facts({ sourceLastFreshAt: speaking.sourceLastFreshAt }),
          "OPERATING",
          NOW,
        ),
      ).toBe(false);
    });
  });

  describe("attractionSourceIsSilent", () => {
    it("carries the same threshold as the TypeScript rule", () => {
      // The two halves decide the ride list and the counter beside it. A second
      // copy of the number is how they come to disagree.
      expect(attractionSourceIsSilent("a")).toContain(
        `INTERVAL '${SOURCE_SILENT_DAYS} days'`,
      );
    });

    it("binds to the alias it was given and rejects null", () => {
      const sql = attractionSourceIsSilent("att");
      expect(sql).toContain("att.source_last_fresh_at IS NOT NULL");
      expect(sql).toContain("att.source_last_fresh_at <");
      expect(sql).not.toContain("a.source_last_fresh_at");
    });
  });
});
