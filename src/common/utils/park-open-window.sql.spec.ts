import {
  normalizedClosingSql,
  overlapMinutesSql,
  parkOpenWindowCtes,
} from "./park-open-window.sql";
import { normalizeClosingTime } from "./operating-window.util";

/** Collapses the SQL's formatting so assertions can match on wording alone. */
const flat = (sql: string) => sql.replace(/\s+/g, " ");

/**
 * `normalizedClosingSql` is a hand-written twin of `normalizeClosingTime()`, and
 * the two halves are only useful while they agree. The TypeScript half runs on
 * the write path with no backfill, so the stored history is full of exactly the
 * cases it repairs and the SQL half is what a query over that history meets.
 *
 * Postgres is not available here, so the SQL is pinned by its branch structure
 * while the TypeScript half's behaviour is exercised directly. A reworded branch
 * fails the first group; a changed RULE fails the second, because the two are
 * asserted to describe the same three cases.
 */
describe("normalizedClosingSql / normalizeClosingTime", () => {
  const sql = flat(normalizedClosingSql("o", "c", "tz"));

  it("leaves a plausible window untouched", () => {
    expect(sql).toContain(
      "WHEN c >= o AND c <= o + INTERVAL '24 hours' THEN c",
    );

    const opens = new Date("2026-07-27T10:00:00Z");
    const closes = new Date("2026-07-27T18:00:00Z");
    expect(normalizeClosingTime(opens, closes, "Europe/Berlin")).toBe(closes);
  });

  it("leaves a zero-length window alone rather than inventing a day", () => {
    // Rolling it forward would manufacture a 24-hour operating day out of a
    // source that reported nothing. The SQL's first branch is `>=`, not `>`,
    // which is what covers this.
    expect(sql).toContain("c >= o");

    const same = new Date("2026-07-27T10:00:00Z");
    expect(normalizeClosingTime(same, same, "Europe/Berlin")).toBe(same);
  });

  it("re-anchors a close that precedes its open", () => {
    // ThemeParks.wiki stamps a past-midnight close with the day's own date, so
    // the close lands twelve hours BEFORE the open and the park reads CLOSED all
    // day. Parque Warner Madrid did, every day.
    const opens = new Date("2026-07-27T10:00:00Z"); // 12:00 Madrid
    const closes = new Date("2026-07-26T22:00:00Z"); // 00:00 Madrid, same day
    const fixed = normalizeClosingTime(opens, closes, "Europe/Madrid");
    expect(fixed.getTime()).toBeGreaterThan(opens.getTime());
    // 00:00 the following park-local morning.
    expect(fixed.toISOString()).toBe("2026-07-27T22:00:00.000Z");

    expect(sql).toContain(
      "ELSE ((((o AT TIME ZONE tz)::date + INTERVAL '1 day')",
    );
  });

  it("re-anchors a window longer than a day", () => {
    // An overshot day or a typo'd year: 34 hours (SeaWorld San Diego) and three
    // years (Busch Gardens Williamsburg) were both stored verbatim.
    const opens = new Date("2026-07-27T17:00:00Z");
    const closes = new Date("2029-07-28T03:00:00Z");
    const fixed = normalizeClosingTime(opens, closes, "America/New_York");
    const spanHours = (fixed.getTime() - opens.getTime()) / 3_600_000;
    expect(spanHours).toBeGreaterThan(0);
    expect(spanHours).toBeLessThanOrEqual(24);
  });

  it("rolls forward through the timezone rather than adding 24 hours", () => {
    // A DST shift inside that night would otherwise move the local closing time
    // by an hour. Both halves re-resolve the clock time on the next local date.
    expect(sql).not.toContain("INTERVAL '24 hours')");
    expect(sql).toContain(
      "::date + INTERVAL '1 day')::date + (c AT TIME ZONE tz)::time",
    );
  });
});

describe("parkOpenWindowCtes", () => {
  const sql = flat(parkOpenWindowCtes());

  it("reads park-level OPERATING rows only", () => {
    // Attraction-level schedule rows are never written, and EXTRA_HOURS /
    // TICKETED_EVENT / PRIVATE_EVENT are hours most visitors cannot attend —
    // excluded from the numerator and the denominator alike.
    expect(sql).toContain(`se."attractionId" IS NULL`);
    expect(sql).toContain(`se."scheduleType" = 'OPERATING'`);
  });

  it("flattens overlapping windows into a disjoint union", () => {
    // Two OPERATING rows on one day otherwise count every overlapping minute
    // twice AND split one outage in two at the seam.
    expect(sql).toContain("MAX(closes_at) OVER");
    expect(sql).toContain("ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING");
    expect(sql).toContain("opens_at > prev_max");
    expect(sql).toContain("GROUP BY park_id, tz, grp");
  });

  it("dates a window by its own opening, not by the instants inside it", () => {
    // A park closing at 02:00 would otherwise split its evening across two
    // operating days and lose it from both.
    expect(sql).toContain("(MIN(opens_at) AT TIME ZONE tz)::date AS op_day");
  });

  it("bounds on the opening, which no source misdates", () => {
    expect(sql).toContain(
      `se."openingTime" > $2::timestamptz - INTERVAL '2 days'`,
    );
    expect(sql).toContain(`se."openingTime" < $3::timestamptz`);
  });

  it("reads capability from configuration and can be asked to ignore it", () => {
    expect(sql).toContain("pk.wiki_entity_id IS NOT NULL");
    expect(flat(parkOpenWindowCtes({ wikiOnly: false }))).not.toContain(
      "wiki_entity_id",
    );
  });

  it("drops a window that stayed impossible after repair", () => {
    expect(sql).toContain("WHERE closes_at > opens_at");
  });
});

describe("overlapMinutesSql", () => {
  const sql = flat(overlapMinutesSql("s", "e", "wo", "wc"));

  it("clamps to the window and never goes negative", () => {
    expect(sql).toContain("LEAST(e, wc) - GREATEST(s, wo)");
    expect(sql).toContain("GREATEST( 0,");
  });

  it("returns minutes, not seconds", () => {
    expect(sql).toContain("/ 60.0");
  });
});
