import { attractionIsOutOfSeason } from "./season-window.sql";
import { LIVE_STATS_SQL } from "../../discovery/discovery.service";

/** Collapses the SQL's formatting so assertions can match on wording alone. */
const flat = (sql: string) => sql.replace(/\s+/g, " ");

/**
 * The predicate is a hand-written twin of `isCurrentlyInSeason(resolveCuratedFacts(a))`,
 * and a twin drifts. These pin the four decisions it makes; the full
 * agreement — all 300 combinations of the five columns, evaluated in
 * Postgres and against the TypeScript, zero disagreements — was checked
 * against a real server when the predicate was written, and belongs in a
 * database test rather than here.
 */
describe("attractionIsOutOfSeason", () => {
  const sql = flat(attractionIsOutOfSeason("a"));

  it("reads a curated month list ahead of the detector's", () => {
    expect(sql).toContain(
      "CASE WHEN jsonb_array_length(COALESCE(a.curated_season_months, '[]'::jsonb)) > 0 THEN a.curated_season_months ELSE a.season_months END",
    );
  });

  it("takes season_out_since out of play once a human has curated the season", () => {
    // A curated statement — either half of it — outranks the detector's note
    // about when the ride was last seen running.
    expect(sql).toContain(
      "NOT (a.curated_is_seasonal IS NOT NULL OR jsonb_array_length(COALESCE(a.curated_season_months, '[]'::jsonb)) > 0) AND a.season_out_since IS NOT NULL",
    );
  });

  it("asks the calendar whenever months are on file", () => {
    expect(sql).toContain("to_jsonb(EXTRACT(MONTH FROM NOW())::int)");
  });

  it("never closes a ride on `is_seasonal` alone", () => {
    // "Seasonal, and nothing else known" is unknown, not closed — it must not
    // hide a ride nobody has understood yet. Both branches of the month CASE
    // need a second fact before they can answer true.
    expect(sql).toContain("COALESCE(a.is_seasonal, FALSE)");
    expect(sql).toMatch(
      /ELSE NOT \(.*curated_is_seasonal IS NOT NULL.*\) AND a\.season_out_since IS NOT NULL/,
    );
  });

  it("applies the alias the caller binds the row to", () => {
    const other = flat(attractionIsOutOfSeason("whatever"));

    expect(other).toContain("whatever.season_months");
    expect(other).toContain("whatever.curated_is_seasonal");
    expect(other).not.toContain(" a.");
  });
});

/**
 * The counter is one sentence — "12 von 45 geöffnet" — rendered from two
 * different sources: the park page counts in TypeScript, the park cards read
 * this CTE. Phantasialand's ice rink is why they have to agree.
 */
describe("the live-stats query leaves out-of-season rides out of the counter", () => {
  const sql = flat(LIVE_STATS_SQL);

  it("drops them from the operating and closed counts", () => {
    expect(sql).toContain(
      "WHERE NOT lad.out_of_season OR lad.status = 'OPERATING'",
    );
  });

  it("drops them from the total as well", () => {
    // Counting a rink in the 45 but never in the 12 makes the park look
    // emptier than it is — which is the shape of the original bug.
    const total = sql.slice(
      sql.indexOf("(SELECT COUNT(*)::int FROM attractions a"),
      sql.indexOf("as total_attractions"),
    );

    expect(total).toContain("curated_season_months");
    expect(total).toContain("lad.status = 'OPERATING'");
  });
});
