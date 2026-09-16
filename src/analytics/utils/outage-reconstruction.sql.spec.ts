import {
  OUTAGE_EXPOSURE_SQL,
  OUTAGE_INTERVALS_SQL,
} from "./outage-reconstruction.sql";

/**
 * The interval statement's day keying, pinned.
 *
 * `startOpDay` and the curated works-period filter ask the same question — which
 * operating day did this interval start on — and for a while they answered it
 * differently: the emitted column read the window, the filter read
 * `(started_at AT TIME ZONE tz)::date`. In a park that closes after midnight the
 * two differ by one day, and only for the rides an editor has declared out of
 * service, which is a population that was empty when the divergence was written.
 * Nothing rejects it: both expressions are valid SQL over the same row.
 *
 * These are shape assertions in the pattern of `closure-gap.sql.spec.ts`, and
 * the behavioural half lives in `test/e2e/outage-curated-window-operating-day`.
 * The split is deliberate — a regular expression cannot say whether Postgres
 * then returns the row, and a container run cannot say whether a later edit
 * reintroduced a second definition of the day somewhere the fixture misses.
 */
describe("outage reconstruction statements", () => {
  /**
   * The text of one CTE, comments and string literals blanked before the
   * parentheses are counted so a bracket inside either cannot truncate the body.
   * Blanked to the same length, so the slice still addresses the original text.
   */
  const cteBody = (sql: string, name: string): string => {
    const blank = (m: string) => " ".repeat(m.length);
    const bare = sql.replace(/--[^\n]*/g, blank).replace(/'[^'\n]*'/g, blank);
    const head = `${name} AS (`;
    const start = bare.indexOf(head);
    expect(start).toBeGreaterThan(-1);
    let depth = 0;
    for (let i = start + head.length - 1; i < bare.length; i++) {
      if (bare[i] === "(") depth++;
      else if (bare[i] === ")" && --depth === 0) {
        const body = sql.slice(start, i);
        // The extraction itself must not be vacuous.
        expect(body).toContain("FROM");
        expect(body.length).toBeGreaterThan(head.length + 60);
        return body;
      }
    }
    throw new Error(`unterminated CTE ${name}`);
  };

  /** Everything after the final `)` of the CTE list. */
  const finalSelect = (sql: string): string => {
    const idx = sql.indexOf("\nSELECT ");
    expect(idx).toBeGreaterThan(-1);
    return sql.slice(idx);
  };

  it("derives the interval's operating day once, in a CTE", () => {
    // A subquery in the SELECT list cannot be read by the WHERE beside it, so
    // the filter either repeats it or drifts away from it. It drifted. The
    // column is the fix, and these three assertions are the three ways back.
    const measured = cteBody(OUTAGE_INTERVALS_SQL, "measured");

    // 1. The day comes from the shared window builder, not a second notion of
    //    when the park is open.
    expect(OUTAGE_INTERVALS_SQL).toContain("win AS (");
    expect(measured).toMatch(/SELECT MIN\(w\.op_day\) FROM win w/);
    expect(measured).toMatch(/AS start_op_day\b/);

    // 2. It is computed exactly once. Two copies are two chances to drift, and
    //    the drift is invisible until a works window exists.
    const occurrences = OUTAGE_INTERVALS_SQL.match(/MIN\(w\.op_day\)/g) ?? [];
    expect(occurrences).toHaveLength(1);

    // 3. And the emitted column is that one, not a third cast.
    expect(finalSelect(OUTAGE_INTERVALS_SQL)).toMatch(
      /\bc\.start_op_day\s+AS "startOpDay"/,
    );
  });

  it("asks the curated works window about the operating day, not the calendar", () => {
    const tail = finalSelect(OUTAGE_INTERVALS_SQL);

    // The predicate is `attractionIsCuratedOutOfService("a", <dayExpr>)`, and
    // dayExpr appears twice in the emitted SQL — once per bound. The two
    // `IS NOT NULL` arms read the attraction's own columns and never the day,
    // so counting the comparisons counts the places the day is used.
    const bounds = tail.match(
      /(?:>=|<=)\s*a\.curated_out_of_service_(?:from|to)/g,
    );
    expect(bounds).toHaveLength(2);
    expect(tail).toMatch(/c\.start_op_day >= a\.curated_out_of_service_from/);
    expect(tail).toMatch(/c\.start_op_day <= a\.curated_out_of_service_to/);

    // And no raw calendar cast of started_at survives in the filter. This is
    // the assertion that fails if someone "simplifies" it back: the old form
    // reads the same and disagrees with the key the row is filed under
    // whenever the interval's window is not the one its start date names.
    expect(tail).not.toMatch(/started_at AT TIME ZONE [^)]*\)::date/);
  });

  it("keys exposure on the window's own day too, so the two sides join", () => {
    // `outageStarts` is keyed into attraction_exposure_days by startOpDay, so a
    // day the interval statement invents and the exposure statement never emits
    // is a start counted against a row that does not exist. Both sides have to
    // read `win.op_day`, and this pins the exposure half of that pair — the
    // interval half is pinned in the case above.
    const segWin = cteBody(OUTAGE_EXPOSURE_SQL, "seg_win");
    expect(segWin).toMatch(/JOIN win w\b/);
    expect(segWin).toContain("w.op_day");
    // Not a `::date` of the segment's own instant. That is the substitution
    // that reads identically and re-splits every after-midnight evening.
    expect(segWin).not.toMatch(/s\.ts[^\n]*\)::date/);
  });

  it.each([
    ["intervals", OUTAGE_INTERVALS_SQL],
    ["exposure", OUTAGE_EXPOSURE_SQL],
  ])("%s is structurally a statement, not a fragment", (_n, sql) => {
    const bare = sql
      .replace(/--[^\n]*/g, (m) => " ".repeat(m.length))
      .replace(/'[^'\n]*'/g, (m) => " ".repeat(m.length));
    // The CTE list ends with a closing paren, never a comma.
    expect(bare).not.toMatch(/\),\s*SELECT\b/);
    // Every parenthesis closed. Counted in locals and asserted twice rather
    // than once per character: these statements are kilobytes long.
    let depth = 0;
    let min = 0;
    for (const ch of bare) {
      if (ch === "(") depth++;
      else if (ch === ")") depth--;
      if (depth < min) min = depth;
    }
    expect(min).toBe(0);
    expect(depth).toBe(0);
  });
});
