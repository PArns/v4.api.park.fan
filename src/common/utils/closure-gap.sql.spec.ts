import {
  CLOSURE_GAP_INTERVALS_SQL,
  CURRENT_CLOSURE_GAP_SQL,
  LIVE_LOOKBACK_HOURS,
  MAX_GAP_HOURS,
  MIN_GAP_MINUTES,
  MIN_PARK_MINUTES_LEFT,
} from "./closure-gap.sql";

/**
 * The two statements share a definition and must not drift apart.
 *
 * They already did once: the live `cycle` CTE claimed in its comment to
 * recognise "the same triple the nightly statement recognises" and did not — it
 * was missing both the 12-hour ceiling and the same-day check, so an ordinary
 * overnight close counted as a gap day. Nothing caught it, because SQL in a
 * template literal is a string to the compiler and the query's only caller
 * swallows failures to protect the page.
 *
 * These are shape assertions, not a parser. They cost nothing and they pin the
 * clauses that were actually lost.
 */
describe("closure-gap statements", () => {
  const both = [
    ["historical", CLOSURE_GAP_INTERVALS_SQL],
    ["live", CURRENT_CLOSURE_GAP_SQL],
  ] as const;

  it("the live statement recognises a gap as a same-park-local-day return", () => {
    // This used to run over both statements. It does not any more, and the
    // split is the point rather than an oversight: the nightly statement takes
    // its day from the window that contains the reading, this one still takes
    // the calendar date, and the assertion below is what keeps that a written
    // decision instead of a silent drift.
    //
    // A gap in a park closing after midnight is therefore filed in the history
    // and invisible on the live surface, and the two count different gap_days
    // against the same MAX_GAP_DAY_SHARE. PAR-129 carries the repair, which
    // needs an EXPLAIN ANALYZE against real data rather than an edit — the
    // whole plan of this statement rests on InitPlans and one materialised
    // CTE. When it lands, this test moves to the operating-day form and the
    // two go back to being asserted together.
    expect(CURRENT_CLOSURE_GAP_SQL).toMatch(
      /AT TIME ZONE [^)]+\)::date\s*\n?\s*=\s*\(/,
    );
    // And the divergence is named where a reader of the SQL will hit it,
    // not only here.
    expect(CURRENT_CLOSURE_GAP_SQL).toContain("PAR-129");
  });

  it("the nightly statement keys the return on the OPERATING day, not the calendar", () => {
    // A park that closes after midnight has its evening split across two
    // calendar dates: the 23:30 reading lands on one and the 00:30 reading on
    // the next, so a raw `::date` comparison fails and the gap is dropped. La
    // Ronde does that every day of its season — the case park_open in the live
    // statement already names, and the case park-open-window.sql §3 exists for.
    //
    // Asserted as three separate things because each can be reverted on its
    // own and only the combination is the fix.
    const edges = cteBody(CLOSURE_GAP_INTERVALS_SQL, "gap_edges");
    const gaps = cteBody(CLOSURE_GAP_INTERVALS_SQL, "raw_gaps");

    // 1. The day comes from the window that contains the instant, and the
    //    window comes from the shared builder rather than a second definition
    //    of "when is this park open".
    expect(CLOSURE_GAP_INTERVALS_SQL).toContain("win AS (");
    expect(edges).toMatch(/LEFT JOIN win wo\b/);
    expect(edges).toMatch(/LEFT JOIN win wb\b/);
    expect(edges).toContain("wo.op_day");
    expect(edges).toContain("wb.op_day");

    // 2. LEFT, with the calendar day as the fallback. An INNER join would
    //    narrow the population to gaps inside published hours, which is a
    //    different change from this one.
    expect(edges).toMatch(/COALESCE\(wo\.op_day,[\s\S]{0,60}?::date\)/);
    expect(edges).toMatch(/COALESCE\(wb\.op_day,[\s\S]{0,60}?::date\)/);

    // 3. And the same-day test compares those two days — never the raw casts
    //    again. This is the assertion that fails if someone "simplifies" the
    //    comparison back into raw_gaps.
    expect(gaps).toContain("start_op_day = end_op_day");
    expect(gaps).not.toMatch(/AT TIME ZONE [^)]+\)::date\s*\n?\s*=\s*\(/);
    // The emitted day is the window-derived one too: it leaves as "startOpDay"
    // and the processor keys outage starts into attraction_exposure_days with
    // it, whose op_day has always come from win.
    expect(gaps).toContain(
      "start_op_day                              AS op_day",
    );
  });

  it.each(both)("%s is structurally a statement, not a fragment", (_n, sql) => {
    // Reordering the CTEs left `),` in front of the final SELECT, and nothing
    // here caught it — the statement went to production as a syntax error that
    // `addClosureGaps` would have swallowed into a warning, which is the exact
    // failure this file's docblock describes. Two cheap invariants that a
    // mis-edit trips and the compiler cannot see.
    const bare = sql
      .replace(/--[^\n]*/g, (m) => " ".repeat(m.length))
      .replace(/'[^'\n]*'/g, (m) => " ".repeat(m.length));
    // The CTE list ends with a closing paren, never a comma.
    expect(bare).not.toMatch(/\),\s*SELECT\b/);
    // And every parenthesis is closed. Tracked in locals and asserted twice
    // rather than once per character: these statements are 20 KB and 10 KB, so
    // an expect() in the loop is ~30 000 expectation objects a run.
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

  it.each(both)("%s bounds how far a gap may reach", (_n, sql) => {
    expect(sql).toContain(`INTERVAL '${MAX_GAP_HOURS} hours'`);
  });

  it("the live lookback covers a whole operating day, not the gap ceiling", () => {
    // These wanted opposite things and shared a constant: the ceiling is kept
    // short to exclude an overnight reopening, the lookback must span the day
    // or the morning falls out of the window. 18 blind parks had 399 operating
    // days over 12 hours, the longest 24.
    expect(LIVE_LOOKBACK_HOURS).toBeGreaterThan(24);
    expect(CURRENT_CLOSURE_GAP_SQL).toContain(
      `INTERVAL '${LIVE_LOOKBACK_HOURS} hours'`,
    );
  });

  it("the live regime test is an EXISTS on the parameter, never a join", () => {
    // This is the whole performance fix, and it is invisible to the compiler.
    //
    // It used to be a CTE brought in with CROSS JOIN. A join is not a guard:
    // the planner has no reason to evaluate that arm first, so every historical
    // CTE ran to completion and the test was applied last — 6 s per call, 79 %
    // of the database, for parks that provably had no row to give. As an
    // uncorrelated EXISTS it becomes an InitPlan and PostgreSQL emits a
    // One-Time Filter that never demands the subtree: Europa-Park 939.8 ms to
    // 2.3 ms, 132 nodes never executed.
    //
    // Correlating it with anything from the rows above would silently undo
    // that, so the assertion pins both halves: it is an EXISTS, and it reads
    // nothing but $4.
    expect(CURRENT_CLOSURE_GAP_SQL).not.toContain("CROSS JOIN blind");
    const exists = CURRENT_CLOSURE_GAP_SQL.match(
      /EXISTS\s*\(\s*SELECT 1\s+FROM park_downtime_coverage c\s+WHERE([\s\S]*?)\)/,
    );
    expect(exists).not.toBeNull();
    const body = exists![1];
    expect(body).toContain('c."parkId" = $4::uuid');
    expect(body).toContain("c.regime = 'never_reports'");
    // Nothing from the outer query may leak in, or it stops being an InitPlan
    // and PostgreSQL stops emitting the One-Time Filter. Asserted as "only c.
    // and $4 appear", not as a list of today's aliases: an allowlist misses
    // every alias added later, and correlating through any of them brings the
    // 6-second statement back with a green suite.
    expect(body.replace(/c\.|\$4/g, "")).not.toMatch(/\b\w+\./);
  });

  it.each(both)("%s cannot divide a gap share by zero", (_n, sql) => {
    // `active_days` is COALESCEd to 0 in the nightly CTE and can be 0 in the
    // live one, and the day-floor test beside it is NOT a guard — SQL does not
    // promise to evaluate OR left to right. In the live statement one raised
    // error costs EVERY ride in the park its closure line, because
    // addClosureGaps catches and returns.
    expect(sql).toMatch(
      /gap_days,?\s*0?\)?\s*\n?\s*\/ NULLIF\(\s*\w*\.?active_days, 0\)/,
    );
  });

  it.each(both)("%s hoists the day close out of the row loop", (_n, sql) => {
    // The 21.5-s-of-24-s regression, and it is one edit away from returning.
    // early_end used to resolve the park's closing time through a LATERAL, so
    // every queue_data row it read carried its own schedule lookup: 28 485
    // executions of one bitmap index scan at Alton Towers, for a table with at
    // most 23 rows to offer. Neither statement has any business running a
    // correlated subquery per reading.
    expect(sql).not.toContain("JOIN LATERAL");
  });

  /**
   * One CTE's body, with `--` comments removed first.
   *
   * Two earlier attempts were vacuous in the same way. Slicing to the next
   * `"),\n"` landed on the terminator only because nothing happened to be
   * formatted that way; counting parentheses over the raw text breaks on a lone
   * `)` inside one of the prose comments these CTEs are wrapped in. Both
   * truncate the body silently, and a `not.toContain` on a truncated body
   * passes on text it never read — which is exactly the failure these
   * assertions exist to catch.
   */
  const cteBody = (sql: string, name: string): string => {
    // Comments AND string literals are blanked before the parentheses are
    // counted: a literal containing a bracket decrements the counter early, and
    // a truncated body makes every not.toContain below pass on text it never
    // read — the same vacuous-assertion failure this helper exists to end, one
    // level down. Blanked to the SAME LENGTH so offsets still address the
    // original, which is what gets returned: the assertions need the real text.
    const blank = (m: string) => " ".repeat(m.length);
    const bare = sql.replace(/--[^\n]*/g, blank).replace(/'[^'\n]*'/g, blank);
    const head = bare.includes(`${name} AS MATERIALIZED (`)
      ? `${name} AS MATERIALIZED (`
      : `${name} AS (`;
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

  it("every column the live cycle CTE reads is selected where it reads it", () => {
    // The same-day fix referenced `next_ts` in a subquery that did not select
    // it. `npm run build` is blind to that; production would have swallowed it.
    //
    // Unconditional, and scoped. The previous version guarded each assertion on
    // `cycle.includes("f.<col>")`, so renaming the alias disabled the whole test
    // in the one scenario it guards, and it searched the entire statement so a
    // column selected in any unrelated CTE satisfied it. Both halves are now
    // taken from the CTE bodies themselves.
    const cycle = cteBody(CURRENT_CLOSURE_GAP_SQL, "cycle");
    const readings = cteBody(CURRENT_CLOSURE_GAP_SQL, "run_readings");
    const cols = [
      ...cycle.matchAll(/\b[a-z]\.(prev_st|next_st|next_ts|st|ts|aid)\b/g),
    ];
    expect(cols.length).toBeGreaterThan(3);
    for (const [, col] of cols) {
      // `AS <col>` only. The previous version alternated with a bare `\\b<col>\\b`,
      // which subsumes it — so renaming the SELECT alias while leaving the name
      // anywhere in the CTE (a WHERE clause, a comment) kept the test green in
      // exactly the scenario it guards.
      expect(readings).toMatch(new RegExp(`\\bAS ${col}\\b`));
    }
  });

  it("run_readings is materialized, not left to the planner's inlining rule", () => {
    // Its entire purpose is reading the 21-day slice once instead of twice, and
    // that rested on "a CTE referenced more than once is materialized". Drop
    // either reference — early_end moving to parkOpenWindowCtes, say — and
    // PostgreSQL inlines it, both consumers push the six predicates into the
    // compressed hypertable again, and nothing fails except CPU.
    expect(CURRENT_CLOSURE_GAP_SQL).toContain("run_readings AS MATERIALIZED (");
  });

  it("the cheap per-ride gates run before the historical CTEs, not after", () => {
    // The same cheap-test-last shape as the two pseudoconstants, one level
    // down. Both are functions of s.started_at and po.closes_at alone, and in
    // the last hour of a blind park's day every ride winds down, enters
    // open_today, and has 21 days materialised a moment before
    // MIN_PARK_MINUTES_LEFT throws it away.
    const openToday = cteBody(CURRENT_CLOSURE_GAP_SQL, "open_today");
    expect(openToday).toContain(`INTERVAL '${MIN_PARK_MINUTES_LEFT} minutes'`);
    expect(openToday).toContain(`INTERVAL '${MIN_GAP_MINUTES} minutes'`);
  });

  it("the live statement resolves the park's day end in one timezone", () => {
    // park_open used to normalize with parks.timezone while park_day_close
    // normalizes with $2 — two sources for one park's day, in one statement,
    // reached through two different load paths in the callers.
    expect(CURRENT_CLOSURE_GAP_SQL).not.toContain("JOIN parks p ON p.id");
  });

  it("the live historical CTEs judge the rides in a closure, not the park", () => {
    // All three used to be handed $1 — the whole roster, which park_closers
    // needs and they do not. A 96-ride park scanned 21 days of queue_data 96
    // times over. Reverting any one of them is silent.
    //
    // The population is open_today, not run_start: the final select INNER JOINs
    // open_today, so a ride in run_start without it has its 21 days computed
    // and then discarded. In a park shut all day that is the whole roster
    // against nothing.
    for (const cte of ["run_readings", "active"]) {
      const body = cteBody(CURRENT_CLOSURE_GAP_SQL, cte);
      expect(body).toContain("ARRAY(SELECT aid FROM open_today)");
      expect(body).not.toContain("ANY($1::uuid[])");
    }
    // And the two that judge those readings share one scan of them rather than
    // repeating the same six predicates over the same hypertable slice.
    for (const cte of ["cycle", "early_end"]) {
      const body = cteBody(CURRENT_CLOSURE_GAP_SQL, cte);
      expect(body).toContain("run_readings");
      expect(body).not.toContain("FROM queue_data");
    }
  });

  it("the nightly denominator is restricted to the parks it serves", () => {
    // The third of the three defects in that CTE, and the only one with no
    // visible symptom: without the join it computes active_days for every
    // attraction in the database in order to use it for the blind ones. Every
    // assertion here stays green if it is deleted, the output is unchanged, and
    // the nightly job is simply slower.
    const body = cteBody(CLOSURE_GAP_INTERVALS_SQL, "active");
    expect(body).toContain("JOIN blind_parks");
    // Through the exposure table's own parkId, which is what its index covers,
    // and not through attractions — that join also drops exposure rows whose
    // attraction has since been merged away.
    expect(body).toContain('b.pid = e."parkId"');
    expect(body).not.toContain("JOIN attractions");
  });

  it.each(both)(
    "%s bounds the duty-cycle denominator on both sides",
    (_n, sql) => {
      // The nightly active CTE had a lower bound and no upper one, so a replay
      // counted every operating day from the window start to today — the leak
      // `reference_sql_rule_replay_windowing` already names. On an ordinary
      // incremental run the same omission made active_days 0–2 for every ride,
      // permanently under MIN_DAYS_FOR_CYCLE_TEST, so the filter never fired.
      const body = cteBody(sql, "active");
      expect(body).toMatch(/op_day\s*>=/);
      expect(body).toMatch(/op_day\s*<=/);
      // Park-local on both sides, because op_day is. A bare ::date takes the
      // session zone and disagrees for 56 of the 91 blind parks.
      expect(body).not.toMatch(/\$\d::timestamptz\)::date/);
      expect(body).toContain("AT TIME ZONE");
      // And no fixed offset in front of the numerator's edge. One day of slack
      // moved the nightly statement from 330 intervals over 112 rides to 370
      // over 120 — all dilution, in the direction that publishes a timetable
      // as a fault.
      expect(body).not.toMatch(/op_day\s*>=[^\n]*::date\s*-\s*\d/);
    },
  );

  it("both pseudoconstant gates are EXISTS, not joins", () => {
    // park_open is the same argument as the regime check and the costlier one:
    // a shut park is where run_start IS the roster, so the three historical
    // CTEs are at their most expensive exactly when the CROSS JOIN is about to
    // throw the result away. Alton Towers 586.4 ms to 10.2, Phantasialand
    // 707.6 to 3.8, both measured after closing time.
    expect(CURRENT_CLOSURE_GAP_SQL).toContain(
      "EXISTS (SELECT 1 FROM park_open)",
    );
    // Cheapest first: the regime probe is one primary-key lookup and rejects
    // the 122 parks that make 70 % of the calls, park_open is a schedule scan.
    // Both are pseudoconstants, so this only orders the InitPlans — but an
    // expensive test written in front of a cheap one is the shape this whole
    // change removes.
    expect(
      CURRENT_CLOSURE_GAP_SQL.indexOf("FROM park_downtime_coverage c"),
    ).toBeLessThan(
      CURRENT_CLOSURE_GAP_SQL.indexOf("EXISTS (SELECT 1 FROM park_open)"),
    );
  });

  it("the nightly statement honours the curated works period", () => {
    // The DOWN reconstruction excludes it and the guarantee carries no signal
    // qualifier — "inside it nothing is reported". A ride mid-rebuild cycles
    // OPERATING/CLOSED during testing, which is exactly the shape this
    // statement recognises, so without the exclusion it would fill the whole
    // declared period with inferred outages.
    expect(CLOSURE_GAP_INTERVALS_SQL).toContain("curated_out_of_service_from");
    expect(CLOSURE_GAP_INTERVALS_SQL).toContain("curated_out_of_service_to");
  });
});
