import { RECOVERY_CURVE_SQL_FOR_TEST } from "./downtime-recovery.service";

/**
 * The estimator must keep censored intervals in the risk set.
 *
 * This exists because a fix overshot and removed censoring entirely: filtering
 * on `duration_usable` looked like a quality gate, but that flag requires
 * `end_reason IN ('recovered','reclassified')` — the same predicate `observed`
 * uses — so every surviving row was observed and Kaplan-Meier degenerated into
 * the empirical distribution of the ones that came back. Verified against
 * production at the time: 23 748 rows admitted, all observed, 4964 censored
 * ones silently dropped.
 *
 * The bias runs one way. An outage still running when we measure is
 * disproportionately a long one, so dropping it makes recovery look faster than
 * it is — the failure the whole section of the doc is written to avoid.
 */
describe("recovery curve SQL", () => {
  it("does not filter on duration_usable", () => {
    expect(RECOVERY_CURVE_SQL_FOR_TEST).not.toMatch(/AND\s+o\.duration_usable/);
  });

  it("keeps the quality conditions that duration_usable also covers", () => {
    // These exclude intervals whose TIME is wrong — a works period, a spell
    // that began before we were looking, one that is mostly carried heartbeat.
    // Censoring cannot rescue those; it can only describe an unknown ending.
    for (const clause of [
      "NOT o.likely_works_period",
      "NOT o.start_censored",
      "o.observed_operating_minutes",
    ]) {
      expect(RECOVERY_CURVE_SQL_FOR_TEST).toContain(clause);
    }
  });

  it("still distinguishes an observed end from a censored one", () => {
    expect(RECOVERY_CURVE_SQL_FOR_TEST).toMatch(
      /o\.end_reason = 'recovered'\s*OR \(o\.signal = 'down' AND o\.end_reason = 'reclassified'\)\)\s*AS observed/,
    );
  });

  it("builds a curve for each signal and never one for both", () => {
    // closed_gap used to be filtered out here, while its stored intervals were
    // recovered by construction. Now that standing closures are kept and
    // censored, the curve is built for it -- but only ever beside the reported
    // one: a curve over both would answer an inferred closure from DOWN spells
    // with a different end definition and a different censoring regime.
    expect(RECOVERY_CURVE_SQL_FOR_TEST).not.toMatch(/o\.signal = 'down'\s*\n/);
    expect(RECOVERY_CURVE_SQL_FOR_TEST).toMatch(
      /PARTITION BY park_id, signal ORDER BY mins DESC/,
    );
    expect(RECOVERY_CURVE_SQL_FOR_TEST).toMatch(
      /PARTITION BY park_id, signal\s*ORDER BY mins\s/,
    );
    expect(RECOVERY_CURVE_SQL_FOR_TEST).toContain(
      "GROUP BY park_id, signal, mins",
    );
  });

  it("a closure that turned into REFURBISHMENT is censored, not recovered", () => {
    // For a reported DOWN, REFURBISHMENT is planned work replacing a fault and
    // counts as the fault ending. For a closure it is the ride still not
    // running, and counting it as an event would shorten the curve.
    const observed = RECOVERY_CURVE_SQL_FOR_TEST.match(
      /\(o\.end_reason[\s\S]*?\) AS observed/,
    )?.[0];
    expect(observed).toBeDefined();
    expect(observed).not.toMatch(
      /end_reason IN \('recovered', 'reclassified'\)/,
    );
    expect(observed).toContain(
      "o.signal = 'down' AND o.end_reason = 'reclassified'",
    );
  });
});
