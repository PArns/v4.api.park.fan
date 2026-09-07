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
    expect(RECOVERY_CURVE_SQL_FOR_TEST).toContain(
      "o.end_reason IN ('recovered', 'reclassified')",
    );
  });
});
