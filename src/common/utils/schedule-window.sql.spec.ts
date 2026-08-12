import {
  DECISIVE_SCHEDULE_TYPES,
  SCHEDULE_TODAY_WINDOW_DAYS,
  scheduleRowSpeaksForToday,
} from "./schedule-window.sql";
import { OPEN_PARKS_CTES } from "../../analytics/utils/open-parks.sql";
import { LIVE_STATS_SQL } from "../../discovery/discovery.service";

/** Collapses the SQL's formatting so assertions can match on wording alone. */
const flat = (sql: string) => sql.replace(/\s+/g, " ");

describe("scheduleRowSpeaksForToday", () => {
  it("restricts to park-level rows", () => {
    expect(flat(scheduleRowSpeaksForToday("s"))).toContain(
      `s."attractionId" IS NULL`,
    );
  });

  it("treats OPERATING and CLOSED as statements about the day, UNKNOWN as none", () => {
    const predicate = flat(scheduleRowSpeaksForToday("s"));

    expect(predicate).toContain(`s."scheduleType" IN ('OPERATING', 'CLOSED')`);
    expect(predicate).not.toContain("UNKNOWN");
    expect(DECISIVE_SCHEDULE_TYPES).not.toContain("UNKNOWN");
  });

  it("narrows to the types the caller asks for", () => {
    expect(flat(scheduleRowSpeaksForToday("se", ["OPERATING"]))).toContain(
      `se."scheduleType" IN ('OPERATING')`,
    );
  });

  it("bounds the rows to a day either side of today, for every park timezone", () => {
    expect(flat(scheduleRowSpeaksForToday("s"))).toContain(
      `s.date BETWEEN CURRENT_DATE - ${SCHEDULE_TODAY_WINDOW_DAYS} AND CURRENT_DATE + ${SCHEDULE_TODAY_WINDOW_DAYS}`,
    );
    // UTC-11 is still on yesterday's date when UTC+14 is already on tomorrow's.
    expect(SCHEDULE_TODAY_WINDOW_DAYS).toBeGreaterThanOrEqual(1);
  });

  it("applies the alias the caller binds the row to", () => {
    const predicate = flat(scheduleRowSpeaksForToday("whatever"));

    expect(predicate).toContain(`whatever."scheduleType"`);
    expect(predicate).toContain(`whatever.date`);
    expect(predicate).not.toContain(" s.");
  });
});

/**
 * The bug these guard against: both queries decided "this park has a schedule, so
 * ignore its rides" from the park's entire schedule history. Once a park had ever
 * published hours the ride fallback became unreachable, so a park whose feed went
 * silent (Energylandia, last hours 2026-07-24) was reported closed while its rides
 * ran. The gate has to stay bounded to today in both.
 */
describe("the park-status queries gate the ride fallback on today", () => {
  const queries: [string, string][] = [
    ["discovery live stats", LIVE_STATS_SQL],
    ["analytics open parks", OPEN_PARKS_CTES],
  ];

  it.each(queries)("%s bounds its schedule gate to today", (_name, sql) => {
    expect(flat(sql)).toContain(
      `date BETWEEN CURRENT_DATE - ${SCHEDULE_TODAY_WINDOW_DAYS} AND CURRENT_DATE + ${SCHEDULE_TODAY_WINDOW_DAYS}`,
    );
  });

  it.each(queries)(
    "%s never tests OPERATING rows without a date bound",
    (_name, sql) => {
      // An `IN (...)`/`= 'OPERATING'` test whose clause carries no `date` is the
      // unbounded "has this park EVER published hours" check that caused the bug.
      // `park_schedules` is exempt: it bounds on openingTime/closingTime instead.
      const clauses = flat(sql)
        .split(/\bWHERE\b|\bAND NOT EXISTS\b/)
        .filter((clause) => clause.includes("scheduleType"));

      expect(clauses.length).toBeGreaterThan(0);
      for (const clause of clauses) {
        expect(clause).toMatch(/\bdate\b|openingTime/);
      }
    },
  );

  it.each(queries)("%s ignores attraction-level rows", (_name, sql) => {
    const clauses = flat(sql)
      .split(/\bWHERE\b|\bAND NOT EXISTS\b/)
      .filter((clause) => clause.includes("scheduleType"));

    for (const clause of clauses) {
      expect(clause).toContain(`"attractionId" IS NULL`);
    }
  });
});
