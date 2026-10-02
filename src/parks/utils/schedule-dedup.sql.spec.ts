import { ScheduleType } from "../entities/schedule-entry.entity";
import {
  CONFLICT_TYPES,
  crossTypeConflictSql,
  sameTypeDuplicateSql,
} from "./schedule-dedup.sql";

/**
 * The statements are strings until a database sees them, so what a unit test can
 * pin is the key they group by. That is exactly the thing that was wrong: both
 * phases partitioned without the ride and therefore kept one row per park and
 * day — the opening hours or a single ride, whichever was written last.
 *
 * Each case below fails the moment `attractionId` leaves its partition.
 */
describe("schedule dedup statements", () => {
  /** `PARTITION BY …` up to the `ORDER BY` that ends it. */
  const partitionOf = (sql: string): string => {
    const match = sql.match(/PARTITION BY([\s\S]*?)ORDER BY/);
    if (!match) throw new Error("no PARTITION BY in statement");
    return match[1].replace(/\s+/g, " ").trim();
  };

  describe("phase 1 — same-type duplicates", () => {
    it.each(["global", "park"] as const)(
      "partitions %s scope by the ride as well as park, day and type",
      (scope) => {
        const columns = partitionOf(sameTypeDuplicateSql(scope));

        expect(columns).toContain(`"attractionId"`);
        expect(columns).toContain(`"parkId"`);
        expect(columns).toContain("date");
        expect(columns).toContain(`"scheduleType"`);
      },
    );

    it("binds the park id rather than interpolating it", () => {
      expect(sameTypeDuplicateSql("park")).toContain(`"parkId" = $1::uuid`);
      expect(sameTypeDuplicateSql("global")).not.toContain("$1");
    });
  });

  describe("phase 2 — cross-type conflicts", () => {
    it.each(["global", "park"] as const)(
      "partitions %s scope by the ride as well as park and day",
      (scope) => {
        const columns = partitionOf(crossTypeConflictSql(scope));

        expect(columns).toContain(`e."attractionId"`);
        expect(columns).toContain(`e."parkId"`);
        expect(columns).toContain("e.date");
      },
    );

    it.each(["global", "park"] as const)(
      "compares the nullable ride with IS NOT DISTINCT FROM in %s scope",
      (scope) => {
        const sql = crossTypeConflictSql(scope);

        // The pre-filter it replaced was a row-wise `("parkId", date) IN
        // (SELECT …)`. Adding the ride to that form would make it NULL for every
        // park-level row — the trap `migrateScheduleEntries` documents — so the
        // filter is an EXISTS and the ride is compared, never equated.
        expect(sql).toMatch(
          /other\."attractionId"\s+IS NOT DISTINCT FROM\s+e\."attractionId"/,
        );
        expect(sql).toMatch(/\bEXISTS \(/);
        expect(sql).not.toMatch(/\(\s*"parkId",\s*date\s*\)\s+IN/);
      },
    );

    it("keeps the four-step priority the docblock promises", () => {
      const sql = crossTypeConflictSql("global");

      expect(sql).toMatch(/'OPERATING' THEN 0/);
      expect(sql).toMatch(/'CLOSED' AND e\.description != 'Gap-filled' THEN 1/);
      expect(sql).toMatch(/'CLOSED' THEN 2/);
      expect(sql).toMatch(/'UNKNOWN' THEN 3/);
    });

    it.each(["global", "park"] as const)(
      "arbitrates only between OPERATING, CLOSED and UNKNOWN in %s scope",
      (scope) => {
        const sql = crossTypeConflictSql(scope);
        const typeList = `('OPERATING', 'CLOSED', 'UNKNOWN')`;

        // Both the rows that get ranked and the sibling that makes a day a
        // conflict are restricted to the three day-status types. Without the
        // first filter an event row is ranked and deleted; without the second
        // an OPERATING row next to an event row enters the ranking for nothing.
        expect(sql).toContain(`e."scheduleType" IN ${typeList}`);
        expect(sql).toContain(`other."scheduleType" IN ${typeList}`);
        expect([...CONFLICT_TYPES].sort()).toEqual(
          ["CLOSED", "OPERATING", "UNKNOWN"].sort(),
        );
      },
    );

    it.each([
      ScheduleType.TICKETED_EVENT,
      ScheduleType.PRIVATE_EVENT,
      ScheduleType.EXTRA_HOURS,
      ScheduleType.MAINTENANCE,
      ScheduleType.INFO,
    ])(
      "never ranks a %s row, so it survives next to an OPERATING row",
      (type) => {
        // PAR-276: Halloween Horror Nights is stored as TICKETED_EVENT on a day
        // that also has OPERATING opening hours. The old ranking put it at
        // `ELSE 4`, below UNKNOWN, and deleted it on every pass. A row can only
        // be deleted here if the CTE selects it, and the CTE selects only the
        // three day-status types.
        const sql = crossTypeConflictSql("global");

        expect(CONFLICT_TYPES).not.toContain(type);
        expect(sql).not.toContain(`'${type}'`);
        expect(sql).not.toMatch(/ELSE\s+\d/);
      },
    );

    it("binds the park id rather than interpolating it", () => {
      expect(crossTypeConflictSql("park")).toContain(`e."parkId" = $1::uuid`);
      expect(crossTypeConflictSql("global")).not.toContain("$1");
    });
  });

  it("builds both scopes from one derivation, so the key cannot drift", () => {
    // The two scopes differ by their park filter and by nothing else. Before
    // this file the global and the per-park statement were two copies, and a fix
    // to one would not have reached the other.
    const stripped = (sql: string) =>
      sql
        .replace(/\s*AND e\."parkId" = \$1::uuid/, "")
        .replace(/\s*WHERE "parkId" = \$1::uuid/, "")
        .replace(/\s+/g, " ")
        .trim();

    expect(stripped(sameTypeDuplicateSql("park"))).toBe(
      stripped(sameTypeDuplicateSql("global")),
    );
    expect(stripped(crossTypeConflictSql("park"))).toBe(
      stripped(crossTypeConflictSql("global")),
    );
  });
});
