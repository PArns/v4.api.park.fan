import {
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
        expect(sql).toContain("WHERE EXISTS");
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
