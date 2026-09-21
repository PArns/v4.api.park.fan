import { RECLASSIFIED_UPSTREAM_REASONS } from "../../attractions/services/attraction-retirement.service";
import { HEARTBEAT_SOURCE } from "../../common/utils/outage-rows.sql";
import { RECONCILIATION_SOURCE } from "../../common/utils/source-absent-status.util";
import { QueuePercentileProcessor } from "./queue-percentile.processor";

/**
 * Guards the correctness fix for queue_data_aggregates: the row id must be a
 * DETERMINISTIC hash of (attractionId, hour) so the (id, hour) PK dedupes and
 * ON CONFLICT actually fires. A regression back to gen_random_uuid() would
 * silently re-introduce duplicate rows that skew every percentile read.
 */
describe("QueuePercentileProcessor — deterministic aggregate id", () => {
  const buildProcessor = (query: jest.Mock) =>
    new QueuePercentileProcessor(
      { query } as never, // aggregateRepository
      {} as never, // attractionRepository (unused by these handlers)
      {} as never, // showRepository (unused)
      {} as never, // dataSource (unused by these handlers)
    );

  it("calculate-percentiles derives id from md5(attractionId|hour), not gen_random_uuid", async () => {
    const query = jest.fn().mockResolvedValue([{ count: 0 }]);
    await buildProcessor(query).handleCalculatePercentiles({} as never);

    expect(query).toHaveBeenCalledTimes(1);
    const sql = query.mock.calls[0][0] as string;
    expect(sql).toMatch(/md5\(\s*qd\."attractionId"/);
    // The id column itself must NOT be a random uuid (the explanatory comment
    // mentions gen_random_uuid, so assert on the actual `... as id` usage).
    expect(sql).not.toContain("gen_random_uuid() as id");
    expect(sql).toContain("ON CONFLICT (id, hour)");
  });

  it("backfill-percentiles uses the same deterministic id", async () => {
    const query = jest.fn().mockResolvedValue([{ count: 0 }]);
    // 1-day window → a single batch, so exactly one INSERT query is issued.
    await buildProcessor(query).handleBackfillPercentiles({
      data: { days: 1 },
    } as never);

    expect(query).toHaveBeenCalled();
    const sql = query.mock.calls[0][0] as string;
    expect(sql).toMatch(/md5\(\s*qd\."attractionId"/);
    expect(sql).not.toContain("gen_random_uuid() as id");
  });
});

describe("QueuePercentileProcessor — dedupe-percentile-aggregates", () => {
  const buildProcessor = (query: jest.Mock) =>
    new QueuePercentileProcessor(
      { query } as never,
      {} as never,
      {} as never,
      {} as never,
    );

  it("is a no-op when there are no duplicate buckets (idempotent)", async () => {
    const query = jest.fn().mockResolvedValueOnce([{ groups: 0 }]);

    await buildProcessor(query).handleDedupePercentileAggregates({} as never);

    // Only the duplicate-count probe runs; no DELETE is issued.
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0][0]).toContain("HAVING count(*) > 1");
  });

  it("collapses duplicates to one row when buckets are duplicated", async () => {
    const query = jest
      .fn()
      .mockResolvedValueOnce([{ groups: 3 }]) // probe finds dupes
      .mockResolvedValueOnce([]); // delete

    await buildProcessor(query).handleDedupePercentileAggregates({} as never);

    expect(query).toHaveBeenCalledTimes(2);
    const deleteSql = query.mock.calls[1][0] as string;
    expect(deleteSql).toContain("DELETE FROM queue_data_aggregates");
    expect(deleteSql).toContain("row_number()");
    expect(deleteSql).toContain("rn > 1");
  });
});

/**
 * Free-flow attractions (playgrounds, splash pads, climbing structures) have no
 * queue, so their feed says CLOSED every day the park is open — which is this
 * detector's exact signature for "seasonal". Three of Phantasialand's four were
 * marked seasonal with no months, and Avoras, a year-round climbing course the
 * park advertises as open "ganzjährig", read as a winter attraction.
 *
 * Step 2's reset keys off queue_data ever saying OPERATING, which a playground
 * never does, so a mislabel is permanent without the explicit clear.
 */
describe("QueuePercentileProcessor — detect-seasonal skips free-flow", () => {
  const runDetectSeasonal = async () => {
    const query = jest.fn().mockResolvedValue([]);
    const processor = new QueuePercentileProcessor(
      {} as never,
      {} as never,
      {} as never,
      { query } as never, // dataSource
    );
    await processor.handleDetectSeasonal({} as never);
    return query.mock.calls.map((c) => c[0] as string);
  };

  it("clears the flag on attractions that are open with the park", async () => {
    const statements = await runDetectSeasonal();

    const reset = statements.find(
      (sql) =>
        /UPDATE attractions/i.test(sql) &&
        /is_seasonal\s*=\s*false/i.test(sql) &&
        /open_with_park/i.test(sql),
    );

    expect(reset).toBeDefined();
    // ...but ONLY where there are no months. Months on a free-flow row are
    // human-written — a curated season such as Europa-Park's summer-only water
    // playgrounds — and this job does not own them. Clearing them here would
    // undo the curation on the next nightly run.
    expect(reset).toMatch(/season_months IS NULL/i);
    expect(reset).not.toMatch(/SET[\s\S]*season_months\s*=/i);
  });

  it("never resets a free-flow row that has started reporting OPERATING", () => {
    const query = jest.fn().mockResolvedValue([{ attractionId: "a1" }]);
    const processor = new QueuePercentileProcessor(
      {} as never,
      {} as never,
      {} as never,
      { query } as never,
    );

    return processor.handleDetectSeasonal({} as never).then(() => {
      const statements = query.mock.calls.map((c) => c[0] as string);
      // Avoras emitted 154 OPERATING records while still being free-flow, so
      // the recently-operating reset would otherwise wipe a curated season the
      // first time one showed up in the feed.
      const recentReset = statements.find(
        (sql) => /UPDATE attractions/i.test(sql) && /id = ANY\(\$1\)/.test(sql),
      );

      expect(recentReset).toBeDefined();
      expect(recentReset).toMatch(/NOT open_with_park/);
    });
  });

  /**
   * The zero-history query is a chain of CTEs that lost its leading WITH in the
   * 2026-06-03 perf refactor (9046535). Postgres rejected it with a syntax
   * error, the handler threw before writing anything, and detect-seasonal
   * silently did nothing for 73 days — including the step-3 candidates it had
   * already computed. A build passes either way, because it is a string.
   */
  it("issues syntactically framed CTE chains (regression: missing WITH)", async () => {
    const statements = await runDetectSeasonal();

    for (const sql of statements) {
      if (!/^\s*(--[^\n]*\n\s*)*\w+ AS \(/m.test(sql)) continue;
      // A statement whose first non-comment token opens a CTE must say WITH.
      const firstToken = sql
        .split("\n")
        .map((l) => l.trim())
        .filter((l) => l.length > 0 && !l.startsWith("--"))[0];
      expect(firstToken).toMatch(/^(WITH|SELECT|UPDATE|INSERT|DELETE|SET)\b/i);
    }
  });

  it("clears the flag on retired attractions, which no reset path can reach", async () => {
    const statements = await runDetectSeasonal();

    // A demolished ride never reports OPERATING, so the recently-operating
    // reset can never clear it, and excluding it from the candidate searches
    // only stops it being marked again.
    const reset = statements.find(
      (sql) =>
        /UPDATE attractions/i.test(sql) && /retired_at IS NOT NULL/i.test(sql),
    );

    expect(reset).toBeDefined();
    expect(reset).toMatch(/is_seasonal = false/i);
    expect(reset).toMatch(/season_months = NULL/i);
  });

  /**
   * That reset rests on retirement being final, and one kind is not: the
   * children sync retires a row whose entity ThemeParks.wiki reclassified as a
   * show, and lifts it again if the wiki changes its mind. Clearing the season
   * of such a row loses it for good — the row receives nothing but
   * `system-reconciliation` rows while retired, so this detector can never
   * re-derive what it erased.
   */
  it("spares the retirement the children sync can undo", async () => {
    const query = jest.fn().mockResolvedValue([]);
    const processor = new QueuePercentileProcessor(
      {} as never,
      {} as never,
      {} as never,
      { query } as never,
    );
    await processor.handleDetectSeasonal({} as never);

    const call = query.mock.calls.find(
      ([sql]) =>
        /UPDATE attractions/i.test(sql as string) &&
        /retired_at IS NOT NULL/i.test(sql as string),
    );

    expect(call?.[0]).toMatch(/retired_reason <> ALL/i);
    // And the bind itself, because the SQL text alone would stay green while
    // Postgres rejects the statement and takes the whole run down with it —
    // step 2c has no try/catch and runs before the candidate searches.
    expect(call?.[1]).toEqual([RECLASSIFIED_UPSTREAM_REASONS]);
  });

  it("excludes them from both candidate searches", async () => {
    const statements = await runDetectSeasonal();

    // The history-based search (step 3) and the zero-history search (step 3b).
    const candidateQueries = statements.filter((sql) =>
      /days_fully_closed|never_operating/.test(sql),
    );

    expect(candidateQueries.length).toBeGreaterThanOrEqual(2);
    for (const sql of candidateQueries) {
      expect(sql).toMatch(/NOT a\.open_with_park/);
    }
  });
});

/**
 * A ride whose feed went quiet keeps getting rows: reverse-reconciliation
 * writes CLOSED for anything no source has mentioned in 24 hours, and the
 * hourly heartbeat carries the previous status AND its `data_source` forward.
 * To this detector that is the exact signature of a season — CLOSED on every
 * park-open day while the park was open — so it marked half of Europa-Park
 * seasonal on rows nobody outside this system wrote.
 *
 * The evidence queries must therefore read observed rows only. `current_status`
 * is the decisive one: filtered, a feed-dropped ride has no row in the last
 * seven days at all, and the INNER JOIN onto that CTE drops it from the
 * candidates.
 */
describe("QueuePercentileProcessor — bookkeeping rows are not feed evidence", () => {
  const runDetectSeasonal = async () => {
    const query = jest.fn().mockResolvedValue([{ attractionId: "a1" }]);
    const processor = new QueuePercentileProcessor(
      {} as never,
      {} as never,
      {} as never,
      { query } as never,
    );
    await processor.handleDetectSeasonal({} as never);
    return query.mock.calls.map((c) => c[0] as string);
  };

  /** Both writers' own exported names, so a rename cannot pass this silently. */
  const excludesBothWriters = (sql: string) => {
    expect(sql).toContain(RECONCILIATION_SOURCE);
    expect(sql).toContain(HEARTBEAT_SOURCE);
    // The carried half. A heartbeat copies the previous row's data_source, so
    // the source list alone does not see it.
    expect(sql).toMatch(/is_heartbeat/);
  };

  it("reads current status from observed rows, so a feed-dropped ride is no candidate", async () => {
    const statements = await runDetectSeasonal();

    const withCurrentStatus = statements.filter((sql) =>
      /current_status AS \(/.test(sql),
    );
    // The history-based search (step 3) and the zero-history search (step 3b).
    expect(withCurrentStatus).toHaveLength(2);

    for (const sql of withCurrentStatus) {
      const cte = sql.slice(
        sql.indexOf("current_status AS ("),
        sql.indexOf('ORDER BY "attractionId", timestamp DESC'),
      );
      excludesBothWriters(cte);
    }
  });

  it("counts a ride's operating history and watched span from observed rows only", async () => {
    const statements = await runDetectSeasonal();

    // `ever_operating` gates on >=20 OPERATING rows; carried heartbeats repeat
    // an OPERATING the feed said hours ago and would clear it for free.
    const candidateSql = statements.find((sql) =>
      /ever_operating AS \(/.test(sql),
    );
    expect(candidateSql).toBeDefined();
    excludesBothWriters(candidateSql as string);

    // The watched span decides whether months may be derived at all. Left
    // unfiltered it grows on reconciliation alone, so a ride clears the
    // 330-day gate on the strength of its own silence.
    const monthSql = statements.find((sql) =>
      /ARRAY_AGG\(DISTINCT EXTRACT\(MONTH FROM q\.timestamp/.test(sql),
    );
    expect(monthSql).toBeDefined();
    excludesBothWriters(monthSql as string);
  });

  it("builds the operating-day rollup from observed rows", async () => {
    const statements = await runDetectSeasonal();

    // Read twice with opposite meanings — "the park was open" and "this ride
    // ran" — and a third time by the show search, which has no queue_data of
    // its own and takes its park-open days from here.
    const rollup = statements.find((sql) =>
      /INSERT INTO attraction_day_operating/.test(sql),
    );
    expect(rollup).toBeDefined();
    excludesBothWriters(rollup as string);
  });

  it("keeps an unclassifiable row counting as an observation", async () => {
    const statements = await runDetectSeasonal();

    // The predicate is NULL, not false, for a row carrying neither
    // is_heartbeat nor lastUpdated. Dropped instead of kept, such a row would
    // remove an operating day from the rollup and hand the ride a fully-closed
    // one — the direction that invents a season. Production holds no such row;
    // a fixture does.
    for (const sql of statements.filter((s) => s.includes("is_heartbeat"))) {
      expect(sql).toMatch(/COALESCE\([\s\S]*?is_heartbeat[\s\S]*?,\s*true\)/);
    }
  });
});

/**
 * Our whole queue_data history was 234 days deep on 2026-08-15, so no entity
 * anywhere had been watched through a full year — and every stored month list
 * was a contiguous run anchored at the start of that window. `[1,2,3,4,12]`
 * (69 attractions), `[1,2,3,4,5,6,12]` (64), `[1,12]` (18): not seasons, just
 * "the span in which we happened to see it open". Europa-Park's 44 feed-dropped
 * rides all carried the same list and read as out of season in August.
 */
describe("QueuePercentileProcessor — no season without a year of watching", () => {
  const runDetectSeasonal = async () => {
    const query = jest.fn().mockResolvedValue([{ attractionId: "a1" }]);
    const processor = new QueuePercentileProcessor(
      {} as never,
      {} as never,
      {} as never,
      { query } as never,
    );
    await processor.handleDetectSeasonal({} as never);
    return query.mock.calls;
  };

  it("derives months only for entities watched long enough — attractions and shows alike", async () => {
    const calls = await runDetectSeasonal();
    const monthQueries = calls.filter(([sql]) =>
      /ARRAY_AGG\(DISTINCT EXTRACT\(MONTH/.test(sql as string),
    );

    // One for attractions, one for shows — the shows table was marked from the
    // same 234-day window and is just as wrong.
    expect(monthQueries).toHaveLength(2);

    for (const [sql, params] of monthQueries) {
      // Watched-span measured over ALL rows: a real winter attraction only
      // operates for ~40 days, which says nothing about how long we watched.
      expect(sql as string).toMatch(/max\(timestamp\) - min\(timestamp\)/);
      expect(sql as string).toMatch(/o\.watched >= /);
      expect(params as unknown[]).toContain(330);
    }
  });
});
