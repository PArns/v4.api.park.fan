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
    // It must clear the months too — a stale [1, 12] is what made Avoras read
    // as out of season in August.
    expect(reset).toMatch(/season_months\s*=\s*NULL/i);
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
