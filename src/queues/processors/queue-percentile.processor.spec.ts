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
