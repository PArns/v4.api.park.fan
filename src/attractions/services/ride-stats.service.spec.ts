import { Logger } from "@nestjs/common";
import { RideStatsService } from "./ride-stats.service";
import { IDS_PER_QUERY } from "../../external-apis/wikidata/wikidata.parser";

/**
 * The import joins Wikidata onto curated ride profiles by RCDB id. The tests
 * pin what ends up written, what stays unstamped, and how batches are cut.
 */
describe("RideStatsService", () => {
  let service: RideStatsService;

  const queryBuilder = {
    select: jest.fn().mockReturnThis(),
    from: jest.fn().mockReturnThis(),
    innerJoin: jest.fn().mockReturnThis(),
    where: jest.fn().mockReturnThis(),
    andWhere: jest.fn().mockReturnThis(),
    orderBy: jest.fn().mockReturnThis(),
    getRawMany: jest.fn(),
  };
  const mockRepo = {
    manager: { createQueryBuilder: jest.fn(() => queryBuilder) },
    update: jest.fn().mockResolvedValue(undefined),
  };
  const mockWikidata = { fetchRideStats: jest.fn() };

  const row = (n: number, parkId = "park-1") => ({
    attractionid: `attr-${n}`,
    parkid: parkId,
    rcdbid: String(1000 + n),
    slug: `ride-${n}`,
  });

  const stats = (entityId: string) => ({
    entityId,
    topSpeedKmh: 120,
    heightM: 60,
    lengthM: 1200,
    durationSeconds: 150,
  });

  beforeEach(() => {
    jest.clearAllMocks();
    jest.spyOn(Logger.prototype, "log").mockImplementation();
    service = new RideStatsService(mockRepo as never, mockWikidata as never);
  });

  afterEach(() => {
    jest.useRealTimers();
    jest.restoreAllMocks();
  });

  it("writes stats for a ride Wikidata knows and reports its park", async () => {
    queryBuilder.getRawMany.mockResolvedValue([row(1)]);
    mockWikidata.fetchRideStats.mockResolvedValue(
      new Map([["1001", stats("Q1")]]),
    );

    const result = await service.import();

    expect(mockWikidata.fetchRideStats).toHaveBeenCalledWith([1001]);
    expect(mockRepo.update).toHaveBeenCalledTimes(1);
    expect(mockRepo.update).toHaveBeenCalledWith("attr-1", {
      stats: {
        topSpeedKmh: 120,
        heightM: 60,
        lengthM: 1200,
        durationSeconds: 150,
        source: "wikidata",
        sourceId: "Q1",
      },
      statsUpdatedAt: expect.any(Date),
    });
    expect(result).toEqual({
      written: 1,
      withoutData: 0,
      touchedParks: [{ parkId: "park-1", attractionIds: ["attr-1"] }],
    });
  });

  it("leaves a ride without measurements unstamped", async () => {
    queryBuilder.getRawMany.mockResolvedValue([row(1), row(2)]);
    mockWikidata.fetchRideStats.mockResolvedValue(
      new Map([["1002", stats("Q2")]]),
    );

    const result = await service.import();

    expect(mockRepo.update).toHaveBeenCalledTimes(1);
    expect(mockRepo.update).toHaveBeenCalledWith("attr-2", expect.anything());
    expect(result.written).toBe(1);
    expect(result.withoutData).toBe(1);
  });

  it("groups touched rides by park", async () => {
    queryBuilder.getRawMany.mockResolvedValue([
      row(1, "park-a"),
      row(2, "park-b"),
      row(3, "park-a"),
    ]);
    mockWikidata.fetchRideStats.mockResolvedValue(
      new Map([
        ["1001", stats("Q1")],
        ["1002", stats("Q2")],
        ["1003", stats("Q3")],
      ]),
    );

    const result = await service.import();

    expect(result.touchedParks).toEqual([
      { parkId: "park-a", attractionIds: ["attr-1", "attr-3"] },
      { parkId: "park-b", attractionIds: ["attr-2"] },
    ]);
  });

  it("does nothing and asks Wikidata nothing when no ride is a candidate", async () => {
    queryBuilder.getRawMany.mockResolvedValue([]);

    const result = await service.import();

    expect(mockWikidata.fetchRideStats).not.toHaveBeenCalled();
    expect(mockRepo.update).not.toHaveBeenCalled();
    expect(result).toEqual({ written: 0, withoutData: 0, touchedParks: [] });
  });

  it("selects only rides with an RCDB id that are unstamped or older than 90 days", async () => {
    queryBuilder.getRawMany.mockResolvedValue([]);
    const before = Date.now();

    await service.import();

    expect(queryBuilder.where).toHaveBeenCalledWith(
      "attraction.rcdb_id IS NOT NULL",
    );
    const [clause, params] = queryBuilder.andWhere.mock.calls[0];
    expect(clause).toContain("stats_updated_at IS NULL");
    const cutoff = (params as { cutoff: Date }).cutoff.getTime();
    const ninetyDays = 90 * 24 * 60 * 60 * 1000;
    expect(cutoff).toBeGreaterThanOrEqual(before - ninetyDays);
    expect(cutoff).toBeLessThanOrEqual(Date.now() - ninetyDays);
  });

  it("caps a trial run at the first N candidates", async () => {
    queryBuilder.getRawMany.mockResolvedValue([row(1), row(2), row(3)]);
    mockWikidata.fetchRideStats.mockResolvedValue(new Map());

    const result = await service.import(2);

    expect(mockWikidata.fetchRideStats).toHaveBeenCalledWith([1001, 1002]);
    expect(result.withoutData).toBe(2);
  });

  it("cuts candidates into batches and pauses between them", async () => {
    jest.useFakeTimers();
    const rows = Array.from({ length: IDS_PER_QUERY + 1 }, (_, i) => row(i));
    queryBuilder.getRawMany.mockResolvedValue(rows);
    mockWikidata.fetchRideStats.mockResolvedValue(new Map());

    const pending = service.import();
    await jest.advanceTimersByTimeAsync(1_000);
    const result = await pending;

    expect(mockWikidata.fetchRideStats).toHaveBeenCalledTimes(2);
    expect(mockWikidata.fetchRideStats.mock.calls[0][0]).toHaveLength(
      IDS_PER_QUERY,
    );
    expect(mockWikidata.fetchRideStats.mock.calls[1][0]).toHaveLength(1);
    expect(result.withoutData).toBe(IDS_PER_QUERY + 1);
  });

  it("does not pause after a single batch", async () => {
    jest.useFakeTimers();
    queryBuilder.getRawMany.mockResolvedValue([row(1)]);
    mockWikidata.fetchRideStats.mockResolvedValue(new Map());

    await service.import();

    expect(jest.getTimerCount()).toBe(0);
  });

  it("passes a Wikidata failure on and keeps what was already written", async () => {
    jest.useFakeTimers();
    const rows = Array.from({ length: IDS_PER_QUERY + 1 }, (_, i) => row(i));
    queryBuilder.getRawMany.mockResolvedValue(rows);
    mockWikidata.fetchRideStats
      .mockResolvedValueOnce(new Map([["1000", stats("Q0")]]))
      .mockRejectedValueOnce(new Error("wikidata down"));

    const pending = service.import();
    const assertion = expect(pending).rejects.toThrow("wikidata down");
    await jest.advanceTimersByTimeAsync(1_000);
    await assertion;

    expect(mockRepo.update).toHaveBeenCalledTimes(1);
  });

  it("passes a database failure on", async () => {
    queryBuilder.getRawMany.mockRejectedValue(new Error("db down"));

    await expect(service.import()).rejects.toThrow("db down");
    expect(mockWikidata.fetchRideStats).not.toHaveBeenCalled();
  });
});
