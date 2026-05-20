import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { StatsService } from "./stats.service";
import { ParkDailyStats } from "./entities/park-daily-stats.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { Park } from "../parks/entities/park.entity";

/**
 * Coverage for StatsService — drives `park_daily_stats`, which feeds the
 * /v1/analytics/realtime stats and ML training labels. The most
 * important guarantees here are:
 *   1. Daily P50/P90/max are computed via the documented percentile
 *      algorithm (not arithmetic mean, not pooled-PERCENTILE_CONT).
 *   2. The outlier cap on `max` actually fires — one bad row (e.g.
 *      waitTime=450 because of a bad upstream feed) must NOT poison
 *      the stats row.
 *   3. Days with no data still get a record so we don't re-scan
 *      queue_data on every endpoint hit.
 */
describe("StatsService", () => {
  let service: StatsService;

  const statsRepo = {
    findOne: jest.fn(),
    save: jest.fn(),
    create: jest.fn((entity) => entity),
    find: jest.fn(),
  };

  // Single shared QB instance so per-test `getMany.mockResolvedValue(...)`
  // is the one the service actually sees on its next call.
  const queueDataQB = {
    select: jest.fn().mockReturnThis(),
    innerJoin: jest.fn().mockReturnThis(),
    where: jest.fn().mockReturnThis(),
    andWhere: jest.fn().mockReturnThis(),
    getMany: jest.fn().mockResolvedValue([]),
  };
  const queueDataRepo = {
    createQueryBuilder: jest.fn(() => queueDataQB),
  };

  const parkRepo = {
    findOne: jest.fn(),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        StatsService,
        { provide: getRepositoryToken(ParkDailyStats), useValue: statsRepo },
        { provide: getRepositoryToken(QueueData), useValue: queueDataRepo },
        { provide: getRepositoryToken(Park), useValue: parkRepo },
      ],
    }).compile();

    service = module.get(StatsService);
  });

  /**
   * Build a queue_data array shaped like the createQueryBuilder().getMany()
   * return. Timestamps in park-local-day-noon UTC so the date filter
   * keeps them on the requested day.
   */
  const queueRows = (
    parkTz: string,
    waitTimes: number[],
    date = "2026-05-18",
  ) => {
    // Noon in the park's local timezone is always inside that day,
    // regardless of UTC offset → matches the filter regardless of tz.
    const noon = new Date(`${date}T12:00:00Z`);
    return waitTimes.map((w, i) => ({
      waitTime: w,
      timestamp: new Date(noon.getTime() + i * 60_000),
    }));
  };

  describe("calculateAndStoreDailyStats", () => {
    it("computes P50/P90/max and writes them via upsert", async () => {
      parkRepo.findOne.mockResolvedValue({ id: "p1", timezone: "UTC" });
      queueDataQB.getMany.mockResolvedValue(
        queueRows("UTC", [10, 20, 30, 40, 50]),
      );
      statsRepo.findOne.mockResolvedValue(null); // create-new path
      statsRepo.save.mockImplementation((entity) =>
        Promise.resolve({ id: "stats-1", ...entity }),
      );

      const result = await service.calculateAndStoreDailyStats(
        "p1",
        "2026-05-18",
      );

      expect(result).not.toBeNull();
      // For [10,20,30,40,50]:
      //   P50 via Math.ceil(5 * 0.5) - 1 = 2 → sorted[2] = 30
      //   P90 via Math.ceil(5 * 0.9) - 1 = 4 → sorted[4] = 50
      expect(result!.p50WaitTime).toBe(30);
      expect(result!.p90WaitTime).toBe(50);
    });

    it("caps the max at 3× P90 when there are enough samples (≥20)", async () => {
      parkRepo.findOne.mockResolvedValue({ id: "p1", timezone: "UTC" });
      statsRepo.findOne.mockResolvedValue(null);
      statsRepo.save.mockImplementation((e) => Promise.resolve(e));

      // 50 sane samples (10..59) + 1 outlier (9999). Median ≈ 35, P90
      // ≈ 55. Cap = max(120, 55*3) = 165. So max = min(9999, 165) = 165.
      queueDataQB.getMany.mockResolvedValue(
        queueRows(
          "UTC",
          [...Array(50).keys()].map((i) => 10 + i).concat([9999]),
        ),
      );
      const result = await service.calculateAndStoreDailyStats(
        "p1",
        "2026-05-18",
      );

      expect(result!.maxWaitTime).toBeLessThanOrEqual(165);
      expect(result!.maxWaitTime).toBeLessThan(9999);
    });

    it("uses the 5× P50 cap for small samples (<20) where P90≈max", async () => {
      // For a small Saisonpark with only 8 samples on a quiet day, the
      // P90-based cap would collapse onto the outlier itself (P90 = max
      // by nearest-rank rule). The implementation switches to a P50-
      // based cap below 20 samples to give a stable anchor.
      parkRepo.findOne.mockResolvedValue({ id: "p1", timezone: "UTC" });
      statsRepo.findOne.mockResolvedValue(null);
      statsRepo.save.mockImplementation((e) => Promise.resolve(e));

      // 7 sane samples (10..70) + 1 outlier (9999). Only 8 samples.
      // P50 of sorted [10,20,30,40,50,60,70,9999] = sorted[3] = 40.
      // Cap = max(120, 40*5) = 200. So max = min(9999, 200) = 200.
      queueDataQB.getMany.mockResolvedValue(
        queueRows("UTC", [10, 20, 30, 40, 50, 60, 70, 9999]),
      );

      const result = await service.calculateAndStoreDailyStats(
        "p1",
        "2026-05-18",
      );

      // With the old "3× P90" cap this would have been ~30000. The new
      // P50-anchored cap keeps it bounded.
      expect(result!.maxWaitTime).toBeLessThanOrEqual(200);
      expect(result!.maxWaitTime).toBeLessThan(9999);
    });

    it("falls back to a 120-min cap when P90 is 0 (no qualifying samples)", async () => {
      parkRepo.findOne.mockResolvedValue({ id: "p1", timezone: "UTC" });
      queueDataQB.getMany.mockResolvedValue(
        queueRows("UTC", [0, 0, 0, 0, 200]),
      );
      statsRepo.findOne.mockResolvedValue(null);
      statsRepo.save.mockImplementation((e) => Promise.resolve(e));

      const result = await service.calculateAndStoreDailyStats(
        "p1",
        "2026-05-18",
      );

      // P50 = 0, P90 = 0 (sorted=[0,0,0,0,200], P90 idx=4 → 200,
      // actually that's the issue — P90 picks the high one). Let's use
      // a more representative dataset.
      void result;
      queueDataQB.getMany.mockResolvedValue(
        queueRows("UTC", Array(10).fill(0).concat([300])),
      );
      const result2 = await service.calculateAndStoreDailyStats(
        "p1",
        "2026-05-18",
      );
      // sorted: 10 zeros + 300. ceil(11*0.9)-1 = 9 → sorted[9] = 0. P90 = 0.
      // P90 is 0 → cap path: min(300, 120) = 120.
      expect(result2!.p90WaitTime).toBe(0);
      expect(result2!.maxWaitTime).toBe(120);
    });

    it("writes a nulls-only row for days with NO matching queue data", async () => {
      parkRepo.findOne.mockResolvedValue({ id: "p1", timezone: "UTC" });
      queueDataQB.getMany.mockResolvedValue([]); // no rows in window at all
      statsRepo.findOne.mockResolvedValue(null);
      statsRepo.save.mockImplementation((e) => Promise.resolve(e));

      const result = await service.calculateAndStoreDailyStats(
        "p1",
        "2026-05-18",
      );

      // We persist a null-payload record — prevents pointless rescans
      // on every read of an empty historical day.
      expect(result).not.toBeNull();
      expect(result!.p50WaitTime).toBeNull();
      expect(result!.p90WaitTime).toBeNull();
      expect(result!.maxWaitTime).toBeNull();
    });

    it("updates the existing row instead of creating a duplicate", async () => {
      parkRepo.findOne.mockResolvedValue({ id: "p1", timezone: "UTC" });
      queueDataQB.getMany.mockResolvedValue(
        queueRows("UTC", [10, 20, 30, 40, 50]),
      );
      // Existing record found — upsert path takes that branch.
      statsRepo.findOne.mockResolvedValue({
        id: "stats-existing",
        parkId: "p1",
        date: "2026-05-18",
        p50WaitTime: 99,
        p90WaitTime: 99,
        maxWaitTime: 99,
        metadata: { sampleSize: 5, lastUpdated: new Date(0) },
      });
      statsRepo.save.mockImplementation((e) => Promise.resolve(e));

      const result = await service.calculateAndStoreDailyStats(
        "p1",
        "2026-05-18",
      );

      // Same id as the existing row — not a new insert.
      expect(result!.id).toBe("stats-existing");
      // But the percentile values are overwritten with the new computation.
      expect(result!.p50WaitTime).toBe(30);
      expect(result!.p90WaitTime).toBe(50);
    });

    it("returns null when the park isn't found (avoids saving orphan stats)", async () => {
      parkRepo.findOne.mockResolvedValue(null);

      const result = await service.calculateAndStoreDailyStats(
        "unknown-park",
        "2026-05-18",
      );

      expect(result).toBeNull();
      expect(statsRepo.save).not.toHaveBeenCalled();
    });
  });

  describe("getDailyStats", () => {
    it("queries the persistent stats table by parkId + date range", async () => {
      const rows = [{ parkId: "p1", date: "2026-05-18", p50WaitTime: 20 }];
      statsRepo.find.mockResolvedValue(rows);

      const result = await service.getDailyStats(
        "p1",
        "2026-05-15",
        "2026-05-20",
      );

      expect(result).toEqual(rows);
      const [criteria] = statsRepo.find.mock.calls[0];
      expect((criteria as Record<string, unknown>).where).toMatchObject({
        parkId: "p1",
      });
    });
  });
});
