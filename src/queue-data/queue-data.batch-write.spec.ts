import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { QueueDataService } from "./queue-data.service";
import { QueueData } from "./entities/queue-data.entity";
import { ForecastData } from "./entities/forecast-data.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { ParksService } from "../parks/parks.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import {
  EntityLiveResponse,
  EntityType,
  LiveStatus,
  QueueType,
} from "../external-apis/themeparks/themeparks.types";

/**
 * Covers the batched write path that the 5-minute wait-times poll uses.
 *
 * The point of these tests is twofold:
 *  1. the delta rules must be IDENTICAL to the per-attraction path they replaced
 *     (a wrong "no change" verdict silently drops wait-time history), and
 *  2. the round-trip count must actually stay flat as the batch grows — that is
 *     the whole reason the batch path exists.
 */
describe("QueueDataService — batched live-data writes", () => {
  let service: QueueDataService;
  let redis: {
    get: jest.Mock;
    set: jest.Mock;
    mget: jest.Mock;
    pipeline: jest.Mock;
  };
  let pipelineSet: jest.Mock;
  let pipelineExec: jest.Mock;
  let queueRepo: {
    create: jest.Mock;
    insert: jest.Mock;
    createQueryBuilder: jest.Mock;
  };
  let latestQueryBuilder: { getMany: jest.Mock };
  let attractionQueryBuilder: { getRawMany: jest.Mock };

  const ATTRACTION = "11111111-1111-1111-1111-111111111111";

  const liveResponse = (
    overrides: Partial<EntityLiveResponse> = {},
  ): EntityLiveResponse => ({
    id: "ext-1",
    name: "Test Coaster",
    entityType: EntityType.ATTRACTION,
    status: LiveStatus.OPERATING,
    lastUpdated: "2026-07-25T10:00:00.000Z",
    queue: { [QueueType.STANDBY]: { waitTime: 30 } },
    ...overrides,
  });

  /** Shape of the Redis-cached "latest known row" for one queue type. */
  const cachedLatest = (over: Record<string, unknown> = {}) =>
    JSON.stringify({
      status: LiveStatus.OPERATING,
      waitTime: 30,
      timestamp: new Date().toISOString(),
      ...over,
    });

  beforeEach(async () => {
    pipelineSet = jest.fn().mockReturnThis();
    pipelineExec = jest.fn().mockResolvedValue([]);
    latestQueryBuilder = { getMany: jest.fn().mockResolvedValue([]) };
    attractionQueryBuilder = { getRawMany: jest.fn().mockResolvedValue([]) };

    const chain = (terminal: object) => {
      const qb: Record<string, unknown> = { ...terminal };
      for (const m of [
        "select",
        "addSelect",
        "innerJoin",
        "where",
        "andWhere",
        "orderBy",
        "addOrderBy",
        "distinctOn",
      ]) {
        qb[m] = jest.fn(() => qb);
      }
      return qb;
    };

    redis = {
      get: jest.fn().mockResolvedValue(null),
      set: jest.fn().mockResolvedValue("OK"),
      mget: jest.fn().mockResolvedValue([]),
      pipeline: jest.fn(() => ({ set: pipelineSet, exec: pipelineExec })),
    };

    queueRepo = {
      create: jest.fn((data) => ({ ...data }) as QueueData),
      insert: jest.fn().mockResolvedValue({}),
      createQueryBuilder: jest.fn(() => chain(latestQueryBuilder)),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        QueueDataService,
        { provide: getRepositoryToken(QueueData), useValue: queueRepo },
        { provide: getRepositoryToken(ForecastData), useValue: {} },
        {
          provide: getRepositoryToken(Attraction),
          useValue: {
            createQueryBuilder: jest.fn(() => chain(attractionQueryBuilder)),
          },
        },
        { provide: ParksService, useValue: {} },
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();

    service = module.get<QueueDataService>(QueueDataService);
  });

  describe("round-trip count", () => {
    it("writes a whole batch with one MGET, one INSERT and one pipeline", async () => {
      const ids = Array.from({ length: 25 }, (_, i) => `attraction-${i}`);
      redis.mget.mockResolvedValue(ids.map(() => null)); // all cache misses

      const saved = await service.saveLiveDataBatch(
        ids.map((attractionId) => ({
          attractionId,
          liveData: liveResponse(),
          source: "themeparks-wiki",
        })),
      );

      expect(redis.mget).toHaveBeenCalledTimes(1);
      expect(queueRepo.insert).toHaveBeenCalledTimes(1);
      expect(redis.pipeline).toHaveBeenCalledTimes(1);

      // One bulk INSERT carrying every row, not 25 separate ones.
      expect(queueRepo.insert.mock.calls[0][0]).toHaveLength(25);
      expect(saved.size).toBe(25);
      expect([...saved.values()].every((n) => n === 1)).toBe(true);
    });

    it("resolves cache misses with a single DISTINCT ON query", async () => {
      redis.mget.mockResolvedValue([null, null, null]);

      await service.saveLiveDataBatch(
        ["a", "b", "c"].map((attractionId) => ({
          attractionId,
          liveData: liveResponse(),
        })),
      );

      // createQueryBuilder is only reached for the latest-row lookup here
      // (timezones are not needed when there is no previous row).
      expect(queueRepo.createQueryBuilder).toHaveBeenCalledTimes(1);
    });

    it("does not touch the DB at all when every entry is cached and unchanged", async () => {
      // Key-aware: the latest-row keys and the timezone keys are read by two
      // separate MGETs and must return their own value shapes.
      redis.mget.mockImplementation((...keys: string[]) =>
        Promise.resolve(
          keys.map((k) =>
            k.includes(":tz:") ? "Europe/Berlin" : cachedLatest(),
          ),
        ),
      );

      const saved = await service.saveLiveDataBatch([
        { attractionId: "a", liveData: liveResponse() },
        { attractionId: "b", liveData: liveResponse() },
      ]);

      expect(queueRepo.createQueryBuilder).not.toHaveBeenCalled();
      expect(queueRepo.insert).not.toHaveBeenCalled();
      expect(saved.size).toBe(0);
    });

    it("does not let a corrupt cached timezone cost the whole batch its readings", async () => {
      // Regression guard: this check runs per batch now, so an unguarded throw
      // inside the park-local-day comparison would drop every ride in the park.
      redis.mget.mockImplementation((...keys: string[]) =>
        Promise.resolve(
          keys.map((k) => (k.includes(":tz:") ? "not-a-timezone" : null)),
        ),
      );

      const saved = await service.saveLiveDataBatch([
        { attractionId: "a", liveData: liveResponse() },
        { attractionId: "b", liveData: liveResponse() },
      ]);

      expect(saved.size).toBe(2);
    });
  });

  describe("delta rules (unchanged from the per-attraction path)", () => {
    const runWithCached = async (
      cached: string | null,
      live: EntityLiveResponse,
    ) => {
      redis.mget.mockImplementation((...keys: string[]) =>
        Promise.resolve(
          keys.map((k) => (k.includes(":tz:") ? "Europe/Berlin" : cached)),
        ),
      );
      return service.saveLiveDataBatch([
        { attractionId: ATTRACTION, liveData: live },
      ]);
    };

    it("skips a write when nothing changed", async () => {
      const saved = await runWithCached(cachedLatest(), liveResponse());
      expect(queueRepo.insert).not.toHaveBeenCalled();
      expect(saved.get(ATTRACTION)).toBeUndefined();
    });

    it("writes on a status change", async () => {
      const saved = await runWithCached(
        cachedLatest({ status: LiveStatus.OPERATING }),
        liveResponse({ status: LiveStatus.DOWN }),
      );
      expect(saved.get(ATTRACTION)).toBe(1);
    });

    it("writes on a wait-time change", async () => {
      const saved = await runWithCached(
        cachedLatest({ waitTime: 30 }),
        liveResponse({ queue: { [QueueType.STANDBY]: { waitTime: 45 } } }),
      );
      expect(saved.get(ATTRACTION)).toBe(1);
    });

    it("writes a heartbeat row when the last one is older than 60 minutes", async () => {
      const saved = await runWithCached(
        cachedLatest({
          timestamp: new Date(Date.now() - 61 * 60 * 1000).toISOString(),
        }),
        liveResponse(),
      );
      expect(saved.get(ATTRACTION)).toBe(1);
    });

    it("writes when the previous row belongs to an earlier park-local day", async () => {
      const saved = await runWithCached(
        cachedLatest({
          timestamp: new Date(Date.now() - 30 * 60 * 60 * 1000).toISOString(),
        }),
        liveResponse(),
      );
      expect(saved.get(ATTRACTION)).toBe(1);
    });

    it("records a status-only row for payloads without a queue", async () => {
      redis.mget.mockResolvedValue([null]);

      const saved = await service.saveLiveDataBatch([
        {
          attractionId: ATTRACTION,
          liveData: liveResponse({
            queue: undefined,
            status: LiveStatus.CLOSED,
          }),
        },
      ]);

      expect(saved.get(ATTRACTION)).toBe(1);
      const [rows] = queueRepo.insert.mock.calls[0];
      expect(rows[0]).toMatchObject({
        queueType: QueueType.STANDBY,
        status: LiveStatus.CLOSED,
        waitTime: 0,
      });
    });

    it("discards implausible wait times but still records the status", async () => {
      redis.mget.mockResolvedValue([null]);

      await service.saveLiveDataBatch([
        {
          attractionId: ATTRACTION,
          liveData: liveResponse({
            queue: { [QueueType.STANDBY]: { waitTime: 999 } },
          }),
        },
      ]);

      const [rows] = queueRepo.insert.mock.calls[0];
      expect(rows[0].waitTime).toBeUndefined();
      expect(rows[0].status).toBe(LiveStatus.OPERATING);
    });
  });

  describe("written rows", () => {
    it("sets the composite primary key explicitly and shares one batch timestamp", async () => {
      redis.mget.mockResolvedValue([null, null]);

      await service.saveLiveDataBatch([
        { attractionId: "a", liveData: liveResponse() },
        { attractionId: "b", liveData: liveResponse() },
      ]);

      const [rows] = queueRepo.insert.mock.calls[0];
      expect(rows).toHaveLength(2);
      for (const row of rows) {
        expect(row.id).toMatch(/^[0-9a-f-]{36}$/);
        expect(row.timestamp).toBeInstanceOf(Date);
      }
      // Same poll → same timestamp, so the rows stay comparable.
      expect(rows[0].timestamp.getTime()).toBe(rows[1].timestamp.getTime());
      expect(rows[0].id).not.toBe(rows[1].id);
    });

    it("refreshes the latest-row cache for every written entry", async () => {
      redis.mget.mockResolvedValue([null, null]);

      await service.saveLiveDataBatch([
        { attractionId: "a", liveData: liveResponse() },
        { attractionId: "b", liveData: liveResponse() },
      ]);

      expect(pipelineSet).toHaveBeenCalledTimes(2);
      expect(pipelineExec).toHaveBeenCalledTimes(1);
    });
  });

  describe("resilience", () => {
    it("falls back to per-row inserts when the bulk insert fails", async () => {
      redis.mget.mockResolvedValue([null, null, null]);
      queueRepo.insert
        .mockRejectedValueOnce(new Error("bulk boom")) // the batch
        .mockResolvedValueOnce({}) // row 1
        .mockRejectedValueOnce(new Error("bad row")) // row 2
        .mockResolvedValueOnce({}); // row 3

      const saved = await service.saveLiveDataBatch([
        { attractionId: "a", liveData: liveResponse() },
        { attractionId: "b", liveData: liveResponse() },
        { attractionId: "c", liveData: liveResponse() },
      ]);

      // 1 bulk attempt + 3 per-row retries
      expect(queueRepo.insert).toHaveBeenCalledTimes(4);
      // The one bad row must not cost the other two.
      expect(saved.get("a")).toBe(1);
      expect(saved.get("b")).toBeUndefined();
      expect(saved.get("c")).toBe(1);
    });

    it("treats a failed latest-row lookup as 'no previous data' instead of dropping the poll", async () => {
      redis.mget.mockResolvedValue([null]);
      latestQueryBuilder.getMany.mockRejectedValue(new Error("db down"));

      const saved = await service.saveLiveDataBatch([
        { attractionId: ATTRACTION, liveData: liveResponse() },
      ]);

      expect(saved.get(ATTRACTION)).toBe(1);
    });

    it("survives Redis being unavailable", async () => {
      redis.mget.mockRejectedValue(new Error("redis down"));

      const saved = await service.saveLiveDataBatch([
        { attractionId: ATTRACTION, liveData: liveResponse() },
      ]);

      expect(saved.get(ATTRACTION)).toBe(1);
    });

    it("writes a repeated (attraction, queueType) only once per batch", async () => {
      redis.mget.mockResolvedValue([null]);

      const saved = await service.saveLiveDataBatch([
        { attractionId: ATTRACTION, liveData: liveResponse() },
        {
          attractionId: ATTRACTION,
          liveData: liveResponse({
            queue: { [QueueType.STANDBY]: { waitTime: 55 } },
          }),
        },
      ]);

      expect(saved.get(ATTRACTION)).toBe(1);
      const [rows] = queueRepo.insert.mock.calls[0];
      expect(rows).toHaveLength(1);
      // Last reading wins, matching "the newest value in this poll".
      expect(rows[0].waitTime).toBe(55);
    });

    it("returns an empty result for an empty batch without any I/O", async () => {
      const saved = await service.saveLiveDataBatch([]);
      expect(saved.size).toBe(0);
      expect(redis.mget).not.toHaveBeenCalled();
      expect(queueRepo.insert).not.toHaveBeenCalled();
    });
  });

  describe("saveLiveData (single) delegates to the batch path", () => {
    it("returns the number of rows written for the attraction", async () => {
      redis.mget.mockResolvedValue([null]);

      const count = await service.saveLiveData(
        ATTRACTION,
        liveResponse({
          queue: {
            [QueueType.STANDBY]: { waitTime: 30 },
            [QueueType.SINGLE_RIDER]: { waitTime: 10 },
          },
        }),
        "themeparks-wiki",
      );

      expect(count).toBe(2);
      expect(queueRepo.insert).toHaveBeenCalledTimes(1);
    });

    it("returns 0 when the delta check finds no change", async () => {
      redis.mget.mockImplementation((...keys: string[]) =>
        Promise.resolve(
          keys.map((k) =>
            k.includes(":tz:") ? "Europe/Berlin" : cachedLatest(),
          ),
        ),
      );

      const count = await service.saveLiveData(ATTRACTION, liveResponse());
      expect(count).toBe(0);
    });
  });
});
