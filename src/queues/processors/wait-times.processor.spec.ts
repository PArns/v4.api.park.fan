import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { WaitTimesProcessor } from "./wait-times.processor";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import { ExternalEntityMapping } from "../../database/entities/external-entity-mapping.entity";
import { Park } from "../../parks/entities/park.entity";
import { ParksService } from "../../parks/parks.service";
import { AttractionsService } from "../../attractions/attractions.service";
import { ShowsService } from "../../shows/shows.service";
import { RestaurantsService } from "../../restaurants/restaurants.service";
import { QueueDataService } from "../../queue-data/queue-data.service";
import { MultiSourceOrchestrator } from "../../external-apis/data-sources/multi-source-orchestrator.service";
import { CacheWarmupService } from "../services/cache-warmup.service";
import { PopularityService } from "../../popularity/popularity.service";
import { PredictionDeviationService } from "../../ml/services/prediction-deviation.service";
import { LiveStatus } from "../../external-apis/themeparks/themeparks.types";
import { getQueueToken } from "@nestjs/bull";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

/**
 * Coverage for the highest-frequency cron in the system (every 5 min).
 * The wait-times processor calls a handful of methods we've recently
 * refactored — `processLandData` (N+1 → bulk diff) and `trackDowntime`
 * (sequential GETs/SETs → MGET + pipelined writes). These tests exercise
 * those two paths directly via the private methods so a regression in
 * either path shows up immediately on every PR.
 */
describe("WaitTimesProcessor", () => {
  let processor: WaitTimesProcessor;

  const mockAttractionRepository = {
    find: jest.fn(),
    update: jest.fn().mockResolvedValue({ affected: 1 }),
  };

  const mockMappingRepository = {
    createQueryBuilder: jest.fn(() => ({
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      getMany: jest.fn().mockResolvedValue([]),
    })),
  };

  // Redis with state — exercise the new MGET + pipeline path.
  const redisStore = new Map<string, string>();
  const pipelineOps: Array<["set" | "del", string, ...unknown[]]> = [];
  const redis = {
    get: jest.fn((k: string) => Promise.resolve(redisStore.get(k) ?? null)),
    set: jest.fn((k: string, v: string) => {
      redisStore.set(k, v);
      return Promise.resolve("OK");
    }),
    mget: jest.fn((...keys: string[]) =>
      Promise.resolve(keys.map((k) => redisStore.get(k) ?? null)),
    ),
    del: jest.fn((k: string) => {
      redisStore.delete(k);
      return Promise.resolve(1);
    }),
    pipeline: jest.fn(() => {
      const local: Array<["set" | "del", string, ...unknown[]]> = [];
      return {
        set: jest.fn(function (
          this: { set: jest.Mock },
          k: string,
          v: string,
          ...rest: unknown[]
        ) {
          local.push(["set", k, v, ...rest]);
          return this;
        }),
        del: jest.fn(function (this: { del: jest.Mock }, k: string) {
          local.push(["del", k]);
          return this;
        }),
        exec: jest.fn(async () => {
          for (const op of local) {
            pipelineOps.push(op);
            if (op[0] === "set") redisStore.set(op[1], op[2] as string);
            else if (op[0] === "del") redisStore.delete(op[1]);
          }
          return [];
        }),
      };
    }),
  };

  const mockAttractionsService = {
    getRepository: () => mockAttractionRepository,
  };

  beforeEach(async () => {
    redisStore.clear();
    pipelineOps.length = 0;
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        WaitTimesProcessor,
        { provide: getQueueToken("wait-times"), useValue: { add: jest.fn() } },
        {
          provide: getRepositoryToken(QueueData),
          useValue: { find: jest.fn(), save: jest.fn() },
        },
        {
          provide: getRepositoryToken(ExternalEntityMapping),
          useValue: mockMappingRepository,
        },
        {
          provide: getRepositoryToken(Park),
          useValue: { findOne: jest.fn(), update: jest.fn() },
        },
        { provide: ParksService, useValue: { findAll: jest.fn() } },
        { provide: AttractionsService, useValue: mockAttractionsService },
        { provide: ShowsService, useValue: { getRepository: () => ({}) } },
        {
          provide: RestaurantsService,
          useValue: { getRepository: () => ({}) },
        },
        { provide: QueueDataService, useValue: {} },
        { provide: MultiSourceOrchestrator, useValue: {} },
        { provide: CacheWarmupService, useValue: {} },
        { provide: PopularityService, useValue: { getTopParks: jest.fn() } },
        { provide: PredictionDeviationService, useValue: {} },
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();

    processor = module.get(WaitTimesProcessor);
  });

  describe("processLandData (N+1 → bulk-diff land sync)", () => {
    // Pull processLandData out of the private surface for testing — this
    // is the every-5-minute land-info sync that used to fire 2-3 queries
    // per attraction.
    const run = (lands: any[], park: any) =>
      (processor as any).processLandData(lands, park);

    it("emits ZERO update calls in steady state (nothing has changed)", async () => {
      const park = { id: "p1" };
      mockAttractionRepository.find.mockResolvedValueOnce([
        {
          id: "a1",
          landName: "Wild West",
          landExternalId: "wild",
          queueTimesEntityId: "100",
        },
        {
          id: "a2",
          landName: "Wild West",
          landExternalId: "wild",
          queueTimesEntityId: "200",
        },
      ]);
      mockMappingRepository.createQueryBuilder.mockReturnValueOnce({
        where: jest.fn().mockReturnThis(),
        andWhere: jest.fn().mockReturnThis(),
        getMany: jest.fn().mockResolvedValue([
          {
            internalEntityId: "a1",
            externalEntityId: "qt-ride-100",
            externalSource: "queue-times",
          },
          {
            internalEntityId: "a2",
            externalEntityId: "qt-ride-200",
            externalSource: "queue-times",
          },
        ]),
      });

      const lands = [
        {
          name: "Wild West",
          id: "wild",
          attractions: ["qt-ride-100", "qt-ride-200"],
        },
      ];

      const changed = await run(lands, park);

      expect(changed).toBe(0);
      // Crucial perf invariant: zero UPDATEs when nothing changed.
      expect(mockAttractionRepository.update).not.toHaveBeenCalled();
    });

    it("groups land-name changes into one UPDATE per target land", async () => {
      const park = { id: "p1" };
      mockAttractionRepository.find.mockResolvedValueOnce([
        {
          id: "a1",
          landName: "Old Land",
          landExternalId: "old",
          queueTimesEntityId: "100",
        },
        {
          id: "a2",
          landName: "Old Land",
          landExternalId: "old",
          queueTimesEntityId: "200",
        },
      ]);
      mockMappingRepository.createQueryBuilder.mockReturnValueOnce({
        where: jest.fn().mockReturnThis(),
        andWhere: jest.fn().mockReturnThis(),
        getMany: jest.fn().mockResolvedValue([
          {
            internalEntityId: "a1",
            externalEntityId: "qt-ride-100",
            externalSource: "queue-times",
          },
          {
            internalEntityId: "a2",
            externalEntityId: "qt-ride-200",
            externalSource: "queue-times",
          },
        ]),
      });

      // Both attractions move to "Wild West" together.
      const lands = [
        {
          name: "Wild West",
          id: "wild",
          attractions: ["qt-ride-100", "qt-ride-200"],
        },
      ];

      const changed = await run(lands, park);

      expect(changed).toBe(2); // two attractions changed land
      // One UPDATE for the land change (with array of ids), not two.
      const landUpdateCalls = mockAttractionRepository.update.mock.calls.filter(
        (c) => c[1].landName === "Wild West",
      );
      expect(landUpdateCalls).toHaveLength(1);
      expect(landUpdateCalls[0][0]).toEqual(["a1", "a2"]);
    });

    it("returns 0 immediately when the park has no attractions (no SQL trip)", async () => {
      mockAttractionRepository.find.mockResolvedValueOnce([]);

      const changed = await run([{ name: "Whatever", attractions: [] }], {
        id: "p1",
      });

      expect(changed).toBe(0);
      // No mapping lookup either — short-circuit on empty parkAttractions.
      expect(mockMappingRepository.createQueryBuilder).not.toHaveBeenCalled();
    });
  });

  describe("trackDowntime (MGET + pipelined writes)", () => {
    const baseMapping = new Map<string, string>([
      ["themeparks-wiki:wiki-ride", "a1"],
    ]);
    const entity = (status: LiveStatus) => ({
      source: "themeparks-wiki",
      externalId: "wiki-ride",
      status,
    });
    const run = (
      status: LiveStatus,
      tz = "UTC",
      closingTime: Date | undefined = undefined,
    ) =>
      (processor as any).trackDowntime(
        entity(status),
        baseMapping,
        tz,
        closingTime,
      );

    it("OPERATING → DOWN: writes downtime-start in the same pipeline as the status SET", async () => {
      redisStore.set("downtime:status:a1", LiveStatus.OPERATING);

      await run(LiveStatus.DOWN);

      // MGET fetched both keys in one round-trip.
      expect(redis.mget).toHaveBeenCalledWith(
        "downtime:status:a1",
        "downtime:start:a1",
      );
      // Pipeline applied: status updated, start recorded.
      expect(redisStore.get("downtime:status:a1")).toBe(LiveStatus.DOWN);
      expect(redisStore.get("downtime:start:a1")).toMatch(/^\d+$/);
      // No direct .set() outside the pipeline — every write went through it.
      expect(redis.set).not.toHaveBeenCalled();
    });

    it("does NOT record downtime-start when within 60 min of close (winding down ≠ outage)", async () => {
      redisStore.set("downtime:status:a1", LiveStatus.OPERATING);
      const closingInThirtyMin = new Date(Date.now() + 30 * 60 * 1000);

      await run(LiveStatus.CLOSED, "UTC", closingInThirtyMin);

      // Status still gets updated.
      expect(redisStore.get("downtime:status:a1")).toBe(LiveStatus.CLOSED);
      // But no start key written.
      expect(redisStore.get("downtime:start:a1")).toBeUndefined();
    });

    it("DOWN → OPERATING: accumulates the daily-total and deletes the start key in one pipeline", async () => {
      const tenMinAgo = Date.now() - 10 * 60 * 1000;
      redisStore.set("downtime:status:a1", LiveStatus.DOWN);
      redisStore.set("downtime:start:a1", tenMinAgo.toString());
      // Date.now() will be ~now; using UTC the formatted day is today's.
      // We can read the actual computed key from the pipeline ops.

      await run(LiveStatus.OPERATING);

      // Daily key written with at least 10 minutes added.
      const dailyEntry = [...redisStore.entries()].find(([k]) =>
        k.startsWith("downtime:daily:a1:"),
      );
      expect(dailyEntry).toBeDefined();
      // Allow ±1 minute drift for clock movement during the test.
      const value = parseInt(dailyEntry![1], 10);
      expect(value).toBeGreaterThanOrEqual(9);
      expect(value).toBeLessThanOrEqual(11);
      // Start key cleared.
      expect(redisStore.get("downtime:start:a1")).toBeUndefined();
    });

    it("sums into an existing daily-total (multi-outage day)", async () => {
      const fiveMinAgo = Date.now() - 5 * 60 * 1000;
      redisStore.set("downtime:status:a1", LiveStatus.DOWN);
      redisStore.set("downtime:start:a1", fiveMinAgo.toString());
      // Pre-existing 20 minutes for today.
      // We pin the timezone to UTC so the test key matches the run.
      const dayStr = new Date().toISOString().split("T")[0];
      redisStore.set(`downtime:daily:a1:${dayStr}`, "20");

      await run(LiveStatus.OPERATING, "UTC");

      const newValue = parseInt(
        redisStore.get(`downtime:daily:a1:${dayStr}`)!,
        10,
      );
      // Original 20 + ~5 new ≈ 24-26.
      expect(newValue).toBeGreaterThanOrEqual(24);
      expect(newValue).toBeLessThanOrEqual(26);
    });

    it("OPERATING → OPERATING (no change): only the status SET fires", async () => {
      redisStore.set("downtime:status:a1", LiveStatus.OPERATING);

      await run(LiveStatus.OPERATING);

      expect(redisStore.get("downtime:status:a1")).toBe(LiveStatus.OPERATING);
      expect(redisStore.get("downtime:start:a1")).toBeUndefined();
      // Pipeline should contain exactly one SET, no DEL.
      const writes = pipelineOps.filter((op) => op[1].includes("a1"));
      expect(writes).toEqual([
        ["set", "downtime:status:a1", LiveStatus.OPERATING, "EX", 3600],
      ]);
    });

    it("returns silently for unmapped attractions (no Redis activity at all)", async () => {
      await (processor as any).trackDowntime(
        { source: "x", externalId: "unknown", status: LiveStatus.DOWN },
        baseMapping,
        "UTC",
      );

      expect(redis.mget).not.toHaveBeenCalled();
      expect(redis.pipeline).not.toHaveBeenCalled();
    });
  });
});
