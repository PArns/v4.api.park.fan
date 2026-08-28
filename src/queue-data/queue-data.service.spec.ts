import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { QueueDataService } from "./queue-data.service";
import { QueueData } from "./entities/queue-data.entity";
import { ForecastData } from "./entities/forecast-data.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { ParksService } from "../parks/parks.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";

describe("QueueDataService", () => {
  let service: QueueDataService;

  // Mock repositories
  const mockQueueDataRepository = {
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
    findAndCount: jest.fn(),
    createQueryBuilder: jest.fn(() => ({
      select: jest.fn().mockReturnThis(),
      addSelect: jest.fn().mockReturnThis(),
      leftJoin: jest.fn().mockReturnThis(),
      innerJoin: jest.fn().mockReturnThis(),
      innerJoinAndSelect: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      addOrderBy: jest.fn().mockReturnThis(),
      limit: jest.fn().mockReturnThis(),
      skip: jest.fn().mockReturnThis(),
      take: jest.fn().mockReturnThis(),
      groupBy: jest.fn().mockReturnThis(),
      distinctOn: jest.fn().mockReturnThis(),
      getMany: jest.fn().mockResolvedValue([]),
      getOne: jest.fn().mockResolvedValue(null),
      getRawMany: jest.fn().mockResolvedValue([]),
      getCount: jest.fn().mockResolvedValue(0),
    })),
    query: jest.fn(),
  };

  const mockForecastDataRepository = {
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
    createQueryBuilder: jest.fn(() => ({
      select: jest.fn().mockReturnThis(),
      leftJoin: jest.fn().mockReturnThis(),
      leftJoinAndSelect: jest.fn().mockReturnThis(),
      innerJoin: jest.fn().mockReturnThis(),
      innerJoinAndSelect: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      addOrderBy: jest.fn().mockReturnThis(),
      limit: jest.fn().mockReturnThis(),
      skip: jest.fn().mockReturnThis(),
      take: jest.fn().mockReturnThis(),
      groupBy: jest.fn().mockReturnThis(),
      distinctOn: jest.fn().mockReturnThis(),
      getMany: jest.fn().mockResolvedValue([]),
      getOne: jest.fn().mockResolvedValue(null),
      getRawMany: jest.fn().mockResolvedValue([]),
      getManyAndCount: jest.fn().mockResolvedValue([[], 0]),
      getCount: jest.fn().mockResolvedValue(0),
    })),
  };

  const mockParksService = {
    findById: jest.fn().mockResolvedValue(null),
    getTodaySchedule: jest.fn().mockResolvedValue([]),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        QueueDataService,
        {
          provide: getRepositoryToken(QueueData),
          useValue: mockQueueDataRepository,
        },
        {
          provide: getRepositoryToken(ForecastData),
          useValue: mockForecastDataRepository,
        },
        {
          provide: getRepositoryToken(Attraction),
          useValue: {
            find: jest.fn().mockResolvedValue([]),
            findOne: jest.fn().mockResolvedValue(null),
          },
        },
        {
          provide: ParksService,
          useValue: mockParksService,
        },
        {
          provide: REDIS_CLIENT,
          useValue: {
            get: jest.fn().mockResolvedValue(null),
            set: jest.fn().mockResolvedValue("OK"),
            setex: jest.fn().mockResolvedValue("OK"),
            del: jest.fn().mockResolvedValue(1),
            mget: jest.fn().mockResolvedValue([]),
            pipeline: jest.fn(() => ({
              set: jest.fn().mockReturnThis(),
              exec: jest.fn().mockResolvedValue([]),
            })),
          },
        },
      ],
    }).compile();

    service = module.get<QueueDataService>(QueueDataService);

    jest.clearAllMocks();

    mockParksService.findById.mockResolvedValue(null);
    mockParksService.getTodaySchedule.mockResolvedValue([]);
  });

  it("should be defined", () => {
    expect(service).toBeDefined();
  });

  describe("findCurrentStatusByAttraction", () => {
    // Implementation switched from N findOne() calls (one per queue type)
    // to a single DISTINCT ON query — tests assert the new shape.
    it("should return current queue data for all queue types", async () => {
      const attractionId = "attr-123";
      const mockRows = [
        { id: "1", attractionId, queueType: "STANDBY", waitTime: 30 },
        {
          id: "2",
          attractionId,
          queueType: "RETURN_TIME",
          returnStart: new Date(),
        },
      ];
      const qb = {
        where: jest.fn().mockReturnThis(),
        andWhere: jest.fn().mockReturnThis(),
        distinctOn: jest.fn().mockReturnThis(),
        orderBy: jest.fn().mockReturnThis(),
        addOrderBy: jest.fn().mockReturnThis(),
        getMany: jest.fn().mockResolvedValue(mockRows),
      };
      (
        mockQueueDataRepository.createQueryBuilder as jest.Mock
      ).mockReturnValueOnce(qb);

      const result = await service.findCurrentStatusByAttraction(attractionId);

      expect(result).toEqual(mockRows);
      expect(qb.distinctOn).toHaveBeenCalledWith(["qd.queueType"]);
      expect(qb.getMany).toHaveBeenCalledTimes(1);
    });

    it("should return empty array when no data available", async () => {
      const qb = {
        where: jest.fn().mockReturnThis(),
        andWhere: jest.fn().mockReturnThis(),
        distinctOn: jest.fn().mockReturnThis(),
        orderBy: jest.fn().mockReturnThis(),
        addOrderBy: jest.fn().mockReturnThis(),
        getMany: jest.fn().mockResolvedValue([]),
      };
      (
        mockQueueDataRepository.createQueryBuilder as jest.Mock
      ).mockReturnValueOnce(qb);

      const result = await service.findCurrentStatusByAttraction("attr-999");

      expect(result).toEqual([]);
    });
  });

  describe("findForecastsByAttraction", () => {
    it("should return forecasts for next N hours", async () => {
      const attractionId = "attr-123";
      const hours = 24;

      const mockForecasts = [
        {
          id: "fc-1",
          attractionId,
          predictedTime: new Date(),
          predictedWaitTime: 35,
        },
      ];

      mockForecastDataRepository.find.mockResolvedValue(mockForecasts);

      const result = await service.findForecastsByAttraction(
        attractionId,
        hours,
      );

      expect(result).toEqual(mockForecasts);
    });
  });

  describe("findWaitTimesByAttraction", () => {
    it("should return wait times with pagination", async () => {
      const attractionId = "attr-123";

      mockQueueDataRepository.findAndCount.mockResolvedValue([[], 0]);

      const result = await service.findWaitTimesByAttraction(attractionId);

      expect(result).toHaveProperty("data");
      expect(result).toHaveProperty("total");
    });
  });

  describe("findWaitTimesByPark", () => {
    it("should return current wait times for all attractions in park", async () => {
      const parkId = "park-123";

      mockQueueDataRepository
        .createQueryBuilder()
        .getMany.mockResolvedValue([]);

      const result = await service.findWaitTimesByPark(parkId);

      expect(result).toEqual([]);
    });

    it("sorts by attraction name and applies a chunk-exclusion cutoff", async () => {
      // Latest reading per attraction comes back ordered by (attractionId,
      // queueType) from DISTINCT ON; the service must restore name order.
      const rows = [
        { attraction: { name: "Zeta Coaster" }, queueType: "STANDBY" },
        { attraction: { name: "Alpha Ride" }, queueType: "STANDBY" },
      ];
      const qb: Record<string, jest.Mock> = {};
      for (const m of [
        "innerJoinAndSelect",
        "where",
        "distinctOn",
        "orderBy",
        "addOrderBy",
        "andWhere",
      ]) {
        qb[m] = jest.fn().mockReturnValue(qb);
      }
      qb.getMany = jest.fn().mockResolvedValue(rows);
      mockQueueDataRepository.createQueryBuilder.mockReturnValueOnce(
        qb as never,
      );

      const result = await service.findWaitTimesByPark("park-123");

      expect(result.map((r) => r.attraction.name)).toEqual([
        "Alpha Ride",
        "Zeta Coaster",
      ]);
      // Time-bound applied (TimescaleDB chunk exclusion), replacing the old
      // unbounded full-hypertable MAX(timestamp) subquery.
      expect(qb.andWhere).toHaveBeenCalledWith(
        "qd.timestamp >= :cutoff",
        expect.objectContaining({ cutoff: expect.any(Date) }),
      );
    });
  });

  /**
   * The window this query applies decides what "we have no live data" means one
   * layer up, where an open park with no row turns a ride optimistically
   * OPERATING. So the window may never be narrower than the feed's own cadence:
   * queue rows are written on change plus an hourly heartbeat, and a ride whose
   * value has not moved therefore has its current reading timestamped BEFORE the
   * park opened. Cutting at the opening time throws that reading away and
   * invents an answer in its place.
   */
  describe("findCurrentStatusByPark cutoff", () => {
    /** Reads back the cutoff the service handed to the query builder. */
    async function cutoffFor(openingTime: Date | null): Promise<Date> {
      mockParksService.getTodaySchedule.mockResolvedValue(
        openingTime
          ? [{ scheduleType: "OPERATING", openingTime, closingTime: null }]
          : [],
      );

      const qb: Record<string, jest.Mock> = {};
      for (const m of [
        "innerJoin",
        "where",
        "distinctOn",
        "orderBy",
        "addOrderBy",
        "andWhere",
      ]) {
        qb[m] = jest.fn().mockReturnValue(qb);
      }
      qb.getMany = jest.fn().mockResolvedValue([]);
      mockQueueDataRepository.createQueryBuilder.mockReturnValueOnce(
        qb as never,
      );

      await service.findCurrentStatusByPark("park-123");

      const call = qb.andWhere.mock.calls.find(
        ([sql]) => sql === "qd.timestamp >= :cutoff",
      );
      expect(call).toBeDefined();
      return (call as [string, { cutoff: Date }])[1].cutoff;
    }

    it("keeps the feed's last word when the park opened minutes ago", async () => {
      // Phantasialand opens at 09:00 and is polled roughly hourly. At 09:05 the
      // newest row for every ride is the 08:20 one — "CLOSED", which is what the
      // source says right now. Cutting at 09:00 hid all 40 of them, and the ride
      // list read "open" for an ice rink in August until the first poll landed.
      const openedFiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);

      const cutoff = await cutoffFor(openedFiveMinutesAgo);

      expect(cutoff.getTime()).toBeLessThanOrEqual(
        Date.now() - 6 * 60 * 60 * 1000 + 5_000,
      );
    });

    it("still keeps the whole day once it is longer than the fallback", async () => {
      // The opening-time cutoff exists to widen the window on long days — a park
      // ten hours into its day must not lose its morning. That direction stays.
      const openedTenHoursAgo = new Date(Date.now() - 10 * 60 * 60 * 1000);

      const cutoff = await cutoffFor(openedTenHoursAgo);

      expect(
        Math.abs(cutoff.getTime() - openedTenHoursAgo.getTime()),
      ).toBeLessThan(5_000);
    });

    it("falls back to the age window when nothing operates today", async () => {
      const cutoff = await cutoffFor(null);

      expect(
        Math.abs(cutoff.getTime() - (Date.now() - 6 * 60 * 60 * 1000)),
      ).toBeLessThan(5_000);
    });
  });
});
