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
          useValue: {
            findById: jest.fn().mockResolvedValue(null),
            getTodaySchedule: jest.fn().mockResolvedValue([]),
          },
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
});
