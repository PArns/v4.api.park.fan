import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { ParksService } from "./parks.service";
import { Park } from "./entities/park.entity";
import { ScheduleEntry } from "./entities/schedule-entry.entity";
import { ThemeParksClient } from "../external-apis/themeparks/themeparks.client";
import { ThemeParksMapper } from "../external-apis/themeparks/themeparks.mapper";
import { DestinationsService } from "../destinations/destinations.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { HolidaysService } from "../holidays/holidays.service";
import { createTestPark } from "../../test/fixtures/park.fixtures";

describe("ParksService", () => {
  let service: ParksService;

  // Mock Redis
  const mockRedis = {
    get: jest.fn(),
    set: jest.fn(),
    del: jest.fn(),
    setex: jest.fn(),
    keys: jest.fn().mockResolvedValue([]),
  };

  // Mock repositories
  const mockParkRepository = {
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
    update: jest.fn(),
    count: jest.fn(),
    createQueryBuilder: jest.fn(() => ({
      leftJoinAndSelect: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      addOrderBy: jest.fn().mockReturnThis(),
      getMany: jest.fn().mockResolvedValue([]),
      getOne: jest.fn().mockResolvedValue(null),
      select: jest.fn().mockReturnThis(),
      getRawMany: jest.fn().mockResolvedValue([]),
    })),
    manager: {
      query: jest.fn(),
      find: jest.fn().mockResolvedValue([]),
    },
  };

  /** Query builder covering both the read (select/getRawMany) and delete chains. */
  const scheduleQueryBuilder = (existingRows: unknown[] = []) => ({
    where: jest.fn().mockReturnThis(),
    andWhere: jest.fn().mockReturnThis(),
    orderBy: jest.fn().mockReturnThis(),
    addOrderBy: jest.fn().mockReturnThis(),
    getMany: jest.fn().mockResolvedValue([]),
    select: jest.fn().mockReturnThis(),
    addSelect: jest.fn().mockReturnThis(),
    getRawMany: jest.fn().mockResolvedValue(existingRows),
    delete: jest.fn().mockReturnThis(),
    from: jest.fn().mockReturnThis(),
    execute: jest.fn().mockResolvedValue({ affected: 0 }),
  });

  const mockScheduleRepository = {
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
    update: jest.fn(),
    delete: jest.fn(),
    query: jest.fn(),
    createQueryBuilder: jest.fn(() => scheduleQueryBuilder()),
  };

  // Mock services
  const mockThemeParksClient = {
    getDestinations: jest.fn(),
    getEntity: jest.fn(),
    getEntityChildren: jest.fn(),
    getLiveData: jest.fn(),
  };

  const mockThemeParksMapper = {
    mapPark: jest.fn(),
    mapAttraction: jest.fn(),
  };

  const mockDestinationsService = {
    findAll: jest.fn(),
    findByExternalId: jest.fn(),
    syncDestinations: jest.fn(),
  };

  const mockHolidaysService = {
    isHoliday: jest.fn(),
    isBridgeDay: jest.fn(),
    getHolidays: jest.fn(),
    saveHolidaysFromApi: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ParksService,
        {
          provide: getRepositoryToken(Park),
          useValue: mockParkRepository,
        },
        {
          provide: getRepositoryToken(ScheduleEntry),
          useValue: mockScheduleRepository,
        },
        {
          provide: ThemeParksClient,
          useValue: mockThemeParksClient,
        },
        {
          provide: ThemeParksMapper,
          useValue: mockThemeParksMapper,
        },
        {
          provide: DestinationsService,
          useValue: mockDestinationsService,
        },
        {
          provide: REDIS_CLIENT,
          useValue: mockRedis,
        },
        {
          provide: HolidaysService,
          useValue: mockHolidaysService,
        },
      ],
    }).compile();

    service = module.get<ParksService>(ParksService);

    // Clear all mocks before each test
    jest.clearAllMocks();
  });

  it("should be defined", () => {
    expect(service).toBeDefined();
  });

  describe("findAll", () => {
    it("should return an array of parks", async () => {
      const testParks = [
        createTestPark({ name: "Test Park 1", slug: "test-park-1" }),
        createTestPark({ name: "Test Park 2", slug: "test-park-2" }),
      ];

      mockParkRepository.find.mockResolvedValue(testParks);

      const result = await service.findAll();

      expect(result).toEqual(testParks);
      expect(mockParkRepository.find).toHaveBeenCalledTimes(1);
      expect(mockParkRepository.find).toHaveBeenCalledWith({
        relations: ["destination"],
        order: { name: "ASC" },
      });
    });

    it("should return empty array when no parks exist", async () => {
      mockParkRepository.find.mockResolvedValue([]);

      const result = await service.findAll();

      expect(result).toEqual([]);
      expect(mockParkRepository.find).toHaveBeenCalledTimes(1);
    });
  });

  describe("findBySlug", () => {
    // findBySlug now loads only `destination` eagerly and hydrates
    // attractions/shows/restaurants via manager.find inside
    // loadParkRelations — the test asserts that contract.
    it("should return a park by slug with relations hydrated", async () => {
      const testPark = createTestPark({ slug: "test-magic-kingdom" });
      mockParkRepository.findOne.mockResolvedValue(testPark);

      const result = await service.findBySlug("test-magic-kingdom");

      expect(result?.id).toBe(testPark.id);
      expect(mockParkRepository.findOne).toHaveBeenCalledWith({
        where: { slug: "test-magic-kingdom" },
        relations: ["destination"],
      });
      // attractions/shows/restaurants come from the entity manager
      expect(result?.attractions).toEqual([]);
      expect(result?.shows).toEqual([]);
      expect(result?.restaurants).toEqual([]);
    });

    it("should return null if park not found", async () => {
      mockParkRepository.findOne.mockResolvedValue(null);

      const result = await service.findBySlug("non-existent");

      expect(result).toBeNull();
      expect(mockParkRepository.findOne).toHaveBeenCalledWith({
        where: { slug: "non-existent" },
        relations: ["destination"],
      });
    });
  });

  describe("findByExternalId", () => {
    it("should return a park by external ID", async () => {
      const testPark = createTestPark({ externalId: "test-ext-123" });

      mockParkRepository.findOne.mockResolvedValue(testPark);

      const result = await service.findByExternalId("test-ext-123");

      expect(result).toEqual(testPark);
      expect(mockParkRepository.findOne).toHaveBeenCalled();
    });

    it("should return null if park not found", async () => {
      mockParkRepository.findOne.mockResolvedValue(null);

      const result = await service.findByExternalId("non-existent");

      expect(result).toBeNull();
    });
  });

  describe("findById", () => {
    it("should return a park by internal ID", async () => {
      const testPark = createTestPark();

      mockParkRepository.findOne.mockResolvedValue(testPark);

      const result = await service.findById(testPark.id);

      expect(result).toEqual(testPark);
      expect(mockParkRepository.findOne).toHaveBeenCalledWith({
        where: { id: testPark.id },
        relations: ["destination"],
      });
    });
  });

  describe("getTodaySchedule", () => {
    it("should return cached schedule if available", async () => {
      const cachedSchedule = JSON.stringify([
        {
          id: "1",
          date: new Date().toISOString(),
          openingTime: new Date().toISOString(),
          closingTime: new Date().toISOString(),
        },
      ]);

      mockRedis.get.mockResolvedValue(cachedSchedule);

      const result = await service.getTodaySchedule("park-123");

      expect(result).toBeDefined();
      expect(result.length).toBe(1);
      expect(mockRedis.get).toHaveBeenCalled();
      expect(mockScheduleRepository.createQueryBuilder).not.toHaveBeenCalled();
    });

    it("should fetch and cache schedule when not cached", async () => {
      mockRedis.get.mockResolvedValue(null);
      mockScheduleRepository.createQueryBuilder().getMany.mockResolvedValue([]);

      const result = await service.getTodaySchedule("park-123");

      expect(result).toEqual([]);
      expect(mockRedis.get).toHaveBeenCalled();
      expect(mockRedis.set).toHaveBeenCalled();
    });
  });

  describe("getUniqueCountries", () => {
    it("should return unique country codes", async () => {
      const mockRawResults = [
        { country: "United States" },
        { country: "Germany" },
      ];

      const queryBuilder = {
        select: jest.fn().mockReturnThis(),
        where: jest.fn().mockReturnThis(),
        andWhere: jest.fn().mockReturnThis(),
        leftJoinAndSelect: jest.fn().mockReturnThis(),
        orderBy: jest.fn().mockReturnThis(),
        addOrderBy: jest.fn().mockReturnThis(),
        getMany: jest.fn().mockResolvedValue([]),
        getOne: jest.fn().mockResolvedValue(null),
        getRawMany: jest.fn().mockResolvedValue(mockRawResults),
      };

      mockParkRepository.createQueryBuilder.mockReturnValue(queryBuilder);

      const result = await service.getUniqueCountries();

      expect(result).toEqual(["United States", "Germany"]);
      expect(mockParkRepository.createQueryBuilder).toHaveBeenCalled();
      expect(queryBuilder.select).toHaveBeenCalledWith(
        "DISTINCT park.country",
        "country",
      );
    });
  });

  /**
   * Sources misdate a past-midnight closing time: ThemeParks.wiki publishes
   * Parque Warner Madrid as `open 12:00+02:00` / `close 00:00+02:00` on the
   * *same* date, so the close lands 12 h before the open and the park reads
   * CLOSED all day. normalizeClosingTime is unit-tested on its own; these
   * cover that saveScheduleData actually applies it on the way to the database.
   */
  describe("saveScheduleData — misdated closing times", () => {
    const parkId = "11111111-2222-3333-4444-555555555555";

    // Verbatim from https://api.themeparks.wiki/v1/entity/{warner}/schedule
    const upstreamEntry = {
      date: "2026-07-28",
      type: "OPERATING",
      openingTime: "2026-07-28T12:00:00+02:00", // 10:00Z
      closingTime: "2026-07-28T00:00:00+02:00", // 22:00Z on the 27th — before opening
    };
    const OPENS = "2026-07-28T10:00:00.000Z";
    const REPAIRED_CLOSE = "2026-07-27T22:00:00.000Z" as const;
    // Midnight *after* the operating day, i.e. 00:00 local on the 29th.
    const EXPECTED_CLOSE = "2026-07-28T22:00:00.000Z";

    beforeEach(() => {
      mockParkRepository.findOne.mockResolvedValue({
        id: parkId,
        countryCode: "ES",
        regionCode: null,
        timezone: "Europe/Madrid",
      });
      mockHolidaysService.getHolidays.mockResolvedValue([]);
      mockScheduleRepository.save.mockResolvedValue([]);
      mockScheduleRepository.query.mockResolvedValue([]);
    });

    it("persists the closing time on the day the park actually shuts", async () => {
      // No existing row for that date → insert path.
      mockScheduleRepository.createQueryBuilder.mockImplementation(() =>
        scheduleQueryBuilder([]),
      );

      await service.saveScheduleData(parkId, [upstreamEntry]);

      expect(mockScheduleRepository.save).toHaveBeenCalledTimes(1);
      const [inserted] = mockScheduleRepository.save.mock.calls[0] as [
        Array<{ openingTime: Date; closingTime: Date }>,
      ];
      expect(inserted[0].openingTime.toISOString()).toBe(OPENS);
      // Raw source value would be REPAIRED_CLOSE's counterpart (before opening).
      expect(inserted[0].closingTime.toISOString()).toBe(EXPECTED_CLOSE);
      expect(inserted[0].closingTime.getTime()).toBeGreaterThan(
        inserted[0].openingTime.getTime(),
      );
    });

    it("does not rewrite a row that already holds the repaired time", async () => {
      // What production looks like after the one-off repair: the source still
      // sends the broken value, the stored row is already correct. The diff
      // upsert must see no change — this is why `updatedAt` stops moving.
      mockScheduleRepository.createQueryBuilder.mockImplementation(() =>
        scheduleQueryBuilder([
          {
            id: "99999999-8888-7777-6666-555555555555",
            date: "2026-07-28",
            scheduleType: "OPERATING",
            openingTime: new Date(OPENS),
            closingTime: new Date(EXPECTED_CLOSE),
            description: null,
            purchases: null,
            isHoliday: false,
            holidayName: null,
            isBridgeDay: false,
          },
        ]),
      );

      await service.saveScheduleData(parkId, [upstreamEntry]);

      expect(mockScheduleRepository.save).not.toHaveBeenCalled();
      expect(mockScheduleRepository.query).not.toHaveBeenCalled();
      // Sanity: the raw value differs from the stored one, so a missing
      // normalization would have produced an update here.
      expect(new Date(REPAIRED_CLOSE).getTime()).not.toBe(
        new Date(EXPECTED_CLOSE).getTime(),
      );
    });
  });
});
