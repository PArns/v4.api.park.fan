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
    query: jest.fn(),
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
      transaction: jest.fn(),
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

  /**
   * `last_merged_at` tells the nightly downtime reconstruction
   * (`outage-reconstruction.sql.ts`, `closure-gap.sql.ts`) that a ride's
   * history holds two interleaved series and must be held out of the current
   * window. `ParkMergeService.consolidateEntities` and
   * `AttractionMergeService.merge` stamp it; the two raw sync-time merge
   * blocks in this service did not, so a collision merge left the seam
   * looking like genuine outages.
   *
   * What these two cases prove is that the statement is issued, for every
   * survivor, before the losing row is deleted. They cannot prove it commits:
   * the fake manager below answers every statement, while against a real
   * database both blocks abort further down — `repairDuplicates` writes
   * `prediction_accuracy."attractionId"` (the column is `attraction_id`) and
   * neither block moves more than 3 of the 19 tables in
   * `ATTRACTION_DEPENDENCIES`, so the `DELETE FROM attractions` runs into the
   * NO ACTION foreign keys. That is its own ticket; see todo.md.
   */
  describe("raw attraction merges stamp last_merged_at", () => {
    type Recorded = { sql: string; params?: unknown[] };

    /**
     * Runs `parkRepository.manager.transaction` against a recording manager.
     * `rowsFor` answers the SELECTs; everything else resolves empty.
     */
    const recordTransaction = (
      rowsFor: (sql: string, params?: unknown[]) => unknown[],
    ) => {
      const calls: Recorded[] = [];
      const transactionalEntityManager = {
        query: jest.fn(async (sql: string, params?: unknown[]) => {
          calls.push({ sql, params });
          return rowsFor(sql, params);
        }),
        delete: jest.fn().mockResolvedValue({ affected: 1 }),
      };
      mockParkRepository.manager.transaction = jest.fn(
        async (cb: (m: typeof transactionalEntityManager) => Promise<void>) =>
          cb(transactionalEntityManager),
      );
      return { calls, transactionalEntityManager };
    };

    const attractionStamps = (calls: Recorded[]) =>
      calls.filter(
        (c) =>
          /UPDATE\s+attractions/i.test(c.sql) && /last_merged_at/i.test(c.sql),
      );

    const indexOfAttractionDelete = (calls: Recorded[]) =>
      calls.findIndex((c) => /DELETE\s+FROM\s+attractions/i.test(c.sql));

    it("stamps every survivor of the sync-time collision merge, before the ghosts are deleted", async () => {
      const survivingParkId = "11111111-1111-1111-1111-111111111111";
      const ghostParkId = "22222222-2222-2222-2222-222222222222";
      // Two collisions, so a stamp that only covered the last one processed
      // would be visible here.
      const attractionRows = [
        {
          id: "aaaa1111-0000-0000-0000-000000000001",
          parkId: survivingParkId,
          slug: "taron",
          queue_times_entity_id: null,
          land_name: null,
          land_external_id: null,
        },
        {
          id: "aaaa1111-0000-0000-0000-000000000002",
          parkId: survivingParkId,
          slug: "black-mamba",
          queue_times_entity_id: null,
          land_name: null,
          land_external_id: null,
        },
        {
          id: "bbbb2222-0000-0000-0000-000000000001",
          parkId: ghostParkId,
          slug: "taron",
          queue_times_entity_id: "4711",
          land_name: "Klugheim",
          land_external_id: "land-klugheim",
        },
        {
          id: "bbbb2222-0000-0000-0000-000000000002",
          parkId: ghostParkId,
          slug: "black-mamba",
          queue_times_entity_id: "4712",
          land_name: "Deep in Africa",
          land_external_id: "land-africa",
        },
      ];

      const { calls } = recordTransaction((sql) =>
        /SELECT id, "parkId", slug/.test(sql) ? attractionRows : [],
      );

      mockDestinationsService.findAll.mockResolvedValue({
        data: [{ id: "dest-1" }],
      });
      mockDestinationsService.findByExternalId.mockResolvedValue({
        id: "dest-1",
      });
      mockThemeParksClient.getDestinations.mockResolvedValue({
        destinations: [{ id: "ext-dest-1", parks: [{ id: "ext-park-1" }] }],
      });
      mockThemeParksClient.getEntity.mockResolvedValue({ id: "ext-park-1" });
      mockThemeParksMapper.mapPark.mockReturnValue({
        externalId: "ext-park-1",
        name: "Phantasialand",
        slug: "phantasialand",
        queueTimesEntityId: "4711",
        latitude: 50.8,
        longitude: 6.87,
        timezone: "Europe/Berlin",
      });
      mockParkRepository.find.mockResolvedValue([
        createTestPark({ id: survivingParkId, externalId: "ext-park-1" }),
      ]);
      mockParkRepository.update.mockResolvedValue({ affected: 1 });
      mockParkRepository.createQueryBuilder.mockImplementation(() => ({
        leftJoinAndSelect: jest.fn().mockReturnThis(),
        where: jest.fn().mockReturnThis(),
        andWhere: jest.fn().mockReturnThis(),
        orderBy: jest.fn().mockReturnThis(),
        addOrderBy: jest.fn().mockReturnThis(),
        getMany: jest.fn().mockResolvedValue([]),
        select: jest.fn().mockReturnThis(),
        getRawMany: jest.fn().mockResolvedValue([]),
        getOne: jest.fn().mockResolvedValue({
          id: ghostParkId,
          name: "Phantasialand (Queue-Times)",
        }),
      }));
      // repairDuplicates runs at the end of syncParks — nothing to repair.
      mockParkRepository.query.mockResolvedValue([]);

      await service.syncParks();

      const stamps = attractionStamps(calls);
      expect(stamps).toHaveLength(1);
      // One statement over a VALUES list, so both survivors are covered.
      expect(stamps[0].params).toEqual(
        expect.arrayContaining([
          attractionRows[0].id,
          attractionRows[1].id,
          "Klugheim",
          "Deep in Africa",
        ]),
      );
      // The ghost rows are gone afterwards, so the stamp cannot come later.
      const deleteIndex = indexOfAttractionDelete(calls);
      expect(deleteIndex).toBeGreaterThan(-1);
      expect(calls.indexOf(stamps[0])).toBeLessThan(deleteIndex);
    });

    it("stamps every survivor of the repairDuplicates ghost merge, before the ghost is deleted", async () => {
      const primaryId = "33333333-3333-3333-3333-333333333333";
      const ghostParkId = "44444444-4444-4444-4444-444444444444";
      const primaryAttractions = [
        {
          id: "cccc3333-0000-0000-0000-000000000001",
          slug: "troy",
          land_name: null,
          land_external_id: null,
        },
        {
          id: "cccc3333-0000-0000-0000-000000000002",
          slug: "fenix",
          land_name: null,
          land_external_id: null,
        },
      ];
      const ghostAttractions = [
        {
          id: "dddd4444-0000-0000-0000-000000000001",
          slug: "troy",
          land_name: "Avalon",
          land_external_id: "land-avalon",
        },
        {
          id: "dddd4444-0000-0000-0000-000000000002",
          slug: "fenix",
          land_name: "Avalon",
          land_external_id: "land-avalon",
        },
      ];

      const { calls, transactionalEntityManager } = recordTransaction(
        (sql, params) => {
          if (!/SELECT id, slug/.test(sql)) return [];
          const [parkId] = (params ?? []) as string[];
          return parkId === primaryId ? primaryAttractions : ghostAttractions;
        },
      );

      mockParkRepository.query.mockResolvedValue([
        { queue_times_entity_id: "4711" },
      ]);
      mockParkRepository.find.mockResolvedValue([
        createTestPark({ id: primaryId, wikiEntityId: "wiki-1" }),
        createTestPark({ id: ghostParkId, wikiEntityId: null }),
      ]);

      await service.repairDuplicates();

      const stamps = attractionStamps(calls);
      // One per collision — the loop must not stamp only the last survivor.
      expect(stamps).toHaveLength(2);
      expect(stamps.map((s) => (s.params as string[])[2])).toEqual([
        primaryAttractions[0].id,
        primaryAttractions[1].id,
      ]);
      const firstDelete = indexOfAttractionDelete(calls);
      expect(firstDelete).toBeGreaterThan(-1);
      expect(calls.indexOf(stamps[0])).toBeLessThan(firstDelete);
      expect(transactionalEntityManager.delete).toHaveBeenCalled();
    });
  });
});
