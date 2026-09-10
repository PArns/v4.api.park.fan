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
import {
  ATTRACTION_DEPENDENCIES,
  PARK_DEPENDENCIES,
  PARK_INLINE_DEPENDENCIES,
} from "./utils/merge-dependencies";

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

    // `jest.clearAllMocks()` clears calls, not implementations. Both cases
    // below replace the query builder and the transaction runner, so without
    // this the next test added to this file would inherit a `getOne` that
    // resolves to a ghost park and silently take the merge branch.
    const defaultCreateQueryBuilder =
      mockParkRepository.createQueryBuilder.getMockImplementation();
    const defaultTransaction = mockParkRepository.manager.transaction;

    afterEach(() => {
      if (defaultCreateQueryBuilder) {
        mockParkRepository.createQueryBuilder.mockImplementation(
          defaultCreateQueryBuilder,
        );
      }
      mockParkRepository.manager.transaction = defaultTransaction;
    });

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
        // Recorded into the same list as the raw statements. The park DELETE
        // goes through the entity manager rather than `query`, and where it
        // sits relative to the park-scoped moves is the whole question — a set
        // of assertions over two separate mocks cannot answer it.
        delete: jest.fn(async (_entity: unknown, id: string) => {
          calls.push({ sql: "DELETE FROM parks", params: [id] });
          return { affected: 1 };
        }),
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

    /**
     * Tables the merge issued a statement against for one losing attraction.
     *
     * `applyMergeDependencies` binds the loser id as a parameter on every
     * statement it makes — the discard DELETE, the conflict DELETE and the
     * reparenting UPDATE alike — so the set of tables reached for a given loser
     * is readable straight off the recorded calls.
     */
    const dependencyTablesTouched = (calls: Recorded[], loserId: string) =>
      new Set(
        calls
          .filter((c) => (c.params ?? []).includes(loserId))
          .map((c) => /(?:UPDATE|DELETE\s+FROM)\s+(\w+)/i.exec(c.sql)?.[1])
          .filter((table): table is string => Boolean(table)),
      );

    const syncSurvivingParkId = "11111111-1111-1111-1111-111111111111";
    const syncGhostParkId = "22222222-2222-2222-2222-222222222222";

    /**
     * Wires up the mocks `syncParks` needs to reach its ghost-park merge, with
     * `attractionRows` answering the one SELECT inside the transaction.
     */
    const primeGhostParkSync = (attractionRows: unknown[]) => {
      const { calls, transactionalEntityManager } = recordTransaction((sql) =>
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
        createTestPark({ id: syncSurvivingParkId, externalId: "ext-park-1" }),
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
          id: syncGhostParkId,
          name: "Phantasialand (Queue-Times)",
        }),
      }));
      // repairDuplicates runs at the end of syncParks — nothing to repair.
      mockParkRepository.query.mockResolvedValue([]);

      return { calls, transactionalEntityManager };
    };

    it("stamps every survivor of the sync-time collision merge, before the ghosts are deleted", async () => {
      // Two collisions, so a stamp that only covered the last one processed
      // would be visible here.
      const attractionRows = [
        {
          id: "aaaa1111-0000-0000-0000-000000000001",
          parkId: syncSurvivingParkId,
          slug: "taron",
          queue_times_entity_id: null,
          land_name: null,
          land_external_id: null,
        },
        {
          id: "aaaa1111-0000-0000-0000-000000000002",
          parkId: syncSurvivingParkId,
          slug: "black-mamba",
          queue_times_entity_id: null,
          land_name: null,
          land_external_id: null,
        },
        {
          id: "bbbb2222-0000-0000-0000-000000000001",
          parkId: syncGhostParkId,
          slug: "taron",
          queue_times_entity_id: "4711",
          land_name: "Klugheim",
          land_external_id: "land-klugheim",
        },
        {
          id: "bbbb2222-0000-0000-0000-000000000002",
          parkId: syncGhostParkId,
          slug: "black-mamba",
          queue_times_entity_id: "4712",
          land_name: "Deep in Africa",
          land_external_id: "land-africa",
        },
      ];

      const { calls } = primeGhostParkSync(attractionRows);

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

    // The two cases above prove the stamp is issued. They cannot prove it
    // survives, and against a real database it did not: the DELETE that
    // follows raises 23503 on four NO ACTION foreign keys, and
    // `repairDuplicates` first raises 42703 on a column that does not exist.
    // What both paths were missing is the helper the two proper merge services
    // already use.
    it("moves every declared dependency of the sync-time collision merge onto the survivor, before the ghosts are deleted", async () => {
      const attractionRows = [
        {
          id: "aaaa1111-0000-0000-0000-000000000001",
          parkId: syncSurvivingParkId,
          slug: "taron",
          queue_times_entity_id: null,
          land_name: null,
          land_external_id: null,
        },
        {
          id: "bbbb2222-0000-0000-0000-000000000001",
          parkId: syncGhostParkId,
          slug: "taron",
          queue_times_entity_id: "4711",
          land_name: "Klugheim",
          land_external_id: "land-klugheim",
        },
      ];
      const survivor = attractionRows[0].id;
      const ghost = attractionRows[1].id;

      const { calls } = primeGhostParkSync(attractionRows);

      await service.syncParks();

      // This block used to move nothing at all and trust the ghost's history
      // to vanish with the row.
      const touched = dependencyTablesTouched(calls, ghost);
      for (const dep of ATTRACTION_DEPENDENCIES) {
        expect(touched).toContain(dep.table);
      }

      // `queue_data` and `ml_prediction_anomalies` are the two the ticket
      // names: the first is the history a merge must not lose, the second the
      // FK that stops the DELETE outright.
      const reparented = (table: string) =>
        calls.find(
          (c) =>
            new RegExp(`UPDATE\\s+${table}\\s+SET`, "i").test(c.sql) &&
            (c.params ?? []).includes(ghost),
        );
      expect(reparented("queue_data")?.params).toEqual([survivor, ghost]);
      expect(reparented("ml_prediction_anomalies")?.params).toEqual([
        survivor,
        ghost,
      ]);

      // Nothing may still be pointing at the ghost when its row goes.
      const deleteIndex = indexOfAttractionDelete(calls);
      expect(deleteIndex).toBeGreaterThan(-1);
      const lastDependency = calls.reduce(
        (last, c, i) => ((c.params ?? []).includes(ghost) ? i : last),
        -1,
      );
      expect(lastDependency).toBeLessThan(deleteIndex);

      // `external_entity_mapping` has no FK, so an orphan here is silent: the
      // wait-times processor loads mappings by the park's live attraction ids
      // and would stop resolving this ride's Queue-Times readings.
      expect(reparented("external_entity_mapping")?.params).toEqual([
        survivor,
        ghost,
      ]);

      // `queue_data` is a hypertable, so the merge moves far more than the
      // default 100000 compressed tuples one statement may decompress. `LOCAL`,
      // so nothing has to reset it and no pooled connection keeps the value.
      const timescale = calls.filter((c) =>
        /max_tuples_decompressed_per_dml_transaction/.test(c.sql),
      );
      expect(timescale.map((c) => c.sql)).toEqual([
        "SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0",
      ]);
    });

    it("moves every declared dependency of the repairDuplicates ghost merge, and writes no attractionId onto prediction_accuracy", async () => {
      const primaryId = "55555555-5555-5555-5555-555555555555";
      const ghostParkId = "66666666-6666-6666-6666-666666666666";
      const primaryAttractions = [
        {
          id: "cccc3333-0000-0000-0000-000000000001",
          slug: "troy",
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
      ];
      const survivor = primaryAttractions[0].id;
      const ghost = ghostAttractions[0].id;

      const { calls } = recordTransaction((sql, params) => {
        if (!/SELECT id, slug/.test(sql)) return [];
        const [parkId] = (params ?? []) as string[];
        return parkId === primaryId ? primaryAttractions : ghostAttractions;
      });

      mockParkRepository.query.mockResolvedValue([
        { queue_times_entity_id: "4711" },
      ]);
      mockParkRepository.find.mockResolvedValue([
        createTestPark({ id: primaryId, wikiEntityId: "wiki-1" }),
        createTestPark({ id: ghostParkId, wikiEntityId: null }),
      ]);

      await service.repairDuplicates();

      // Three tables before; the helper knows the rest.
      const touched = dependencyTablesTouched(calls, ghost);
      for (const dep of ATTRACTION_DEPENDENCIES) {
        expect(touched).toContain(dep.table);
      }

      // The three it did move must still land on the survivor.
      const reparented = (table: string) =>
        calls.find(
          (c) =>
            new RegExp(`UPDATE\\s+${table}\\s+SET`, "i").test(c.sql) &&
            (c.params ?? []).includes(ghost),
        );
      expect(reparented("queue_data")?.params).toEqual([survivor, ghost]);
      expect(reparented("wait_time_predictions")?.params).toEqual([
        survivor,
        ghost,
      ]);
      expect(reparented("prediction_accuracy")?.params).toEqual([
        survivor,
        ghost,
      ]);
      // Moved by this path before the refactor too, now from the shared helper.
      expect(reparented("external_entity_mapping")?.params).toEqual([
        survivor,
        ghost,
      ]);

      // The column is `attraction_id`. The hand-written UPDATE named
      // `attractionId`, which is 42703 on every collision this path handles.
      const predictionAccuracy = calls.filter((c) =>
        /prediction_accuracy/i.test(c.sql),
      );
      expect(predictionAccuracy.length).toBeGreaterThan(0);
      for (const call of predictionAccuracy) {
        expect(call.sql).not.toMatch(/"attractionId"/);
      }

      const deleteIndex = indexOfAttractionDelete(calls);
      expect(deleteIndex).toBeGreaterThan(-1);
      const lastDependency = calls.reduce(
        (last, c, i) =>
          (c.params ?? []).includes(ghost) &&
          !/DELETE\s+FROM\s+attractions/i.test(c.sql)
            ? i
            : last,
        -1,
      );
      expect(lastDependency).toBeLessThan(deleteIndex);
    });

    /**
     * The same failure one level up, and the reason acceptance criterion 4 of
     * the attraction ticket stayed half-met: both paths went straight from the
     * last `UPDATE restaurants` to `manager.delete(Park, ghostPark.id)`.
     *
     * `park_occupancy` declares `@ManyToOne(() => Park)` with no `onDelete`, so
     * that DELETE raises 23503 — one statement later than the attraction DELETE
     * used to, and just as fatal. Where it does not abort, the merge loses
     * things silently instead: the moved rides keep a denormalised `parkId`
     * pointing at the deleted park, and `park_seasons`, `schedule_entries`,
     * `weather_data`, `attraction_rope_drop` and `attraction_typical_waits`
     * cascade away inside a transaction that then reports success.
     *
     * As with the attraction cases above, these prove the statements are
     * issued in the right order against a recording manager. Only a real
     * database can prove the transaction commits.
     */
    const parkDependencyTables = [
      ...PARK_INLINE_DEPENDENCIES.map((d) => d.table),
      ...PARK_DEPENDENCIES.map((d) => d.table),
    ];

    const indexOfParkDelete = (calls: Recorded[]) =>
      calls.findIndex((c) => /DELETE\s+FROM\s+parks/i.test(c.sql));

    const expectParkRowsMovedBeforeTheDelete = (
      calls: Recorded[],
      survivingParkId: string,
      ghostParkId: string,
    ) => {
      const touched = dependencyTablesTouched(calls, ghostParkId);
      for (const table of parkDependencyTables) {
        expect(touched).toContain(table);
      }

      const reparented = (table: string) =>
        calls.find(
          (c) =>
            new RegExp(`UPDATE\\s+${table}\\s+SET`, "i").test(c.sql) &&
            (c.params ?? []).includes(ghostParkId),
        );

      // The one that stops the DELETE outright.
      expect(reparented("park_occupancy")?.params).toEqual([
        survivingParkId,
        ghostParkId,
      ]);
      // The denormalised parkId the attraction merge leaves behind. Read by
      // park id in analytics.service.ts and park-historical-stats.service.ts,
      // so a stale value hides the inherited history rather than erroring.
      expect(reparented("attraction_hourly_history")?.params).toEqual([
        survivingParkId,
        ghostParkId,
      ]);
      expect(reparented("queue_data_aggregates")?.params).toEqual([
        survivingParkId,
        ghostParkId,
      ]);
      expect(reparented("attraction_p90_baselines")?.params).toEqual([
        survivingParkId,
        ghostParkId,
      ]);
      // Cascade-deleted today, and reproducible from no feed.
      expect(reparented("park_seasons")?.params).toEqual([
        survivingParkId,
        ghostParkId,
      ]);
      expect(reparented("park_slug_aliases")?.params).toEqual([
        survivingParkId,
        ghostParkId,
      ]);
      // The open question the ticket left: they move, they do not cascade.
      expect(reparented("attraction_rope_drop")?.params).toEqual([
        survivingParkId,
        ghostParkId,
      ]);
      expect(reparented("attraction_typical_waits")?.params).toEqual([
        survivingParkId,
        ghostParkId,
      ]);
      // Park-level mappings carry no FK, so an orphan survives in silence.
      const mapping = calls.find(
        (c) =>
          /UPDATE\s+external_entity_mapping/i.test(c.sql) &&
          (c.params ?? []).includes(ghostParkId),
      );
      expect(mapping?.sql).toMatch(/internal_entity_type/);
      expect(mapping?.params).toEqual([survivingParkId, ghostParkId]);

      // Nothing may still point at the ghost park when its row goes.
      const deleteIndex = indexOfParkDelete(calls);
      expect(deleteIndex).toBeGreaterThan(-1);
      const lastParkStatement = calls.reduce(
        (last, c, i) =>
          (c.params ?? []).includes(ghostParkId) &&
          !/DELETE\s+FROM\s+parks/i.test(c.sql)
            ? i
            : last,
        -1,
      );
      expect(lastParkStatement).toBeLessThan(deleteIndex);
    };

    it("moves every park-scoped row of the sync-time ghost merge before the park is deleted", async () => {
      const attractionRows = [
        {
          id: "aaaa1111-0000-0000-0000-000000000001",
          parkId: syncSurvivingParkId,
          slug: "taron",
          queue_times_entity_id: null,
          land_name: null,
          land_external_id: null,
        },
        {
          id: "bbbb2222-0000-0000-0000-000000000001",
          parkId: syncGhostParkId,
          slug: "taron",
          queue_times_entity_id: "4711",
          land_name: "Klugheim",
          land_external_id: "land-klugheim",
        },
        {
          // Moves across without colliding — the case that carries a stale
          // parkId into every one of its dependent tables.
          id: "bbbb2222-0000-0000-0000-000000000002",
          parkId: syncGhostParkId,
          slug: "chiapas",
          queue_times_entity_id: "4712",
          land_name: "Mexico",
          land_external_id: "land-mexico",
        },
      ];

      const { calls } = primeGhostParkSync(attractionRows);

      await service.syncParks();

      expectParkRowsMovedBeforeTheDelete(
        calls,
        syncSurvivingParkId,
        syncGhostParkId,
      );

      // The park step runs whether or not anything collided, and
      // `park_occupancy` is a hypertable, so the decompression cap belongs to
      // the transaction rather than to the collision path. Still exactly once.
      const timescale = calls.filter((c) =>
        /max_tuples_decompressed_per_dml_transaction/.test(c.sql),
      );
      expect(timescale.map((c) => c.sql)).toEqual([
        "SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0",
      ]);
    });

    it("moves every park-scoped row of the repairDuplicates ghost merge before the park is deleted", async () => {
      const primaryId = "77777777-7777-7777-7777-777777777777";
      const ghostParkId = "88888888-8888-8888-8888-888888888888";
      const primaryAttractions = [
        {
          id: "cccc3333-0000-0000-0000-000000000001",
          slug: "troy",
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

      const { calls } = recordTransaction((sql, params) => {
        if (!/SELECT id, slug/.test(sql)) return [];
        const [parkId] = (params ?? []) as string[];
        return parkId === primaryId ? primaryAttractions : ghostAttractions;
      });

      mockParkRepository.query.mockResolvedValue([
        { queue_times_entity_id: "4711" },
      ]);
      mockParkRepository.find.mockResolvedValue([
        createTestPark({ id: primaryId, wikiEntityId: "wiki-1" }),
        createTestPark({ id: ghostParkId, wikiEntityId: null }),
      ]);

      await service.repairDuplicates();

      expectParkRowsMovedBeforeTheDelete(calls, primaryId, ghostParkId);
    });

    it("keeps the survivor's own park_p50_baseline and inherits the ghost's only when it has none", async () => {
      // One row per park, and it is load-bearing: live crowd levels and an ML
      // feature both read it. The survivor's own always wins, but discarding
      // the ghost's where the survivor has none would rate the park `unknown`
      // until the next baseline run for no reason.
      const primaryId = "99999999-9999-9999-9999-999999999999";
      const ghostParkId = "aaaaaaaa-9999-9999-9999-999999999999";
      const primaryAttractions = [
        { id: "eeee5555-0000-0000-0000-000000000001", slug: "troy" },
      ];

      const runWith = async (survivorHasBaseline: boolean) => {
        const { calls } = recordTransaction((sql, params) => {
          if (/FROM park_p50_baselines/.test(sql)) {
            return survivorHasBaseline ? [{ "?column?": 1 }] : [];
          }
          if (!/SELECT id, slug/.test(sql)) return [];
          const [parkId] = (params ?? []) as string[];
          return parkId === primaryId ? primaryAttractions : [];
        });

        mockParkRepository.query.mockResolvedValue([
          { queue_times_entity_id: "4711" },
        ]);
        mockParkRepository.find.mockResolvedValue([
          createTestPark({ id: primaryId, wikiEntityId: "wiki-1" }),
          createTestPark({ id: ghostParkId, wikiEntityId: null }),
        ]);

        await service.repairDuplicates();
        return calls.filter((c) => /park_p50_baselines/i.test(c.sql));
      };

      const withBaseline = await runWith(true);
      expect(withBaseline.map((c) => c.sql.trim().split(/\s+/)[0])).toEqual([
        "SELECT",
        "DELETE",
      ]);
      expect(withBaseline[1].params).toEqual([ghostParkId]);

      const withoutBaseline = await runWith(false);
      expect(withoutBaseline.map((c) => c.sql.trim().split(/\s+/)[0])).toEqual([
        "SELECT",
        "UPDATE",
      ]);
      expect(withoutBaseline[1].params).toEqual([primaryId, ghostParkId]);
    });
  });
});
