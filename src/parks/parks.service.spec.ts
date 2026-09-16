import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { ParksService } from "./parks.service";
import { Park } from "./entities/park.entity";
import { ScheduleEntry } from "./entities/schedule-entry.entity";
import { ThemeParksClient } from "../external-apis/themeparks/themeparks.client";
import { ThemeParksMapper } from "../external-apis/themeparks/themeparks.mapper";
import { DestinationsService } from "../destinations/destinations.service";
import { CacheKeys } from "../common/cache/cache-keys";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { RevalidationService } from "../common/revalidation/revalidation.service";
import { HolidaysService } from "../holidays/holidays.service";
import { createTestPark } from "../../test/fixtures/park.fixtures";
import {
  formatInParkTimezone,
  getCurrentDateInTimezone,
} from "../common/utils/date.util";
import {
  ATTRACTION_DEPENDENCIES,
  PARK_DEPENDENCIES,
  PARK_INLINE_DEPENDENCIES,
  RESTAURANT_DEPENDENCIES,
  SHOW_DEPENDENCIES,
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
    mget: jest.fn().mockResolvedValue([]),
  };

  // Mock repositories
  const mockParkRepository = {
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
    update: jest.fn(),
    count: jest.fn(),
    delete: jest.fn(),
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
    count: jest.fn(),
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

  const mockRevalidationService = {
    revalidateTags: jest.fn().mockResolvedValue(true),
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
        {
          provide: RevalidationService,
          useValue: mockRevalidationService,
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

  // `schedule_entries.date` is a DATE column that TypeORM hands back as
  // "YYYY-MM-DD". Every cache branch used to rebuild it with `new Date(...)`,
  // i.e. midnight UTC, and `ParkIntegrationService` then compared THAT against
  // the park's own today — one day early for every park west of Greenwich, and
  // only while the cache was warm. See PAR-285.
  describe("the schedule cache hands back the day the database hands back", () => {
    const parkId = "11111111-2222-3333-4444-555555555555";

    /** One park each side of Greenwich: the bug only shows on the west one. */
    const ZONES = [
      ["America/Los_Angeles", "negative UTC offset"],
      ["Asia/Tokyo", "positive UTC offset"],
    ] as const;

    /**
     * The row as TypeORM yields it: `date` a date-only string, the two
     * timestamps real `Date`s. Building the cached payload out of THIS, via
     * the service's own `redis.set`, is what keeps the case honest — the
     * fixture is never hand-written JSON that happens to agree.
     */
    const dbRow = (timezone: string) => ({
      id: "row-1",
      parkId,
      attractionId: null,
      date: getCurrentDateInTimezone(timezone),
      scheduleType: "OPERATING",
      openingTime: new Date("2026-09-16T16:00:00.000Z"),
      closingTime: new Date("2026-09-17T04:00:00.000Z"),
      description: null,
      purchases: null,
    });

    /** Everything the service reads on the way to a schedule query. */
    const arrange = (timezone: string, rows: unknown[]) => {
      mockParkRepository.findOne.mockResolvedValue({ id: parkId, timezone });
      mockParkRepository.find.mockResolvedValue([{ id: parkId, timezone }]);
      mockScheduleRepository.createQueryBuilder.mockImplementation(
        () =>
          ({
            where: jest.fn().mockReturnThis(),
            andWhere: jest.fn().mockReturnThis(),
            orderBy: jest.fn().mockReturnThis(),
            addOrderBy: jest.fn().mockReturnThis(),
            limit: jest.fn().mockReturnThis(),
            getMany: jest.fn().mockResolvedValue(rows),
            getOne: jest.fn().mockResolvedValue(rows[0] ?? null),
          }) as never,
      );
    };

    /** The string the service last wrote under `key`. */
    const cachedPayload = (key: string): string => {
      const write = [...mockRedis.set.mock.calls]
        .reverse()
        .find((call) => String(call[0]).includes(key));
      if (!write) throw new Error(`nothing was cached under *${key}*`);
      return write[1] as string;
    };

    /**
     * The four methods that deserialise a cached schedule row, each reduced to
     * "give me one entry's `date`". `cacheKey` is the fragment the payload is
     * written under, so the hit run replays the miss run's own bytes.
     */
    const METHODS = [
      {
        name: "getTodaySchedule",
        cacheKey: "schedule:today",
        call: async (tz: string) =>
          (await service.getTodaySchedule(parkId, tz))[0]?.date,
      },
      {
        name: "getNextSchedule",
        cacheKey: "schedule:next",
        call: async () => (await service.getNextSchedule(parkId))?.date,
      },
      {
        name: "getUpcomingSchedule",
        cacheKey: "schedule:upcoming",
        call: async () =>
          (await service.getUpcomingSchedule(parkId, 7))[0]?.date,
      },
      {
        name: "getBatchSchedules",
        cacheKey: "schedule:today",
        call: async () =>
          (await service.getBatchSchedules([parkId])).today.get(parkId)?.[0]
            ?.date,
      },
    ] as const;

    beforeEach(() => {
      mockRedis.set.mockResolvedValue("OK");
      mockRedis.get.mockResolvedValue(null);
      // `mockReset` and not just `mockResolvedValue`: `jest.clearAllMocks()`
      // drops recorded calls but keeps an unconsumed `mockResolvedValueOnce`
      // queue, and the cases below queue per-call answers for the two `mget`s
      // in `getBatchSchedules`. A leftover from an earlier case would be served
      // to the next one. Same trap the two blocks further down call out.
      mockRedis.mget.mockReset();
      mockRedis.mget.mockResolvedValue([null, null]);
    });

    afterEach(() => {
      mockScheduleRepository.createQueryBuilder.mockImplementation(() =>
        scheduleQueryBuilder(),
      );
      mockParkRepository.findOne.mockReset();
      mockParkRepository.find.mockReset();
      mockRedis.mget.mockResolvedValue([]);
    });

    describe.each(ZONES)("in %s (%s)", (timezone) => {
      it.each(METHODS.map((m) => [m.name, m] as const))(
        "%s: a cache hit answers with the same day as a cache miss",
        async (_name, method) => {
          const row = dbRow(timezone);
          arrange(timezone, [row]);

          // 1. Cache miss. This also produces the bytes Redis will hold.
          const fromDatabase = await method.call(timezone);
          expect(fromDatabase).toBe(row.date);

          const payload = cachedPayload(method.cacheKey);
          // The write side was never the bug, and the hit run below is only
          // worth anything if the payload really is the date-only string.
          expect(payload).toContain(`"date":"${row.date}"`);

          // 2. Cache hit on exactly those bytes, with the database unplugged:
          // an entry that came from the query instead of from Redis would make
          // the comparison below vacuous.
          arrange(timezone, []);
          mockRedis.get.mockResolvedValue(payload);
          // `getBatchSchedules` calls mget twice — today first, then next.
          // One answer for both would feed the "next" slot an array, which is
          // not the shape that branch reads.
          mockRedis.mget
            .mockResolvedValueOnce([payload])
            .mockResolvedValueOnce(["null"]);

          const fromCache = await method.call(timezone);

          expect(fromCache).toBe(fromDatabase);
        },
      );

      it.each(METHODS.map((m) => [m.name, m] as const))(
        "%s: the day a cached row reports is the day a reader asks about",
        async (_name, method) => {
          const row = dbRow(timezone);
          arrange(timezone, [row]);
          await method.call(timezone);

          const payload = cachedPayload(method.cacheKey);
          arrange(timezone, []);
          mockRedis.get.mockResolvedValue(payload);
          mockRedis.mget
            .mockResolvedValueOnce([payload])
            .mockResolvedValueOnce(["null"]);

          const date = await method.call(timezone);

          // This is the comparison `ParkIntegrationService` makes to pick
          // today's row. With `new Date(date)` in the cache branch every row
          // reads a day early in Los Angeles, so the row it matches is
          // tomorrow's — and tomorrow's status, rope-drop window and closing
          // time are what the park page then shows. Only past the last
          // published row does the match come up empty.
          expect(formatInParkTimezone(date as never, timezone)).toBe(
            getCurrentDateInTimezone(timezone),
          );
        },
      );
    });

    // `new Date(null)`, `new Date(0)` and `new Date(false)` are the epoch, not
    // an Invalid Date, so a coercing reader turns an absent day into
    // 1970-01-01 and hands back a row that quietly matches nothing. None of
    // these can reach Redis today — `date` is NOT NULL and every writer
    // stringifies a database row — so what is pinned here is the rule, not a
    // population: a value that does not name a day is a cache miss.
    it.each([
      ["a missing date", undefined],
      ["a null date", null],
      ["a numeric date", 0],
      ["a boolean date", false],
      ["an empty string", ""],
      ["a string that is not a date at all", "not-a-date"],
      ["a day with a text suffix", "2026-09-16-ish"],
    ])(
      "treats %s in a cached row as a miss and rebuilds from the database",
      async (_label, date) => {
        const row = dbRow("America/Los_Angeles");
        arrange("America/Los_Angeles", [row]);
        mockRedis.get.mockResolvedValue(
          JSON.stringify([{ ...row, date, openingTime: null }]),
        );

        const result = await service.getTodaySchedule(
          parkId,
          "America/Los_Angeles",
        );

        // The database row, not 1970-01-01 and not a throw.
        expect(result).toHaveLength(1);
        expect(result[0].date).toBe(row.date);
      },
    );

    // The opposite of the cases above, and the reason they say "names no day"
    // rather than "is not a plain date": a full ISO timestamp DOES name one,
    // and reading the UTC day off it is the inverse of the `toISOString` that
    // would have written it — a hit, not a miss, and no rebuild.
    //
    // No writer produces this shape, here or historically: all five `redis.set`
    // calls stringify a database row, and `date` is a DATE column that TypeORM
    // yields as a string. What is pinned is the tolerance itself, so that
    // narrowing the accepted shape later has to break a test rather than a
    // deploy.
    it("accepts a cached ISO timestamp as the day it names", async () => {
      const row = dbRow("America/Los_Angeles");
      arrange("America/Los_Angeles", []);
      mockRedis.get.mockResolvedValue(
        JSON.stringify([{ ...row, date: `${row.date}T00:00:00.000Z` }]),
      );

      const result = await service.getTodaySchedule(
        parkId,
        "America/Los_Angeles",
      );

      expect(result[0].date).toBe(row.date);
      // Served from the cache: no rebuild was needed.
      expect(mockScheduleRepository.createQueryBuilder).not.toHaveBeenCalled();
    });

    // `getBatchSchedules` keeps two refetch lists, and an unreadable row has to
    // land in its own. Pushing to the other one leaves the park with no entry
    // at all rather than a rebuilt one — and no assertion above notices,
    // because both lists lead to a query.
    it("refetches the day from the database when the cached today row names no day", async () => {
      const row = dbRow("America/Los_Angeles");
      arrange("America/Los_Angeles", [row]);
      mockRedis.mget
        .mockResolvedValueOnce([JSON.stringify([{ ...row, date: null }])])
        .mockResolvedValueOnce(["null"]);

      const { today, next } = await service.getBatchSchedules([parkId]);

      // Rebuilt from the database, not dropped and not 1970-01-01.
      expect(today.get(parkId)).toHaveLength(1);
      expect(today.get(parkId)?.[0].date).toBe(row.date);
      // And the cache is healed, so the next request does not pay for it again.
      expect(
        mockRedis.set.mock.calls.some((c) =>
          String(c[0]).startsWith("schedule:today:"),
        ),
      ).toBe(true);
      // The `next` slot was a clean negative and must not have been disturbed.
      expect(next.get(parkId)).toBeNull();
    });

    it("refetches the next opening from the database when its cached row names no day", async () => {
      const row = dbRow("America/Los_Angeles");
      arrange("America/Los_Angeles", [row]);
      mockRedis.mget
        .mockResolvedValueOnce([JSON.stringify([row])])
        .mockResolvedValueOnce([JSON.stringify({ ...row, date: null })]);

      const { next } = await service.getBatchSchedules([parkId]);

      expect(next.get(parkId)?.date).toBe(row.date);
      expect(
        mockRedis.set.mock.calls.some((c) =>
          String(c[0]).startsWith("schedule:next:"),
        ),
      ).toBe(true);
    });

    it("keeps a cached `null` as the negative result it is, not as a corrupt entry", async () => {
      arrange("America/Los_Angeles", []);
      mockRedis.get.mockResolvedValue("null");

      // "no upcoming operating day" is an answer, and re-querying for it on
      // every request is what the negative cache exists to avoid.
      await expect(service.getNextSchedule(parkId)).resolves.toBeNull();
      expect(mockScheduleRepository.createQueryBuilder).not.toHaveBeenCalled();
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
   * A source that publishes a 12-hour clock unlabelled reports a midnight close
   * as `12:00`. Six Flags Qiddiya City does: `opens 15:00 / closes 12:00`, five
   * days in 2026, which the generic repair turns into a 21-hour operating day —
   * right during the evening, wrong for the eleven hours afterwards.
   *
   * `correctTwelveHourClockClose` is unit-tested on its own; what these cover is
   * the gate, because the gate is the whole design. The same `12:00` means two
   * different things depending on one curated column, and a park nobody flagged
   * has to come out of `saveScheduleData` byte-identical.
   */
  describe("saveScheduleData — the 12-hour-clock override", () => {
    const parkId = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

    // Verbatim shape of the Qiddiya rows: opens 15:00, closes 12:00 SAME day.
    const qiddiyaEntry = {
      date: "2026-04-17",
      type: "OPERATING",
      openingTime: "2026-04-17T15:00:00+03:00", // 12:00Z
      closingTime: "2026-04-17T12:00:00+03:00", // 09:00Z — three hours before opening
    };
    const QIDDIYA_OPENS = "2026-04-17T12:00:00.000Z";
    /** 00:00 Riyadh on the 18th: a nine-hour evening. */
    const READ_AS_MIDNIGHT = "2026-04-17T21:00:00.000Z";
    /** What the generic repair alone produces: 12:00 Riyadh on the 18th. */
    const RE_ANCHORED = "2026-04-18T09:00:00.000Z";

    function aRiyadhPark(usesTwelveHourClock: boolean | null) {
      return {
        id: parkId,
        countryCode: "SA",
        regionCode: null,
        timezone: "Asia/Riyadh",
        curatedUsesTwelveHourClock: usesTwelveHourClock,
      };
    }

    function savedEntry() {
      expect(mockScheduleRepository.save).toHaveBeenCalledTimes(1);
      const [inserted] = mockScheduleRepository.save.mock.calls[0] as [
        Array<{ openingTime: Date; closingTime: Date }>,
      ];
      return inserted[0];
    }

    beforeEach(() => {
      mockHolidaysService.getHolidays.mockResolvedValue([]);
      mockScheduleRepository.save.mockResolvedValue([]);
      mockScheduleRepository.query.mockResolvedValue([]);
      mockScheduleRepository.createQueryBuilder.mockImplementation(() =>
        scheduleQueryBuilder([]),
      );
    });

    it("reads a flagged park's noon close as midnight", async () => {
      mockParkRepository.findOne.mockResolvedValue(aRiyadhPark(true));

      await service.saveScheduleData(parkId, [qiddiyaEntry]);

      const entry = savedEntry();
      expect(entry.openingTime.toISOString()).toBe(QIDDIYA_OPENS);
      expect(entry.closingTime.toISOString()).toBe(READ_AS_MIDNIGHT);
      expect(entry.closingTime.getTime() - entry.openingTime.getTime()).toBe(
        9 * 60 * 60 * 1000,
      );
    });

    it("leaves the same row to the generic repair when the park is not flagged", async () => {
      mockParkRepository.findOne.mockResolvedValue(aRiyadhPark(null));

      await service.saveScheduleData(parkId, [qiddiyaEntry]);

      const entry = savedEntry();
      expect(entry.closingTime.toISOString()).toBe(RE_ANCHORED);
      // 21 h — the state this ticket describes, deliberately unchanged for
      // every park nobody has written the flag on.
      expect(entry.closingTime.getTime() - entry.openingTime.getTime()).toBe(
        21 * 60 * 60 * 1000,
      );
    });

    it("leaves a genuine noon closing alone even on a flagged park", async () => {
      // The flag says the source misprints midnight, not that noon never
      // happens. A half-day that closes at 12:00 AFTER opening is a real one,
      // and this is the case a blanket "reinterpret 12:00" rule would destroy.
      mockParkRepository.findOne.mockResolvedValue(aRiyadhPark(true));

      await service.saveScheduleData(parkId, [
        {
          date: "2026-04-18",
          type: "OPERATING",
          openingTime: "2026-04-18T09:00:00+03:00", // 06:00Z
          closingTime: "2026-04-18T12:00:00+03:00", // 09:00Z — a three-hour morning
        },
      ]);

      const entry = savedEntry();
      expect(entry.openingTime.toISOString()).toBe("2026-04-18T06:00:00.000Z");
      expect(entry.closingTime.toISOString()).toBe("2026-04-18T09:00:00.000Z");
    });
  });

  /**
   * The park's opening hours and a ride's own schedule share `schedule_entries`,
   * told apart by `attractionId` — NULL on the park-level row. Every row these
   * two methods write is built from `parkId` alone, so both may only read and
   * delete park-level rows. Neither said so, and each had its own way of going
   * wrong: the sync would hand a ride's row the park's opening times, and the
   * gap-fill would let a ride's row occupy the day and then promote or demote it
   * by the PARK's operating range.
   *
   * Every case here fails if the filter leaves its statement.
   */
  describe("the schedule sync and the gap-fill speak only about park-level rows", () => {
    const parkId = "cccccccc-dddd-eeee-ffff-000000000000";

    type RecordedBuilder = {
      conditions: string[];
      kind: "read" | "delete" | "other";
      inserted: Array<Record<string, unknown>>;
      updatedIds: string[];
    };

    let builders: RecordedBuilder[];

    /** Records every `andWhere` and what the chain was eventually used for. */
    const recordingBuilder = (
      rows: unknown[] = [],
      rawOne: unknown = { minDate: null, maxDate: null },
    ) => {
      const recorded: RecordedBuilder = {
        conditions: [],
        kind: "other",
        inserted: [],
        updatedIds: [],
      };
      builders.push(recorded);

      const mark = (kind: RecordedBuilder["kind"]) => () => {
        recorded.kind = kind;
      };

      const builder: Record<string, jest.Mock> = {
        select: jest.fn().mockReturnThis(),
        addSelect: jest.fn().mockReturnThis(),
        where: jest.fn().mockReturnThis(),
        andWhere: jest.fn((condition: string) => {
          recorded.conditions.push(condition);
          return builder;
        }),
        orderBy: jest.fn().mockReturnThis(),
        addOrderBy: jest.fn().mockReturnThis(),
        insert: jest.fn().mockReturnThis(),
        into: jest.fn().mockReturnThis(),
        values: jest.fn((rows: Array<Record<string, unknown>>) => {
          recorded.inserted.push(...(Array.isArray(rows) ? rows : [rows]));
          return builder;
        }),
        update: jest.fn().mockReturnThis(),
        set: jest.fn().mockReturnThis(),
        whereInIds: jest.fn((ids: string[]) => {
          recorded.updatedIds.push(...ids);
          return builder;
        }),
        delete: jest.fn(() => {
          mark("delete")();
          return builder;
        }),
        from: jest.fn().mockReturnThis(),
        getMany: jest.fn(async () => {
          mark("read")();
          return rows;
        }),
        getRawMany: jest.fn(async () => {
          mark("read")();
          return rows;
        }),
        limit: jest.fn().mockReturnThis(),
        getOne: jest.fn(async () => {
          mark("read")();
          return null;
        }),
        getRawOne: jest.fn().mockResolvedValue(rawOne),
        execute: jest.fn().mockResolvedValue({ affected: 0 }),
      };

      return builder;
    };

    const RIDE_FILTER = /attractionId" IS NULL|attractionId IS NULL/;

    // `jest.clearAllMocks()` drops calls, not implementations, and this block
    // replaces three of them — the trap the `last_merged_at` block below closes
    // the same way.
    afterEach(() => {
      mockScheduleRepository.createQueryBuilder.mockImplementation(() =>
        scheduleQueryBuilder(),
      );
      mockParkRepository.findOne.mockReset();
      mockParkRepository.find.mockReset();
      mockScheduleRepository.findOne.mockReset();
    });

    const conditionsOf = (kind: RecordedBuilder["kind"]): string[][] =>
      builders.filter((b) => b.kind === kind).map((b) => b.conditions);

    beforeEach(() => {
      builders = [];
      mockRedis.get.mockResolvedValue(null);
      mockRedis.set.mockResolvedValue("OK");
      mockRedis.keys.mockResolvedValue([]);
      mockHolidaysService.getHolidays.mockResolvedValue([]);
      mockScheduleRepository.save.mockResolvedValue([]);
      mockScheduleRepository.query.mockResolvedValue([]);
      mockScheduleRepository.createQueryBuilder.mockImplementation(
        () =>
          recordingBuilder([]) as unknown as ReturnType<
            typeof scheduleQueryBuilder
          >,
      );
      mockParkRepository.findOne.mockResolvedValue({
        id: parkId,
        countryCode: "DE",
        regionCode: null,
        timezone: "Europe/Berlin",
      });
    });

    it("looks up the row it is about to overwrite among park-level rows only", async () => {
      await service.saveScheduleData(parkId, [
        {
          date: "2026-07-28",
          type: "OPERATING",
          openingTime: "2026-07-28T09:00:00+02:00",
          closingTime: "2026-07-28T18:00:00+02:00",
        },
      ]);

      const reads = conditionsOf("read");
      expect(reads).toHaveLength(1);
      // Without this the key is `date|scheduleType`, which a ride's row of the
      // same day and type answers just as well — and it would then be updated
      // with the park's opening hours while staying attached to its ride.
      expect(reads[0].some((c) => RIDE_FILTER.test(c))).toBe(true);
    });

    it("deletes the placeholders of the park, never a ride's row for that day", async () => {
      await service.saveScheduleData(parkId, [
        {
          date: "2026-07-28",
          type: "OPERATING",
          openingTime: "2026-07-28T09:00:00+02:00",
          closingTime: "2026-07-28T18:00:00+02:00",
        },
        {
          date: "2026-07-29",
          type: "CLOSED",
          openingTime: null,
          closingTime: null,
        },
      ]);

      const deletes = conditionsOf("delete");
      // An OPERATING day clears UNKNOWN and CLOSED placeholders, a CLOSED day
      // clears UNKNOWN and OPERATING — three statements for this payload.
      expect(deletes).toHaveLength(3);
      for (const conditions of deletes) {
        expect(conditions.some((c) => RIDE_FILTER.test(c))).toBe(true);
      }
    });

    it.each([
      [
        "getSchedule",
        (id: string) =>
          service.getSchedule(
            id,
            new Date("2026-07-01"),
            new Date("2026-07-31"),
          ),
      ],
      [
        "getScheduleForDate",
        (id: string) => service.getScheduleForDate(id, "2026-07-28"),
      ],
      ["getNextSchedule", (id: string) => service.getNextSchedule(id)],
      ["getBatchSchedules", (id: string) => service.getBatchSchedules([id])],
    ] as const)(
      "%s asks about the park, so it reads park-level rows only",
      async (_name, call) => {
        mockParkRepository.find.mockResolvedValue([
          { id: parkId, timezone: "Europe/Berlin" },
        ]);

        await call(parkId);

        const reads = conditionsOf("read");
        expect(reads.length).toBeGreaterThan(0);
        for (const conditions of reads) {
          expect(conditions.some((c) => RIDE_FILTER.test(c))).toBe(true);
        }
      },
    );

    it.each([
      ["isParkCurrentlyOpen", (id: string) => service.isParkCurrentlyOpen(id)],
      [
        "isParkOperatingToday",
        (id: string) => service.isParkOperatingToday(id),
      ],
    ] as const)(
      "%s asks for the park's own row, not whichever row comes back first",
      async (_name, call) => {
        // A CLOSED row with no times ends both methods on their first branch,
        // which keeps the case about the `where` clause and nothing else.
        mockScheduleRepository.findOne.mockResolvedValue({
          scheduleType: "CLOSED",
          openingTime: null,
          closingTime: null,
        });

        await call(parkId);

        const calls = mockScheduleRepository.findOne.mock.calls;
        const [options] = calls[calls.length - 1] as [
          { where: Record<string, unknown> },
        ];
        // Neither call has an ORDER BY, so without this the plan decides which
        // row answers for the park — and a ride's row is a valid candidate as
        // soon as the cleanup stops deleting it.
        expect(options.where).toHaveProperty("attractionId");
      },
    );

    it("keys a row on its own day, not on the day before it west of Greenwich", async () => {
      // TypeORM hands a DATE column back as "YYYY-MM-DD". Reading that as
      // `new Date(str)` gives UTC midnight, and formatting THAT in a park west
      // of Greenwich answers with the previous day — so the park's row for the
      // 16th would be filed under the 15th, the 16th would look like a gap, and
      // the promotion and demotion would land on the neighbouring day.
      mockParkRepository.findOne.mockResolvedValue({
        id: parkId,
        countryCode: "US",
        regionCode: null,
        timezone: "America/Los_Angeles",
      });

      const occupied = getCurrentDateInTimezone("America/Los_Angeles");
      mockScheduleRepository.createQueryBuilder.mockImplementation(
        () =>
          recordingBuilder([
            {
              id: "55555555-6666-7777-8888-999999999999",
              parkId,
              attractionId: null,
              // As the driver delivers it.
              date: occupied,
              scheduleType: "OPERATING",
              description: null,
              isHoliday: false,
              holidayName: null,
              isBridgeDay: false,
            },
          ]) as unknown as ReturnType<typeof scheduleQueryBuilder>,
      );

      await service.fillScheduleGaps(parkId, 1, 1);

      const inserted = builders
        .flatMap((b) => b.inserted)
        .map((row) => formatInParkTimezone(row.date as Date, "UTC"));

      // The park's own day is taken; only its two neighbours are gaps.
      expect(inserted).not.toContain(occupied);
      expect(inserted).toHaveLength(2);
    });

    it("writes no gap-filled row onto a day that only a ride has a row for", async () => {
      // The one day in the window that carries a per-ride row and nothing else —
      // what a park-day looks like after the old cleanup deleted the park's own
      // row. `fillScheduleGaps` may neither rewrite that ride's row (it would be
      // promoted or demoted by the PARK's operating range) nor declare the day
      // CLOSED, which is a confident claim built from an absence and which the
      // analytics joins read as "drop this day from P50/P90".
      const occupied = getCurrentDateInTimezone("Europe/Berlin");
      const noon = (offsetDays: number) =>
        new Date(
          new Date(`${occupied}T12:00:00Z`).getTime() +
            offsetDays * 24 * 60 * 60 * 1000,
        );
      // An operating range that brackets the day, so `isGapClosed` is true for
      // it: that is what turns a gap into CLOSED, and what would promote the
      // ride's UNKNOWN row if it were mistaken for the park's.
      const operatingRange = {
        minDate: formatInParkTimezone(noon(-5), "Europe/Berlin"),
        maxDate: formatInParkTimezone(noon(5), "Europe/Berlin"),
      };
      mockScheduleRepository.createQueryBuilder.mockImplementation(
        () =>
          recordingBuilder(
            [
              {
                id: "77777777-8888-9999-aaaa-bbbbbbbbbbbb",
                parkId,
                attractionId: "aaaaaaaa-0000-0000-0000-000000000001",
                date: new Date(`${occupied}T12:00:00Z`),
                scheduleType: "UNKNOWN",
                description: null,
                isHoliday: false,
                holidayName: null,
                isBridgeDay: false,
              },
            ],
            operatingRange,
          ) as unknown as ReturnType<typeof scheduleQueryBuilder>,
      );

      await service.fillScheduleGaps(parkId, 1, 1);

      const inserted = builders
        .flatMap((b) => b.inserted)
        .map((row) => formatInParkTimezone(row.date as Date, "Europe/Berlin"));

      // The two neighbouring days are genuine gaps and still get their row.
      expect(inserted.length).toBeGreaterThan(0);
      expect(inserted).not.toContain(occupied);
      // And the ride's UNKNOWN row was not promoted to CLOSED by the park's
      // own operating range, which brackets the day.
      expect(builders.flatMap((b) => b.updatedIds)).toEqual([]);
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
    /**
     * The two reads the `winner-authoritative` branch makes before it decides
     * what happens to a losing ride's curated profile, answered for every case
     * below: the loser HAS one and the survivor has none.
     *
     * That is the case the strategy exists for — the profile is hand-written,
     * reproducible from no feed, and the FK is ON DELETE CASCADE — so it is the
     * one worth wiring into the shared harness rather than into one case. It is
     * also what keeps `attraction_ride_profiles` visible to
     * `dependencyTablesTouched`: with no losing row the branch correctly issues
     * no write at all, and a table the merge touched only by reading is
     * indistinguishable there from one it forgot.
     *
     * Which of the two reads is which is decided by their ORDER, not by the
     * SQL: since PAR-179 the entry ranks rows, so both sides are read with the
     * same `SELECT *` and the branch reads the loser first, every time. Hence
     * the counter — a stateless answer would hand the survivor a profile too
     * and turn every case here into a collision, which is a different test.
     */
    const curatedRideProfileReader = () => {
      let read = 0;
      return (sql: string, params?: unknown[]): unknown[] | undefined => {
        if (/SELECT \* FROM attraction_ride_profiles/i.test(sql)) {
          const isLoser = read++ % 2 === 0;
          return isLoser
            ? [
                {
                  attractionId: params?.[0],
                  elements: ["lifthill", "vertical-loop"],
                  types: ["launch-coaster"],
                },
              ]
            : [];
        }
        if (/SELECT 1 FROM attraction_ride_profiles/i.test(sql)) return [];
        return undefined;
      };
    };

    /**
     * The same arrangement for review marks, and for the same reason: the
     * custom branch opens with a `SELECT 1 … LIMIT 1` and returns having
     * written nothing when the loser carries no mark — which is the ordinary
     * case in production and an invisible one here, because
     * `dependencyTablesTouched` reads writes.
     *
     * So every case below runs with the loser holding a mark. Stateless, unlike
     * the profile reader: this gate asks one question about one id, and the
     * statements after it are the same whatever the winner holds.
     *
     * Anchored at the start of the statement, which is not pedantry: the same
     * words occur again inside the second DELETE, as `EXISTS (SELECT 1 FROM
     * attraction_review_marks AS w …)`. An unanchored match answered that
     * statement too — harmlessly, since the SQL is recorded before the reader
     * runs, but a helper whose comment says it asks one question while it
     * answers two is the kind of thing a later case builds on.
     */
    const reviewMarkReader = (sql: string): unknown[] | undefined =>
      /^\s*SELECT 1 FROM attraction_review_marks/i.test(sql)
        ? [{ "?column?": 1 }]
        : undefined;

    const recordTransaction = (
      rowsFor: (sql: string, params?: unknown[]) => unknown[],
    ) => {
      const calls: Recorded[] = [];
      const curatedRideProfileReads = curatedRideProfileReader();
      const transactionalEntityManager = {
        query: jest.fn(async (sql: string, params?: unknown[]) => {
          calls.push({ sql, params });
          return (
            curatedRideProfileReads(sql, params) ??
            reviewMarkReader(sql) ??
            rowsFor(sql, params)
          );
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
     * Rows for the child-entity SELECT of `migrateParkChildEntities`, keyed by
     * table. Its statement reads `SELECT id, "parkId", slug FROM <table>`,
     * which the attraction matcher below must not answer — that one selects
     * three more columns and is told apart by them rather than by the table
     * name, so the two regexes stay disjoint.
     */
    type ChildRow = { id: string; parkId: string; slug: string };
    const childRowsFor =
      (rowsByTable: { shows?: ChildRow[]; restaurants?: ChildRow[] }) =>
      (sql: string) => {
        const table = /SELECT id, "parkId", slug FROM (\w+)/.exec(sql)?.[1];
        return table ? (rowsByTable[table as "shows"] ?? []) : undefined;
      };

    /**
     * Wires up the mocks `syncParks` needs to reach its ghost-park merge.
     *
     * `attractionRows` answers the attraction SELECT inside the transaction and
     * `options.shows` / `options.restaurants` the shows/restaurants one.
     *
     * `options.survivor` / `options.ghost` override the two park rows. The
     * ghost's default is the shape the merge cases below need and nothing more
     * — an id and a name — which is also the honest default for step 5c: a row
     * with no slugs has no path to preserve, so those cases neither expect nor
     * get an alias.
     */
    const primeGhostParkSync = (
      attractionRows: unknown[],
      options: {
        shows?: ChildRow[];
        restaurants?: ChildRow[];
        survivor?: Partial<Park>;
        ghost?: Partial<Park>;
      } = {},
    ) => {
      const children = childRowsFor(options);
      const { calls, transactionalEntityManager } = recordTransaction((sql) => {
        if (/SELECT id, "parkId", slug, "queue_times_entity_id"/.test(sql)) {
          return attractionRows;
        }
        return children(sql) ?? [];
      });

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
        createTestPark({
          id: syncSurvivingParkId,
          externalId: "ext-park-1",
          ...options.survivor,
        }),
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
          ...options.ghost,
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

      // And the curated ride profile of the ghost, which the survivor has none
      // of: it is inherited rather than cascaded away with the DELETE below.
      // Nothing in this codebase writes that row, so there is no second copy
      // and no job that would rebuild it.
      expect(reparented("attraction_ride_profiles")?.params).toEqual([
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
     * Shows and restaurants carry the same unique `(parkId, slug)` as
     * attractions, and both raw paths moved them blind — under a comment that
     * said so out loud: "Blind update OK if slugs distinctive, else duplicate
     * logic needed? mostly safe for now".
     *
     * A ghost park is the same park from a second source, so a shared slug is
     * the ordinary case and the UPDATE raises **23505** — before the attraction
     * step and before the park step, which is what made everything the two
     * cases above assert unreachable on exactly the merges that had something
     * to merge.
     *
     * A statement is asserted absent here and that is worth the sentence: the
     * whole defect IS a statement, so the cases below pin both that the blind
     * form is gone and that the id-scoped one carries the right rows.
     */
    const expectNoBlindChildMove = (calls: Recorded[]) => {
      for (const table of ["shows", "restaurants"]) {
        expect(
          calls.filter((c) =>
            new RegExp(
              `UPDATE\\s+${table}\\s+SET\\s+"parkId"[\\s\\S]*WHERE\\s+"parkId"`,
              "i",
            ).test(c.sql),
          ),
        ).toEqual([]);
      }
    };

    const childStatement = (
      calls: Recorded[],
      verb: "UPDATE" | "DELETE FROM",
      table: string,
    ) =>
      calls.find((c) =>
        new RegExp(`^\\s*${verb}\\s+${table}\\b`, "i").test(c.sql),
      );

    it("partitions the sync-time ghost merge's shows and restaurants by slug instead of moving them into the unique index", async () => {
      const survivingShow = "5150c0de-0000-0000-0000-00000000000a";
      const ghostCollidingShow = "5150c0de-0000-0000-0000-00000000000b";
      const ghostMovingShow = "5150c0de-0000-0000-0000-00000000000c";
      const survivingRestaurant = "4e57c0de-0000-0000-0000-00000000000a";
      const ghostCollidingRestaurant = "4e57c0de-0000-0000-0000-00000000000b";

      const { calls } = primeGhostParkSync([], {
        shows: [
          {
            id: survivingShow,
            parkId: syncSurvivingParkId,
            slug: "raveleijn",
          },
          {
            id: ghostCollidingShow,
            parkId: syncGhostParkId,
            slug: "raveleijn",
          },
          {
            id: ghostMovingShow,
            parkId: syncGhostParkId,
            slug: "aquanura",
          },
        ],
        restaurants: [
          {
            id: survivingRestaurant,
            parkId: syncSurvivingParkId,
            slug: "polles-keuken",
          },
          {
            id: ghostCollidingRestaurant,
            parkId: syncGhostParkId,
            slug: "polles-keuken",
          },
        ],
      });

      await service.syncParks();

      expectNoBlindChildMove(calls);

      // The one that does not collide moves by id.
      expect(childStatement(calls, "UPDATE", "shows")?.params).toEqual([
        syncSurvivingParkId,
        [ghostMovingShow],
      ]);
      // Every restaurant of the ghost collided, so there is nothing to move.
      expect(childStatement(calls, "UPDATE", "restaurants")).toBeUndefined();

      // The colliding ones are drained of their dependent rows and deleted by
      // id — the delete is what the CASCADEs hang off, so it may not come
      // before the moves.
      for (const dep of SHOW_DEPENDENCIES) {
        expect(dependencyTablesTouched(calls, ghostCollidingShow)).toContain(
          dep.table,
        );
      }
      for (const dep of RESTAURANT_DEPENDENCIES) {
        expect(
          dependencyTablesTouched(calls, ghostCollidingRestaurant),
        ).toContain(dep.table);
      }

      const showDelete = childStatement(calls, "DELETE FROM", "shows");
      expect(showDelete?.params).toEqual([[ghostCollidingShow]]);
      const restaurantDelete = childStatement(
        calls,
        "DELETE FROM",
        "restaurants",
      );
      expect(restaurantDelete?.params).toEqual([[ghostCollidingRestaurant]]);

      const lastDependency = calls.reduce(
        (last, c, i) =>
          (c.params ?? []).includes(ghostCollidingShow) &&
          !/DELETE\s+FROM\s+shows/i.test(c.sql)
            ? i
            : last,
        -1,
      );
      expect(lastDependency).toBeGreaterThan(-1);
      expect(lastDependency).toBeLessThan(calls.indexOf(showDelete!));
      // And all of it before the park goes, or the CASCADE takes the survivor's
      // inherited rows with the ghost park.
      expect(calls.indexOf(restaurantDelete!)).toBeLessThan(
        indexOfParkDelete(calls),
      );
    });

    it("keeps a colliding show's history, its schedule pattern and its follower on the survivor", async () => {
      // The three tables fail in three different ways, and only one of them is
      // an error: `show_live_data` and `show_follows` are ON DELETE CASCADE, so
      // the losing row's whole showtime history and somebody's push reminder
      // disappear inside a transaction that then reports success;
      // `show_schedule_patterns` carries no FK at all and would be left
      // pointing at a show that is gone.
      const primaryId = "77777777-7777-7777-7777-777777777777";
      const ghostParkId = "88888888-8888-8888-8888-888888888888";
      const survivor = "5150c0de-1111-0000-0000-00000000000a";
      const ghost = "5150c0de-1111-0000-0000-00000000000b";

      const { calls } = recordTransaction((sql) => {
        if (/SELECT id, "parkId", slug FROM shows/.test(sql)) {
          return [
            { id: survivor, parkId: primaryId, slug: "raveleijn" },
            { id: ghost, parkId: ghostParkId, slug: "raveleijn" },
          ];
        }
        return [];
      });

      mockParkRepository.query.mockResolvedValue([
        { queue_times_entity_id: "4711" },
      ]);
      mockParkRepository.find.mockResolvedValue([
        createTestPark({ id: primaryId, wikiEntityId: "wiki-1" }),
        createTestPark({ id: ghostParkId, wikiEntityId: null }),
      ]);

      await service.repairDuplicates();

      expectNoBlindChildMove(calls);

      const reparented = (table: string) =>
        calls.find(
          (c) =>
            new RegExp(`UPDATE\\s+${table}\\s+SET`, "i").test(c.sql) &&
            (c.params ?? []).includes(ghost),
        );
      expect(reparented("show_live_data")?.params).toEqual([survivor, ghost]);
      expect(reparented("show_follows")?.params).toEqual([survivor, ghost]);
      expect(reparented("show_schedule_patterns")?.params).toEqual([
        survivor,
        ghost,
      ]);
      // No FK, so an orphan here survives the DELETE in silence — same shape as
      // the attraction side.
      expect(reparented("external_entity_mapping")?.params).toEqual([
        survivor,
        ghost,
      ]);

      // Both keyed tables drop the row the survivor already holds before the
      // move, or the UPDATE trips their own unique key instead of the park's.
      const dedupe = (table: string) =>
        calls.find(
          (c) =>
            new RegExp(`DELETE\\s+FROM\\s+${table}\\b`, "i").test(c.sql) &&
            (c.params ?? []).includes(ghost),
        );
      expect(dedupe("show_schedule_patterns")?.sql).toMatch(/"weekday"/);
      expect(dedupe("show_follows")?.sql).toMatch(/"subscriptionId"/);
      for (const table of ["show_schedule_patterns", "show_follows"]) {
        expect(calls.indexOf(dedupe(table)!)).toBeLessThan(
          calls.indexOf(reparented(table)!),
        );
      }
      // The time series has no key of its own, so nothing is ever dropped.
      expect(dedupe("show_live_data")).toBeUndefined();

      const showDelete = childStatement(calls, "DELETE FROM", "shows");
      expect(showDelete?.params).toEqual([[ghost]]);
      expect(calls.indexOf(reparented("show_live_data")!)).toBeLessThan(
        calls.indexOf(showDelete!),
      );
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
      // Hand-curated, written by nothing in this codebase, ON DELETE CASCADE.
      expect(reparented("attraction_ride_profiles")?.params).toEqual([
        survivingParkId,
        ghostParkId,
      ]);

      // The schedule moves whole, and its per-ride rows are only dropped where
      // the survivor holds the same day, type AND ride. A key of (date, type)
      // alone reads across the nullable attractionId and takes the ghost's
      // entire per-ride schedule with it.
      const scheduleDelete = calls.find(
        (c) =>
          /DELETE\s+FROM\s+schedule_entries/i.test(c.sql) &&
          (c.params ?? []).includes(ghostParkId),
      );
      expect(scheduleDelete?.sql).toMatch(
        /"attractionId"\s+IS NOT DISTINCT FROM/,
      );
      expect(scheduleDelete?.params).toEqual([survivingParkId, ghostParkId]);
      expect(reparented("schedule_entries")?.params).toEqual([
        survivingParkId,
        ghostParkId,
      ]);
      expect(calls.indexOf(scheduleDelete!)).toBeLessThan(
        calls.indexOf(reparented("schedule_entries")!),
      );
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

    /**
     * The third raw path: `syncParks`' priority merge. It is driven directly
     * rather than through `syncParks`, because `syncParks` cannot reach it —
     * the case below this one is what pins that down, and PAR-142 decides
     * whether the block stays. Until then it may not be the one park DELETE in
     * this file that runs outside a transaction and without moving what points
     * at the park.
     */
    const priorityWinnerId = "abababab-1111-1111-1111-111111111111";
    const priorityLoserId = "cdcdcdcd-2222-2222-2222-222222222222";

    const primePriorityMerge = (attractionRows: unknown[]) =>
      recordTransaction((sql) => {
        if (/SELECT id, "parkId", slug/.test(sql)) return attractionRows;
        // The emptiness check that guards the DELETE: everything moved.
        if (/SELECT\s*\n?\s*\(SELECT COUNT/.test(sql)) {
          return [{ shows: "0", restaurants: "0", attractions: "0" }];
        }
        return [];
      });

    const runPriorityMerge = async () =>
      (
        service as unknown as {
          mergePriorityDuplicateLoser: (w: Park, l: Park) => Promise<void>;
        }
      ).mergePriorityDuplicateLoser(
        createTestPark({ id: priorityWinnerId, name: "Phantasialand" }),
        createTestPark({
          id: priorityLoserId,
          name: "Phantasialand (Queue-Times)",
        }),
      );

    it("moves every park-scoped row of the priority merge before the park is deleted", async () => {
      const { calls } = primePriorityMerge([
        {
          id: "eeee6666-0000-0000-0000-000000000001",
          parkId: priorityWinnerId,
          slug: "taron",
          queue_times_entity_id: null,
          land_name: null,
          land_external_id: null,
        },
        {
          id: "ffff7777-0000-0000-0000-000000000001",
          parkId: priorityLoserId,
          slug: "taron",
          queue_times_entity_id: "4711",
          land_name: "Klugheim",
          land_external_id: "land-klugheim",
        },
        {
          // Moves across without colliding, so it carries a stale parkId into
          // every one of its dependent tables.
          id: "ffff7777-0000-0000-0000-000000000002",
          parkId: priorityLoserId,
          slug: "chiapas",
          queue_times_entity_id: "4712",
          land_name: "Mexico",
          land_external_id: "land-mexico",
        },
      ]);

      await runPriorityMerge();

      expectParkRowsMovedBeforeTheDelete(
        calls,
        priorityWinnerId,
        priorityLoserId,
      );

      // `park_occupancy` is a hypertable and the park step runs whether or not
      // a ride collided, so the cap belongs to the transaction. Exactly once.
      expect(
        calls
          .filter((c) =>
            /max_tuples_decompressed_per_dml_transaction/.test(c.sql),
          )
          .map((c) => c.sql),
      ).toEqual([
        "SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0",
      ]);
    });

    it("partitions the priority merge's attractions by slug instead of moving them blindly into the unique index", async () => {
      const collidingLoser = "ffff7777-0000-0000-0000-000000000001";
      const movingLoser = "ffff7777-0000-0000-0000-000000000002";
      const survivor = "eeee6666-0000-0000-0000-000000000001";

      const { calls } = primePriorityMerge([
        {
          id: survivor,
          parkId: priorityWinnerId,
          slug: "taron",
          queue_times_entity_id: null,
          land_name: null,
          land_external_id: null,
        },
        {
          id: collidingLoser,
          parkId: priorityLoserId,
          slug: "taron",
          queue_times_entity_id: "4711",
          land_name: "Klugheim",
          land_external_id: "land-klugheim",
        },
        {
          id: movingLoser,
          parkId: priorityLoserId,
          slug: "chiapas",
          queue_times_entity_id: "4712",
          land_name: "Mexico",
          land_external_id: "land-mexico",
        },
      ]);

      await runPriorityMerge();

      // A blind `UPDATE attractions SET "parkId" = $1 WHERE "parkId" = $2` is
      // 23505 against the unique (parkId, slug) the moment both parks know a
      // ride by the same slug. Rides move by id, never by park.
      expect(
        calls.filter(
          (c) =>
            /UPDATE\s+attractions\s+SET\s+"parkId"/i.test(c.sql) &&
            /WHERE\s+"parkId"/i.test(c.sql),
        ),
      ).toEqual([]);
      const movedById = calls.find((c) =>
        /UPDATE\s+attractions\s+SET\s+"parkId"\s*=\s*\$1\s+WHERE\s+id\s*=\s*ANY/i.test(
          c.sql,
        ),
      );
      expect(movedById?.params).toEqual([priorityWinnerId, [movingLoser]]);

      // The colliding loser is stamped onto its survivor, drained of its
      // dependent rows, and only then deleted.
      const stamps = attractionStamps(calls);
      expect(stamps).toHaveLength(1);
      expect(stamps[0].params).toEqual([
        survivor,
        "Klugheim",
        "land-klugheim",
        "4711",
      ]);

      const touched = dependencyTablesTouched(calls, collidingLoser);
      for (const dep of ATTRACTION_DEPENDENCIES) {
        expect(touched).toContain(dep.table);
      }

      const attractionDelete = calls.find((c) =>
        /DELETE\s+FROM\s+attractions/i.test(c.sql),
      );
      expect(attractionDelete?.params).toEqual([[collidingLoser]]);
      expect(calls.indexOf(stamps[0])).toBeLessThan(
        calls.indexOf(attractionDelete!),
      );
      expect(calls.indexOf(attractionDelete!)).toBeLessThan(
        indexOfParkDelete(calls),
      );
    });

    it("aborts the whole merge where something is left behind, instead of committing the moves without the delete", async () => {
      // Every child has just been moved or merged away, so a leftover means a
      // concurrent insert. Keeping the park and committing anyway would leave
      // every moved ride's denormalised parkId pointing at a park the survivor
      // is not — `consolidateMergedPark` is the only thing that moves it, and
      // it sits below this check. So the transaction rolls back instead.
      const { calls } = recordTransaction((sql) => {
        if (/SELECT id, "parkId", slug/.test(sql)) return [];
        if (/SELECT\s*\n?\s*\(SELECT COUNT/.test(sql)) {
          return [{ shows: "1", restaurants: "0", attractions: "0" }];
        }
        return [];
      });
      // Rejecting is the whole point and the only thing that separates this
      // from an early `return`: a recording manager commits nothing either way,
      // but a real transaction only rolls the reparenting back if the callback
      // throws. So the inner method must reject...
      await expect(runPriorityMerge()).rejects.toThrow(/non-empty/);

      // ...and the wrapper must swallow exactly that, so one contested merge
      // does not take the sync run with it.
      const rejected = jest.fn();
      await (
        service as unknown as {
          mergePriorityDuplicateLoserSafely: (
            w: Park,
            l: Park,
          ) => Promise<void>;
        }
      )
        .mergePriorityDuplicateLoserSafely(
          createTestPark({ id: priorityWinnerId, name: "Phantasialand" }),
          createTestPark({ id: priorityLoserId, name: "Phantasialand (QT)" }),
        )
        .catch(rejected);
      expect(rejected).not.toHaveBeenCalled();

      // Nothing past the count ran, so nothing was committed without its DELETE.
      expect(indexOfParkDelete(calls)).toBe(-1);
      expect(
        calls.filter((c) => /UPDATE\s+park_occupancy/i.test(c.sql)),
      ).toEqual([]);
    });

    it("refuses to merge a park into itself, whatever the call site believes", async () => {
      // Every ride would collide with itself, so the whole park's attractions
      // would go to consolidateMergedAttractions as their own losers and then
      // be deleted, one statement before the park. Both sibling paths establish
      // this before they call anything; this one may not depend on that.
      const { calls } = primePriorityMerge([
        {
          id: "eeee6666-0000-0000-0000-000000000001",
          parkId: priorityWinnerId,
          slug: "taron",
          queue_times_entity_id: null,
          land_name: null,
          land_external_id: null,
        },
      ]);
      const samePark = createTestPark({
        id: priorityWinnerId,
        name: "Phantasialand",
      });

      await (
        service as unknown as {
          mergePriorityDuplicateLoser: (w: Park, l: Park) => Promise<void>;
        }
      ).mergePriorityDuplicateLoser(samePark, samePark);

      expect(calls).toEqual([]);
    });

    it("is not reachable from syncParks: the priority merge's loser lookup can only miss", async () => {
      // `existing` is read from the same map, falling back to a global findOne
      // on the same externalId. A hit in either skips the duplicate branch, so
      // standing in it means both missed and the map cannot answer — the branch
      // is entered *because* no park carries this externalId. PAR-142 decides
      // whether the block stays; this is what makes a change to the lookup fail
      // loudly instead of quietly arming a park DELETE.
      const { calls } = recordTransaction(() => []);

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
        latitude: 50.8,
        longitude: 6.87,
        timezone: "Europe/Berlin",
      });
      // A name duplicate under a DIFFERENT externalId: the only shape that
      // reaches the priority branch at all.
      mockParkRepository.find.mockResolvedValue([
        createTestPark({
          id: priorityWinnerId,
          externalId: "ext-park-other",
          name: "Phantasialand",
        }),
      ]);
      mockParkRepository.findOne.mockResolvedValue(null);
      mockParkRepository.update.mockResolvedValue({ affected: 1 });
      mockScheduleRepository.count.mockResolvedValue(0);
      mockParkRepository.query.mockResolvedValue([]);
      mockParkRepository.manager.query.mockResolvedValue([]);

      await service.syncParks();

      // First: prove the run actually stood in the priority branch, or the two
      // absences below are green for the wrong reason. Only that branch keeps
      // an existing park without a matching externalId — had the name-duplicate
      // detection or the priority comparison not fired, `existing` would still
      // be null and the incoming park would have been inserted instead.
      expect(mockParkRepository.save).not.toHaveBeenCalled();
      expect(mockParkRepository.update).toHaveBeenCalledWith(
        priorityWinnerId,
        expect.objectContaining({ name: "Phantasialand" }),
      );

      // And there, the loser lookup missed: no park was merged away, no
      // statement inside a transaction, no DELETE outside one either.
      expect(calls).toEqual([]);
      expect(mockParkRepository.delete).not.toHaveBeenCalled();
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

    /**
     * `mergeParks` step 5c, missing on both raw paths.
     *
     * The rows in `park_slug_aliases` are moved by `PARK_DEPENDENCIES` — that
     * is the ghost's older paths, and it was already true. What was not is the
     * path the ghost itself was served under: it exists nowhere as a row, only
     * as four columns on the park that the next statement deletes. A ghost park
     * is not a theoretical URL — it stood in the database with its own slug, so
     * in the sitemap, so in the index — and without an alias every one of those
     * URLs answers 404 rather than redirecting to the survivor.
     */
    describe("the ghost park's own path survives the delete", () => {
      const aliasInserts = (calls: Recorded[]) =>
        calls.filter((c) => /INSERT INTO park_slug_aliases/i.test(c.sql));

      /** The 5b move, which has to happen before the 5c insert. */
      const indexOfAliasMove = (calls: Recorded[]) =>
        calls.findIndex((c) => /UPDATE\s+park_slug_aliases\s+SET/i.test(c.sql));

      const ghostPath = {
        continentSlug: "europe",
        countrySlug: "germany",
        citySlug: "bruehl",
        slug: "phantasialand-queue-times",
      };

      it("aliases the ghost's path onto the survivor before the sync-time merge deletes it", async () => {
        const { calls } = primeGhostParkSync([], {
          survivor: { ...ghostPath, slug: "phantasialand" },
          ghost: ghostPath,
        });

        await service.syncParks();

        const inserts = aliasInserts(calls);
        expect(inserts).toHaveLength(1);
        // The survivor's id against the ghost's four slugs — the other way
        // round would point the survivor's live URL at a park that is gone.
        expect(inserts[0].params).toEqual([
          syncSurvivingParkId,
          ghostPath.continentSlug,
          ghostPath.countrySlug,
          ghostPath.citySlug,
          ghostPath.slug,
        ]);
        // A path already recorded — an earlier rename, or the same pair merged
        // once before — must not abort a transaction that `syncParks` awaits
        // unguarded. `orIgnore()` in `mergeParks`, written out here.
        expect(inserts[0].sql).toMatch(/ON CONFLICT DO NOTHING/i);

        // Afterwards is too late: the row carrying those slugs is gone.
        const insertIndex = calls.indexOf(inserts[0]);
        expect(insertIndex).toBeLessThan(indexOfParkDelete(calls));
        // And after 5b, so a ghost that had already been renamed once arrives
        // with its whole history and not just its last URL.
        expect(indexOfAliasMove(calls)).toBeGreaterThan(-1);
        expect(indexOfAliasMove(calls)).toBeLessThan(insertIndex);
      });

      it("aliases the ghost's path onto the survivor before the repairDuplicates merge deletes it", async () => {
        const primaryId = "bbbbbbbb-1111-1111-1111-111111111111";
        const ghostParkId = "bbbbbbbb-2222-2222-2222-222222222222";
        const { calls } = recordTransaction(() => []);

        mockParkRepository.query.mockResolvedValue([
          { queue_times_entity_id: "4711" },
        ]);
        mockParkRepository.find.mockResolvedValue([
          createTestPark({
            id: primaryId,
            wikiEntityId: "wiki-1",
            ...ghostPath,
            slug: "phantasialand",
          }),
          createTestPark({
            id: ghostParkId,
            wikiEntityId: null,
            ...ghostPath,
          }),
        ]);

        await service.repairDuplicates();

        const inserts = aliasInserts(calls);
        expect(inserts).toHaveLength(1);
        expect(inserts[0].params).toEqual([
          primaryId,
          ghostPath.continentSlug,
          ghostPath.countrySlug,
          ghostPath.citySlug,
          ghostPath.slug,
        ]);
        expect(inserts[0].sql).toMatch(/ON CONFLICT DO NOTHING/i);
        expect(calls.indexOf(inserts[0])).toBeLessThan(
          indexOfParkDelete(calls),
        );
      });

      it("writes no alias where both parks were served under the same path", async () => {
        // `repairDuplicates` merges parks that share a Queue-Times id, and two
        // rows of one park can perfectly well carry the same four slugs. An
        // alias pointing a path at the park that already answers it is a row
        // the lookup has to step over for nothing.
        const primaryId = "cccccccc-1111-1111-1111-111111111111";
        const ghostParkId = "cccccccc-2222-2222-2222-222222222222";
        const { calls } = recordTransaction(() => []);

        mockParkRepository.query.mockResolvedValue([
          { queue_times_entity_id: "4711" },
        ]);
        mockParkRepository.find.mockResolvedValue([
          createTestPark({ id: primaryId, wikiEntityId: "wiki-1" }),
          createTestPark({ id: ghostParkId, wikiEntityId: null }),
        ]);

        await service.repairDuplicates();

        // The merge itself ran — otherwise this case proves nothing about the
        // alias and everything about a branch that was never entered.
        expect(indexOfParkDelete(calls)).toBeGreaterThan(-1);
        expect(aliasInserts(calls)).toHaveLength(0);
      });

      it("writes no alias for a park whose path is incomplete", async () => {
        // `captureParkPath` returns null as soon as one of the four slugs is
        // missing, and three quarters of a path is not a URL anyone could have
        // indexed. Three of four here, so the guard is what refuses and not an
        // empty row.
        const { calls } = primeGhostParkSync([], {
          survivor: { ...ghostPath, slug: "phantasialand" },
          ghost: { ...ghostPath, citySlug: undefined },
        });

        await service.syncParks();

        expect(indexOfParkDelete(calls)).toBeGreaterThan(-1);
        expect(aliasInserts(calls)).toHaveLength(0);
      });
    });

    /**
     * Step 6, and the one both raw paths skipped entirely: saying that the
     * merge happened. `ParkMergeService.mergeParks` evicts the park-scoped
     * caches of both parks plus the winner's attractions and revalidates the
     * three frontend tags; the two paths here deleted a park and left the geo
     * skeleton — cached for 24h and the source of every park link and sitemap
     * entry — advertising it.
     *
     * The redis deletions are recorded into the same list as the statements,
     * because where they sit RELATIVE to the park DELETE is half the question:
     * an eviction inside the transaction survives a rollback the merge does
     * not, and refills from the rows the merge was about to change.
     */
    describe("both raw merges announce themselves after the commit", () => {
      const REVALIDATED_TAGS = ["geo", "parks", "attractions"];

      /**
       * Routes `redis.del` into `calls` and answers the post-commit attraction
       * read. Returns the flat list of evicted keys.
       */
      const recordEvictions = (
        calls: Recorded[],
        winnerAttractionIds: string[] = [],
      ) => {
        const evicted: string[] = [];
        mockRedis.del.mockImplementation(async (...keys: string[]) => {
          calls.push({ sql: "REDIS DEL", params: keys });
          evicted.push(...keys);
          return keys.length;
        });
        mockParkRepository.manager.query.mockResolvedValue(
          winnerAttractionIds.map((id) => ({ id })),
        );
        return evicted;
      };

      afterEach(() => {
        mockRedis.del.mockReset();
        mockParkRepository.manager.query.mockReset();
        mockRevalidationService.revalidateTags.mockReset();
        mockRevalidationService.revalidateTags.mockResolvedValue(true);
      });

      const indexOfFirstEviction = (calls: Recorded[]) =>
        calls.findIndex((c) => c.sql === "REDIS DEL");

      it("evicts both parks and the winner's attractions after the sync-time merge, then revalidates", async () => {
        const inheritedAttractionId = "eeee5555-0000-0000-0000-000000000001";
        const { calls } = primeGhostParkSync([]);
        const evicted = recordEvictions(calls, [inheritedAttractionId]);

        await service.syncParks();

        // Both parks: the survivor because its ride list changed, the ghost
        // because it no longer exists.
        expect(evicted).toContain(
          CacheKeys.parkIntegrated(syncSurvivingParkId),
        );
        expect(evicted).toContain(CacheKeys.parkIntegrated(syncGhostParkId));
        // The geo skeleton is the entry named in the issue: it lists every
        // park and the frontend builds links and sitemap entries from it.
        expect(evicted).toContain(CacheKeys.discoveryGeoStructure());
        // The inherited ride, whose integrated payload embeds park context.
        // Passing no attraction ids would evict every key above and still
        // leave this one, which is why it is asserted separately.
        expect(evicted).toContain(
          CacheKeys.attractionIntegrated(inheritedAttractionId),
        );
        expect(mockRevalidationService.revalidateTags).toHaveBeenCalledWith(
          REVALIDATED_TAGS,
        );

        // After the DELETE, so no rollback can strand an emptied cache.
        expect(indexOfParkDelete(calls)).toBeGreaterThan(-1);
        expect(indexOfFirstEviction(calls)).toBeGreaterThan(
          indexOfParkDelete(calls),
        );
      });

      it("evicts both parks and the winner's attractions after the repairDuplicates merge, then revalidates", async () => {
        const primaryId = "dddddddd-1111-1111-1111-111111111111";
        const ghostParkId = "dddddddd-2222-2222-2222-222222222222";
        const inheritedAttractionId = "eeee5555-0000-0000-0000-000000000002";
        const { calls } = recordTransaction(() => []);
        const evicted = recordEvictions(calls, [inheritedAttractionId]);

        mockParkRepository.query.mockResolvedValue([
          { queue_times_entity_id: "4711" },
        ]);
        mockParkRepository.find.mockResolvedValue([
          createTestPark({ id: primaryId, wikiEntityId: "wiki-1" }),
          createTestPark({ id: ghostParkId, wikiEntityId: null }),
        ]);

        await service.repairDuplicates();

        expect(evicted).toContain(CacheKeys.parkIntegrated(primaryId));
        expect(evicted).toContain(CacheKeys.parkIntegrated(ghostParkId));
        expect(evicted).toContain(CacheKeys.discoveryGeoStructure());
        expect(evicted).toContain(
          CacheKeys.attractionIntegrated(inheritedAttractionId),
        );
        expect(mockRevalidationService.revalidateTags).toHaveBeenCalledWith(
          REVALIDATED_TAGS,
        );
        expect(indexOfParkDelete(calls)).toBeGreaterThan(-1);
        expect(indexOfFirstEviction(calls)).toBeGreaterThan(
          indexOfParkDelete(calls),
        );
      });

      it("evicts nothing when the merge rolls back, and everything when the same merge commits", async () => {
        // The park is still there afterwards, so an emptied cache would refill
        // from exactly the rows the merge was about to change — and the geo
        // skeleton would be re-cached WITHOUT the park that still exists.
        //
        // Both halves in one case on purpose: an assertion that nothing was
        // evicted is equally green for a path that never announces anything at
        // all (📚 G-44). The commit below is what says the announcement was
        // wired up and the rollback is what suppressed it — with the call site
        // removed, this case fails on its second half.
        const primaryId = "dddddddd-3333-3333-3333-333333333333";
        const ghostParkId = "dddddddd-4444-4444-4444-444444444444";
        const rolledBack: Recorded[] = [];
        const evicted = recordEvictions(rolledBack);
        mockParkRepository.manager.transaction = jest.fn(async () => {
          rolledBack.push({ sql: "DELETE FROM parks", params: [ghostParkId] });
          throw new Error("23503: park_occupancy still points at the ghost");
        });

        mockParkRepository.query.mockResolvedValue([
          { queue_times_entity_id: "4711" },
        ]);
        mockParkRepository.find.mockResolvedValue([
          createTestPark({ id: primaryId, wikiEntityId: "wiki-1" }),
          createTestPark({ id: ghostParkId, wikiEntityId: null }),
        ]);

        await expect(service.repairDuplicates()).rejects.toThrow("23503");

        // The statement ran and rolled back — without this line the case
        // passes for a merge that was never attempted.
        expect(indexOfParkDelete(rolledBack)).toBeGreaterThan(-1);
        expect(evicted).toHaveLength(0);
        expect(mockRevalidationService.revalidateTags).not.toHaveBeenCalled();

        // Same two parks, same harness, this time the transaction commits.
        const { calls: committed } = recordTransaction(() => []);
        const evictedOnCommit = recordEvictions(committed);

        await service.repairDuplicates();

        expect(indexOfParkDelete(committed)).toBeGreaterThan(-1);
        expect(evictedOnCommit).toContain(
          CacheKeys.parkIntegrated(ghostParkId),
        );
        expect(mockRevalidationService.revalidateTags).toHaveBeenCalledWith(
          REVALIDATED_TAGS,
        );
      });

      it("keeps a committed merge successful when redis and the webhook both fail", async () => {
        // Both paths run unattended off the sync; the merge is committed by
        // the time either is asked, so neither may take the run down with it.
        const primaryId = "dddddddd-5555-5555-5555-555555555555";
        const ghostParkId = "dddddddd-6666-6666-6666-666666666666";
        const { calls } = recordTransaction(() => []);
        recordEvictions(calls);
        mockRedis.del.mockRejectedValue(new Error("redis is down"));
        mockRevalidationService.revalidateTags.mockRejectedValue(
          new Error("frontend did not answer"),
        );

        mockParkRepository.query.mockResolvedValue([
          { queue_times_entity_id: "4711" },
        ]);
        mockParkRepository.find.mockResolvedValue([
          createTestPark({ id: primaryId, wikiEntityId: "wiki-1" }),
          createTestPark({ id: ghostParkId, wikiEntityId: null }),
        ]);

        await expect(service.repairDuplicates()).resolves.toBeUndefined();

        expect(indexOfParkDelete(calls)).toBeGreaterThan(-1);
        // The webhook was still attempted: a redis failure on the survivor
        // must not skip the rest of the announcement.
        expect(mockRevalidationService.revalidateTags).toHaveBeenCalledWith(
          REVALIDATED_TAGS,
        );
      });
    });
  });
});
