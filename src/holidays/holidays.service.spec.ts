import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { HOLIDAY_UPSERT_OPTIONS, HolidaysService } from "./holidays.service";
import { Holiday } from "./entities/holiday.entity";
import { REDIS_CLIENT } from "../common/redis/redis.module";

describe("HolidaysService", () => {
  let service: HolidaysService;

  const mockQueryBuilder = {
    where: jest.fn().mockReturnThis(),
    andWhere: jest.fn().mockReturnThis(),
    getOne: jest.fn(),
    getMany: jest.fn(),
    getCount: jest.fn(),
  };

  const mockHolidayRepository = {
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
    upsert: jest.fn(),
    createQueryBuilder: jest.fn(() => mockQueryBuilder),
  };

  const mockRedis = {
    get: jest.fn(),
    set: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        HolidaysService,
        {
          provide: getRepositoryToken(Holiday),
          useValue: mockHolidayRepository,
        },
        {
          provide: REDIS_CLIENT,
          useValue: mockRedis,
        },
      ],
    }).compile();

    service = module.get<HolidaysService>(HolidaysService);
    jest.clearAllMocks();
  });

  describe("getHolidays", () => {
    const row = (date: string, name: string) => ({
      id: `id-${date}`,
      externalId: `nager:NL:${date}:${name}`,
      date,
      name,
      localName: name,
      country: "NL",
      region: "NL-UT",
      holidayType: "school",
      isNationwide: false,
      createdAt: new Date(),
      updatedAt: new Date(),
    });

    beforeEach(() => {
      mockRedis.get.mockResolvedValue(null);
      mockRedis.set.mockResolvedValue("OK");
      Object.assign(mockQueryBuilder, {
        select: jest.fn().mockReturnThis(),
        orderBy: jest.fn().mockReturnThis(),
      });
    });

    it("caches per country and year, not per requested range", async () => {
      mockQueryBuilder.getMany
        .mockResolvedValueOnce([row("2026-12-24", "Christmas Holidays")])
        .mockResolvedValueOnce([row("2027-01-02", "Christmas Holidays")]);

      const result = await service.getHolidays(
        "NL",
        "2026-12-01",
        "2027-01-31",
      );

      expect(result.map((h) => h.date)).toEqual(["2026-12-24", "2027-01-02"]);
      expect(mockRedis.set.mock.calls.map((c) => c[0])).toEqual([
        "holiday:year:NL:2026",
        "holiday:year:NL:2027",
      ]);
      expect(mockQueryBuilder.andWhere).toHaveBeenCalledWith(
        "holiday.date BETWEEN :startDate AND :endDate",
        { startDate: "2026-01-01", endDate: "2026-12-31" },
      );
    });

    it("trims the year to the requested range, bounds inclusive", async () => {
      mockQueryBuilder.getMany.mockResolvedValueOnce([
        row("2026-03-31", "a"),
        row("2026-04-01", "b"),
        row("2026-04-30", "c"),
        row("2026-05-01", "d"),
      ]);

      const result = await service.getHolidays(
        "NL",
        "2026-04-01",
        "2026-04-30",
      );

      expect(result.map((h) => h.name)).toEqual(["b", "c"]);
    });

    it("keeps only the columns callers read", async () => {
      mockQueryBuilder.getMany.mockResolvedValueOnce([
        row("2026-04-05", "Easter"),
      ]);

      const [h] = await service.getHolidays("NL", "2026-04-01", "2026-04-30");

      expect(h).toEqual({
        date: "2026-04-05",
        name: "Easter",
        localName: "Easter",
        country: "NL",
        region: "NL-UT",
        holidayType: "school",
        isNationwide: false,
      });
      const cached = JSON.parse(mockRedis.set.mock.calls[0][1]);
      expect(Object.keys(cached[0])).not.toContain("externalId");
    });

    it("serves a second range in the same year from memory", async () => {
      mockQueryBuilder.getMany.mockResolvedValueOnce([
        row("2026-04-05", "Easter"),
      ]);

      await service.getHolidays("NL", "2026-04-01", "2026-04-30");
      await service.getHolidays("NL", "2026-03-15", "2026-05-15");

      expect(mockQueryBuilder.getMany).toHaveBeenCalledTimes(1);
      expect(mockRedis.get).toHaveBeenCalledTimes(1);
    });

    it("reads a year Redis already holds without touching the DB", async () => {
      mockRedis.get.mockResolvedValueOnce(
        JSON.stringify([{ ...row("2026-04-05", "Easter") }]),
      );

      const result = await service.getHolidays(
        "NL",
        "2026-04-01",
        "2026-04-30",
      );

      expect(result).toHaveLength(1);
      expect(mockQueryBuilder.getMany).not.toHaveBeenCalled();
    });
  });

  describe("isHoliday", () => {
    it("should return true if holiday exists for the date in park timezone", async () => {
      const date = new Date("2023-12-25T23:00:00Z"); // Dec 26 in Europe/Berlin
      const timezone = "Europe/Berlin";

      mockRedis.get.mockResolvedValue(null);
      mockQueryBuilder.getCount.mockResolvedValue(1);

      const result = await service.isHoliday(date, "DE", "NW", timezone);

      expect(result).toBe(true);
      // Date column is now compared directly (the previous CAST was a
      // workaround for a column type that no longer exists).
      expect(mockQueryBuilder.andWhere).toHaveBeenCalledWith(
        "holiday.date = :dateStr",
        { dateStr: "2023-12-26" },
      );
    });

    it("should return false if no holiday exists", async () => {
      const date = new Date("2023-12-24T12:00:00Z");
      mockRedis.get.mockResolvedValue(null);
      mockQueryBuilder.getCount.mockResolvedValue(0);

      const result = await service.isHoliday(date, "DE");

      expect(result).toBe(false);
    });
  });

  describe("logSchoolCoverageGaps (PAR-1049)", () => {
    it("names countries whose school data ends within 90 days or holds none", async () => {
      (mockHolidayRepository as any).query = jest.fn().mockResolvedValue([
        { country: "ES", region: "ES-CT", through: "2026-09-11" },
        { country: "ES", region: "ES-MD", through: "2026-09-10" },
        { country: "DE", region: "DE-NW", through: "2027-07-20" },
      ]);
      const warn = jest.spyOn((service as any).logger, "warn");
      const gaps = await service.logSchoolCoverageGaps(
        ["ES", "DE", "SE"],
        "2026-10-24",
      );
      expect(gaps).toEqual(["ES: MD 2026-09-10, CT 2026-09-11", "SE: none"]);
      expect(warn).toHaveBeenCalledWith(expect.stringContaining("SE: none"));
    });
  });

  describe("synthesizeProjectedSummerHolidays", () => {
    const now = new Date().getUTCFullYear();

    it("never projects a hand-curated region, and still projects a synced one (PAR-1071)", async () => {
      // LAUSD's file ends 2028-06-30: 23 June days in 2028 look marker-only.
      const marker = (region: string) => [
        {
          region,
          yr: String(now - 1),
          days: "70",
          mn: `${now - 1}-06-05`,
          mx: `${now - 1}-08-11`,
        },
        {
          region,
          yr: String(now),
          days: "23",
          mn: `${now}-06-08`,
          mx: `${now}-06-30`,
        },
      ];
      const spy = jest
        .spyOn(service as any, "saveRawHolidays")
        .mockResolvedValue(0);

      (mockHolidayRepository as any).query = jest
        .fn()
        .mockResolvedValue([...marker("US-CA"), ...marker("US-FL")]);
      expect(await service.synthesizeProjectedSummerHolidays("US")).toBe(0);
      expect(spy).not.toHaveBeenCalled();

      (mockHolidayRepository as any).query = jest
        .fn()
        .mockResolvedValue(marker("IT-ER"));
      await service.synthesizeProjectedSummerHolidays("IT");
      expect(spy).toHaveBeenCalledTimes(1);
      const entries = spy.mock.calls[0][0] as Array<{ externalId: string }>;
      expect(entries[0].externalId).toMatch(/^synth-summer:IT:IT-ER:/);
    });

    it("projects last year's summer range forward for a marker-only region", async () => {
      (mockHolidayRepository as any).query = jest.fn().mockResolvedValue([
        // prev year: a genuine ~99-day summer range
        {
          region: "IT-ER",
          yr: String(now - 1),
          days: "99",
          mn: `${now - 1}-06-08`,
          mx: `${now - 1}-09-14`,
        },
        // target year: marker-only (1 day) → needs projection
        {
          region: "IT-ER",
          yr: String(now),
          days: "1",
          mn: `${now}-06-06`,
          mx: `${now}-06-06`,
        },
        // a region that already has a real range this year → left alone
        {
          region: "IT-XX",
          yr: String(now),
          days: "70",
          mn: `${now}-06-10`,
          mx: `${now}-09-01`,
        },
      ]);
      const spy = jest
        .spyOn(service as any, "saveRawHolidays")
        .mockResolvedValue(0);

      await service.synthesizeProjectedSummerHolidays("IT");

      expect(spy).toHaveBeenCalledTimes(1);
      const entries = spy.mock.calls[0][0] as Array<{
        region: string;
        date: Date;
        name: string;
        holidayType: string;
        externalId: string;
      }>;
      expect(new Set(entries.map((e) => e.region))).toEqual(new Set(["IT-ER"]));
      expect(entries[0].date.toISOString().slice(0, 10)).toBe(`${now}-06-08`);
      expect(entries[entries.length - 1].date.toISOString().slice(0, 10)).toBe(
        `${now}-09-14`,
      );
      expect(entries.length).toBeGreaterThan(90); // full range, not a marker
      expect(entries.every((e) => e.holidayType === "school")).toBe(true);
      expect(entries[0].externalId).toMatch(/^synth-summer:IT:IT-ER:/);
    });

    it("is a no-op when the region already publishes a real range this year", async () => {
      (mockHolidayRepository as any).query = jest.fn().mockResolvedValue([
        {
          region: "DE-NW",
          yr: String(now - 1),
          days: "45",
          mn: `${now - 1}-06-29`,
          mx: `${now - 1}-08-07`,
        },
        {
          region: "DE-NW",
          yr: String(now),
          days: "40",
          mn: `${now}-06-22`,
          mx: `${now}-08-04`,
        },
      ]);
      const spy = jest
        .spyOn(service as any, "saveRawHolidays")
        .mockResolvedValue(0);

      const result = await service.synthesizeProjectedSummerHolidays("DE");

      expect(spy).not.toHaveBeenCalled();
      expect(result).toBe(0);
    });
  });

  describe("saveHolidaysFromApi: Nager types (PAR-1071)", () => {
    const nager = (types: string[], counties: string[] | null) => ({
      date: "2026-05-08",
      localName: "Truman Day",
      name: "Truman Day",
      countryCode: "US",
      fixed: false,
      global: counties === null,
      counties,
      launchYear: null,
      types,
    });

    const savedTypes = async (country: string, types: string[]) => {
      await service.saveHolidaysFromApi(
        [{ ...nager(types, [`${country}-MO`]), countryCode: country } as never],
        country,
      );
      const rows = mockHolidayRepository.upsert.mock.calls[0][0];
      return rows.map((r: { holidayType: string }) => r.holidayType);
    };

    it("stores Truman Day (US-MO, School+Authorities) as a public holiday, not a school one", async () => {
      expect(await savedTypes("US", ["School", "Authorities"])).toEqual([
        "public",
      ]);
    });

    it("keeps Nager's School tag outside the US (Austrian patron saints' days)", async () => {
      expect(await savedTypes("AT", ["School"])).toEqual(["school"]);
    });
  });

  // PAR-872: every sync re-sent the same calendar and rewrote every stored row
  // (22 M updates on 200 k rows). The upsert now skips a row nothing changed in.
  describe("saveRawHolidays", () => {
    const h = {
      externalId: "x:DE:2026-10-03",
      date: new Date("2026-10-03T12:00:00Z"),
      name: "Tag der Deutschen Einheit",
      localName: null,
      country: "DE",
      region: null,
      holidayType: "public" as const,
      isNationwide: true,
    };

    it("upserts on externalId and skips rows whose values did not change", async () => {
      await service.saveRawHolidays([h as never]);
      expect(HOLIDAY_UPSERT_OPTIONS).toEqual({
        conflictPaths: ["externalId"],
        skipUpdateIfNoValuesChanged: true,
      });
      expect(mockHolidayRepository.upsert).toHaveBeenCalledWith(
        [h],
        HOLIDAY_UPSERT_OPTIONS,
      );
    });

    it("hands TypeORM a copy, so a write-back never reaches the caller's object", async () => {
      mockHolidayRepository.upsert.mockImplementation(async (rows) => {
        rows[0].createdAt = new Date();
      });
      await service.saveRawHolidays([h as never]);
      expect(h).not.toHaveProperty("createdAt");
    });
  });

  describe("working days (PAR-1065)", () => {
    // China's 调休: a Saturday people work. It is stored, but no reader that
    // takes every holiday type may count it as one.
    const filter = "holiday.holidayType <> 'working_day'";

    beforeEach(() => {
      mockRedis.get.mockResolvedValue(null);
      mockRedis.set.mockResolvedValue("OK");
      Object.assign(mockQueryBuilder, {
        select: jest.fn().mockReturnThis(),
        orderBy: jest.fn().mockReturnThis(),
        addOrderBy: jest.fn().mockReturnThis(),
      });
      mockQueryBuilder.getMany.mockResolvedValue([]);
      mockQueryBuilder.getCount.mockResolvedValue(0);
    });

    it("isHoliday leaves them out", async () => {
      await service.isHoliday(new Date("2026-02-14T12:00:00Z"), "CN");
      expect(mockQueryBuilder.andWhere).toHaveBeenCalledWith(filter);
    });

    it("getHolidays leaves them out", async () => {
      await service.getHolidays("CN", "2026-02-01", "2026-02-28");
      expect(mockQueryBuilder.andWhere).toHaveBeenCalledWith(filter);
    });

    it("getAllHolidays leaves them out", async () => {
      await service.getAllHolidays(new Date(), new Date());
      expect(mockQueryBuilder.andWhere).toHaveBeenCalledWith(filter);
    });
  });

  describe("replaceHolidaysBySource (PAR-1065)", () => {
    const deleteQb = {
      delete: jest.fn().mockReturnThis(),
      from: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      execute: jest.fn().mockResolvedValue({ affected: 2 }),
    };
    const row = (day: string) => ({
      externalId: `curated:SG:national:school:${day}`,
      date: new Date(`${day}T00:00:00Z`),
      name: "x",
      country: "SG",
      holidayType: "school" as const,
      isNationwide: true,
    });

    beforeEach(() => {
      mockHolidayRepository.createQueryBuilder.mockReturnValue(
        deleteQb as never,
      );
    });
    afterEach(() => {
      mockHolidayRepository.createQueryBuilder.mockImplementation(
        () => mockQueryBuilder,
      );
    });

    it("upserts the rows and deletes that source's rows no longer listed", async () => {
      const result = await service.replaceHolidaysBySource(
        "curated:SG:",
        [row("2026-03-14"), row("2026-03-15")],
        "2025-01-01",
      );

      expect(result).toEqual({ saved: 2, removed: 2 });
      expect(mockHolidayRepository.upsert).toHaveBeenCalled();
      expect(deleteQb.where).toHaveBeenCalledWith(
        `"externalId" LIKE :pattern`,
        {
          pattern: "curated:SG:%",
        },
      );
      expect(deleteQb.andWhere).toHaveBeenCalledWith(`date >= :fromDate`, {
        fromDate: "2025-01-01",
      });
      expect(deleteQb.andWhere).toHaveBeenCalledWith(
        `NOT ("externalId" = ANY(:ids))`,
        {
          ids: [
            "curated:SG:national:school:2026-03-14",
            "curated:SG:national:school:2026-03-15",
          ],
        },
      );
    });

    it("deletes nothing when the source came back empty", async () => {
      expect(await service.replaceHolidaysBySource("neis:KR:", [])).toEqual({
        saved: 0,
        removed: 0,
      });
      expect(deleteQb.execute).not.toHaveBeenCalled();
    });
  });
});
