import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { HolidaysService } from "./holidays.service";
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

  describe("synthesizeProjectedSummerHolidays", () => {
    const now = new Date().getUTCFullYear();

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
});
