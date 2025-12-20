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
});
