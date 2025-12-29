import { Test, TestingModule } from "@nestjs/testing";
import { CalendarService } from "../../src/parks/services/calendar.service";
import { ParksService } from "../../src/parks/parks.service";
import { WeatherService } from "../../src/parks/weather.service";
import { MLService } from "../../src/ml/ml.service";
import { HolidaysService } from "../../src/holidays/holidays.service";
import { AttractionsService } from "../../src/attractions/attractions.service";
import { ShowsService } from "../../src/shows/shows.service";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../src/common/redis/redis.module";

/**
 * Simple verification test for CalendarService
 *
 * This test verifies that the CalendarService can be instantiated
 * and has all required dependencies properly injected.
 */
describe("CalendarService - Basic Verification", () => {
  let service: CalendarService;

  beforeEach(async () => {
    const mockRedis = {
      get: jest.fn().mockResolvedValue(null),
      set: jest.fn().mockResolvedValue("OK"),
    };

    const mockParksService = {
      getSchedule: jest.fn().mockResolvedValue([]),
    };

    const mockWeatherService = {
      getWeatherData: jest.fn().mockResolvedValue([]),
    };

    const mockMLService = {
      getParkPredictions: jest.fn().mockResolvedValue({ predictions: [] }),
    };

    const mockHolidaysService = {
      getHolidays: jest.fn().mockResolvedValue([]),
      isHoliday: jest.fn().mockResolvedValue(false),
      isBridgeDay: jest.fn().mockResolvedValue(false),
      isSchoolHoliday: jest.fn().mockResolvedValue(false),
    };

    const mockAttractionsService = {
      findByParkId: jest.fn().mockResolvedValue({ data: [], total: 0 }),
    };

    const mockShowsService = {
      findCurrentStatusByPark: jest.fn().mockResolvedValue(new Map()),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        CalendarService,
        { provide: ParksService, useValue: mockParksService },
        { provide: WeatherService, useValue: mockWeatherService },
        { provide: MLService, useValue: mockMLService },
        { provide: HolidaysService, useValue: mockHolidaysService },
        { provide: AttractionsService, useValue: mockAttractionsService },
        { provide: ShowsService, useValue: mockShowsService },
        { provide: REDIS_CLIENT, useValue: mockRedis },
      ],
    }).compile();

    service = module.get<CalendarService>(CalendarService);
  });

  it("should be defined", () => {
    expect(service).toBeDefined();
  });

  it("should have buildCalendarResponse method", () => {
    expect(service.buildCalendarResponse).toBeDefined();
    expect(typeof service.buildCalendarResponse).toBe("function");
  });

  it("should build calendar response with mock park", async () => {
    const mockPark = {
      id: "test-id",
      slug: "test-park",
      name: "Test Park",
      timezone: "Europe/Berlin",
      countryCode: "DE",
      regionCode: null,
    } as any;

    const fromDate = new Date("2025-12-28");
    const toDate = new Date("2025-12-30");

    const result = await service.buildCalendarResponse(
      mockPark,
      fromDate,
      toDate,
      "today",
    );

    // Verify response structure
    expect(result).toBeDefined();
    expect(result.meta).toBeDefined();
    expect(result.meta.parkId).toBe("test-id");
    expect(result.meta.slug).toBe("test-park");
    expect(result.meta.timezone).toBe("Europe/Berlin");
    expect(result.days).toBeDefined();
    expect(Array.isArray(result.days)).toBe(true);
    expect(result.days.length).toBeGreaterThan(0);
  });
});
