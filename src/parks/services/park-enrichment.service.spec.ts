import { Test, TestingModule } from "@nestjs/testing";
import { ParkEnrichmentService } from "./park-enrichment.service";
import { ParksService } from "../parks.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { HolidaysService } from "../../holidays/holidays.service";
import { Park } from "../entities/park.entity";

describe("ParkEnrichmentService", () => {
  let service: ParkEnrichmentService;

  const mockHolidaysService = {
    isEffectiveSchoolHoliday: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ParkEnrichmentService,
        { provide: ParksService, useValue: {} },
        { provide: AnalyticsService, useValue: {} },
        { provide: HolidaysService, useValue: mockHolidaysService },
      ],
    }).compile();

    service = module.get<ParkEnrichmentService>(ParkEnrichmentService);
    jest.clearAllMocks();
  });

  // getBatchSchoolHolidayStatus is private; exercise it directly to verify the
  // dedup behaviour in isolation.
  const callDedup = (
    parks: Array<Record<string, unknown>>,
  ): Promise<Map<string, boolean>> =>
    (
      service as unknown as {
        getBatchSchoolHolidayStatus: (
          p: Park[],
        ) => Promise<Map<string, boolean>>;
      }
    ).getBatchSchoolHolidayStatus(parks as unknown as Park[]);

  it("should be defined", () => {
    expect(service).toBeDefined();
  });

  it("resolves each distinct (country, region, timezone) only once", async () => {
    mockHolidaysService.isEffectiveSchoolHoliday.mockImplementation(
      async (_date: Date, country: string, region?: string) =>
        country === "DE" && region === "BY",
    );

    const parks = [
      {
        id: "1",
        name: "A",
        countryCode: "DE",
        regionCode: "BY",
        timezone: "Europe/Berlin",
      },
      {
        id: "2",
        name: "B",
        countryCode: "DE",
        regionCode: "BY",
        timezone: "Europe/Berlin",
      },
      {
        id: "3",
        name: "C",
        countryCode: "DE",
        regionCode: "NW",
        timezone: "Europe/Berlin",
      },
      {
        id: "4",
        name: "D",
        countryCode: "FR",
        regionCode: null,
        timezone: "Europe/Paris",
      },
    ];

    const result = await callDedup(parks);

    // 4 parks, but only 3 distinct combinations -> 3 lookups instead of 4.
    expect(mockHolidaysService.isEffectiveSchoolHoliday).toHaveBeenCalledTimes(
      3,
    );

    expect(result.get("1")).toBe(true);
    expect(result.get("2")).toBe(true); // shares DE/BY result with park 1
    expect(result.get("3")).toBe(false);
    expect(result.get("4")).toBe(false);
  });

  it("skips parks without a country code and never calls the holiday service for them", async () => {
    mockHolidaysService.isEffectiveSchoolHoliday.mockResolvedValue(true);

    const parks = [
      {
        id: "1",
        name: "A",
        countryCode: "DE",
        regionCode: "BY",
        timezone: "Europe/Berlin",
      },
      { id: "2", name: "NoCountry", countryCode: null, timezone: "UTC" },
    ];

    const result = await callDedup(parks);

    expect(mockHolidaysService.isEffectiveSchoolHoliday).toHaveBeenCalledTimes(
      1,
    );
    expect(result.get("1")).toBe(true);
    expect(result.get("2")).toBe(false);
  });

  it("falls back to false when the holiday lookup throws", async () => {
    mockHolidaysService.isEffectiveSchoolHoliday.mockRejectedValue(
      new Error("upstream down"),
    );

    const parks = [
      {
        id: "1",
        name: "A",
        countryCode: "DE",
        regionCode: "BY",
        timezone: "Europe/Berlin",
      },
    ];

    const result = await callDedup(parks);

    expect(result.get("1")).toBe(false);
  });
});
