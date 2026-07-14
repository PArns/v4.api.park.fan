import { Test, TestingModule } from "@nestjs/testing";
import { ParksController } from "./parks.controller";
import { ParksService } from "./parks.service";
import { WeatherService } from "./weather.service";
import { WeatherWarningsService } from "./weather-warnings.service";
import { AttractionsService } from "../attractions/attractions.service";
import { AttractionIntegrationService } from "../attractions/services/attraction-integration.service";
import { ShowsService } from "../shows/shows.service";
import { RestaurantsService } from "../restaurants/restaurants.service";
import { QueueDataService } from "../queue-data/queue-data.service";
import { AnalyticsService } from "../analytics/analytics.service";
import { ParkHistoricalStatsService } from "../analytics/park-historical-stats.service";
import { MLService } from "../ml/ml.service";
import { PredictionAccuracyService } from "../ml/services/prediction-accuracy.service";
import { ParkIntegrationService } from "./services/park-integration.service";
import { ParkEnrichmentService } from "./services/park-enrichment.service";
import { CalendarService } from "./services/calendar.service";
import { BestDaysService } from "./services/best-days.service";
import { PopularityService } from "../popularity/popularity.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { Park } from "./entities/park.entity";
import {
  IntegratedCalendarResponse,
  CalendarDay,
} from "./dto/integrated-calendar.dto";

/**
 * P1 calendar payload diet: per-day `influencingHolidays` (≈98% of the ~2.25 MB
 * body, read by no consumer of this endpoint) is stripped from `/calendar` by
 * default and only returned with `?include=influencingHolidays`.
 */
describe("ParksController › /calendar payload diet", () => {
  let controller: ParksController;
  let calendarService: { buildCalendarResponse: jest.Mock };

  const park = {
    id: "park-1",
    slug: "phantasialand",
    timezone: "Europe/Berlin",
  } as unknown as Park;

  const dayWith = {
    date: "2026-07-14",
    status: "OPERATING",
    crowdLevel: "low",
    isToday: false,
    isHoliday: false,
    isBridgeDay: false,
    isSchoolVacation: true,
    influencingHolidays: [
      {
        name: "X",
        source: { countryCode: "NL", regionCode: null },
        holidayType: "public",
      },
    ],
  } as unknown as CalendarDay;
  const dayWithout = {
    date: "2026-07-15",
    status: "OPERATING",
    crowdLevel: "moderate",
    isToday: false,
    isHoliday: false,
    isBridgeDay: false,
    isSchoolVacation: true,
  } as unknown as CalendarDay;

  const makeRes = () => ({
    setHeader: jest.fn(),
  });

  beforeEach(async () => {
    calendarService = {
      buildCalendarResponse: jest.fn().mockImplementation(
        async (): Promise<IntegratedCalendarResponse> => ({
          meta: {
            slug: "phantasialand",
            timezone: "Europe/Berlin",
            hasOperatingSchedule: true,
          },
          // Fresh objects per call so mutation-safety is genuinely exercised.
          days: [
            JSON.parse(JSON.stringify(dayWith)),
            JSON.parse(JSON.stringify(dayWithout)),
          ],
        }),
      ),
    };

    const noop = {};
    const module: TestingModule = await Test.createTestingModule({
      controllers: [ParksController],
      providers: [
        {
          provide: ParksService,
          useValue: { findByGeographicPath: jest.fn().mockResolvedValue(park) },
        },
        { provide: CalendarService, useValue: calendarService },
        { provide: BestDaysService, useValue: noop },
        { provide: WeatherService, useValue: noop },
        { provide: WeatherWarningsService, useValue: noop },
        { provide: AttractionsService, useValue: noop },
        { provide: AttractionIntegrationService, useValue: noop },
        { provide: ShowsService, useValue: noop },
        { provide: RestaurantsService, useValue: noop },
        { provide: QueueDataService, useValue: noop },
        { provide: AnalyticsService, useValue: noop },
        { provide: ParkHistoricalStatsService, useValue: noop },
        { provide: MLService, useValue: noop },
        { provide: PredictionAccuracyService, useValue: noop },
        { provide: ParkIntegrationService, useValue: noop },
        { provide: ParkEnrichmentService, useValue: noop },
        { provide: PopularityService, useValue: noop },
        { provide: REDIS_CLIENT, useValue: noop },
      ],
    }).compile();

    controller = module.get<ParksController>(ParksController);
  });

  const call = (include?: string) =>
    controller.getCalendarByGeographicPath(
      "europe",
      "germany",
      "bruhl",
      "phantasialand",
      undefined,
      undefined,
      undefined,
      include,
      makeRes(),
    );

  it("strips influencingHolidays from every day by default", async () => {
    const res = await call(undefined);

    expect(res.days).toHaveLength(2);
    expect(res.days[0]).not.toHaveProperty("influencingHolidays");
    expect(res.days[1]).not.toHaveProperty("influencingHolidays");
    // Other fields untouched.
    expect(res.days[0].date).toBe("2026-07-14");
    expect(res.days[0].crowdLevel).toBe("low");
  });

  it("keeps influencingHolidays when ?include=influencingHolidays", async () => {
    const res = await call("influencingHolidays");

    expect(res.days[0].influencingHolidays).toHaveLength(1);
    expect(res.days[0].influencingHolidays![0].name).toBe("X");
  });

  it("parses a comma-separated include list (with whitespace)", async () => {
    const res = await call(" foo , influencingHolidays ");

    expect(res.days[0].influencingHolidays).toHaveLength(1);
  });

  it("ignores an unrelated include value (still strips)", async () => {
    const res = await call("weather");

    expect(res.days[0]).not.toHaveProperty("influencingHolidays");
  });
});
