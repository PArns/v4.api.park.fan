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
import { ParkRenameService } from "./services/park-rename.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { Park } from "./entities/park.entity";
import {
  IntegratedCalendarResponse,
  CalendarDay,
} from "./dto/integrated-calendar.dto";

/**
 * The Cache-Control this endpoint sends.
 *
 * `/calendar` was the only endpoint on this controller without a
 * `stale-while-revalidate`, and it is the one that rebuilds slowest: on a
 * month-cache gap the response falls through to a live aggregation, one query
 * per day. Without SWR every expiry handed that wait to whoever asked first.
 *
 * The two max-age branches are asserted alongside it because they share the one
 * header string — changing either silently is how the two halves drift.
 */
describe("ParksController › /calendar Cache-Control", () => {
  let controller: ParksController;
  let calendarService: { buildCalendarResponse: jest.Mock };

  const park = {
    id: "park-1",
    slug: "phantasialand",
    timezone: "Europe/Berlin",
  } as unknown as Park;

  /**
   * Park-local `YYYY-MM-DD`, `offsetDays` from today.
   *
   * Derived rather than hard-coded: the handler asks the real clock what "today"
   * is, so a literal date would flip this test from the past branch to the
   * future branch as the calendar moved past it.
   */
  const parkDate = (offsetDays: number): string => {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() + offsetDays);
    return new Intl.DateTimeFormat("en-CA", {
      timeZone: park.timezone,
    }).format(d);
  };

  const dayOn = (date: string) =>
    ({
      date,
      status: "OPERATING",
      crowdLevel: "low",
      isToday: false,
      isHoliday: false,
      isBridgeDay: false,
      isSchoolVacation: false,
    }) as unknown as CalendarDay;

  const makeRes = () => {
    const headers: Record<string, string> = {};
    return {
      headers,
      setHeader: (k: string, v: string) => {
        headers[k] = v;
      },
    };
  };

  const mountWithDays = async (days: CalendarDay[]) => {
    calendarService = {
      buildCalendarResponse: jest
        .fn()
        .mockImplementation(async (): Promise<IntegratedCalendarResponse> => ({
          meta: {
            slug: "phantasialand",
            timezone: "Europe/Berlin",
            hasOperatingSchedule: true,
            scheduleCoverage: { from: "2026-01-01", to: "2027-12-31" },
          },
          days,
        })) as jest.Mock,
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
        { provide: ParkRenameService, useValue: noop },
        { provide: REDIS_CLIENT, useValue: noop },
      ],
    }).compile();

    controller = module.get<ParksController>(ParksController);
  };

  const call = (res: ReturnType<typeof makeRes>) =>
    controller.getCalendarByGeographicPath(
      "europe",
      "germany",
      "bruhl",
      "phantasialand",
      undefined,
      undefined,
      undefined,
      undefined,
      res,
    );

  it("sends 15 min + SWR for a range that reaches today or earlier", async () => {
    await mountWithDays([dayOn(parkDate(-2)), dayOn(parkDate(0))]);
    const res = makeRes();

    await call(res);

    expect(res.headers["Cache-Control"]).toBe(
      "public, max-age=900, s-maxage=900, stale-while-revalidate=3600",
    );
  });

  it("sends 30 min + SWR for a pure-future range", async () => {
    await mountWithDays([dayOn(parkDate(20)), dayOn(parkDate(21))]);
    const res = makeRes();

    await call(res);

    expect(res.headers["Cache-Control"]).toBe(
      "public, max-age=1800, s-maxage=1800, stale-while-revalidate=3600",
    );
  });

  it("always carries a stale-while-revalidate, whichever branch it took", async () => {
    for (const days of [
      [dayOn(parkDate(-1))],
      [dayOn(parkDate(0))],
      [dayOn(parkDate(45))],
    ]) {
      await mountWithDays(days);
      const res = makeRes();
      await call(res);
      expect(res.headers["Cache-Control"]).toContain(
        "stale-while-revalidate=3600",
      );
    }
  });
});
