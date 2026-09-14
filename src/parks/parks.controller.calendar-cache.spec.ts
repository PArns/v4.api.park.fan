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
import { PlanDayService } from "./services/plan-day.service";
import { BestDaysService } from "./services/best-days.service";
import { PopularityService } from "../popularity/popularity.service";
import { ParkRenameService } from "./services/park-rename.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { secondsUntilEndOfDayInTimezone } from "../common/utils/date.util";
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

  /** The same day, carrying the hourly countdown the day-detail dialog asks for. */
  const dayWithHourlyOn = (date: string) =>
    ({
      ...dayOn(date),
      hourly: [
        { hour: 12, crowdLevel: "low", predictedWaitTime: 15 },
        { hour: 13, crowdLevel: "moderate", predictedWaitTime: 25 },
      ],
    }) as unknown as CalendarDay;

  const secondsLeftThisHour = (): number => {
    const now = Date.now();
    return Math.ceil((Math.ceil(now / 3_600_000) * 3_600_000 - now) / 1000) + 5;
  };

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
        { provide: PlanDayService, useValue: calendarService },
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

  it("sends a day + a day of SWR for a pure-future range", async () => {
    await mountWithDays([dayOn(parkDate(20)), dayOn(parkDate(21))]);
    const res = makeRes();

    await call(res);

    expect(res.headers["Cache-Control"]).toBe(
      "public, max-age=86400, s-maxage=86400, stale-while-revalidate=86400",
    );
  });

  it("never lets a range containing today outlive the day", async () => {
    await mountWithDays([dayOn(parkDate(-2)), dayOn(parkDate(0))]);
    const res = makeRes();

    await call(res);

    const left = secondsUntilEndOfDayInTimezone(park.timezone);
    const header = res.headers["Cache-Control"];
    const maxAge = Number(/max-age=(\d+)/.exec(header)![1]);
    const sMaxAge = Number(/s-maxage=(\d+)/.exec(header)![1]);
    const swr = Number(/stale-while-revalidate=(\d+)/.exec(header)![1]);

    // The cap, and the reason for it: `isToday`, `todayCrowdLevel` and a past day's
    // "closed" are statements about which day today is. The origin re-derives them when it
    // serves the month cache; a CDN copy cannot, and a browser copy cannot even be purged.
    // A second of slack because the header is computed a moment before this assertion.
    expect(maxAge).toBeLessThanOrEqual(left + 1);
    expect(sMaxAge).toBe(maxAge);
    expect(maxAge).toBe(Math.min(24 * 60 * 60, maxAge));

    // Today is also the one day whose status can flip while it is being read, so it does not
    // get a day of stale grace on top of its freshness — and never more than the day has left.
    expect(swr).toBeLessThanOrEqual(60 * 60);
    expect(swr).toBeLessThanOrEqual(left + 1);
  });

  it("sends a week for a range that ended before today", async () => {
    await mountWithDays([dayOn(parkDate(-5)), dayOn(parkDate(-2))]);
    const res = makeRes();

    await call(res);

    // Past days are measurements; a measurement of last Tuesday does not get a second
    // opinion, and a late backfill reaches the frontend through the per-park tag.
    expect(res.headers["Cache-Control"]).toBe(
      "public, max-age=604800, s-maxage=604800, stale-while-revalidate=604800",
    );
  });

  /**
   * `hourly` is a countdown over the next few open hours, not a statement about a day:
   * measured at Phantasialand on 2026-09-14 the same URL answered `11 12 13 14 15` at
   * 11:41 and `12 13 14 15` at 12:56, and the day-long window froze the first copy of
   * the day for everyone else until the park's midnight.
   */
  describe("a response carrying the hourly curve", () => {
    const parse = (header: string) => ({
      maxAge: Number(/max-age=(\d+)/.exec(header)![1]),
      sMaxAge: Number(/s-maxage=(\d+)/.exec(header)![1]),
      swr: Number(/stale-while-revalidate=(\d+)/.exec(header)![1]),
    });

    it("does not outlive the hour it was built in", async () => {
      await mountWithDays([dayWithHourlyOn(parkDate(0))]);
      const res = makeRes();

      await call(res);

      const { maxAge, sMaxAge, swr } = parse(res.headers["Cache-Control"]);
      // A second of slack in each direction: the header is computed a moment before the
      // expectation, so the two readings can straddle a whole second.
      expect(maxAge).toBeLessThanOrEqual(secondsLeftThisHour() + 1);
      expect(maxAge).toBeGreaterThanOrEqual(secondsLeftThisHour() - 2);
      expect(sMaxAge).toBe(maxAge);
      // A minute of grace past the boundary, so the CDN does not revalidate every park at
      // :00 into the live aggregation.
      expect(swr).toBeLessThanOrEqual(60);
    });

    it("is the only thing shortened — the same range without a curve keeps its window", async () => {
      // The pair is taken on a FUTURE range, where the window without a curve is a flat
      // 86400 whatever the clock says. On a range containing today the two can legitimately
      // meet — in the park's last hour the day is shorter than the hour — and a control the
      // two halves can tie on proves nothing (📚 G-76).
      await mountWithDays([dayWithHourlyOn(parkDate(20))]);
      const withCurve = makeRes();
      await call(withCurve);

      await mountWithDays([dayOn(parkDate(20))]);
      const withoutCurve = makeRes();
      await call(withoutCurve);

      // The calendar grid asks with `includeHourly=none` and gets no curve, so its window is
      // untouched — which is what AK 2 of PAR-217 asks for.
      expect(parse(withoutCurve.headers["Cache-Control"]).maxAge).toBe(
        24 * 60 * 60,
      );
      expect(parse(withCurve.headers["Cache-Control"]).maxAge).toBeLessThan(
        parse(withoutCurve.headers["Cache-Control"]).maxAge,
      );

      // And the range containing today keeps the cap it already had when no curve rides
      // along: the rest of the park's day, never more.
      await mountWithDays([dayOn(parkDate(0))]);
      const todayWithoutCurve = makeRes();
      await call(todayWithoutCurve);
      const left = secondsUntilEndOfDayInTimezone(park.timezone);
      const todayMaxAge = parse(
        todayWithoutCurve.headers["Cache-Control"],
      ).maxAge;
      expect(todayMaxAge).toBeLessThanOrEqual(left + 1);
      expect(todayMaxAge).toBeGreaterThanOrEqual(left - 2);
    });

    it("reads the response, not the range — a future day with a curve is capped too", async () => {
      await mountWithDays([dayWithHourlyOn(parkDate(20))]);
      const res = makeRes();

      await call(res);

      // Without the curve this range sends a day (86400); the cap is decided by what the
      // response actually carries.
      expect(parse(res.headers["Cache-Control"]).maxAge).toBeLessThanOrEqual(
        secondsLeftThisHour() + 1,
      );
    });

    it("counts an empty curve as no curve", async () => {
      await mountWithDays([
        { ...dayOn(parkDate(20)), hourly: [] } as unknown as CalendarDay,
      ]);
      const res = makeRes();

      await call(res);

      // `buildHourlyPredictionsFromList` answers `undefined` for a day it has no prediction
      // for, so an empty array is not a state the origin produces today — but a day with no
      // hours in it is not a countdown either, and shortening its window would buy nothing.
      expect(res.headers["Cache-Control"]).toBe(
        "public, max-age=86400, s-maxage=86400, stale-while-revalidate=86400",
      );
    });
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
      expect(res.headers["Cache-Control"]).toMatch(
        /stale-while-revalidate=[1-9]\d*/,
      );
    }
  });
});
