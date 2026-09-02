import { Test } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { PlanDayService } from "./plan-day.service";
import { CalendarService } from "./calendar.service";
import { MLService } from "../../ml/ml.service";
import { ParkHistoricalStatsService } from "../../analytics/park-historical-stats.service";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { Park } from "../entities/park.entity";

/**
 * The endpoint's job is to be honest about which of three regimes produced a
 * number, and to say nothing rather than something plausible when it has no
 * basis. Both failures are invisible in the rendered output — a composed curve
 * and a measured one draw identically — so they have to be pinned here.
 */
describe("PlanDayService", () => {
  const park = {
    id: "park-1",
    slug: "phantasialand",
    timezone: "Europe/Berlin",
  } as Park;

  const OPEN = "2026-10-17T07:00:00.000Z"; // 09:00 Berlin
  const CLOSE = "2026-10-17T16:00:00.000Z"; // 18:00 Berlin

  let service: PlanDayService;
  let calendarDay: Record<string, unknown> | null;
  let dailyPredictions: unknown[];
  let hourlyPredictions: unknown[];
  let profile: unknown;
  let attractions: Array<Partial<Attraction>>;

  const build = async () => {
    const moduleRef = await Test.createTestingModule({
      providers: [
        PlanDayService,
        {
          provide: getRepositoryToken(Attraction),
          useValue: {
            find: jest.fn().mockImplementation(async () => attractions),
          },
        },
        {
          provide: MLService,
          useValue: {
            getParkPredictions: jest.fn().mockImplementation(async () => ({
              predictions: hourlyPredictions,
            })),
            getServingDailyPredictions: jest
              .fn()
              .mockImplementation(async () => ({
                predictions: dailyPredictions,
              })),
          },
        },
        {
          provide: CalendarService,
          useValue: {
            buildCalendarResponse: jest.fn().mockImplementation(async () => ({
              days: calendarDay ? [calendarDay] : [],
            })),
          },
        },
        {
          provide: ParkHistoricalStatsService,
          useValue: {
            getParkHourlyProfile: jest
              .fn()
              .mockImplementation(async () => profile),
          },
        },
      ],
    }).compile();
    return moduleRef.get(PlanDayService);
  };

  beforeEach(async () => {
    attractions = [
      { id: "a-taron", slug: "taron", name: "Taron", landName: "Mystery" },
    ];
    calendarDay = {
      date: "2026-10-17",
      status: "OPERATING",
      hours: { openingTime: OPEN, closingTime: CLOSE },
      crowdLevel: "high",
      isHoliday: false,
      isBridgeDay: false,
      isSchoolVacation: false,
    };
    profile = {
      hours: [10, 14, 18],
      attractions: [
        {
          attractionSlug: "taron",
          attractionName: "Taron",
          land: "Mystery",
          p50: [30, 60, 20],
          sampleDays: 141,
        },
      ],
    };
    dailyPredictions = [
      {
        attractionId: "a-taron",
        predictedTime: "2026-10-17T12:00:00.000Z",
        predictedWaitTime: 60,
        predictionType: "daily",
        uncertaintyMinutes: 12,
      },
    ];
    hourlyPredictions = [];
    service = await build();
  });

  // A date far enough out that the tier is unambiguous whatever "today" is when
  // the suite runs. The service reads the clock, so the tests that care about a
  // specific tier fake it rather than depending on the calendar.
  const farDate = () => {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() + 30);
    return d.toISOString().slice(0, 10);
  };

  it("composes a curve from the day level and the historical shape", async () => {
    const date = farDate();
    calendarDay = { ...calendarDay!, date };
    dailyPredictions = [
      {
        ...(dailyPredictions[0] as object),
        predictedTime: `${date}T12:00:00.000Z`,
      },
    ];

    const plan = await service.buildPlanDay(park, date);

    expect(plan.tier).toBe("composed");
    expect(plan.rides).toHaveLength(1);
    const taron = plan.rides[0];
    // The shape peaks at 14:00 and the day level is 60, so that is where the
    // curve's maximum lands.
    expect(Math.max(...taron.hours.map((h) => h.wait))).toBe(60);
    expect(taron.hours.find((h) => h.wait === 60)!.hour).toBe(14);
    // Open hours only, inclusive: 09:00–18:00 Berlin.
    expect(taron.hours[0].hour).toBe(9);
    expect(taron.hours[taron.hours.length - 1].hour).toBe(18);
    expect(taron.uncertaintyMinutes).toBe(12);
  });

  it("omits a ride with no measured shape rather than drawing it flat", async () => {
    const date = farDate();
    calendarDay = { ...calendarDay!, date };
    dailyPredictions = [
      {
        ...(dailyPredictions[0] as object),
        predictedTime: `${date}T12:00:00.000Z`,
      },
    ];
    profile = {
      hours: [10, 14, 18],
      attractions: [
        {
          attractionSlug: "taron",
          attractionName: "Taron",
          land: "Mystery",
          p50: [null, null, null],
          sampleDays: 0,
        },
      ],
    };

    const plan = await service.buildPlanDay(park, date);

    // A flat line at the day's level would assert the queue never changes,
    // which is a claim about the ride rather than about our data.
    expect(plan.rides).toHaveLength(0);
  });

  it("returns the day's context with no rides when the park is closed", async () => {
    const date = farDate();
    calendarDay = {
      date,
      status: "CLOSED",
      hours: null,
      crowdLevel: "closed",
      isHoliday: false,
      isBridgeDay: false,
      isSchoolVacation: false,
    };

    const plan = await service.buildPlanDay(park, date);

    expect(plan.context.status).toBe("CLOSED");
    expect(plan.context.openHour).toBeNull();
    expect(plan.rides).toEqual([]);
  });

  it("leaves weather null past the forecast rather than substituting one", async () => {
    // The calendar simply stops sending weather about two weeks out. Filling it
    // with a climate normal would move every bar on the day, and the caller
    // could not tell the invented value from a real one.
    const date = farDate();
    calendarDay = { ...calendarDay!, date, weather: undefined };
    dailyPredictions = [
      {
        ...(dailyPredictions[0] as object),
        predictedTime: `${date}T12:00:00.000Z`,
      },
    ];

    const plan = await service.buildPlanDay(park, date);

    expect(plan.context.weather).toBeNull();
  });

  it("reports no lead-time error figure while none has been measured", async () => {
    // The archive that measures error by lead distance starts empty and takes
    // as many days as the bucket to say anything. Absent is the honest answer;
    // a caller widens the band with distance without attaching a number.
    const plan = await service.buildPlanDay(park, farDate());
    expect(plan.leadTimeMae).toBeNull();
  });

  it("derives isWeekend, which the calendar does not carry", async () => {
    // 2026-10-17 is a Saturday.
    calendarDay = { ...calendarDay!, date: "2026-10-17" };
    const plan = await service.buildPlanDay(park, "2026-10-17");
    expect(plan.context.isWeekend).toBe(true);

    // 2026-10-19 is a Monday.
    calendarDay = { ...calendarDay!, date: "2026-10-19" };
    const monday = await service.buildPlanDay(park, "2026-10-19");
    expect(monday.context.isWeekend).toBe(false);
  });

  it("falls back to composing when today's hourly rows are missing", async () => {
    // `measured` is claimed only when the hourly rows actually exist. A park the
    // generator skipped tonight would otherwise get an empty day labelled as the
    // model's own answer, which is the worst of both.
    const today = new Date().toISOString().slice(0, 10);
    calendarDay = { ...calendarDay!, date: today };
    dailyPredictions = [
      {
        ...(dailyPredictions[0] as object),
        predictedTime: `${today}T12:00:00.000Z`,
      },
    ];
    hourlyPredictions = [];

    const plan = await service.buildPlanDay(park, today);

    expect(plan.tier).toBe("measured");
    // Composed rides rather than nothing.
    expect(plan.rides.length).toBeGreaterThan(0);
  });

  it("survives every upstream going missing at once", async () => {
    calendarDay = null;
    profile = null;
    dailyPredictions = [];
    hourlyPredictions = [];

    const plan = await service.buildPlanDay(park, farDate());

    expect(plan.context.status).toBe("UNKNOWN");
    expect(plan.rides).toEqual([]);
    expect(plan.shows).toEqual([]);
  });
});
