import { Test } from "@nestjs/testing";
import { formatInTimeZone } from "date-fns-tz";
import { getRepositoryToken } from "@nestjs/typeorm";
import { PlanDayService } from "./plan-day.service";
import { CalendarService } from "./calendar.service";
import { MLService } from "../../ml/ml.service";
import { ParkHistoricalStatsService } from "../../analytics/park-historical-stats.service";
import { AnalyticsService } from "../../analytics/analytics.service";
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
  let downRows: Array<{ attractionId: string }>;
  let headlinerIds: Set<string>;
  let headlinerFails: boolean;
  let queryCalls: unknown[][];
  let hourlyHistory: Map<string, { slots: unknown[] }>;
  let historyFails: boolean;

  const build = async () => {
    const moduleRef = await Test.createTestingModule({
      providers: [
        PlanDayService,
        {
          provide: getRepositoryToken(Attraction),
          useValue: {
            find: jest.fn().mockImplementation(async () => attractions),
            manager: {
              query: jest
                .fn()
                .mockImplementation(async (...args: unknown[]) => {
                  queryCalls.push(args);
                  return downRows;
                }),
            },
          },
        },
        {
          provide: AnalyticsService,
          useValue: {
            getHeadlinerAttractionIds: jest
              .fn()
              .mockImplementation(async () => {
                if (headlinerFails) throw new Error("analytics down");
                return headlinerIds;
              }),
            getParkHourlyHistory: jest.fn().mockImplementation(async () => {
              if (historyFails) throw new Error("history down");
              return hourlyHistory;
            }),
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
    downRows = [];
    headlinerIds = new Set<string>();
    headlinerFails = false;
    queryCalls = [];
    hourlyHistory = new Map();
    historyFails = false;
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

  it("ships coordinates as numbers, not as the strings Postgres returns", async () => {
    // A `decimal` column comes back from TypeORM as a string whatever the
    // entity declares, which is why the park payload has always carried
    // `"50.7992616"` with quotes. A planner measures distances with these, so
    // the conversion happens here rather than in every consumer that forgets.
    attractions = [
      {
        id: "a-taron",
        slug: "taron",
        name: "Taron",
        landName: "Mystery",
        latitude: "50.7992616" as unknown as number,
        longitude: "6.8839089" as unknown as number,
      },
    ];

    const date = farDate();
    calendarDay = { ...calendarDay!, date };
    dailyPredictions = [
      {
        ...(dailyPredictions[0] as object),
        predictedTime: `${date}T12:00:00.000Z`,
      },
    ];

    const plan = await service.buildPlanDay(park, date);
    const ride = plan.rides.find((r) => r.attractionSlug === "taron");

    expect(typeof ride?.latitude).toBe("number");
    expect(ride?.latitude).toBeCloseTo(50.7992616, 6);
    expect(ride?.longitude).toBeCloseTo(6.8839089, 6);
  });

  it("reports an unreadable coordinate as null rather than NaN", async () => {
    // Not every attraction has been placed on a map, and a column can hold
    // something that is not a number at all. NaN in a distance is worse than an
    // absent one: it propagates silently through every sum and comparison after
    // it, and `NaN < 5` is false, so a broken leg reads as a comfortable one.
    attractions = [
      {
        id: "a-taron",
        slug: "taron",
        name: "Taron",
        landName: "Mystery",
        latitude: "" as unknown as number,
        longitude: "n/a" as unknown as number,
      },
    ];
    const date = farDate();
    calendarDay = { ...calendarDay!, date };
    dailyPredictions = [
      {
        ...(dailyPredictions[0] as object),
        predictedTime: `${date}T12:00:00.000Z`,
      },
    ];

    const plan = await service.buildPlanDay(park, date);
    const ride = plan.rides.find((r) => r.attractionSlug === "taron");

    expect(ride).toBeDefined();
    // "" coerces to 0 through Number(), which would put every unplaced ride off
    // the coast of Africa; "n/a" coerces to NaN.
    expect(ride?.latitude).toBeNull();
    expect(ride?.longitude).toBeNull();
  });

  it("reports a ride that was observed all day yesterday and never operating", async () => {
    // "Down" and "unobserved" are different answers. The query only returns a
    // ride that WAS seen and was never OPERATING; a ride with no rows at all is
    // silence, and warning about silence would assert something nobody observed.
    downRows = [{ attractionId: "a-taron" }];
    // The PARK's today, not UTC's. Late in the evening Berlin has already rolled
    // over while `toISOString()` has not, and the service asks in park time — so
    // a UTC date here would make this test about yesterday.
    const today = formatInTimeZone(new Date(), park.timezone, "yyyy-MM-dd");
    calendarDay = { ...calendarDay!, date: today };
    dailyPredictions = [
      {
        ...(dailyPredictions[0] as object),
        predictedTime: `${today}T12:00:00.000Z`,
      },
    ];

    const plan = await service.buildPlanDay(park, today);
    const ride = plan.rides.find((r) => r.attractionSlug === "taron");

    expect(ride?.downYesterday).toBe(true);
  });

  it("marks the park's curated headliners and nothing else", async () => {
    // The CURATED set, threaded through — never re-derived from `dayPeak`. A
    // planner that guessed headliners from the day's tallest bars would point at
    // whatever happens to be busy, which is the opposite of the question the
    // visitor is asking: "did I miss the big one".
    headlinerIds = new Set(["a-taron"]);
    const today = formatInTimeZone(new Date(), park.timezone, "yyyy-MM-dd");
    calendarDay = { ...calendarDay!, date: today };
    dailyPredictions = [
      {
        ...(dailyPredictions[0] as object),
        predictedTime: `${today}T12:00:00.000Z`,
      },
    ];

    const plan = await service.buildPlanDay(park, today);
    const taron = plan.rides.find((r) => r.attractionSlug === "taron");

    expect(taron?.isHeadliner).toBe(true);
    // Absent, not false: the field is omitted for an ordinary ride, so the
    // payload does not carry a boolean per ride for every park in the catalogue.
    expect(
      plan.rides
        .filter((r) => r.attractionSlug !== "taron")
        .every((r) => r.isHeadliner === undefined),
    ).toBe(true);
  });

  it("serves the day anyway when the headliner lookup fails", async () => {
    // One analytics query must not cost the visitor their plan. The mock has to
    // actually REJECT — asserting this against a working lookup would pass
    // whatever the service does with the error.
    headlinerFails = true;
    const plan = await service.buildPlanDay(park, "2026-10-17");

    expect(plan.rides.length).toBeGreaterThan(0);
    expect(plan.rides.every((r) => r.isHeadliner === undefined)).toBe(true);
  });

  it("does not ask about yesterday for a date weeks out", async () => {
    // Whether a ride broke yesterday says nothing a visitor can act on for a
    // Tuesday in November, and the query is not worth its cost there.
    downRows = [{ attractionId: "a-taron" }];
    const date = farDate();
    calendarDay = { ...calendarDay!, date };
    dailyPredictions = [
      {
        ...(dailyPredictions[0] as object),
        predictedTime: `${date}T12:00:00.000Z`,
      },
    ];

    const plan = await service.buildPlanDay(park, date);
    const ride = plan.rides.find((r) => r.attractionSlug === "taron");

    expect(queryCalls).toHaveLength(0);
    expect(ride?.downYesterday).toBeUndefined();
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

  // ── A day that already happened ────────────────────────────────────────────
  // Before this the model was asked about the past. It generates forwards, so
  // nothing matched and the response came back with an empty ride list under
  // `tier: "measured"` and a negative `leadDays` — the most trustworthy label on
  // the emptiest answer.
  describe("a date in the past", () => {
    const pastDate = () => {
      const d = new Date();
      d.setUTCDate(d.getUTCDate() - 7);
      return d.toISOString().slice(0, 10);
    };

    const slots = (
      entries: Array<[string, number, number]>,
    ): { slots: Array<Record<string, unknown>> } => ({
      slots: entries.map(([time_slot, avgWait, sampleCount]) => ({
        time_slot,
        avgWait,
        p90: avgWait + 20,
        sampleCount,
      })),
    });

    it("is answered from what the queues actually did", async () => {
      const date = pastDate();
      calendarDay = { ...calendarDay!, date };
      hourlyHistory = new Map([
        [
          "a-taron",
          slots([
            ["10:00", 30, 4],
            ["10:15", 30, 4],
            ["10:30", 30, 4],
            ["10:45", 30, 4],
            ["14:00", 70, 4],
          ]),
        ],
      ]) as never;

      const plan = await service.buildPlanDay(park, date);

      expect(plan.tier).toBe("observed");
      expect(plan.leadDays).toBe(-7);
      expect(plan.rides).toHaveLength(1);
      const taron = plan.rides[0];
      expect(taron.hours).toEqual([
        { hour: 10, wait: 30 },
        { hour: 14, wait: 70 },
      ]);
      expect(taron.dayPeak).toBe(70);
      // An observation has no band. A width of zero would be a claim about
      // precision rather than the absence of one.
      expect(taron.uncertaintyMinutes).toBeNull();
      // Exactly one measured day stands behind this curve: this one.
      expect(taron.sampleDays).toBe(1);
    });

    it("weights each slot by how often the feed answered", async () => {
      const date = pastDate();
      calendarDay = { ...calendarDay!, date };
      // Three quarters of the hour at 20 minutes with twelve readings each, and
      // one quarter at 80 with a single reading — a two-minute outage. A plain
      // mean of the four slots is 35; the weighted one is 21.6 → 20.
      hourlyHistory = new Map([
        [
          "a-taron",
          slots([
            ["11:00", 20, 12],
            ["11:15", 20, 12],
            ["11:30", 20, 12],
            ["11:45", 80, 1],
          ]),
        ],
      ]) as never;

      const plan = await service.buildPlanDay(park, date);

      expect(plan.rides[0].hours).toEqual([{ hour: 11, wait: 20 }]);
    });

    it("keeps a slot the writer left no count on", async () => {
      const date = pastDate();
      calendarDay = { ...calendarDay!, date };
      hourlyHistory = new Map([
        ["a-taron", slots([["12:00", 45, 0]])],
      ]) as never;

      const plan = await service.buildPlanDay(park, date);

      // A gap in the writer's bookkeeping must not delete an hour somebody
      // actually stood in.
      expect(plan.rides[0].hours).toEqual([{ hour: 12, wait: 45 }]);
    });

    it("omits a ride the rollup has no row for rather than drawing it at zero", async () => {
      const date = pastDate();
      calendarDay = { ...calendarDay!, date };
      attractions = [
        { id: "a-taron", slug: "taron", name: "Taron", landName: "Mystery" },
        { id: "a-fly", slug: "f-l-y", name: "F.L.Y.", landName: "Rookburgh" },
      ];
      service = await build();
      hourlyHistory = new Map([
        ["a-taron", slots([["10:00", 25, 4]])],
      ]) as never;

      const plan = await service.buildPlanDay(park, date);

      // Absence in that table means "not rolled up", never "empty queue".
      expect(plan.rides.map((r) => r.attractionSlug)).toEqual(["taron"]);
    });

    it("drops slots outside the park's own hours", async () => {
      const date = pastDate();
      calendarDay = { ...calendarDay!, date };
      hourlyHistory = new Map([
        [
          "a-taron",
          slots([
            ["07:00", 15, 4],
            ["10:00", 40, 4],
            ["22:00", 15, 4],
          ]),
        ],
      ]) as never;

      const plan = await service.buildPlanDay(park, date);

      expect(plan.rides[0].hours).toEqual([{ hour: 10, wait: 40 }]);
    });

    it("asks for no forecast at all", async () => {
      const date = pastDate();
      calendarDay = { ...calendarDay!, date };
      hourlyHistory = new Map([
        ["a-taron", slots([["10:00", 25, 4]])],
      ]) as never;

      const plan = await service.buildPlanDay(park, date);
      expect(plan.rides).toHaveLength(1);

      // Composing a shape onto a past date would draw a forecast for a day the
      // visitor already walked — and `getParkHourlyProfile` is a year of
      // aggregates over every ride in the park, paid for nothing.
      const stats = (
        service as unknown as {
          historicalStatsService: { getParkHourlyProfile: jest.Mock };
        }
      ).historicalStatsService;
      expect(stats.getParkHourlyProfile).not.toHaveBeenCalled();
      const ml = (
        service as unknown as {
          mlService: {
            getParkPredictions: jest.Mock;
            getServingDailyPredictions: jest.Mock;
          };
        }
      ).mlService;
      expect(ml.getParkPredictions).not.toHaveBeenCalled();
      expect(ml.getServingDailyPredictions).not.toHaveBeenCalled();
    });

    it("survives the rollup being unavailable", async () => {
      const date = pastDate();
      calendarDay = { ...calendarDay!, date };
      historyFails = true;

      const plan = await service.buildPlanDay(park, date);

      expect(plan.tier).toBe("observed");
      expect(plan.rides).toEqual([]);
      expect(plan.context.status).toBe("OPERATING");
    });
  });
});
