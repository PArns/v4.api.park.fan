import { Test } from "@nestjs/testing";
import { formatInTimeZone } from "date-fns-tz";
import { getRepositoryToken } from "@nestjs/typeorm";
import { PlanDayService } from "./plan-day.service";
import { CalendarService } from "./calendar.service";
import { MLService } from "../../ml/ml.service";
import { ParkHistoricalStatsService } from "../../analytics/park-historical-stats.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { PredictionLeadSnapshotService } from "../../ml/services/prediction-lead-snapshot.service";
import { ShowsService } from "../../shows/shows.service";
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
  let leadMae: number | null;
  let rideOpenings: Map<string, string>;
  let profileMock: jest.Mock;
  let leadMaeMock: jest.Mock;
  let parkShows: Array<{ id: string; slug: string; name: string }>;
  let scheduledTimes: Map<string, string[]>;
  let patterns: Map<string, unknown>;

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
            getRideOpeningTimes: jest
              .fn()
              .mockImplementation(async () => rideOpenings),
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
          useValue: { getParkHourlyProfile: profileMock },
        },
        {
          provide: PredictionLeadSnapshotService,
          useValue: { getLeadTimeMae: leadMaeMock },
        },
        {
          provide: ShowsService,
          useValue: {
            findByParkId: jest.fn().mockImplementation(async () => parkShows),
            getShowtimesOnDate: jest
              .fn()
              .mockImplementation(async () => scheduledTimes),
            getSchedulePatterns: jest
              .fn()
              .mockImplementation(async () => patterns),
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
    leadMae = null;
    rideOpenings = new Map();
    parkShows = [];
    scheduledTimes = new Map();
    patterns = new Map();
    profileMock = jest.fn().mockImplementation(async () => profile);
    leadMaeMock = jest.fn().mockImplementation(async () => leadMae);
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

  it("says composed when today's hourly rows are missing, not measured", async () => {
    // The tier comes from the curves that were built, never from the distance.
    // A park the generator skipped tonight, or an ML service having a bad
    // minute, still gets a usable day — but under the label that describes what
    // it actually got. Deciding by distance meant composed data went out as the
    // model's own hourly answer, which is the one failure this design exists to
    // prevent, and the assertion here used to pin it.
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

    expect(plan.tier).toBe("composed");
    // Composed rides rather than nothing.
    expect(plan.rides.length).toBeGreaterThan(0);
    // And every hour is the response's own tier, so none of them needs a source.
    expect(plan.rides[0].hours.every((h) => h.source === undefined)).toBe(true);
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

  it("reports a ride that broke and stayed broken through yesterday", async () => {
    // Three states, all of which have to stay apart: a ride that was DOWN all
    // day, a ride the feed never mentioned (silence — warning about it would
    // assert something nobody observed), and a ride the feed called CLOSED all
    // day (a season or a refurbishment, not a fault).
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

    // The SQL is only parsed when it runs, so the two things that were wrong
    // with it are asserted on its text.
    const sql = String(queryCalls[0][0]);
    // A ride the feed called CLOSED all day is not a fault. Without this
    // clause the endpoint reported nine of Phantasialand's winter-only and
    // water attractions as down, every day of the summer — all nine of them
    // CLOSED-only, verified against the live database.
    expect(sql).toContain("qd.status = 'DOWN'");
    // And the day is a half-open RANGE on the raw column, never a cast on it:
    // `(qd.timestamp AT TIME ZONE $2)::date = …` hides the column from
    // TimescaleDB's chunk exclusion, which on the live database meant reading
    // all 254 chunks (35,967 buffers, 1.1 s cold) to find nine rows in one.
    expect(sql).not.toMatch(/AT TIME ZONE \$2\)::date/);
    expect(sql).toContain("qd.timestamp >=");
    expect(sql).toContain("qd.timestamp <");
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

  // ── The model's 24-hour window ─────────────────────────────────────────────
  // Hourly predictions exist for the next 24 hours and not one minute further,
  // so a day inside that window is part measured and part composed. Measured
  // against the live service at 17:15, today's plan for a park open 09:00-22:00
  // carried 17:00-21:00 and tomorrow's carried 09:00-17:00 — the evening simply
  // absent, and `dayPeak` the maximum of what was left.
  describe("inside the hourly window", () => {
    /** The UTC instant of a park-local hour on a park-local date. */
    const atParkHour = (date: string, hour: number): string => {
      for (let step = 0; step <= 48; step++) {
        const at = new Date(`${date}T00:00:00.000Z`);
        at.setUTCHours(at.getUTCHours() - 12 + step);
        if (
          formatInTimeZone(at, park.timezone, "yyyy-MM-dd") === date &&
          Number(formatInTimeZone(at, park.timezone, "HH")) === hour
        ) {
          return at.toISOString();
        }
      }
      throw new Error(`no instant for ${date} ${hour}:00`);
    };

    const today = () =>
      formatInTimeZone(new Date(), park.timezone, "yyyy-MM-dd");

    /** Hourly rows for a park-local hour range, one slot each. */
    const hourlyFor = (date: string, from: number, to: number) => {
      const rows = [];
      for (let h = from; h <= to; h++) {
        rows.push({
          attractionId: "a-taron",
          predictedTime: atParkHour(date, h),
          predictedWaitTime: 20,
          predictionType: "hourly",
          uncertaintyMinutes: 7,
        });
      }
      return rows;
    };

    beforeEach(() => {
      const date = today();
      calendarDay = { ...calendarDay!, date };
      dailyPredictions = [
        {
          ...(dailyPredictions[0] as object),
          predictedTime: `${date}T12:00:00.000Z`,
        },
      ];
    });

    it("fills the hours the window does not reach, and marks them", async () => {
      const date = today();
      hourlyPredictions = hourlyFor(date, 9, 11);

      const plan = await service.buildPlanDay(park, date);

      expect(plan.tier).toBe("measured");
      const taron = plan.rides[0];
      // The park is open 09:00-18:00 and every hour of it is answered.
      expect(taron.hours.map((h) => h.hour)).toEqual([
        9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
      ]);
      // The three the model answered carry no `source`: they ARE the tier.
      expect(
        taron.hours.filter((h) => h.hour <= 11).every((h) => !h.source),
      ).toBe(true);
      // The rest say where they came from, rather than being missing.
      expect(
        taron.hours
          .filter((h) => h.hour >= 12)
          .every((h) => h.source === "composed"),
      ).toBe(true);
    });

    it("keeps the day's own peak, not the peak of the hours that survived", async () => {
      const date = today();
      // The model answers 20 minutes for the three hours it reaches; the day
      // level says the day peaks at 60. Reading the maximum of `hours` made
      // today read 20 where the same ride read 42 five days out, and the whole
      // difference was which statistic the field carried.
      hourlyPredictions = hourlyFor(date, 9, 11);

      const plan = await service.buildPlanDay(park, date);

      expect(plan.rides[0].dayPeak).toBe(60);
      // And the band comes from the same row as the number it surrounds.
      expect(plan.rides[0].uncertaintyMinutes).toBe(12);
    });

    it("still answers measured hours for a ride with no shape at all", async () => {
      const date = today();
      profile = { hours: [], attractions: [] };
      hourlyPredictions = hourlyFor(date, 9, 11);

      const plan = await service.buildPlanDay(park, date);

      expect(plan.tier).toBe("measured");
      expect(plan.rides[0].hours.map((h) => h.hour)).toEqual([9, 10, 11]);
      expect(plan.rides[0].sampleDays).toBe(0);
    });
  });

  // ── When the RIDE opens, which is not when the park opens ──────────────────
  describe("a ride that opens later than its park", () => {
    it("starts the curve at the ride's own opening and says when that is", async () => {
      // Phantasialand opens at 09:00 and Taron runs from 10:00 — measured over
      // 30 days, its rides split into two groups an hour apart. Starting every
      // curve at the park's hour invented two hours of queue on the coasters
      // people plan their morning around.
      const date = farDate();
      calendarDay = { ...calendarDay!, date };
      dailyPredictions = [
        {
          ...(dailyPredictions[0] as object),
          predictedTime: `${date}T12:00:00.000Z`,
        },
      ];
      rideOpenings = new Map([["a-taron|09:00", "10:00"]]);

      const plan = await service.buildPlanDay(park, date);

      const taron = plan.rides[0];
      expect(taron.opensAt).toBe("10:00");
      expect(taron.hours[0].hour).toBe(10);
      // And the day still runs to the park's close.
      expect(taron.hours[taron.hours.length - 1].hour).toBe(18);
    });

    it("ignores an opening earlier than the park's own", async () => {
      // Half the rides report OPERATING before the gates open — the feed carries
      // the operator's system state, not whether a visitor can walk up. The
      // park's hour is the floor.
      const date = farDate();
      calendarDay = { ...calendarDay!, date };
      dailyPredictions = [
        {
          ...(dailyPredictions[0] as object),
          predictedTime: `${date}T12:00:00.000Z`,
        },
      ];
      rideOpenings = new Map([["a-taron|09:00", "08:00"]]);

      const plan = await service.buildPlanDay(park, date);

      expect(plan.rides[0].hours[0].hour).toBe(9);
      // Nothing to tell the reader: it opens with the park.
      expect(plan.rides[0].opensAt).toBeUndefined();
    });

    it("does not carry a summer opening into a winter morning", async () => {
      // Measured at Phantasialand over a year: the coasters run an hour after a
      // 09:00 gate and WITH an 11:00 one. Keying the lookup on the day's own
      // park opening is what stops a July median claiming 10:00 for a Christmas
      // morning — and a January one capping July at 11:00.
      const date = farDate();
      calendarDay = { ...calendarDay!, date };
      dailyPredictions = [
        {
          ...(dailyPredictions[0] as object),
          predictedTime: `${date}T12:00:00.000Z`,
        },
      ];
      // Only a winter observation exists; this day opens at 09:00.
      rideOpenings = new Map([["a-taron|11:00", "11:00"]]);

      const plan = await service.buildPlanDay(park, date);

      expect(plan.rides[0].opensAt).toBeUndefined();
      expect(plan.rides[0].hours[0].hour).toBe(9);
    });

    it("draws the park's day when the opening is unknown", async () => {
      const date = farDate();
      calendarDay = { ...calendarDay!, date };
      dailyPredictions = [
        {
          ...(dailyPredictions[0] as object),
          predictedTime: `${date}T12:00:00.000Z`,
        },
      ];

      const plan = await service.buildPlanDay(park, date);

      expect(plan.rides[0].hours[0].hour).toBe(9);
      expect(plan.rides[0].opensAt).toBeUndefined();
    });
  });

  it("asks the shape for every ride it can cover, not the top twenty", async () => {
    // The profile's SQL fetches `min(topN * 3, 60)` rides so its peak-hour
    // re-ranking has something to re-rank and then discards everything past
    // topN, so asking for 20 ran the identical query and threw two thirds away.
    // Phantasialand's list went 34 -> 16 between tomorrow and the day after,
    // which reads as rides closing.
    await service.buildPlanDay(park, farDate());

    expect(profileMock).toHaveBeenCalledWith(park, 1, 60, 20);
  });

  it("says long_range when the model has said nothing about the date", async () => {
    const date = farDate();
    calendarDay = { ...calendarDay!, date };
    dailyPredictions = [];

    const plan = await service.buildPlanDay(park, date);

    // Not `composed` with an empty list: there is no day level to compose from,
    // and the distance that happens to be is not the reason.
    expect(plan.tier).toBe("long_range");
    expect(plan.rides).toEqual([]);
  });

  it("publishes the measured lead-time error, and asks for the right distance", async () => {
    leadMae = 8.4;
    const date = farDate();
    calendarDay = { ...calendarDay!, date };
    dailyPredictions = [
      {
        ...(dailyPredictions[0] as object),
        predictedTime: `${date}T12:00:00.000Z`,
      },
    ];

    const plan = await service.buildPlanDay(park, date);

    expect(plan.leadTimeMae).toBe(8.4);
    expect(leadMaeMock).toHaveBeenCalledWith(plan.leadDays);
  });

  // ── Opening hours the operator has not published ───────────────────────────
  describe("past the operator's publishing horizon", () => {
    it("derives the window from the hours we have actually measured", async () => {
      // Of 177 live parks with published hours, 91 reach 60 days and 38 reach
      // 120 — so a summer date asked in January has none, and the response was
      // an empty shell for exactly the distance the planner exists for.
      const date = farDate();
      calendarDay = { date, status: "UNKNOWN", isHoliday: false };
      dailyPredictions = [
        {
          ...(dailyPredictions[0] as object),
          predictedTime: `${date}T12:00:00.000Z`,
        },
      ];

      const plan = await service.buildPlanDay(park, date);

      // The profile's measured hours are 10, 14 and 18.
      expect(plan.context.openHour).toBe(10);
      expect(plan.context.closeHour).toBe(18);
      // Labelled, because it is a recording window and not a schedule: narrower
      // than the gates' hours, never wider.
      expect(plan.context.hoursSource).toBe("observed");
      expect(plan.rides.length).toBeGreaterThan(0);
    });

    it("does not invent hours for a day the operator says is closed", async () => {
      const date = farDate();
      calendarDay = { date, status: "CLOSED", isHoliday: false };

      const plan = await service.buildPlanDay(park, date);

      expect(plan.context.openHour).toBeNull();
      expect(plan.context.hoursSource).toBeUndefined();
      expect(plan.rides).toEqual([]);
    });

    it("invents nothing for a park nobody has watched", async () => {
      const date = farDate();
      calendarDay = { date, status: "UNKNOWN", isHoliday: false };
      profile = { hours: [], attractions: [] };

      const plan = await service.buildPlanDay(park, date);

      expect(plan.context.openHour).toBeNull();
      expect(plan.context.hoursSource).toBeUndefined();
      expect(plan.rides).toEqual([]);
    });

    it("marks hours that came from the schedule as such", async () => {
      const date = farDate();
      calendarDay = { ...calendarDay!, date };

      const plan = await service.buildPlanDay(park, date);

      expect(plan.context.hoursSource).toBe("schedule");
    });
  });

  // ── Shows ──────────────────────────────────────────────────────────────────
  // No feed publishes showtimes beyond the current day — checked at
  // ThemeParks.wiki, which answers for today and then serves entries it never
  // cleared, some from 2022. So a planned date can only be answered by
  // projecting, and the whole risk is that a projection reads as a promise.
  describe("shows", () => {
    const pattern = (over: Record<string, unknown> = {}) => ({
      showId: "s-1",
      weekday: 0,
      times: ["12:30", "14:30"],
      observedDays: 7,
      lastObservedOn: todayMinus(7),
      ...over,
    });

    function todayMinus(days: number): string {
      const d = new Date();
      d.setUTCDate(d.getUTCDate() - days);
      return d.toISOString().slice(0, 10);
    }

    beforeEach(() => {
      parkShows = [{ id: "s-1", slug: "big-moments", name: "Big Moments" }];
    });

    it("serves the operator's own times as scheduled", async () => {
      const date = farDate();
      calendarDay = { ...calendarDay!, date };
      scheduledTimes = new Map([["s-1", ["12:30", "14:30", "17:45"]]]);

      const plan = await service.buildPlanDay(park, date);

      expect(plan.shows).toEqual([
        {
          showSlug: "big-moments",
          showName: "Big Moments",
          times: ["12:30", "14:30", "17:45"],
          source: "scheduled",
        },
      ]);
    });

    it("projects the last matching weekday, and says so", async () => {
      const date = farDate();
      calendarDay = { ...calendarDay!, date };
      patterns = new Map([["s-1", pattern({ lastObservedOn: todayMinus(7) })]]);

      const plan = await service.buildPlanDay(park, date);

      expect(plan.shows).toHaveLength(1);
      const show = plan.shows[0];
      expect(show.source).toBe("projected");
      expect(show.times).toEqual(["12:30", "14:30"]);
      // A projection that cannot say what it came from is a schedule.
      expect(show.observedOn).toBe(todayMinus(7));
      expect(show.sampleDays).toBe(7);
    });

    it("refuses to turn a one-off event into a weekly show", async () => {
      // Europa-Park ran "Crazy Summer with Ross Antony & Paul Reeves" on exactly
      // one Thursday in July. Projecting it forward would have put a concert on
      // every remaining Thursday of the year.
      const date = farDate();
      calendarDay = { ...calendarDay!, date };
      patterns = new Map([["s-1", pattern({ observedDays: 1 })]]);

      const plan = await service.buildPlanDay(park, date);

      expect(plan.shows).toEqual([]);
    });

    it("stops projecting a programme nobody has seen for a month", async () => {
      const date = farDate();
      calendarDay = { ...calendarDay!, date };
      patterns = new Map([
        ["s-1", pattern({ lastObservedOn: todayMinus(40) })],
      ]);

      const plan = await service.buildPlanDay(park, date);

      expect(plan.shows).toEqual([]);
    });

    it("measures staleness against today, not against the date asked about", async () => {
      // Measuring against the target would reject every date more than four
      // weeks out — which is most of what a planner asks.
      const far = new Date();
      far.setUTCDate(far.getUTCDate() + 200);
      const date = far.toISOString().slice(0, 10);
      calendarDay = { ...calendarDay!, date };
      patterns = new Map([["s-1", pattern({ lastObservedOn: todayMinus(3) })]]);

      const plan = await service.buildPlanDay(park, date);

      expect(plan.shows).toHaveLength(1);
      expect(plan.shows[0].source).toBe("projected");
    });

    it("prefers the operator's answer over its own projection", async () => {
      const date = farDate();
      calendarDay = { ...calendarDay!, date };
      scheduledTimes = new Map([["s-1", ["20:00"]]]);
      patterns = new Map([["s-1", pattern()]]);

      const plan = await service.buildPlanDay(park, date);

      expect(plan.shows[0].source).toBe("scheduled");
      expect(plan.shows[0].times).toEqual(["20:00"]);
      expect(plan.shows[0].observedOn).toBeUndefined();
    });

    it("says nothing about a park whose shows it has never watched", async () => {
      const date = farDate();
      calendarDay = { ...calendarDay!, date };

      const plan = await service.buildPlanDay(park, date);

      // Not "this park has no shows" — a different statement, and the DTO
      // says which one this is.
      expect(plan.shows).toEqual([]);
    });

    it("keeps the day readable top to bottom", async () => {
      const date = farDate();
      calendarDay = { ...calendarDay!, date };
      parkShows = [
        { id: "s-1", slug: "late", name: "Late Show" },
        { id: "s-2", slug: "early", name: "Early Show" },
      ];
      scheduledTimes = new Map([
        ["s-1", ["21:45"]],
        ["s-2", ["10:00", "12:00"]],
      ]);

      const plan = await service.buildPlanDay(park, date);

      expect(plan.shows.map((s) => s.showSlug)).toEqual(["early", "late"]);
    });
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
      // The day's PEAK, which for a measured day is its P90 — the statistic the
      // forecast side of this field carries, and the one the calendar scores
      // past days with. The maximum of `hours` is 70, and using that made
      // `dayPeak` mean something different on each side of today.
      expect(taron.dayPeak).toBe(90);
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
