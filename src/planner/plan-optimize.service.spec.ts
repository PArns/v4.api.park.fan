import {
  ArgumentMetadata,
  BadRequestException,
  NotFoundException,
  ValidationPipe,
} from "@nestjs/common";
import { PlanOptimizeRequestDto } from "./dto/plan-optimize.dto";
import { PlanOptimizeService, dayClockFor } from "./plan-optimize.service";

/**
 * What the service adds around the engine: the clock from the park's zone, the day from the
 * plan-day cache with the rides in works beside it, and the 400s class-validator cannot say alone.
 * The body's shape is pinned against the same `ValidationPipe` options `main.ts` mounts, because a
 * controller spec calls the method directly and never meets the pipe.
 */

const PARK = {
  id: "8355a0b7-26e9-4a90-af47-246ec143e99a",
  slug: "test-park",
  timezone: "Europe/Berlin",
};

function plan(rides: string[]) {
  return {
    parkSlug: "test-park",
    tier: "measured",
    context: {
      date: "2026-11-14",
      openHour: 9,
      closeHour: 18,
      liveWaitTimes: { available: true, reason: null },
    },
    rides: rides.map((slug, i) => ({
      attractionSlug: slug,
      attractionName: slug,
      hours: Array.from({ length: 10 }, (_, h) => ({
        hour: 9 + h,
        wait: 10 + i * 10,
      })),
      dayPeak: 10 + i * 10,
      sampleDays: 90,
      latitude: 50.8,
      longitude: 6.87 + i * 0.001,
    })),
    shows: [],
  };
}

describe("dayClockFor", () => {
  afterEach(() => jest.useRealTimers());

  it("reads past, today and future in the park's zone", () => {
    // 23:30 UTC on the 13th is already the 14th in Berlin.
    jest.useFakeTimers({ now: new Date("2026-11-13T23:30:00Z") });
    expect(dayClockFor("Europe/Berlin", "2026-11-13")).toEqual({
      phase: "past",
    });
    expect(dayClockFor("Europe/Berlin", "2026-11-15")).toEqual({
      phase: "future",
    });
    expect(dayClockFor("Europe/Berlin", "2026-11-14")).toEqual({
      phase: "today",
      nowMinute: 30,
    });
    // And still the 13th in Orlando.
    expect(dayClockFor("America/New_York", "2026-11-13")).toEqual({
      phase: "today",
      nowMinute: 18 * 60 + 30,
    });
  });

  it("takes the caller's minute on today's day only", () => {
    jest.useFakeTimers({ now: new Date("2026-11-14T10:00:00Z") });
    expect(dayClockFor("Europe/Berlin", "2026-11-14", 600)).toEqual({
      phase: "today",
      nowMinute: 600,
    });
    expect(dayClockFor("Europe/Berlin", "2026-11-20", 600)).toEqual({
      phase: "future",
    });
  });
});

describe("PlanOptimizeService", () => {
  let parks: { findByGeographicPath: jest.Mock };
  let planDays: { buildPlanDay: jest.Mock; ridesInWorks: jest.Mock };
  let redis: { get: jest.Mock; set: jest.Mock };
  let service: PlanOptimizeService;
  const path = {
    continent: "europe",
    country: "germany",
    city: "test",
    parkSlug: "test-park",
  };

  beforeEach(() => {
    jest.useFakeTimers({ now: new Date("2026-11-12T10:00:00Z") });
    parks = { findByGeographicPath: jest.fn().mockResolvedValue(PARK) };
    planDays = {
      buildPlanDay: jest.fn().mockResolvedValue(plan(["a", "b", "c"])),
      ridesInWorks: jest.fn().mockResolvedValue([
        {
          attractionSlug: "shut",
          attractionName: "Shut",
          worksPeriod: { from: "2026-11-01", to: null, toUncertain: false },
        },
        {
          attractionSlug: "a",
          attractionName: "a",
          worksPeriod: { from: "2026-11-01", to: null, toUncertain: false },
        },
      ]),
    };
    redis = {
      get: jest.fn().mockResolvedValue(null),
      set: jest.fn().mockResolvedValue("OK"),
    };
    service = new PlanOptimizeService(
      parks as never,
      planDays as never,
      redis as never,
    );
  });

  afterEach(async () => {
    jest.useRealTimers();
    await service.onModuleDestroy();
  });

  const body = (extra: Partial<PlanOptimizeRequestDto> = {}) =>
    ({
      date: "2026-11-14",
      mode: "day",
      entries: [
        { id: "c1", attractionSlug: "c", startMinute: 9 * 60 + 15 },
        { id: "a1", attractionSlug: "a", startMinute: 14 * 60 },
        { id: "shut1", attractionSlug: "shut", startMinute: 12 * 60 },
      ],
      ...extra,
    }) as PlanOptimizeRequestDto;

  it("plans against the cached day and holds a ride in works still", async () => {
    const answer = await service.optimize(path, body());
    expect(answer.date).toBe("2026-11-14");
    expect(answer.clock).toEqual({ phase: "future" });
    // "shut" is in works and not in the plan: it is not movable; "a" is planned, so it is.
    expect(answer.movable).toBe(2);
    expect(answer.status).toBe("optimized");
    expect(answer.plan?.stops.map((s) => s.entryId)).not.toContain("shut1");
    expect(redis.get).toHaveBeenCalledWith(
      `plan-day:resp:v2:${PARK.id}:2026-11-14`,
    );
    expect(redis.get).toHaveBeenCalledWith(
      `plan-day:resp:v2:${PARK.id}:2026-11-14:works`,
    );
  });

  it("refuses a past day without building it", async () => {
    const answer = await service.optimize(path, body({ date: "2026-11-01" }));
    expect(answer).toMatchObject({ status: "refused", refusal: "past-day" });
    expect(planDays.buildPlanDay).not.toHaveBeenCalled();
  });

  it("answers 404 for an unknown park", async () => {
    parks.findByGeographicPath.mockResolvedValue(null);
    await expect(service.optimize(path, body())).rejects.toBeInstanceOf(
      NotFoundException,
    );
  });

  it.each([
    ["an impossible date", { date: "2026-02-31" }],
    [
      "a duplicate id",
      {
        entries: [
          { id: "x", attractionSlug: "a", startMinute: 600 },
          { id: "x", attractionSlug: "b", startMinute: 660 },
        ],
      },
    ],
    ["a fit field on a day press", { add: ["a"] }],
    ["levers on a day press", { levers: true }],
    ["preview on a fit request", { mode: "fit", preview: true }],
  ] as const)("answers 400 for %s", async (_name, extra) => {
    await expect(
      service.optimize(path, body(extra as Partial<PlanOptimizeRequestDto>)),
    ).rejects.toBeInstanceOf(BadRequestException);
  });
});

describe("PlanOptimizeRequestDto", () => {
  const pipe = new ValidationPipe({
    whitelist: true,
    forbidNonWhitelisted: true,
    transform: true,
    transformOptions: { enableImplicitConversion: true },
  });
  const meta: ArgumentMetadata = {
    type: "body",
    metatype: PlanOptimizeRequestDto,
  };
  const valid = {
    date: "2026-11-14",
    mode: "fit",
    entries: [
      {
        id: "a1",
        attractionSlug: "a",
        attractionName: "A",
        startMinute: 600,
        hour: 10,
      },
      {
        id: "lunch",
        custom: { label: "Lunch", icon: "food", durationMinutes: 60 },
        startMinute: 750,
      },
      {
        id: "done",
        attractionSlug: "b",
        startMinute: 540,
        done: true,
        actualWait: 25,
      },
    ],
    prefs: { riderHeightCm: 110, avoidWet: true, earlyEntry: false },
    nowMinute: 615,
    add: ["c"],
    choice: {
      dropped: ["e:a1"],
      droppedBlocks: [],
      shortBlocks: ["lunch"],
      priority: ["a:c"],
    },
    levers: true,
  };

  it("accepts a day as the browser stores it, the deprecated hour included", async () => {
    await expect(pipe.transform(valid, meta)).resolves.toBeInstanceOf(
      PlanOptimizeRequestDto,
    );
  });

  it.each([
    ["an unknown field", { ...valid, secret: 1 }],
    ["an unknown mode", { ...valid, mode: "week" }],
    ["a malformed date", { ...valid, date: "14.11.2026" }],
    [
      "too many entries",
      {
        ...valid,
        entries: Array.from({ length: 81 }, (_, i) => ({
          id: `e${i}`,
          startMinute: 600,
        })),
      },
    ],
    [
      "an icon outside the set",
      {
        ...valid,
        entries: [
          {
            id: "x",
            custom: { label: "x", icon: "rocket", durationMinutes: 30 },
            startMinute: 600,
          },
        ],
      },
    ],
    [
      "a minute past the next day",
      {
        ...valid,
        entries: [{ id: "x", attractionSlug: "a", startMinute: 2881 }],
      },
    ],
    [
      "a fractional minute",
      {
        ...valid,
        entries: [{ id: "x", attractionSlug: "a", startMinute: 600.5 }],
      },
    ],
    [
      "an empty id",
      {
        ...valid,
        entries: [{ id: "", attractionSlug: "a", startMinute: 600 }],
      },
    ],
    ["the same ride added twice", { ...valid, add: ["c", "c"] }],
    ["a now past midnight", { ...valid, nowMinute: 1440 }],
    [
      "an unknown field on an entry",
      { ...valid, entries: [{ id: "x", startMinute: 600, colour: "red" }] },
    ],
  ])("refuses %s", async (_name, input) => {
    await expect(pipe.transform(input, meta)).rejects.toBeInstanceOf(
      BadRequestException,
    );
  });
});
