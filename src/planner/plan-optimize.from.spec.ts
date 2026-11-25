import { BadRequestException, Logger, ValidationPipe } from "@nestjs/common";
import { PlanOptimizeRequestDto } from "./dto/plan-optimize.dto";
import { PlanOptimizeService } from "./plan-optimize.service";

/**
 * `from` on `/plan/optimize`: the ride the visitor stands at. The service checks it against the
 * park's catalogue, hands the engine its place, and keeps the slug out of logs and cache keys.
 */

const PARK = {
  id: "8355a0b7-26e9-4a90-af47-246ec143e99a",
  slug: "test-park",
  timezone: "Europe/Berlin",
};

const rides = ["a", "b", "c"].map((slug, i) => ({
  attractionSlug: slug,
  attractionName: slug,
  hours: Array.from({ length: 10 }, (_, h) => ({ hour: 9 + h, wait: 20 })),
  dayPeak: 20,
  sampleDays: 90,
  latitude: 50.8,
  longitude: 6.87 + i * 0.005,
}));

describe("PlanOptimizeService with from", () => {
  let planDays: {
    buildPlanDay: jest.Mock;
    ridesInWorks: jest.Mock;
    rideStart: jest.Mock;
  };
  let redis: { get: jest.Mock; set: jest.Mock };
  let service: PlanOptimizeService;
  const path = {
    continent: "europe",
    country: "germany",
    city: "test",
    parkSlug: "test-park",
  };
  const body = (extra: object = {}) =>
    ({
      date: "2026-11-14",
      mode: "day",
      preview: true,
      nowMinute: 14 * 60,
      entries: [
        { id: "a1", attractionSlug: "a", startMinute: 14 * 60 + 15 },
        { id: "c1", attractionSlug: "c", startMinute: 15 * 60 },
        { id: "b1", attractionSlug: "b", startMinute: 16 * 60 },
      ],
      ...extra,
    }) as PlanOptimizeRequestDto;

  beforeEach(() => {
    // 14:00 in Berlin on the planned day.
    jest.useFakeTimers({ now: new Date("2026-11-14T13:00:00Z") });
    planDays = {
      buildPlanDay: jest.fn().mockResolvedValue({
        tier: "measured",
        context: {
          openHour: 9,
          closeHour: 18,
          liveWaitTimes: { available: true, reason: null },
        },
        rides,
        shows: [],
      }),
      ridesInWorks: jest.fn().mockResolvedValue([]),
      rideStart: jest.fn().mockResolvedValue({
        attractionSlug: "kiosk",
        attractionName: "Kiosk",
        latitude: 50.8,
        longitude: 6.88,
        land: null,
      }),
    };
    redis = {
      get: jest.fn().mockResolvedValue(null),
      set: jest.fn().mockResolvedValue("OK"),
    };
    service = new PlanOptimizeService(
      { findByGeographicPath: jest.fn().mockResolvedValue(PARK) } as never,
      planDays as never,
      redis as never,
    );
  });

  afterEach(async () => {
    jest.useRealTimers();
    jest.restoreAllMocks();
    await service.onModuleDestroy();
  });

  it("plans today from the ride and answers with a receipt", async () => {
    const answer = await service.optimize(
      path,
      body({ from: { attractionSlug: "kiosk" } }),
    );
    expect(planDays.rideStart).toHaveBeenCalledWith(PARK, "kiosk");
    expect(answer.receipt?.from).toEqual({
      attractionSlug: "kiosk",
      attractionName: "Kiosk",
    });
    // The kiosk stands at C, so C leads; as sent, the day began at A, 700 m away.
    expect(answer.receipt?.first).toMatchObject({
      attractionSlug: "c",
      walkMinutes: 0,
    });
  });

  it("answers 400 for a ride that is not this park's", async () => {
    planDays.rideStart.mockResolvedValue(null);
    await expect(
      service.optimize(path, body({ from: { attractionSlug: "elsewhere" } })),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(planDays.buildPlanDay).not.toHaveBeenCalled();
  });

  it("asks nothing and adds nothing without from", async () => {
    const answer = await service.optimize(path, body());
    expect(planDays.rideStart).not.toHaveBeenCalled();
    expect(answer).not.toHaveProperty("receipt");
  });

  it("keeps the slug out of every log line and cache key", async () => {
    const logged: unknown[] = [];
    for (const level of ["log", "warn", "error", "debug", "verbose"] as const)
      jest
        .spyOn(Logger.prototype, level)
        .mockImplementation((...args) => void logged.push(...args));
    await service.optimize(path, body({ from: { attractionSlug: "kiosk" } }));
    expect(JSON.stringify(logged)).not.toContain("kiosk");
    expect(
      JSON.stringify([...redis.get.mock.calls, ...redis.set.mock.calls]),
    ).not.toContain("kiosk");
  });

  it("accepts from as a slug and nothing else", async () => {
    const pipe = new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
      transformOptions: { enableImplicitConversion: true },
    });
    const meta = { type: "body", metatype: PlanOptimizeRequestDto } as const;
    await expect(
      pipe.transform(body({ from: { attractionSlug: "kiosk" } }), meta),
    ).resolves.toBeInstanceOf(PlanOptimizeRequestDto);
    for (const from of [
      { attractionSlug: "kiosk", latitude: 50.8, longitude: 6.88 },
      { attractionSlug: "" },
      {},
    ]) {
      await expect(pipe.transform(body({ from }), meta)).rejects.toBeInstanceOf(
        BadRequestException,
      );
    }
  });
});
