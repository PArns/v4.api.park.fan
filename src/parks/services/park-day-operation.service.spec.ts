import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { ParkDayOperation } from "../entities/park-day-operation.entity";
import { ParksService } from "../parks.service";
import {
  enumerateParkLocalDays,
  ParkDayOperationService,
} from "./park-day-operation.service";

/**
 * PAR-697. The verdict is taken here and read by five places, so what this
 * suite holds is the shape of what gets written: one row per park-local day,
 * the gate's answer in `measuredOperation`, and the derived hours beside it
 * only when the rollup had any.
 *
 * The thresholds themselves are tested in `measured-operation.gate.spec.ts` —
 * here the aggregate is mocked, because the point is the wiring.
 */
describe("ParkDayOperationService", () => {
  let service: ParkDayOperationService;
  let repository: {
    query: jest.Mock;
    upsert: jest.Mock;
    create: jest.Mock;
  };
  let parksService: { getDerivedHistoricalHours: jest.Mock };

  const park = { id: "p1", slug: "test-park", timezone: "Europe/Amsterdam" };

  /** One row as the aggregate returns it: a day that clears the gate. */
  const operatingStats = [
    { distinct_waits: 12, dead_hour_readings: 0, block_minutes: 480 },
  ];

  beforeEach(async () => {
    repository = {
      query: jest.fn(),
      upsert: jest.fn().mockResolvedValue(undefined),
      create: jest.fn((row) => row),
    };
    parksService = {
      getDerivedHistoricalHours: jest.fn().mockResolvedValue(new Map()),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ParkDayOperationService,
        {
          provide: getRepositoryToken(ParkDayOperation),
          useValue: repository,
        },
        { provide: ParksService, useValue: parksService },
      ],
    }).compile();

    service = module.get(ParkDayOperationService);
  });

  it("writes one row per day with the gate's verdict and the derived hours", async () => {
    repository.query.mockResolvedValue(operatingStats);
    parksService.getDerivedHistoricalHours.mockResolvedValue(
      new Map([
        [
          "2026-04-18",
          {
            openingTime: "2026-04-18T10:00:00.000Z",
            closingTime: "2026-04-18T18:00:00.000Z",
          },
        ],
      ]),
    );

    const result = await service.computeRange(park, "2026-04-18", "2026-04-19");

    expect(result).toEqual({ daysJudged: 2, operatingDays: 2 });
    const [rows, conflictColumns] = repository.upsert.mock.calls[0];
    expect(conflictColumns).toEqual(["parkId", "day"]);
    expect(rows.map((r: ParkDayOperation) => r.day)).toEqual([
      "2026-04-18",
      "2026-04-19",
    ]);
    expect(rows[0].derivedOpen).toEqual(new Date("2026-04-18T10:00:00.000Z"));
    expect(rows[0].derivedClose).toEqual(new Date("2026-04-18T18:00:00.000Z"));
    // The rollup had nothing for the second day: a verdict without hours is a
    // verdict, so the row is still written.
    expect(rows[1].derivedOpen).toBeNull();
    expect(rows[1].measuredOperation).toBe(true);
  });

  it("stores a refusal as a row rather than leaving the day unjudged", async () => {
    // Walibi Holland's 2026-09-11: a 55-minute burst.
    repository.query.mockResolvedValue([
      { distinct_waits: 7, dead_hour_readings: 0, block_minutes: 55 },
    ]);

    const result = await service.computeRange(park, "2026-09-11", "2026-09-11");

    expect(result).toEqual({ daysJudged: 1, operatingDays: 0 });
    expect(repository.upsert.mock.calls[0][0][0].measuredOperation).toBe(false);
  });

  it("judges a day whose aggregate saw no qualifying reading", async () => {
    repository.query.mockResolvedValue([
      { distinct_waits: 0, dead_hour_readings: 0, block_minutes: null },
    ]);

    await service.computeRange(park, "2026-09-11", "2026-09-11");

    expect(repository.upsert.mock.calls[0][0][0].measuredOperation).toBe(false);
  });

  it("still judges the days when the derived hours are unavailable", async () => {
    // The hours are stored beside the verdict, not part of it — a rollup that
    // is not there yet must not cost the day its judgement.
    repository.query.mockResolvedValue(operatingStats);
    parksService.getDerivedHistoricalHours.mockRejectedValue(
      new Error('relation "attraction_hourly_history" does not exist'),
    );

    const result = await service.computeRange(park, "2026-04-18", "2026-04-18");

    expect(result).toEqual({ daysJudged: 1, operatingDays: 1 });
    expect(repository.upsert.mock.calls[0][0][0].derivedOpen).toBeNull();
  });

  it("asks the aggregate for the park's own timezone, once per day", async () => {
    repository.query.mockResolvedValue(operatingStats);

    await service.computeRange(park, "2026-04-18", "2026-04-20");

    expect(repository.query).toHaveBeenCalledTimes(3);
    expect(repository.query.mock.calls.map((c) => c[1])).toEqual([
      ["p1", "2026-04-18", "Europe/Amsterdam"],
      ["p1", "2026-04-19", "Europe/Amsterdam"],
      ["p1", "2026-04-20", "Europe/Amsterdam"],
    ]);
  });

  it("stops at the park's last finished day", async () => {
    // 2026-10-07 11:00 UTC is 13:00 on the 7th in Amsterdam, so the park's last
    // finished day is the 6th. A fill asked for "… → today" may not write a
    // verdict for a day in progress: the calendar refuses a non-past day on its
    // own, the four statistics callers do not, and the two disagreeing about
    // one day is the split this issue closes.
    jest.useFakeTimers().setSystemTime(new Date("2026-10-07T11:00:00Z"));
    repository.query.mockResolvedValue(operatingStats);

    const result = await service.computeRange(park, "2026-10-04", "2026-10-07");

    expect(result.daysJudged).toBe(3);
    expect(
      repository.upsert.mock.calls[0][0].map((r: ParkDayOperation) => r.day),
    ).toEqual(["2026-10-04", "2026-10-05", "2026-10-06"]);
    jest.useRealTimers();
  });

  it("writes nothing when the whole range is still running", async () => {
    jest.useFakeTimers().setSystemTime(new Date("2026-10-07T11:00:00Z"));

    const result = await service.computeRange(park, "2026-10-07", "2026-10-09");

    expect(result).toEqual({ daysJudged: 0, operatingDays: 0 });
    expect(repository.upsert).not.toHaveBeenCalled();
    jest.useRealTimers();
  });

  it("writes nothing for an inverted range", async () => {
    const result = await service.computeRange(park, "2026-04-20", "2026-04-18");

    expect(result).toEqual({ daysJudged: 0, operatingDays: 0 });
    expect(repository.upsert).not.toHaveBeenCalled();
  });

  describe("getMeasuredOperationDays", () => {
    it("reads back both driver mappings of a date column", async () => {
      repository.query.mockResolvedValue([
        { day: "2026-04-18" },
        { day: new Date("2026-04-19T00:00:00.000Z") },
      ]);

      const days = await service.getMeasuredOperationDays(
        "p1",
        "2026-04-01",
        "2026-04-30",
      );

      expect([...days].sort()).toEqual(["2026-04-18", "2026-04-19"]);
    });
  });

  describe("enumerateParkLocalDays", () => {
    it("is inclusive on both ends and crosses a DST boundary once per day", () => {
      // 2026-10-25 is the European fall-back day. The walk is calendar
      // arithmetic in UTC on purpose: a 24-hour step from local midnight lands
      // twice on the 25-hour day (PAR-535).
      expect(enumerateParkLocalDays("2026-10-24", "2026-10-26")).toEqual([
        "2026-10-24",
        "2026-10-25",
        "2026-10-26",
      ]);
    });
  });
});
