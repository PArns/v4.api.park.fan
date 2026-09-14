import { Test } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { PlanDayCoverageService } from "./plan-day-coverage.service";
import { PlanDayService } from "./plan-day.service";
import { PlanDayCoverage } from "../entities/plan-day-coverage.entity";
import { Park } from "../entities/park.entity";
import type { PlanDayDto } from "../dto/plan-day.dto";

/**
 * The sweep's whole value is that the number it writes can go down. A count
 * that lumps a park we will never read in with a feed that broke last night
 * has a floor nobody can move, and a floor is what turns a metric into
 * wallpaper — so the split is what is pinned here.
 */
describe("PlanDayCoverageService", () => {
  const parks = [
    { id: "p-1", slug: "phantasialand" },
    { id: "p-2", slug: "hansa-park" },
    { id: "p-3", slug: "la-ronde" },
    { id: "p-4", slug: "ocean-park" },
  ] as Park[];

  let plans: Map<string, Partial<PlanDayDto> | Error>;
  let upserted: PlanDayCoverage[];
  let service: PlanDayCoverageService;

  const build = async () => {
    const moduleRef = await Test.createTestingModule({
      providers: [
        PlanDayCoverageService,
        {
          provide: getRepositoryToken(Park),
          useValue: {
            manager: {
              query: jest
                .fn()
                .mockImplementation(async () =>
                  parks.map((p) => ({ id: p.id })),
                ),
            },
            findByIds: jest.fn().mockImplementation(async () => parks),
          },
        },
        {
          provide: getRepositoryToken(PlanDayCoverage),
          useValue: {
            create: jest.fn().mockImplementation((row: PlanDayCoverage) => row),
            upsert: jest.fn().mockImplementation(async (rows: unknown) => {
              upserted = rows as PlanDayCoverage[];
            }),
            find: jest.fn().mockImplementation(async () => []),
          },
        },
        {
          provide: PlanDayService,
          useValue: {
            buildPlanDay: jest.fn().mockImplementation(async (park: Park) => {
              const answer = plans.get(park.slug);
              if (answer instanceof Error) throw answer;
              return { rides: [], ...answer } as PlanDayDto;
            }),
          },
        },
      ],
    }).compile();
    return moduleRef.get(PlanDayCoverageService);
  };

  beforeEach(async () => {
    upserted = [];
    plans = new Map<string, Partial<PlanDayDto> | Error>([
      ["phantasialand", { rides: [{}, {}] as PlanDayDto["rides"] }],
      ["hansa-park", { ridesUnavailable: { reason: "no_wait_time_source" } }],
      [
        "la-ronde",
        { ridesUnavailable: { reason: "feed_stale", staleDays: 96 } },
      ],
      ["ocean-park", { ridesUnavailable: { reason: "insufficient_history" } }],
    ]);
    service = await build();
  });

  it("counts the gaps we can close apart from the ones we cannot", async () => {
    const summary = await service.sweep(new Date("2026-09-14T09:00:00.000Z"));

    expect(summary.parksOpen).toBe(4);
    expect(summary.parksWithPlan).toBe(1);
    // Hansa-Park is an answer about the park, not a hole in the data, so it is
    // counted separately — otherwise this number could never reach zero.
    expect(summary.parksStructural).toBe(1);
    expect(summary.parksWithoutPlan).toBe(2);
    expect(summary.byReason).toEqual({
      no_wait_time_source: 1,
      feed_stale: 1,
      insufficient_history: 1,
    });
  });

  it("asks about a day a month out, and records which one", async () => {
    const summary = await service.sweep(new Date("2026-09-14T09:00:00.000Z"));

    // Near days hide the problem: inside the model's 24-hour window a park can
    // carry hourly predictions with no measured history at all.
    expect(summary.plannedDate).toBe("2026-10-14");
    expect(summary.measuredOn).toBe("2026-09-14");
    expect(upserted.every((row) => row.leadDays === 30)).toBe(true);
    expect(upserted.every((row) => row.plannedDate === "2026-10-14")).toBe(
      true,
    );
  });

  it("stores the reason per park, not just the total", async () => {
    await service.sweep(new Date("2026-09-14T09:00:00.000Z"));

    const byPark = new Map(upserted.map((row) => [row.parkId, row]));
    expect(byPark.get("p-1")?.reason).toBeNull();
    expect(byPark.get("p-1")?.rideCount).toBe(2);
    expect(byPark.get("p-2")?.isStructural).toBe(true);
    expect(byPark.get("p-3")?.reason).toBe("feed_stale");
    expect(byPark.get("p-3")?.isStructural).toBe(false);
  });

  it("leaves a park it could not ask out of the count entirely", async () => {
    plans.set("ocean-park", new Error("calendar down"));
    service = await build();

    const summary = await service.sweep(new Date("2026-09-14T09:00:00.000Z"));

    // "We could not ask" and "there was no answer" are the two things this
    // whole endpoint exists to keep apart; a failed park must not be recorded
    // as a park without a plan.
    expect(summary.parksOpen).toBe(3);
    expect(summary.byReason.insufficient_history).toBeUndefined();
    expect(upserted.map((row) => row.parkId)).not.toContain("p-4");
  });
});
