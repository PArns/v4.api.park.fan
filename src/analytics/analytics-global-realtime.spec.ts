import { IsNull } from "typeorm";
import { AnalyticsService } from "./analytics.service";

/**
 * `/v1/analytics/realtime` (PAR-1131): the parks' crowd level is the one
 * `calculateParkOccupancy` gated, and the catalogue counts leave retired rows
 * out.
 */
describe("AnalyticsService.getGlobalRealtimeStats", () => {
  function build() {
    const service = Object.create(
      AnalyticsService.prototype,
    ) as AnalyticsService;
    const count = () => ({ count: jest.fn().mockResolvedValue(3) });
    const parkRow = (id: string, wait: number) => ({
      id,
      name: id,
      slug: id,
      city: "c",
      country: "x",
      timezone: "UTC",
      continentSlug: "europe",
      countrySlug: "x",
      citySlug: "c",
      avg_wait: wait,
      active_rides: 1,
      total_attractions: 1,
      explicitly_closed_attractions: 0,
    });
    const deps = {
      redis: { get: jest.fn().mockResolvedValue(null), set: jest.fn() },
      queueDataRepository: {
        ...count(),
        query: jest
          .fn()
          .mockResolvedValueOnce([parkRow("thin", 40), parkRow("rated", 10)])
          .mockResolvedValueOnce([]),
      },
      parkRepository: count(),
      attractionRepository: {
        ...count(),
        query: jest.fn().mockResolvedValue([{ count: "1" }]),
      },
      showRepository: count(),
      restaurantRepository: count(),
      logger: { warn: jest.fn() },
    };
    Object.assign(service, deps);
    jest
      .spyOn(service, "calculateParkOccupancy")
      .mockImplementation((parkId: string) =>
        Promise.resolve(
          (parkId === "thin"
            ? { current: 20, crowdLevel: "unknown" }
            : { current: 20, crowdLevel: "very_low" }) as never,
        ),
      );
    jest
      .spyOn(
        service as unknown as {
          getAttractionSparklinesBatch: () => Promise<Map<string, unknown>>;
        },
        "getAttractionSparklinesBatch",
      )
      .mockResolvedValue(new Map());
    return { service, deps };
  }

  it("serves the gated level, not one re-derived from occupancy", async () => {
    const { service } = build();
    const stats = await service.getGlobalRealtimeStats();
    // 20 % would re-derive to very_low; the thin park is `unknown`.
    expect(stats.mostCrowdedPark?.crowdLevel).toBe("unknown");
    expect(stats.leastCrowdedPark?.crowdLevel).toBe("very_low");
  });

  it("counts parks, rides, shows and restaurants without retired rows", async () => {
    const { service, deps } = build();
    await service.getGlobalRealtimeStats();
    for (const repo of [
      deps.parkRepository,
      deps.attractionRepository,
      deps.showRepository,
      deps.restaurantRepository,
    ]) {
      expect(repo.count).toHaveBeenCalledWith({
        where: { retiredAt: IsNull() },
      });
    }
  });
});
