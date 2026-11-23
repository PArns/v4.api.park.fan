import { WeatherClosureService } from "./weather-closure.service";
import type { WeatherClosureProfile } from "../utils/weather-closure.util";

const cold: WeatherClosureProfile = {
  thresholdC: 3,
  closedShare: 0.8,
  closedShareLower: 0.6,
  baselineShare: 0.002,
  attributableShare: 0.93,
  hours: 140,
  days: 20,
};
const asOf = new Date("2026-11-23T10:20:00Z");

function service(profiles: Array<Record<string, unknown>>) {
  const openMeteo = {
    getHourlyForecast: jest.fn().mockResolvedValue({
      hours: [1, 2, 4].map((t, k) => ({
        time: new Date(
          Date.parse("2026-11-23T10:00:00Z") + k * 3600_000,
        ).toISOString(),
        temperature: t,
        snowfall: 0,
      })),
    }),
  };
  const dataSource = {
    transaction: jest.fn(async (fn: (m: unknown) => unknown) =>
      fn({
        query: jest
          .fn()
          .mockResolvedValue([{ closesAt: "2026-11-23T17:00:00Z" }]),
      }),
    ),
  };
  const svc = new WeatherClosureService(
    dataSource as never,
    {
      findOne: jest.fn().mockResolvedValue({ latitude: 48.3, longitude: 7.7 }),
    } as never,
    { findOne: jest.fn().mockResolvedValue(null) } as never,
    { find: jest.fn().mockResolvedValue(profiles) } as never,
    openMeteo as never,
  );
  return { svc, openMeteo };
}

describe("WeatherClosureService.listProfiles", () => {
  it("flags what the page shows, counts the near ones and drops the weak", async () => {
    const row = (name: string, p: number, extra = {}) => ({
      attractionId: name,
      attractionName: name,
      attractionSlug: name,
      parkId: "p",
      parkName: "Park",
      parkSlug: "park",
      parkPath: "europe/germany/rust/park",
      cold: { ...cold, attributableShare: p, ...extra },
      snow: null,
      generatedAt: "2026-11-30T05:46:00Z",
    });
    const dataSource = {
      transaction: jest.fn(async (fn: (m: unknown) => unknown) =>
        fn({
          query: jest
            .fn()
            .mockResolvedValue([
              row("near", 0.8),
              row("shown", 0.93),
              row("weak", 0.3),
              row("floor", 0.95, { closedShareLower: 0.1 }),
            ]),
        }),
      ),
    };
    const svc = new WeatherClosureService(
      dataSource as never,
      {} as never,
      {} as never,
      {} as never,
      {} as never,
    );
    const out = await svc.listProfiles(0.5);
    expect(out.totals).toEqual({ rides: 4, publishable: 1, near: 2 });
    expect(out.rides.map((r) => [r.attractionName, r.publishable])).toEqual([
      ["floor", false],
      ["shown", true],
      ["near", false],
    ]);
    expect(out.generatedAt).toBe("2026-11-30T05:46:00.000Z");
  });
});

describe("WeatherClosureService.getVerdicts", () => {
  it("asks for no forecast while no profile clears the bar", async () => {
    const { svc, openMeteo } = service([
      {
        attractionId: "a",
        cold: { ...cold, attributableShare: 0.85 },
        snow: null,
      },
    ]);
    expect(
      (
        await svc.getVerdicts(
          { id: "p", timezone: "Europe/Berlin" },
          ["a"],
          asOf,
        )
      ).size,
    ).toBe(0);
    expect(openMeteo.getHourlyForecast).not.toHaveBeenCalled();
  });

  it("names the cold and the forecast hour for a publishable ride", async () => {
    const { svc } = service([{ attractionId: "a", cold, snow: null }]);
    const out = await svc.getVerdicts(
      { id: "p", timezone: "Europe/Berlin" },
      ["a"],
      asOf,
    );
    expect(out.get("a")).toMatchObject({
      cause: "cold",
      outlook: "opens_when_warmer",
      expectedOpenAt: new Date("2026-11-23T12:00:00Z"),
    });
  });

  it("never throws: a failed forecast costs the line, not the page", async () => {
    const { svc, openMeteo } = service([
      { attractionId: "a", cold, snow: null },
    ]);
    openMeteo.getHourlyForecast.mockRejectedValue(new Error("upstream down"));
    expect(
      (
        await svc.getVerdicts(
          { id: "p", timezone: "Europe/Berlin" },
          ["a"],
          asOf,
        )
      ).size,
    ).toBe(0);
  });
});
