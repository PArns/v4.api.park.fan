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
