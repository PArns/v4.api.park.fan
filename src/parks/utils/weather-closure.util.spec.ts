import {
  buildWeatherClosureSamples,
  decideWeatherClosure,
  learnWeatherClosureProfiles,
  WeatherClosureProfile,
  WeatherClosureSample,
} from "./weather-closure.util";

const HOUR = 3600_000;
const T0 = Date.parse("2026-01-01T10:00:00Z");

/** `days` days of `perDay` hours each, at a fixed temperature. */
function hours(
  days: number,
  perDay: number,
  temperatureC: number,
  closed: (i: number) => boolean,
  opts: { snow?: boolean; dayOffset?: number } = {},
): WeatherClosureSample[] {
  const out: WeatherClosureSample[] = [];
  let i = 0;
  for (let d = 0; d < days; d++) {
    for (let h = 0; h < perDay; h++) {
      out.push({
        hourStart: new Date(
          T0 + ((opts.dayOffset ?? 0) + d) * 24 * HOUR + h * HOUR,
        ),
        temperatureC,
        snow: opts.snow ?? false,
        closed: closed(i++),
      });
    }
  }
  return out;
}

const warmSummer = hours(40, 8, 18, () => false, { dayOffset: 200 });

describe("learnWeatherClosureProfiles", () => {
  it("finds a cold threshold and clears the bar for a ride shut in every cold hour", () => {
    const cold = hours(30, 4, 1, () => true);
    const mild = hours(20, 4, 8, () => false, { dayOffset: 60 });
    const { cold: profile } = learnWeatherClosureProfiles([
      ...warmSummer,
      ...cold,
      ...mild,
    ]);
    expect(profile).not.toBeNull();
    expect(profile!.thresholdC).toBeGreaterThan(1);
    expect(profile!.thresholdC).toBeLessThanOrEqual(8);
    expect(profile!.attributableShare).toBeGreaterThanOrEqual(0.9);
  });

  it("counts a day once: ten closed hours on two days do not clear the bar", () => {
    // Shut all day on 2 of 10 cold days — 20 % of cold hours, but two events.
    const cold = hours(10, 10, 1, (i) => i < 20);
    const { cold: profile } = learnWeatherClosureProfiles([
      ...warmSummer,
      ...cold,
    ]);
    expect(profile?.attributableShare ?? 0).toBeLessThan(0.9);
  });

  it("does not call a snow-only ride cold-sensitive", () => {
    const dryCold = hours(20, 4, 0, () => false);
    const snowy = hours(10, 4, 0, () => true, { snow: true, dayOffset: 30 });
    const p = learnWeatherClosureProfiles([
      ...warmSummer,
      ...dryCold,
      ...snowy,
    ]);
    expect(p.cold?.attributableShare ?? 0).toBeLessThan(0.9);
    expect(p.snow).not.toBeNull();
    expect(p.snow!.attributableShare).toBeGreaterThanOrEqual(0.9);
  });

  it("learns nothing without a summer to compare against", () => {
    const p = learnWeatherClosureProfiles(hours(30, 4, 1, () => true));
    expect(p).toEqual({ cold: null, snow: null });
  });
});

describe("buildWeatherClosureSamples", () => {
  const h = (k: number) => new Date(T0 + k * HOUR);
  const wx = new Map(
    Array.from({ length: 48 }, (_, k) => [
      T0 + k * HOUR,
      { temperatureC: 1, snowfallCm: k === 2 ? 0.4 : 0 },
    ]),
  );

  it("drops hours in which most of the park was shut", () => {
    const states = Array.from({ length: 8 }, (_, r) => ({
      attractionId: `r${r}`,
      hourStart: h(0),
      closed: r > 0, // seven of eight shut
    })).concat(
      Array.from({ length: 8 }, (_, r) => ({
        attractionId: `r${r}`,
        hourStart: h(30),
        closed: false,
      })),
    );
    const out = buildWeatherClosureSamples(states, wx);
    expect(out.get("r1")!.map((s) => s.hourStart)).toEqual([h(30)]);
  });

  it("flags snow for the 24 hours after it fell", () => {
    const states = [3, 26, 27].map((k) => ({
      attractionId: "a",
      hourStart: h(k),
      closed: false,
    }));
    const out = buildWeatherClosureSamples(states, wx);
    expect(out.get("a")!.map((s) => s.snow)).toEqual([true, true, false]);
  });

  it("drops a ride that never ran within a week: closed for the season", () => {
    const states = [{ attractionId: "a", hourStart: h(0), closed: true }];
    expect(buildWeatherClosureSamples(states, wx).size).toBe(0);
  });
});

describe("decideWeatherClosure", () => {
  const cold: WeatherClosureProfile = {
    thresholdC: 3,
    closedShare: 0.8,
    baselineShare: 0.002,
    attributableShare: 0.93,
    hours: 140,
    days: 20,
  };
  const asOf = new Date("2026-11-23T10:20:00Z");
  const closesAt = new Date("2026-11-23T17:00:00Z");
  const curve = (temps: number[], snowAt = -1) =>
    temps.map((t, k) => ({
      time: new Date(Date.parse("2026-11-23T10:00:00Z") + k * HOUR),
      temperatureC: t,
      snowfallCm: k === snowAt ? 0.5 : 0,
    }));

  it("names the first forecast hour that reaches the threshold", () => {
    const v = decideWeatherClosure(
      { cold, snow: null },
      curve([1, 2, 3.4, 4]),
      false,
      asOf,
      closesAt,
    );
    expect(v).toMatchObject({
      cause: "cold",
      probability: 0.93,
      thresholdC: 3,
      outlook: "opens_when_warmer",
    });
    expect(v!.expectedOpenAt!.toISOString()).toBe("2026-11-23T12:00:00.000Z");
  });

  it("says the ride stays shut when the curve stays below until close", () => {
    const v = decideWeatherClosure(
      { cold, snow: null },
      curve([1, 2, 2, 2, 2, 2, 2, 9]), // 9 °C only at 17:00, after close
      false,
      asOf,
      closesAt,
    );
    expect(v).toMatchObject({
      outlook: "likely_closed_today",
      expectedOpenAt: null,
    });
  });

  it("says nothing when it is warmer than the threshold now", () => {
    expect(
      decideWeatherClosure(
        { cold, snow: null },
        curve([5]),
        false,
        asOf,
        closesAt,
      ),
    ).toBeNull();
  });

  it("says nothing below the 90 % bar", () => {
    expect(
      decideWeatherClosure(
        { cold: { ...cold, attributableShare: 0.89 }, snow: null },
        curve([1]),
        false,
        asOf,
        closesAt,
      ),
    ).toBeNull();
  });

  it("never blames the cold after snow, and names snow only with its own profile", () => {
    expect(
      decideWeatherClosure(
        { cold, snow: null },
        curve([1]),
        true,
        asOf,
        closesAt,
      ),
    ).toBeNull();
    const v = decideWeatherClosure(
      { cold, snow: { ...cold, thresholdC: null } },
      curve([1]),
      true,
      asOf,
      closesAt,
    );
    expect(v).toMatchObject({ cause: "snow", outlook: null, thresholdC: null });
  });
});
