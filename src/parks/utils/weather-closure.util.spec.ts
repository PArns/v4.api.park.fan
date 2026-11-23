import {
  buildWeatherClosureSamples,
  decideWeatherClosure,
  isPublishable,
  learnWeatherClosureProfiles,
  RideHourState,
  WeatherClosureProfile,
  WeatherClosureSample,
} from "./weather-closure.util";
import { formatInParkTimezone } from "../../common/utils/date.util";

const HOUR = 3600_000;
const DAY = 24 * HOUR;
const T0 = Date.parse("2026-01-01T10:00:00Z");
const utcDay = (d: Date) => d.toISOString().slice(0, 10);

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
      const hourStart = new Date(
        T0 + ((opts.dayOffset ?? 0) + d) * DAY + h * HOUR,
      );
      out.push({
        hourStart,
        day: utcDay(hourStart),
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
    expect(isPublishable(profile)).toBe(true);
  });

  it("counts a day once: ten closed hours on two days do not clear the bar", () => {
    // Shut all day on 2 of 10 cold days — 20 % of cold hours, but two events.
    const cold = hours(10, 10, 1, (i) => i < 20);
    const { cold: profile } = learnWeatherClosureProfiles([
      ...warmSummer,
      ...cold,
    ]);
    expect(isPublishable(profile)).toBe(false);
  });

  it("needs eight days behind a profile", () => {
    const cold = hours(7, 6, 1, () => true);
    expect(
      learnWeatherClosureProfiles([...warmSummer, ...cold]).cold,
    ).toBeNull();
  });

  it("does not call a snow-only ride cold-sensitive", () => {
    const dryCold = hours(20, 4, 0, () => false);
    const snowy = hours(10, 4, 0, () => true, { snow: true, dayOffset: 30 });
    const p = learnWeatherClosureProfiles([
      ...warmSummer,
      ...dryCold,
      ...snowy,
    ]);
    expect(isPublishable(p.cold)).toBe(false);
    expect(isPublishable(p.snow)).toBe(true);
  });

  it("does not learn a snow profile for a ride the cold already explains", () => {
    const dryCold = hours(20, 4, 1, () => true);
    const snowyCold = hours(10, 4, 1, () => true, {
      snow: true,
      dayOffset: 30,
    });
    const p = learnWeatherClosureProfiles([
      ...warmSummer,
      ...dryCold,
      ...snowyCold,
    ]);
    expect(isPublishable(p.cold)).toBe(true);
    expect(p.snow).toBeNull();
  });

  it("learns nothing without a summer to compare against", () => {
    const p = learnWeatherClosureProfiles(hours(30, 4, 1, () => true));
    expect(p).toEqual({ cold: null, snow: null });
  });
});

describe("buildWeatherClosureSamples", () => {
  const h = (k: number) => new Date(T0 + k * HOUR);
  const wx = new Map(
    Array.from({ length: 24 * 20 }, (_, k) => [
      T0 + k * HOUR,
      { temperatureC: 1, snowfallCm: k === 2 ? 0.4 : k === 60 ? 0.05 : 0 },
    ]),
  );
  const open = (id: string, k: number): RideHourState => ({
    attractionId: id,
    hourStart: h(k),
    closed: false,
  });
  const shut = (id: string, k: number): RideHourState => ({
    attractionId: id,
    hourStart: h(k),
    closed: true,
  });

  it("drops hours in which most of the park was shut", () => {
    const rides = Array.from({ length: 8 }, (_, r) => `r${r}`);
    const states = [
      ...rides.map((r, i) => (i > 0 ? shut(r, 24) : open(r, 24))), // seven of eight shut
      ...rides.map((r) => open(r, 0)),
      ...rides.map((r) => open(r, 48)),
    ];
    const out = buildWeatherClosureSamples(states, wx, utcDay);
    expect(out.get("r1")!.map((s) => s.hourStart)).toEqual([h(0), h(48)]);
  });

  it("flags snow for the 24 hours after it fell, and not for a trace", () => {
    const states = [3, 26, 27, 61].map((k) => open("a", k));
    const out = buildWeatherClosureSamples(states, wx, utcDay);
    expect(out.get("a")!.map((s) => s.snow)).toEqual([
      true,
      true,
      false,
      false,
    ]);
  });

  it("drops the days after a ride's last run: its season ended, the weather did not shut it", () => {
    const states = [
      open("a", 0),
      open("a", 24),
      ...[3, 4, 5, 6].map((d) => shut("a", d * 24)),
    ];
    const out = buildWeatherClosureSamples(states, wx, utcDay);
    expect(out.get("a")!.every((s) => !s.closed)).toBe(true);
  });

  it("drops a streak of whole closed days between two runs: a rehab, not the cold", () => {
    const states = [
      open("a", 0),
      ...[1, 2, 3, 4, 5].flatMap((d) => [
        shut("a", d * 24),
        shut("a", d * 24 + 1),
      ]),
      open("a", 6 * 24),
      // A single cold morning stays.
      shut("a", 7 * 24),
      open("a", 7 * 24 + 1),
      open("a", 8 * 24),
    ];
    const out = buildWeatherClosureSamples(states, wx, utcDay);
    const closed = out.get("a")!.filter((s) => s.closed);
    expect(closed.map((s) => s.hourStart)).toEqual([h(7 * 24)]);
  });

  it("counts days in the park's own calendar", () => {
    // 23:00 and 01:00 UTC are both the evening of the same day in New York.
    const evening = Date.parse("2026-01-05T23:00:00Z");
    const weather = new Map([
      [evening, { temperatureC: 1, snowfallCm: 0 }],
      [evening + 2 * HOUR, { temperatureC: 1, snowfallCm: 0 }],
    ]);
    const states = [0, 2].map((k) => ({
      attractionId: "a",
      hourStart: new Date(evening + k * HOUR),
      closed: false,
    }));
    const out = buildWeatherClosureSamples(states, weather, (d) =>
      formatInParkTimezone(d, "America/New_York"),
    );
    expect(new Set(out.get("a")!.map((s) => s.day))).toEqual(
      new Set(["2026-01-05"]),
    );
  });
});

describe("decideWeatherClosure", () => {
  const cold: WeatherClosureProfile = {
    thresholdC: 3,
    closedShare: 0.8,
    closedShareLower: 0.6,
    baselineShare: 0.002,
    attributableShare: 0.93,
    hours: 140,
    days: 20,
  };
  const snow: WeatherClosureProfile = { ...cold, thresholdC: null };
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

  it("says nothing below the 90 % bar or the closed-share floor", () => {
    for (const weak of [
      { ...cold, attributableShare: 0.89 },
      { ...cold, closedShareLower: 0.1 },
    ]) {
      expect(
        decideWeatherClosure(
          { cold: weak, snow: null },
          curve([1]),
          false,
          asOf,
          closesAt,
        ),
      ).toBeNull();
    }
  });

  it("keeps the cold and its opening time for a cold ride after snow", () => {
    const v = decideWeatherClosure(
      { cold, snow },
      curve([1, 2, 4]),
      true,
      asOf,
      closesAt,
    );
    expect(v).toMatchObject({ cause: "cold", outlook: "opens_when_warmer" });
  });

  it("names snow only with its own profile, recent snow and a temperature it can lie at", () => {
    const warmNow = { cold, snow: null };
    expect(
      decideWeatherClosure(warmNow, curve([4]), true, asOf, closesAt),
    ).toBeNull();
    expect(
      decideWeatherClosure(
        { cold: null, snow },
        curve([4]),
        true,
        asOf,
        closesAt,
      ),
    ).toMatchObject({ cause: "snow", outlook: null, thresholdC: null });
    expect(
      decideWeatherClosure(
        { cold: null, snow },
        curve([10]),
        true,
        asOf,
        closesAt,
      ),
    ).toBeNull();
    expect(
      decideWeatherClosure(
        { cold: null, snow },
        curve([1]),
        false,
        asOf,
        closesAt,
      ),
    ).toBeNull();
  });
});
