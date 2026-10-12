import {
  dayViewSlots,
  lastCompleteDate,
  rankDayOptions,
  truthCoverage,
} from "./model-comparison-day.util";
import { ArchivedCurve } from "./forecast-archive-scoring.util";

const hhmm = (ms: number[]) =>
  ms.map((s) => new Date(s).toISOString().slice(11, 16));

describe("lastCompleteDate", () => {
  it("is the day before the earliest park-local today", () => {
    // 03:00 UTC: Berlin 05:00 on the 11th, Chicago 22:00 on the 10th.
    const now = new Date("2026-10-11T03:00:00Z");
    expect(lastCompleteDate(["Europe/Berlin"], now)).toBe("2026-10-10");
    expect(lastCompleteDate(["Europe/Berlin", "America/Chicago"], now)).toBe(
      "2026-10-09",
    );
    // 06:00 UTC: Chicago has passed midnight, the 10th is over everywhere.
    expect(
      lastCompleteDate(
        ["Europe/Berlin", "America/Chicago"],
        new Date("2026-10-11T06:00:00Z"),
      ),
    ).toBe("2026-10-10");
  });
});

describe("rankDayOptions", () => {
  const base = {
    parkId: "p",
    parkName: "P",
    timezone: "Europe/Berlin",
    name: "R",
    isHeadliner: false,
    models: 6,
  };

  it("returns no suggestion when no ride has truth in half its window", () => {
    const r = rankDayOptions("2026-10-10", [
      { ...base, attractionId: "a", coverage: 0.49 },
      { ...base, attractionId: "b", coverage: 0 },
    ]);
    expect(r.suggested).toBeNull();
    expect(r.parks[0].rides.map((x) => x.id)).toEqual(["a"]);
  });

  it("ranks parks by rides with good truth before rides with any", () => {
    const big = { ...base, parkId: "big", parkName: "Big", coverage: 0.1 };
    const r = rankDayOptions("2026-10-10", [
      { ...big, attractionId: "1" },
      { ...big, attractionId: "2" },
      { ...big, attractionId: "3" },
      {
        ...base,
        parkId: "rich",
        parkName: "Rich",
        attractionId: "4",
        coverage: 0.9,
      },
    ]);
    expect(r.parks.map((p) => p.id)).toEqual(["rich", "big"]);
    expect(r.suggested?.attractionId).toBe("4");
  });
});

describe("dayViewSlots", () => {
  const served = (waits: (number | null)[]): ArchivedCurve =>
    ({
      slotStart: Date.parse("2026-10-10T04:00:00Z"),
      slotMinutes: 60,
      waits,
    }) as ArchivedCurve;

  it("prefers the published window, floored to the quarter-hour", () => {
    const open = Date.parse("2026-10-10T14:05:00Z");
    const close = Date.parse("2026-10-10T14:40:00Z");
    expect(
      hhmm(
        dayViewSlots({
          windows: [{ open, close }],
          truthSlots: [Date.parse("2026-10-10T20:00:00Z")],
          served: served([10, 10]),
        }),
      ),
    ).toEqual(["14:00", "14:15", "14:30"]);
  });

  it("falls back to the truth span ± 1 h, then to the served curve's values", () => {
    expect(
      hhmm(
        dayViewSlots({
          windows: [],
          truthSlots: [Date.parse("2026-10-10T10:00:00Z")],
          served: served([10]),
        }),
      ),
    ).toEqual([
      "09:00",
      "09:15",
      "09:30",
      "09:45",
      "10:00",
      "10:15",
      "10:30",
      "10:45",
      "11:00",
    ]);
    // A curve from 04:00 whose first two hours are empty: 06:00–07:00 only.
    expect(
      hhmm(
        dayViewSlots({
          windows: [],
          truthSlots: [],
          served: served([null, null, 15, null]),
        }),
      ),
    ).toEqual(["06:00", "06:15", "06:30", "06:45"]);
    expect(dayViewSlots({ windows: [], truthSlots: [], served: null })).toEqual(
      [],
    );
  });
});

describe("truthCoverage", () => {
  it("is the share of slots with a value", () => {
    expect(truthCoverage([1, null, 3])).toBe(0.667);
    expect(truthCoverage([])).toBe(0);
  });
});
