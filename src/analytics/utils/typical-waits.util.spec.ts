import {
  buildTypicalWaits,
  percentileCont,
  typicalWaitsWindow,
  TypicalWaitsDayInput,
} from "./typical-waits.util";

const DE_WEEKEND = new Set([0, 6]);
const WINDOW = { dataFrom: "2025-10-17", dataTo: "2026-10-16" };
const NOW = new Date("2026-10-17T08:00:00Z");

/** `n` consecutive days starting on a Monday, all with the same peak. */
function days(n: number, peak: number): TypicalWaitsDayInput[] {
  const out: TypicalWaitsDayInput[] = [];
  for (let i = 0; i < n; i++) {
    const d = new Date(Date.UTC(2026, 5, 1 + i)); // 2026-06-01 is a Monday
    out.push({
      date: d.toISOString().slice(0, 10),
      dow: d.getUTCDay(),
      peak,
    });
  }
  return out;
}

describe("percentileCont", () => {
  it("interpolates like PostgreSQL percentile_cont", () => {
    expect(percentileCont([20, 30, 40], 0.5)).toBe(30);
    expect(percentileCont([20, 30, 40], 0.9)).toBe(38);
    expect(percentileCont([50], 0.9)).toBe(50);
    expect(percentileCont([], 0.5)).toBe(0);
  });
});

describe("buildTypicalWaits", () => {
  it("splits weekday/weekend, per weekday and the record peak", () => {
    const result = buildTypicalWaits(
      [
        { date: "2025-06-02", dow: 1, peak: 20 },
        { date: "2025-06-07", dow: 6, peak: 50 },
        { date: "2025-06-09", dow: 1, peak: 30 },
        { date: "2025-06-14", dow: 6, peak: 60 },
        { date: "2025-06-16", dow: 1, peak: 40 },
      ],
      DE_WEEKEND,
      WINDOW,
      NOW,
    );

    expect(result.weekday).toEqual({ typical: 30, busy: 40, sampleDays: 3 });
    expect(result.weekend).toEqual({ typical: 55, busy: 60, sampleDays: 2 });
    expect(result.byDayOfWeek).toEqual([
      { dayOfWeek: 1, isWeekend: false, typical: 30, busy: 40, sampleDays: 3 },
      { dayOfWeek: 6, isWeekend: true, typical: 55, busy: 60, sampleDays: 2 },
    ]);
    expect(result.peak).toEqual({ value: 60, date: "2025-06-14" });
    expect(result.windowDays).toBe(365);
    expect(result.dataFrom).toBe("2025-10-17");
    expect(result.dataTo).toBe("2026-10-16");
    expect(result.generatedAt).toBe(NOW.toISOString());
  });

  it("is displayable from 20 measured days on, not before", () => {
    expect(
      buildTypicalWaits(days(19, 10), DE_WEEKEND, WINDOW, NOW).displayable,
    ).toBe(false);
    expect(
      buildTypicalWaits(days(20, 10), DE_WEEKEND, WINDOW, NOW).displayable,
    ).toBe(true);
  });

  it("follows the country's weekend, not Sat/Sun", () => {
    const gulf = new Set([5, 6]); // Fri+Sat
    const result = buildTypicalWaits(
      [
        { date: "2026-06-05", dow: 5, peak: 70 },
        { date: "2026-06-07", dow: 0, peak: 10 },
      ],
      gulf,
      WINDOW,
      NOW,
    );
    expect(result.weekend.sampleDays).toBe(1);
    expect(result.weekend.typical).toBe(70);
    expect(result.weekday.typical).toBe(10);
  });

  it("keeps the most recent date on a tied record", () => {
    const result = buildTypicalWaits(
      [
        { date: "2026-06-01", dow: 1, peak: 45 },
        { date: "2026-06-08", dow: 1, peak: 45 },
      ],
      DE_WEEKEND,
      WINDOW,
      NOW,
    );
    expect(result.peak).toEqual({ value: 45, date: "2026-06-08" });
  });

  it("answers an empty bucket with nulls and no record", () => {
    const result = buildTypicalWaits([], DE_WEEKEND, WINDOW, NOW);
    expect(result.weekday).toEqual({
      typical: null,
      busy: null,
      sampleDays: 0,
    });
    expect(result.byDayOfWeek).toEqual([]);
    expect(result.peak).toBeNull();
  });
});

describe("typicalWaitsWindow", () => {
  it("ends yesterday in the park's timezone and bounds the scan in UTC", () => {
    // 23:30 UTC on Oct 16 is already Oct 17 in Berlin → yesterday = Oct 16.
    const w = typicalWaitsWindow(
      new Date("2026-10-16T23:30:00Z"),
      "Europe/Berlin",
    );
    expect(w.dataTo).toBe("2026-10-16");
    expect(w.dataFrom).toBe("2025-10-17");
    expect(w.startUtc.toISOString()).toBe("2025-10-16T22:00:00.000Z");
    expect(w.endUtc.toISOString()).toBe("2026-10-16T22:00:00.000Z");
  });
});
