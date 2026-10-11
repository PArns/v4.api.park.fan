import {
  DaySlots,
  WEEKDAY_AVG_WEEKS,
  forwardFillDay,
  weekdayAverageAt,
  weekdayAverageCurve,
  weekdayAverageDates,
  weekdayHistoryDates,
} from "./weekday-average.util";

const day = (entries: Array<[string, number]>): DaySlots => new Map(entries);

describe("forwardFillDay", () => {
  it("carries a reading for at most 3 h (12 quarter-hours)", () => {
    const d = forwardFillDay([
      { time_slot: "10:00", avgWait: 20 },
      { time_slot: "10:30", avgWait: 30 },
    ]);
    expect(d.get("09:45")).toBeUndefined();
    expect(d.get("10:00")).toBe(20);
    expect(d.get("10:15")).toBe(20);
    expect(d.get("10:30")).toBe(30);
    expect(d.get("13:30")).toBe(30); // 12 steps after 10:30
    expect(d.get("13:45")).toBeUndefined();
  });

  it("ignores malformed slots", () => {
    expect(forwardFillDay([{ time_slot: "x", avgWait: 5 }]).size).toBe(0);
  });
});

describe("weekdayHistoryDates", () => {
  it("lists the same weekday back far enough for the longest lead", () => {
    expect(weekdayHistoryDates("2026-10-10", 0)).toHaveLength(
      WEEKDAY_AVG_WEEKS,
    );
    expect(weekdayHistoryDates("2026-10-10", 0)[0]).toBe("2026-10-03");
    // d90: 12 weeks are inside the lead, then 8 usable ones.
    expect(weekdayHistoryDates("2026-10-10", 90)).toHaveLength(
      12 + WEEKDAY_AVG_WEEKS,
    );
  });
});

describe("weekday average — leakage guard", () => {
  // Saturdays before 2026-10-10, plus the target day itself and a Friday.
  const history = new Map<string, DaySlots>([
    ["2026-10-10", day([["12:00", 999]])], // the target day: never
    ["2026-10-09", day([["12:00", 500]])], // not the weekday
    ["2026-10-03", day([["12:00", 40]])],
    ["2026-09-26", day([["12:00", 20]])],
    ["2026-09-19", day([["12:00", 30]])],
    ["2026-09-12", day([["12:00", 10]])],
  ]);

  it("averages same-weekday dates strictly before the origin date", () => {
    // Origin on the target day (lead 0): 03.10., 26.09., 19.09., 12.09.
    expect(weekdayAverageDates(history, "2026-10-10", "2026-10-10")).toEqual([
      "2026-10-03",
      "2026-09-26",
      "2026-09-19",
      "2026-09-12",
    ]);
    expect(weekdayAverageAt(history, "2026-10-10", "2026-10-10", "12:00")).toBe(
      25,
    );
  });

  it("uses nothing at or after the origin, even when the rollup has it", () => {
    // Lead 10 → origin 2026-09-30: 03.10. is after it and must not count.
    expect(weekdayAverageDates(history, "2026-10-10", "2026-09-30")).toEqual([
      "2026-09-26",
      "2026-09-19",
      "2026-09-12",
    ]);
    expect(weekdayAverageAt(history, "2026-10-10", "2026-09-30", "12:00")).toBe(
      20,
    );
    // Origin date itself is excluded too (the 06:00 origin has not seen it).
    expect(
      weekdayAverageDates(history, "2026-10-10", "2026-09-26"),
    ).not.toContain("2026-09-26");
  });

  it("needs at least two dates per quarter-hour", () => {
    expect(
      weekdayAverageAt(history, "2026-10-10", "2026-09-19", "12:00"),
    ).toBeNull();
    expect(
      weekdayAverageCurve(history, "2026-10-10", "2026-10-10", [
        "12:00",
        "13:00",
      ]),
    ).toEqual([25, null]);
  });
});
