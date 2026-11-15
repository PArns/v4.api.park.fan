import {
  nextPerformanceTime,
  normalizePerformanceSchedule,
  performanceSchedulesOf,
  performanceTimesOn,
  resolvePerformanceSchedules,
} from "./performance-schedule.util";

describe("performance schedules (PAR-1355)", () => {
  const winter = {
    from: "2026-11-14",
    to: "2027-01-24",
    times: ["13:00", "14:00", "15:00", "16:00", "17:00"],
  };
  const summer = { from: "2026-04-01", to: "2026-10-31", times: ["11:00"] };

  it("puts the keys in a fixed order and the times in sequence", () => {
    // jsonb hands keys back sorted by length; the admin diff compares strings.
    const stored = { times: ["14:00", "13:00", "14:00"], to: null, from: null };
    expect(JSON.stringify(normalizePerformanceSchedule(stored))).toBe(
      '{"from":null,"to":null,"times":["13:00","14:00"]}',
    );
  });

  it("sorts schedules by start and drops one without times", () => {
    expect(
      resolvePerformanceSchedules({
        curatedPerformanceSchedules: [
          winter,
          { from: null, to: "2026-03-31", times: [] },
          summer,
        ],
      }),
    ).toEqual([summer, winter]);
  });

  it("adds no key to a ride without schedules", () => {
    expect(performanceSchedulesOf({})).toEqual({});
    expect(
      performanceSchedulesOf({ curatedPerformanceSchedules: null }),
    ).toEqual({});
    expect(
      performanceSchedulesOf({ curatedPerformanceSchedules: [winter] }),
    ).toEqual({ performanceSchedules: [winter] });
  });

  it.each([
    ["the first day", "2026-11-14", winter.times],
    ["the last day", "2027-01-24", winter.times],
    ["a summer day", "2026-07-01", summer.times],
    ["a day between the two", "2026-11-05", null],
    ["the day after the last", "2027-01-25", null],
  ])("finds the start times on %s", (_, day, times) => {
    expect(performanceTimesOn([summer, winter], day)).toEqual(times);
  });

  it.each([
    ["before the first", "11:05", "13:00"],
    ["between two", "13:37", "14:00"],
    ["in the minute one starts", "14:00", "14:00"],
    ["after the last has begun", "17:01", null],
  ])("finds the next start %s", (_, now, next) => {
    expect(nextPerformanceTime(winter.times, now)).toBe(next);
  });

  it("reads a schedule without dates as holding every day", () => {
    const always = { from: null, to: null, times: ["10:00"] };
    expect(performanceTimesOn([always], "2030-02-29")).toEqual(["10:00"]);
  });
});
