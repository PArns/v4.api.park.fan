import {
  secondsUntilEndOfDayInTimezone,
  getTomorrowDateInTimezoneAt,
} from "./date.util";

describe("secondsUntilEndOfDayInTimezone", () => {
  it("counts to the park's own midnight, not to UTC's", () => {
    // 22:30 UTC = 23:30 in Berlin (CEST, UTC+2 in July → 00:30 next day).
    const at = new Date("2026-07-15T22:30:00Z");
    // Berlin is UTC+2 here, so it is already 00:30 on the 16th: 23.5 h to go.
    expect(secondsUntilEndOfDayInTimezone("Europe/Berlin", at)).toBe(
      23.5 * 3600,
    );
    // In London (UTC+1) it is 23:30 on the 15th: half an hour to go.
    expect(secondsUntilEndOfDayInTimezone("Europe/London", at)).toBe(30 * 60);
    // In New York (UTC-4) it is 18:30 on the 15th.
    expect(secondsUntilEndOfDayInTimezone("America/New_York", at)).toBe(
      5.5 * 3600,
    );
  });

  it("never answers zero, so the last second of a day still caches", () => {
    // 21:59:59.500 UTC = 23:59:59.5 in Berlin (CEST).
    const at = new Date("2026-07-15T21:59:59.500Z");
    expect(secondsUntilEndOfDayInTimezone("Europe/Berlin", at)).toBe(1);
  });

  it("measures the real length of a DST day", () => {
    // The night the clocks go forward in Berlin: 2026-03-29 02:00 local
    // becomes 03:00, so the day is 23 hours long. Measured from midnight it
    // must be 23 h to the next midnight, not 24.
    const justAfterMidnight = new Date("2026-03-28T23:00:00Z"); // 00:00 CET
    expect(
      secondsUntilEndOfDayInTimezone("Europe/Berlin", justAfterMidnight),
    ).toBe(23 * 3600);
  });
});

describe("getTomorrowDateInTimezoneAt", () => {
  it("reads the explicit instant, not the wall clock", () => {
    // 22:30 UTC on the 15th is already 00:30 on the 16th in Berlin (CEST).
    const at = new Date("2026-07-15T22:30:00Z").getTime();
    expect(getTomorrowDateInTimezoneAt(at, "Europe/Berlin")).toBe(
      "2026-07-17",
    );
  });

  it("crosses the spring-forward transition without skipping or repeating a day", () => {
    // 2026-03-29 is the 23-hour day in Berlin (02:00 -> 03:00 at 01:00 UTC).
    // Anchored at midnight instead of noon, adding one calendar day here
    // used to land back on the 29th instead of the 30th.
    const midnightOnTransitionDay = new Date("2026-03-28T23:00:00Z"); // 00:00 CET on the 29th
    expect(
      getTomorrowDateInTimezoneAt(
        midnightOnTransitionDay.getTime(),
        "Europe/Berlin",
      ),
    ).toBe("2026-03-30");
  });

  it("crosses the fall-back transition without skipping or repeating a day", () => {
    // 2026-10-25 is the 25-hour day in Berlin (03:00 -> 02:00 at 01:00 UTC).
    const midnightOnTransitionDay = new Date("2026-10-24T22:00:00Z"); // 00:00 CEST on the 25th
    expect(
      getTomorrowDateInTimezoneAt(
        midnightOnTransitionDay.getTime(),
        "Europe/Berlin",
      ),
    ).toBe("2026-10-26");
  });
});
