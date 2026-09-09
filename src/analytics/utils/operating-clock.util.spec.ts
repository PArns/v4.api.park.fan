import {
  projectOperatingMinutes,
  recoveryWindowFrom,
  type OperatingWindow,
} from "./operating-clock.util";

/**
 * The four cases the ticket named, plus the two that break a naive version.
 *
 * A wall-clock implementation passes the first case and only the first case,
 * which is what makes it worth pinning: the difference between the two only
 * shows up once a closing time is crossed, and every park crosses one every
 * night.
 */

/** Europe/Berlin summer: 10:00-20:00 local is 08:00-18:00 UTC. */
const win = (opens: string, closes: string): OperatingWindow => ({
  opensAt: new Date(opens),
  closesAt: new Date(closes),
});

const TODAY = win("2026-09-09T08:00:00.000Z", "2026-09-09T18:00:00.000Z");
const TOMORROW = win("2026-09-10T08:00:00.000Z", "2026-09-10T18:00:00.000Z");

describe("projectOperatingMinutes", () => {
  it("stays inside today's window when the minutes fit", () => {
    const at = projectOperatingMinutes(
      [TODAY, TOMORROW],
      new Date("2026-09-09T13:00:00.000Z"),
      90,
    );
    expect(at?.toISOString()).toBe("2026-09-09T14:30:00.000Z");
  });

  it("carries what does not fit into the next opening, never into the night", () => {
    // 17:20 UTC, park shuts at 18:00: forty operating minutes left today, so
    // 120 minutes of outage ends eighty minutes after tomorrow's opening. A
    // clock would have answered 19:20 tonight, with the park shut.
    const at = projectOperatingMinutes(
      [TODAY, TOMORROW],
      new Date("2026-09-09T17:20:00.000Z"),
      120,
    );
    expect(at?.toISOString()).toBe("2026-09-10T09:20:00.000Z");
  });

  it("starts counting at the next opening when the park is shut right now", () => {
    // 22:00 the previous night. Nothing ticks until 08:00.
    const at = projectOperatingMinutes(
      [TODAY, TOMORROW],
      new Date("2026-09-09T04:00:00.000Z"),
      30,
    );
    expect(at?.toISOString()).toBe("2026-09-09T08:30:00.000Z");
  });

  it("spans more than one closing when it has to", () => {
    // Ten operating hours a day: 25 hours of outage is today (5 h left from
    // 13:00), all of tomorrow (10 h) and 10 h into the day after — one minute
    // past its closing, so it lands on the fourth day.
    const dayAfter = win(
      "2026-09-11T08:00:00.000Z",
      "2026-09-11T18:00:00.000Z",
    );
    const fourth = win("2026-09-12T08:00:00.000Z", "2026-09-12T18:00:00.000Z");
    const at = projectOperatingMinutes(
      [TODAY, TOMORROW, dayAfter, fourth],
      new Date("2026-09-09T13:00:00.000Z"),
      25 * 60 + 1,
    );
    expect(at?.toISOString()).toBe("2026-09-12T08:01:00.000Z");
  });

  it("answers null rather than a wall-clock guess when the calendar runs out", () => {
    // The season ends tonight. Adding the minutes to `from` would place the
    // recovery in a park that is not going to open.
    expect(
      projectOperatingMinutes(
        [TODAY],
        new Date("2026-09-09T17:30:00.000Z"),
        120,
      ),
    ).toBeNull();
  });

  it("answers null on no calendar at all", () => {
    expect(
      projectOperatingMinutes([], new Date("2026-09-09T13:00:00.000Z"), 30),
    ).toBeNull();
  });

  it("steps over a window that is already over and one with no length", () => {
    const past = win("2026-09-08T08:00:00.000Z", "2026-09-08T18:00:00.000Z");
    // `normalizedClosingSql` keeps a zero-length row rather than rolling it
    // forward into an invented 24-hour day, so one can reach this function.
    const degenerate = win(
      "2026-09-09T09:00:00.000Z",
      "2026-09-09T09:00:00.000Z",
    );
    const at = projectOperatingMinutes(
      [past, degenerate, TODAY],
      new Date("2026-09-09T07:00:00.000Z"),
      15,
    );
    expect(at?.toISOString()).toBe("2026-09-09T08:15:00.000Z");
  });

  it("answers the next open instant for zero minutes, and nothing for nonsense", () => {
    expect(
      projectOperatingMinutes(
        [TODAY],
        new Date("2026-09-09T04:00:00.000Z"),
        0,
      )?.toISOString(),
    ).toBe("2026-09-09T08:00:00.000Z");
    expect(
      projectOperatingMinutes(
        [TODAY],
        new Date("2026-09-09T13:00:00.000Z"),
        -1,
      ),
    ).toBeNull();
    expect(
      projectOperatingMinutes(
        [TODAY],
        new Date("2026-09-09T13:00:00.000Z"),
        NaN,
      ),
    ).toBeNull();
  });
});

describe("recoveryWindowFrom", () => {
  const asOf = new Date("2026-09-09T13:00:00.000Z");

  it("places both quartiles when both resolve", () => {
    // The measured 60-minute bucket: p25 25, p75 255.
    expect(
      recoveryWindowFrom([TODAY, TOMORROW], asOf, { p25: 25, p75: 255 }),
    ).toEqual({
      from: "2026-09-09T13:25:00.000Z",
      to: "2026-09-09T17:15:00.000Z",
    });
  });

  it("keeps the range open when the curve did not resolve the upper quartile", () => {
    // The 120-minute bucket: p25 50, p75 null. The lower bound is still worth
    // having — "back at 13:50 at the earliest, no upper bound we can measure".
    expect(
      recoveryWindowFrom([TODAY, TOMORROW], asOf, { p25: 50, p75: null }),
    ).toEqual({ from: "2026-09-09T13:50:00.000Z", to: null });
  });

  it("keeps the range open when the calendar cannot reach the upper quartile", () => {
    // p75 of 460 operating minutes: five hours left today, the rest would need
    // a tomorrow this park has not published. The lower bound still lands.
    expect(recoveryWindowFrom([TODAY], asOf, { p25: 35, p75: 460 })).toEqual({
      from: "2026-09-09T13:35:00.000Z",
      to: null,
    });
  });

  it("withholds the whole window when the lower bound cannot be placed", () => {
    // Half of the object is not a range, and a `from` nobody can compute is not
    // improved by shipping a `to`.
    expect(
      recoveryWindowFrom([TODAY], asOf, { p25: 400, p75: 460 }),
    ).toBeUndefined();
  });

  it("says nothing at all for a park that publishes no hours", () => {
    // 21 parks. There is no operating minute to project onto, and a wall-clock
    // answer there would be the confident number on the thinnest evidence.
    expect(recoveryWindowFrom([], asOf, { p25: 25, p75: 255 })).toBeUndefined();
  });
});
