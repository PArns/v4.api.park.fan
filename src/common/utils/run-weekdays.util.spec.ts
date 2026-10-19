import { isoWeekdayOf, runWeekdays } from "./run-weekdays.util";

/** Every date from `from` to `to`, inclusive, whose ISO weekday is listed. */
function datesOn(from: string, to: string, weekdays: number[]): string[] {
  const out: string[] = [];
  const d = new Date(`${from}T00:00:00Z`);
  const end = new Date(`${to}T00:00:00Z`);
  for (; d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
    const iso = d.toISOString().slice(0, 10);
    if (weekdays.includes(isoWeekdayOf(iso))) out.push(iso);
  }
  return out;
}

describe("isoWeekdayOf", () => {
  it("numbers Monday 1 and Sunday 7", () => {
    expect(isoWeekdayOf("2026-10-19")).toBe(1);
    expect(isoWeekdayOf("2026-10-25")).toBe(7);
  });
});

describe("runWeekdays", () => {
  // Monday 2026-10-19, the day the mazes read „opens at 17:00".
  const today = "2026-10-19";

  it("reads Movie Park's mazes as Fri–Sun, without the one Wednesday", () => {
    // The season began on Saturday 26.09. The mazes ran every Fri–Sun since,
    // and on Wednesday 30.09. once.
    const days = [
      ...datesOn("2026-09-26", "2026-10-18", [5, 6, 7]),
      "2026-09-30",
    ];

    expect(runWeekdays(days, today)).toEqual([5, 6, 7]);
  });

  it("measures the share from the ride's first day, not the window's", () => {
    // Three Fridays of eight would fail a share taken over the whole window.
    const days = datesOn("2026-09-26", "2026-10-18", [5, 6, 7]);
    expect(runWeekdays(days, today)).toEqual([5, 6, 7]);
  });

  it("gives a ride that runs every day every weekday", () => {
    const days = datesOn("2026-08-24", "2026-10-19", [1, 2, 3, 4, 5, 6, 7]);
    expect(runWeekdays(days, today)).toEqual([1, 2, 3, 4, 5, 6, 7]);
  });

  it("keeps a weekday the ride ran on half the time", () => {
    const days = [
      ...datesOn("2026-09-21", "2026-10-18", [6, 7]),
      "2026-09-25",
      "2026-10-09",
    ];
    expect(runWeekdays(days, today)).toEqual([5, 6, 7]);
  });

  it("has no pattern below two weeks of history", () => {
    expect(
      runWeekdays(datesOn("2026-10-09", "2026-10-18", [5, 6, 7]), today),
    ).toBeNull();
  });

  it("has no pattern for a ride idle for more than a week", () => {
    // Last ran on Sunday 11.10., eight days before today.
    expect(
      runWeekdays(datesOn("2026-08-24", "2026-10-11", [5, 6, 7]), today),
    ).toBeNull();
  });

  it("keeps the pattern for a ride idle for exactly a week", () => {
    expect(
      runWeekdays(datesOn("2026-08-24", "2026-10-12", [1, 6, 7]), today),
    ).toEqual([1, 6, 7]);
  });

  it("has no pattern for a ride that runs on no weekday in particular", () => {
    const days = ["2026-09-01", "2026-09-10", "2026-09-26", "2026-10-15"];
    expect(runWeekdays(days, today)).toBeNull();
  });

  it("does not count today towards the shares", () => {
    // A ride that ran this Monday for the first time is still a weekend ride.
    const days = [...datesOn("2026-09-26", "2026-10-18", [5, 6, 7]), today];
    expect(runWeekdays(days, today)).toEqual([5, 6, 7]);
  });

  it("has no pattern without a single operating day", () => {
    expect(runWeekdays([], today)).toBeNull();
  });
});
