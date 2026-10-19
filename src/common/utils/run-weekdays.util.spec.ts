import {
  isoWeekdayOf,
  partialRunWeekdays,
  runWeekdays,
} from "./run-weekdays.util";

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

const EVERY_DAY = [1, 2, 3, 4, 5, 6, 7];

describe("isoWeekdayOf", () => {
  it("numbers Monday 1 and Sunday 7", () => {
    expect(isoWeekdayOf("2026-10-19")).toBe(1);
    expect(isoWeekdayOf("2026-10-25")).toBe(7);
  });
});

describe("runWeekdays", () => {
  // Monday 2026-10-19, the day the mazes read „opens at 17:00".
  const today = "2026-10-19";

  // Movie Park as measured that day: open every day in late August, then
  // Thu–Sun, once on Wednesday 30.09., and on the holiday Monday itself.
  const movieParkOpen = [
    ...datesOn("2026-08-24", "2026-09-06", EVERY_DAY),
    ...datesOn("2026-09-07", "2026-10-18", [4, 5, 6, 7]),
    "2026-09-30",
    today,
  ];

  it("reads Movie Park's mazes as Fri–Sun, without the one Wednesday", () => {
    // The park opened on one Wednesday since the mazes began, and they ran on
    // it. 1 of 1 is an anecdote, not a Wednesday attraction.
    const mazes = [
      ...datesOn("2026-09-26", "2026-10-18", [5, 6, 7]),
      "2026-09-30",
    ];

    expect(runWeekdays(mazes, movieParkOpen, today)).toEqual([5, 6, 7]);
  });

  it("keeps the weekdays a ride that runs whenever the park does runs on", () => {
    // Star Trek ran on every open day. The park opens on Mondays only in
    // holiday weeks, and Monday must not drop out because of that.
    const starTrek = movieParkOpen.filter((d) => d < today);

    expect(runWeekdays(starTrek, movieParkOpen, today)).toEqual(EVERY_DAY);
  });

  it("keeps a weekday the park was open on only once, for a ride that runs whenever it is", () => {
    const parkOpen = [
      ...datesOn("2026-08-24", "2026-10-18", [4, 5, 6, 7]),
      "2026-10-07",
    ];

    expect(runWeekdays(parkOpen, parkOpen, today)).toEqual([3, 4, 5, 6, 7]);
  });

  it("gives today's weekday to a ride that runs whenever the park does, on the first such day", () => {
    // The first holiday Monday: no earlier open Monday to count, but rides of
    // the park have run today.
    const parkOpen = [...datesOn("2026-08-24", "2026-10-18", [4, 5, 6, 7])];

    expect(runWeekdays(parkOpen, [...parkOpen, today], today)).toEqual([
      1, 4, 5, 6, 7,
    ]);
    // Before any ride has run today, the park is not known to be open.
    expect(runWeekdays(parkOpen, parkOpen, today)).toEqual([4, 5, 6, 7]);
  });

  it("leaves out a weekday the park was never open on", () => {
    const parkOpen = datesOn("2026-08-24", "2026-10-18", [5, 6, 7]);

    expect(runWeekdays(parkOpen, parkOpen, today)).toEqual([5, 6, 7]);
  });

  it("measures the share from the ride's first day, not the window's", () => {
    // Three Fridays of eight would fail a share taken over the whole window.
    const mazes = datesOn("2026-09-26", "2026-10-18", [5, 6, 7]);
    expect(runWeekdays(mazes, movieParkOpen, today)).toEqual([5, 6, 7]);
  });

  it("keeps a weekday the ride ran on half of the park's open days", () => {
    const parkOpen = datesOn("2026-09-21", "2026-10-18", [5, 6, 7]);
    const ride = [
      ...datesOn("2026-09-21", "2026-10-18", [6, 7]),
      "2026-09-25",
      "2026-10-09",
    ];
    expect(runWeekdays(ride, parkOpen, today)).toEqual([5, 6, 7]);
  });

  it("has no pattern below two weeks of history", () => {
    const ride = datesOn("2026-10-09", "2026-10-18", [5, 6, 7]);
    expect(runWeekdays(ride, movieParkOpen, today)).toBeNull();
  });

  it("has no pattern for a ride idle for more than a week", () => {
    // Last ran on Sunday 11.10., eight days before today.
    const ride = datesOn("2026-08-24", "2026-10-11", [5, 6, 7]);
    expect(runWeekdays(ride, movieParkOpen, today)).toBeNull();
  });

  it("keeps the pattern for a ride idle for exactly a week", () => {
    const parkOpen = datesOn("2026-08-24", "2026-10-18", [1, 6, 7]);
    const ride = datesOn("2026-08-24", "2026-10-12", [1, 6, 7]);
    expect(runWeekdays(ride, parkOpen, today)).toEqual([1, 6, 7]);
  });

  it("has no pattern for a ride that runs on no weekday in particular", () => {
    const parkOpen = datesOn("2026-08-24", "2026-10-18", EVERY_DAY);
    const ride = ["2026-09-01", "2026-09-10", "2026-09-26", "2026-10-15"];
    expect(runWeekdays(ride, parkOpen, today)).toBeNull();
  });

  it("does not count today towards the shares", () => {
    // A maze that ran this Monday for the first time is still a weekend ride.
    const mazes = [...datesOn("2026-09-26", "2026-10-18", [5, 6, 7]), today];
    expect(runWeekdays(mazes, movieParkOpen, today)).toEqual([5, 6, 7]);
  });

  it("has no pattern without a single operating day", () => {
    expect(runWeekdays([], movieParkOpen, today)).toBeNull();
  });
});

describe("partialRunWeekdays", () => {
  it("keeps a pattern that leaves a weekday out", () => {
    expect(partialRunWeekdays([5, 6, 7])).toEqual([5, 6, 7]);
    expect(partialRunWeekdays([1, 2, 3, 4, 5, 6])).toEqual([1, 2, 3, 4, 5, 6]);
  });

  it("drops a ride that runs every day, and a ride with no pattern", () => {
    expect(partialRunWeekdays([1, 2, 3, 4, 5, 6, 7])).toBeUndefined();
    expect(partialRunWeekdays(undefined)).toBeUndefined();
    expect(partialRunWeekdays(null)).toBeUndefined();
    expect(partialRunWeekdays([])).toBeUndefined();
  });

  it("hands out a copy, so the cached map cannot be changed through the payload", () => {
    const cached = [5, 6, 7];
    const served = partialRunWeekdays(cached);
    served?.push(1);
    expect(cached).toEqual([5, 6, 7]);
  });
});
