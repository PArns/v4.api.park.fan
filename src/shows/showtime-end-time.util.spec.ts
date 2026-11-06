import { showtimeEndTime } from "./showtime-end-time.util";

describe("showtimeEndTime", () => {
  const startTime = "2026-11-06T14:00:00+01:00";

  it("keeps an end after the start and at most three hours later", () => {
    const endTime = "2026-11-06T14:30:00+01:00";
    expect(showtimeEndTime({ startTime, endTime })).toBe(endTime);
    expect(
      showtimeEndTime({ startTime, endTime: "2026-11-06T17:00:00+01:00" }),
    ).toBe("2026-11-06T17:00:00+01:00");
  });

  it("drops an end equal to the start (feed placeholder)", () => {
    expect(showtimeEndTime({ startTime, endTime: startTime })).toBeNull();
  });

  it("drops a missing or unparseable end", () => {
    expect(showtimeEndTime({ startTime })).toBeNull();
    expect(showtimeEndTime({ startTime, endTime: null })).toBeNull();
    expect(showtimeEndTime({ startTime, endTime: "soon" })).toBeNull();
  });

  it("drops an end before the start or more than three hours later", () => {
    expect(
      showtimeEndTime({ startTime, endTime: "2026-11-06T13:00:00+01:00" }),
    ).toBeNull();
    expect(
      showtimeEndTime({ startTime, endTime: "2026-11-06T17:00:01+01:00" }),
    ).toBeNull();
  });

  it("keeps the duration from a projected start", () => {
    expect(
      showtimeEndTime(
        {
          startTime: "2026-11-05T14:00:00+01:00",
          endTime: "2026-11-05T14:20:00+01:00",
        },
        "2026-11-06T14:00:00+01:00",
      ),
    ).toBe("2026-11-06T13:20:00.000Z");
  });
});
