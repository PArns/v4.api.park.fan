import {
  computeRopeDrop,
  RopeDropDayInput,
  DEFAULT_ROPE_DROP_THRESHOLDS,
} from "./rope-drop.util";

/**
 * A "busy" rope-drop day: opening wait 30, midday peak 120, evening trough 10.
 * Ratio curve: bin0=0.25, bin15=0.50, bin30=1.0, bin45=1.0, bin600=0.083.
 */
function busyDay(date: string, dow: number): RopeDropDayInput {
  return {
    date,
    dow,
    slots: [
      { minutesAfterOpen: 0, p90: 30 },
      { minutesAfterOpen: 15, p90: 60 },
      { minutesAfterOpen: 30, p90: 120 },
      { minutesAfterOpen: 45, p90: 120 },
      { minutesAfterOpen: 600, p90: 10 },
    ],
  };
}

/** A quiet day: peak only 30, open 10 — not worth rope-dropping. */
function quietDay(date: string, dow: number): RopeDropDayInput {
  return {
    date,
    dow,
    slots: [
      { minutesAfterOpen: 0, p90: 10 },
      { minutesAfterOpen: 30, p90: 30 },
      { minutesAfterOpen: 600, p90: 5 },
    ],
  };
}

/** Build N dated days in May 2026 (all >= windowStart) alternating none. */
function days(
  builder: (date: string, dow: number) => RopeDropDayInput,
  dow: number,
  count: number,
  month = "05",
): RopeDropDayInput[] {
  const out: RopeDropDayInput[] = [];
  for (let i = 1; i <= count; i++) {
    const dd = String(i).padStart(2, "0");
    out.push(builder(`2026-${month}-${dd}`, dow));
  }
  return out;
}

const WINDOW_START = "2026-04-01";

describe("computeRopeDrop", () => {
  it("returns null when there is no data", () => {
    expect(computeRopeDrop([], WINDOW_START)).toBeNull();
  });

  it("flags a busy headliner as worth + high strength with the right levels", () => {
    // 20 Saturdays of busy data, all within the window.
    const result = computeRopeDrop(days(busyDay, 6, 20), WINDOW_START);
    expect(result).not.toBeNull();
    const r = result!;
    expect(r.worth).toBe(true);
    expect(r.strength).toBe("high"); // peak>=90 && savings>=60
    expect(r.busyPeak).toBe(120);
    expect(r.openWait).toBe(30);
    expect(r.savings).toBe(90);
  });

  it("computes rideBy at the 50%-of-peak crossing", () => {
    const r = computeRopeDrop(days(busyDay, 6, 20), WINDOW_START)!;
    // bin15 ratio = 0.50 (not > 0.5); bin30 ratio = 1.0 → first bin exceeding.
    expect(r.rideByMinutesAfterOpen).toBe(30);
  });

  it("picks the absolute best slot (evening) by minimum ratio", () => {
    const r = computeRopeDrop(days(busyDay, 6, 20), WINDOW_START)!;
    // Evening trough (600) has the lowest ratio (0.083), below the open bin (0.25).
    expect(r.bestSlotMinutesAfterOpen).toBe(600);
  });

  it("still flags an always-busy flagship ride (high open, big savings)", () => {
    // Anna & Elsa-style: morning is already busy (open 150 = 62.5% of peak 240),
    // but rope-dropping still saves 90 min. Absolute savings wins — worth=true,
    // strength=high. (The old openWait<=0.5*peak gate wrongly excluded these.)
    const flagship = (date: string, dow: number): RopeDropDayInput => ({
      date,
      dow,
      slots: [
        { minutesAfterOpen: 0, p90: 150 },
        { minutesAfterOpen: 60, p90: 240 },
        { minutesAfterOpen: 600, p90: 200 },
      ],
    });
    const r = computeRopeDrop(days(flagship, 6, 20), WINDOW_START)!;
    expect(r.busyPeak).toBe(240);
    expect(r.openWait).toBe(150);
    expect(r.savings).toBe(90);
    expect(r.worth).toBe(true);
    expect(r.strength).toBe("high");
    // No real low-wait window: open ratio 0.625 > 0.5 already at bin 0.
    expect(r.rideByMinutesAfterOpen).toBe(0);
  });

  it("does not flag a quiet ride (peak below the floor)", () => {
    const r = computeRopeDrop(days(quietDay, 6, 20), WINDOW_START)!;
    expect(r.busyPeak).toBe(30);
    expect(r.worth).toBe(false);
    expect(r.strength).toBeNull();
  });

  it("uses the busier of the weekend/weekday buckets as the headline", () => {
    // Weekend busy (peak 120), weekday quiet (peak 30).
    const input = [...days(busyDay, 6, 10), ...days(quietDay, 3, 10)];
    const r = computeRopeDrop(input, WINDOW_START)!;
    expect(r.busyPeak).toBe(120); // headline follows the weekend bucket
    expect(r.byDaytype.weekend.busyPeak).toBe(120);
    expect(r.byDaytype.weekday.busyPeak).toBe(30);
  });

  it("excludes out-of-window days from the level layer", () => {
    // In-window busy peak 120; older (Jan) days are even busier (peak 200) but
    // must NOT raise the trailing-window levels.
    const older = days(
      (date, dow) => ({
        date,
        dow,
        slots: [
          { minutesAfterOpen: 0, p90: 50 },
          { minutesAfterOpen: 30, p90: 200 },
        ],
      }),
      6,
      10,
      "01",
    );
    const r = computeRopeDrop(
      [...days(busyDay, 6, 20), ...older],
      WINDOW_START,
    )!;
    expect(r.busyPeak).toBe(120); // 200-peak January days excluded from levels
  });

  it("derives confidence from the number of in-window operating days", () => {
    const medium = computeRopeDrop(days(busyDay, 6, 20), WINDOW_START)!;
    expect(medium.sampleDays).toBe(20);
    expect(medium.confidence).toBe("medium"); // >=20 && <40

    // 40 days → high, 10 → low
    const hi = computeRopeDrop(
      [...days(busyDay, 6, 20), ...days(busyDay, 0, 20)],
      WINDOW_START,
    )!;
    expect(hi.sampleDays).toBe(40);
    expect(hi.confidence).toBe("high");

    const lo = computeRopeDrop(days(busyDay, 6, 10), WINDOW_START)!;
    expect(lo.sampleDays).toBe(10);
    expect(lo.confidence).toBe("low");
  });

  it("ignores days with no opening slot for the level layer", () => {
    const noOpen: RopeDropDayInput = {
      date: "2026-05-25",
      dow: 6,
      slots: [
        { minutesAfterOpen: 60, p90: 120 },
        { minutesAfterOpen: 600, p90: 10 },
      ],
    };
    const r = computeRopeDrop([...days(busyDay, 6, 20), noOpen], WINDOW_START)!;
    // 20 valid days contribute; the no-open day is dropped.
    expect(r.sampleDays).toBe(20);
  });

  it("uses the default window length in the result", () => {
    const r = computeRopeDrop(days(busyDay, 6, 20), WINDOW_START)!;
    expect(r.windowDays).toBe(DEFAULT_ROPE_DROP_THRESHOLDS.windowDays);
  });
});
