import {
  computeRopeDrop,
  RopeDropDayInput,
  DEFAULT_ROPE_DROP_THRESHOLDS,
} from "./rope-drop.util";

/**
 * A "busy" rope-drop day: opening wait 30, midday peak 120, a genuine evening
 * trough at 510 (wait 20), then a closing drain at 540 (wait 3) that the closing
 * guard must discard. Ratio curve (pre-guard): bin0=0.25, bin15=0.50, bin30=1.0,
 * bin45=1.0, bin300=0.417, bin480=0.208, bin510=0.167, bin540=0.025.
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
      { minutesAfterOpen: 300, p90: 50 },
      { minutesAfterOpen: 480, p90: 25 },
      { minutesAfterOpen: 510, p90: 20 }, // genuine evening trough
      { minutesAfterOpen: 540, p90: 3 }, // closing drain — must be guarded out
    ],
  };
}

/**
 * A busy day on a ride that opens AFTER the park gates (Phantasialand-style:
 * park opens 09:00, ride at 10:00). Same shape as busyDay but shifted so the
 * first slot is 60 min after PARK open — there is deliberately NO slot in the
 * [0,15) park-open window. The ride must still be ratable (openWait anchored on
 * its earliest slot, not park open).
 */
function staggeredOpenDay(date: string, dow: number): RopeDropDayInput {
  return {
    date,
    dow,
    slots: [
      { minutesAfterOpen: 60, p90: 30 }, // ride opening wait (earliest slot)
      { minutesAfterOpen: 75, p90: 60 },
      { minutesAfterOpen: 90, p90: 120 },
      { minutesAfterOpen: 105, p90: 120 },
      { minutesAfterOpen: 360, p90: 50 },
      { minutesAfterOpen: 540, p90: 25 },
      { minutesAfterOpen: 570, p90: 20 }, // genuine evening trough
      { minutesAfterOpen: 600, p90: 3 }, // closing drain — guarded out
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
      { minutesAfterOpen: 300, p90: 15 },
      { minutesAfterOpen: 330, p90: 5 },
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

  it("rates a ride that opens after the park gates (staggered opening)", () => {
    // 20 Saturdays where the ride only opens 60 min after PARK open (no slot in
    // the [0,15) park-open window). Regression test for the bug that dropped
    // every such day → sampleDays=0 → busyPeak=0 → worth=false (Phantasialand
    // headliners like Taron, F.L.Y.).
    const r = computeRopeDrop(days(staggeredOpenDay, 6, 20), WINDOW_START);
    expect(r).not.toBeNull();
    expect(r!.sampleDays).toBe(20);
    // Staggered opening (first slot 60 min after PARK open) → open wait is
    // floored to the rope-drop near-walk-on (10), not the first-slot p90 (30),
    // because the rope-dropper queued during the gates→ride gap and rides first.
    expect(r!.openWait).toBe(10);
    expect(r!.busyPeak).toBe(120);
    expect(r!.savings).toBe(110);
    expect(r!.worth).toBe(true);
  });

  it("computes rideBy at the 50%-of-peak crossing", () => {
    const r = computeRopeDrop(days(busyDay, 6, 20), WINDOW_START)!;
    // bin15 ratio = 0.50 (not > 0.5); bin30 ratio = 1.0 → first bin exceeding.
    expect(r.rideByMinutesAfterOpen).toBe(30);
  });

  it("picks the absolute best slot (evening) by minimum ratio", () => {
    const r = computeRopeDrop(days(busyDay, 6, 20), WINDOW_START)!;
    // Genuine evening trough at 510 (ratio 0.167) wins — the 540 closing-drain
    // slot (ratio 0.025) is guarded out, so it can't steal the trough.
    expect(r.bestSlotMinutesAfterOpen).toBe(510);
  });

  it("guards out the pre-closing drain so it is never the trough", () => {
    const r = computeRopeDrop(days(busyDay, 6, 20), WINDOW_START)!;
    // The 540 drain (wait 3) is within the closing guard of the last slot.
    expect(r.bestSlotMinutesAfterOpen).not.toBe(540);
    // The reported trough wait reflects the genuine evening low (~20), not ~3.
    expect(r.bestSlotWait).toBe(20);
    expect(r.bestSlotWait).toBeLessThanOrEqual(r.openWait);
  });

  it("flags a busy ride with a real evening trough as end-of-day worth", () => {
    const r = computeRopeDrop(days(busyDay, 6, 20), WINDOW_START)!;
    // Trough at 510 is past 60% of the guarded day (510) → counts as end-of-day,
    // and busyPeak − bestSlotWait = 100 clears the savings floor.
    expect(r.endOfDayWorth).toBe(true);
    expect(r.endOfDaySavings).toBe(r.busyPeak - r.bestSlotWait);
  });

  it("does not flag end-of-day when the trough is in the morning", () => {
    // Open IS the trough; busy all afternoon, no evening low.
    const morningTrough = (date: string, dow: number): RopeDropDayInput => ({
      date,
      dow,
      slots: [
        { minutesAfterOpen: 0, p90: 20 },
        { minutesAfterOpen: 60, p90: 120 },
        { minutesAfterOpen: 300, p90: 110 },
        { minutesAfterOpen: 480, p90: 100 },
        { minutesAfterOpen: 510, p90: 90 },
      ],
    });
    const r = computeRopeDrop(days(morningTrough, 6, 20), WINDOW_START)!;
    expect(r.bestSlotMinutesAfterOpen).toBe(0); // trough is at open
    expect(r.endOfDayWorth).toBe(false);
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

  it("ignores a day with no operating (post-open) slot for the level layer", () => {
    // A day with only a pre-open heartbeat and no operating reading at all has
    // no opening wait → dropped. (A first slot AFTER park open is NOT this case
    // — that's a staggered ride opening, covered above and now counted.)
    const preOpenOnly: RopeDropDayInput = {
      date: "2026-05-25",
      dow: 6,
      slots: [{ minutesAfterOpen: -60, p90: 5 }],
    };
    const r = computeRopeDrop([...days(busyDay, 6, 20), preOpenOnly], WINDOW_START)!;
    // 20 valid days contribute; the pre-open-only day is dropped.
    expect(r.sampleDays).toBe(20);
  });

  it("uses the default window length in the result", () => {
    const r = computeRopeDrop(days(busyDay, 6, 20), WINDOW_START)!;
    expect(r.windowDays).toBe(DEFAULT_ROPE_DROP_THRESHOLDS.windowDays);
  });

  it("ignores low-support bins from rare extended-hours days", () => {
    // 20 regular days (busyDay: ~510-min day, genuine trough at 510) plus 2
    // extended-hours days whose deep-evening slots (810/825/840) only exist on
    // those long days. Without the support floor the 810 bin (ratio 0.05) would
    // win the trough — and "810 min after open" resolved against a regular
    // day's opening lands hours past closing (the 23:45-on-a-21:00-close bug).
    const longDay = (date: string, dow: number): RopeDropDayInput => ({
      date,
      dow,
      slots: [
        { minutesAfterOpen: 0, p90: 30 },
        { minutesAfterOpen: 30, p90: 120 },
        { minutesAfterOpen: 300, p90: 50 },
        { minutesAfterOpen: 810, p90: 6 },
        { minutesAfterOpen: 825, p90: 8 },
        { minutesAfterOpen: 840, p90: 10 }, // last slot; 810 survives the closing guard
      ],
    });
    const r = computeRopeDrop(
      [...days(busyDay, 6, 20), ...days(longDay, 0, 2, "04")],
      WINDOW_START,
    )!;
    // The trough stays on the typical day's curve, not the rare long-day tail.
    expect(r.bestSlotMinutesAfterOpen).toBe(510);
  });

  it("falls back to all bins when no bin reaches the support floor", () => {
    // Two days with disjoint slots — every bin has support 1 of 2 (< 50%...
    // exactly at ceil(2*0.5)=1, so use three days with disjoint bins instead).
    const dayAt = (
      date: string,
      dow: number,
      offsets: number[],
    ): RopeDropDayInput => ({
      date,
      dow,
      slots: offsets.map((m, i) => ({
        minutesAfterOpen: m,
        p90: i === 0 ? 30 : 60,
      })),
    });
    // 3 shape days, every bin supported by exactly 1 day (< ceil(3*0.5)=2) →
    // the floor would empty the curve; the fallback keeps a recommendation.
    const r = computeRopeDrop(
      [
        dayAt("2026-05-01", 6, [0, 60]),
        dayAt("2026-05-02", 6, [120, 180]),
        dayAt("2026-05-03", 6, [240, 300]),
      ],
      WINDOW_START,
    );
    expect(r).not.toBeNull();
  });
});
