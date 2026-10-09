import {
  buildH5Profile,
  composeH5Day,
  gridOf,
  H5HistoryDay,
  H5RideProfile,
  historyDaysOf,
  isWeekendDate,
  quantile,
  truthSlotsOf,
} from "./h5-profile.util";

/**
 * The H5 port (PAR-834) is pinned against hand-computed numbers from the
 * benchmark's own definitions (`ml-bench/mlbench/baselines.py`), so a later
 * "simplification" that changes the arithmetic fails here rather than on the
 * forward archive's board a week later.
 */
describe("h5-profile.util", () => {
  /** A hand-made profile, so composition is tested apart from the build. */
  const profile = (over: Partial<H5RideProfile> = {}): H5RideProfile => ({
    open: { wd: {}, we: {}, all: { 0: 5, 1: 10, 2: 20, 3: 30 } },
    close: { wd: {}, we: {}, all: { 0: 15, 1: 15, 2: 15, 3: 15 } },
    hourly: {
      wd: {},
      we: {},
      all: { 10: 30, 11: 40, 12: 50, 13: 30, 14: 20 },
    },
    slot: { wd: {}, we: {}, all: {} },
    refLevel: 40,
    days: 20,
    ...over,
  });

  const valueAt = (slots: Array<{ minute: number; wait: number }>, m: number) =>
    slots.find((s) => s.minute === m)?.wait;

  describe("gridOf", () => {
    it("lays slots whose start is in [open, close)", () => {
      expect(gridOf(600, 660)).toEqual([600, 615, 630, 645]);
    });

    it("starts an off-grid opening at the next quarter-hour", () => {
      expect(gridOf(610, 660)).toEqual([615, 630, 645]);
    });
  });

  describe("truthSlotsOf", () => {
    it("forward-fills the change log onto the window's grid", () => {
      const slots = truthSlotsOf({
        weekend: false,
        openMin: 600,
        closeMin: 720,
        readings: [
          { minute: 660, wait: 40 },
          { minute: 600, wait: 20 },
        ],
      });
      expect(slots.map((s) => [s.minute, s.wait])).toEqual([
        [600, 20],
        [615, 20],
        [630, 20],
        [645, 20],
        [660, 40],
        [675, 40],
        [690, 40],
        [705, 40],
      ]);
      // Slots since the opening and before the closing.
      expect(slots[0]).toMatchObject({ ko: 0, kc: 7 });
      expect(slots[7]).toMatchObject({ ko: 7, kc: 0 });
    });

    it("stops a reading after three hours instead of holding it all day", () => {
      const slots = truthSlotsOf({
        weekend: false,
        openMin: 600,
        closeMin: 900,
        readings: [{ minute: 600, wait: 20 }],
      });
      expect(slots[slots.length - 1].minute).toBe(780);
    });

    it("carries a reading from before the opening into it", () => {
      const slots = truthSlotsOf({
        weekend: false,
        openMin: 600,
        closeMin: 630,
        readings: [{ minute: 570, wait: 10 }],
      });
      expect(slots.map((s) => s.wait)).toEqual([10, 10]);
    });

    it("does not count a wait under five minutes as a queue", () => {
      const slots = truthSlotsOf({
        weekend: false,
        openMin: 600,
        closeMin: 630,
        readings: [{ minute: 600, wait: 0 }],
      });
      expect(slots).toEqual([]);
    });
  });

  describe("historyDaysOf", () => {
    it("reads the small hours of a day past midnight from the next date's row", () => {
      // 2026-10-16 is a Friday, open 18:00 → 01:00.
      const days = historyDaysOf(
        new Map([
          ["2026-10-16", [{ minute: 1080, wait: 30 }]],
          ["2026-10-17", [{ minute: 15, wait: 50 }]],
        ]),
        new Map([["2026-10-16", { openMin: 1080, closeMin: 1500 }]]),
      );
      expect(days).toHaveLength(1);
      expect(days[0].weekend).toBe(false);
      expect(days[0].readings).toContainEqual({ minute: 1455, wait: 50 });
      const slots = truthSlotsOf(days[0]);
      expect(slots[slots.length - 1]).toMatchObject({
        minute: 1485,
        wait: 50,
        kc: 0,
      });
    });

    it("does not read the next date into an ordinary day", () => {
      const days = historyDaysOf(
        new Map([
          ["2026-10-16", [{ minute: 600, wait: 30 }]],
          ["2026-10-17", [{ minute: 15, wait: 50 }]],
        ]),
        new Map([["2026-10-16", { openMin: 600, closeMin: 1080 }]]),
      );
      expect(days[0].readings).toEqual([{ minute: 600, wait: 30 }]);
    });

    it("drops a day without a published window", () => {
      expect(
        historyDaysOf(
          new Map([["2026-10-16", [{ minute: 600, wait: 30 }]]]),
          new Map(),
        ),
      ).toEqual([]);
    });

    it("knows a weekend from the date string alone", () => {
      expect(isWeekendDate("2026-10-17")).toBe(true); // Saturday
      expect(isWeekendDate("2026-10-18")).toBe(true); // Sunday
      expect(isWeekendDate("2026-10-19")).toBe(false);
    });
  });

  describe("buildH5Profile", () => {
    /** A 10:00–12:00 day holding `wait` all along. */
    const flatDay = (wait: number, weekend = false): H5HistoryDay => ({
      weekend,
      openMin: 600,
      closeMin: 720,
      readings: [{ minute: 600, wait }],
    });

    it("needs four days for an all-days median and three for a day type", () => {
      const three = buildH5Profile([flatDay(10), flatDay(20), flatDay(30)])!;
      expect(three.hourly.wd[10]).toBe(20);
      expect(three.hourly.all[10]).toBeUndefined();

      const four = buildH5Profile([
        flatDay(10),
        flatDay(20),
        flatDay(30),
        flatDay(40),
      ])!;
      // Interpolated median, as percentile_cont computes it.
      expect(four.hourly.all[10]).toBe(25);
      expect(four.open.all[0]).toBe(25);
      expect(four.close.all[0]).toBe(25);
      expect(four.slot.all[40]).toBe(25); // 10:00 = quarter-hour 40
    });

    it("keeps weekends apart from weekdays", () => {
      const p = buildH5Profile([
        flatDay(10),
        flatDay(10),
        flatDay(10),
        flatDay(50, true),
        flatDay(50, true),
        flatDay(50, true),
      ])!;
      expect(p.hourly.wd[10]).toBe(10);
      expect(p.hourly.we[10]).toBe(50);
      expect(p.hourly.all[10]).toBe(30);
    });

    it("takes the reference level as the median of daily P90s", () => {
      // Each day 8 slots, values rising 10 … 80 by tens plus a day offset.
      const day = (offset: number): H5HistoryDay => ({
        weekend: false,
        openMin: 600,
        closeMin: 720,
        readings: Array.from({ length: 8 }, (_, i) => ({
          minute: 600 + i * 15,
          wait: 10 * (i + 1) + offset,
        })),
      });
      const p = buildH5Profile([day(0), day(10), day(20), day(30)])!;
      // P90 of 10..80 is 73; the days' P90s are 73, 83, 93, 103.
      expect(p.refLevel).toBeCloseTo(88, 6);
      expect(p.days).toBe(4);
    });

    it("has no reference level from days too short to have a P90", () => {
      const short: H5HistoryDay = {
        weekend: false,
        openMin: 600,
        closeMin: 660,
        readings: [{ minute: 600, wait: 30 }],
      };
      expect(buildH5Profile([short, short, short, short])!.refLevel).toBeNull();
    });

    it("is null for a ride with no usable day", () => {
      expect(buildH5Profile([])).toBeNull();
    });
  });

  describe("composeH5Day", () => {
    const base = {
      openMin: 600,
      closeMin: 900,
      alignToSchedule: true,
      weekend: false,
      level: null,
    };

    it("aligns the first hour to the opening and the last to the closing", () => {
      const { slots, levelApplied } = composeH5Day({
        ...base,
        profile: profile(),
      });
      expect(levelApplied).toBe(false);
      expect(slots.slice(0, 4).map((s) => s.wait)).toEqual([5, 10, 20, 30]);
      expect(slots.slice(-4).map((s) => s.wait)).toEqual([15, 15, 15, 15]);
      expect(slots).toHaveLength(20);
    });

    it("moves the rope-drop ramp with the opening, not with the clock", () => {
      // The same profile on an 11:00 day: the ramp is at 11:00, and the 10:00
      // hour the 10-o'clock day ramped in does not exist.
      const { slots } = composeH5Day({
        ...base,
        openMin: 660,
        profile: profile(),
      });
      expect(slots[0]).toEqual({ minute: 660, wait: 5 });
      expect(valueAt(slots, 705)).toBe(30);
    });

    it("interpolates the hourly median between hour centres", () => {
      const { slots } = composeH5Day({ ...base, profile: profile() });
      // 11:00–11:45, hourly 10 → 30, 11 → 40, 12 → 50.
      expect(valueAt(slots, 660)).toBeCloseTo(36.25, 6);
      expect(valueAt(slots, 675)).toBeCloseTo(38.75, 6);
      expect(valueAt(slots, 690)).toBeCloseTo(41.25, 6);
      expect(valueAt(slots, 705)).toBeCloseTo(43.75, 6);
    });

    it("scales by the ratio of the level to the ride's typical P90, never the ramp", () => {
      const { slots, levelApplied } = composeH5Day({
        ...base,
        level: 60,
        profile: profile(),
      });
      expect(levelApplied).toBe(true);
      // 60 / 40 = 1.5 everywhere but the opening hour.
      expect(slots.slice(0, 4).map((s) => s.wait)).toEqual([5, 10, 20, 30]);
      expect(valueAt(slots, 660)).toBeCloseTo(36.25 * 1.5, 6);
      expect(slots.slice(-4).map((s) => s.wait)).toEqual([
        22.5, 22.5, 22.5, 22.5,
      ]);
    });

    it("does not stretch the profile's maximum to the level", () => {
      // The old composer put the curve's maximum AT the level (60); a ratio of
      // 1.5 on a profile whose middle peaks at 50 leaves it well below.
      const { slots } = composeH5Day({
        ...base,
        level: 60,
        profile: profile({ refLevel: 60 }),
      });
      expect(Math.max(...slots.map((s) => s.wait))).toBeCloseTo(48.75, 6);
    });

    it("serves the plain profile when the ride has no reference level", () => {
      const { slots, levelApplied } = composeH5Day({
        ...base,
        level: 60,
        profile: profile({ refLevel: null }),
      });
      expect(levelApplied).toBe(false);
      expect(valueAt(slots, 660)).toBeCloseTo(36.25, 6);
    });

    it("uses the weekday split without a level and all days with one", () => {
      const split = profile({
        hourly: {
          wd: { 11: 100 },
          we: {},
          all: { 10: 30, 11: 40, 12: 50, 13: 30, 14: 20 },
        },
      });
      const plain = composeH5Day({ ...base, profile: split });
      // 11:00 reads the weekday 100 for h0, all-days 30 for the hour before.
      expect(valueAt(plain.slots, 660)).toBeCloseTo(100 - 0.375 * 70, 6);
      const leveled = composeH5Day({ ...base, level: 40, profile: split });
      expect(valueAt(leveled.slots, 660)).toBeCloseTo(36.25, 6);
    });

    it("falls back to the hourly shape where the opening hour was never measured", () => {
      const { slots } = composeH5Day({
        ...base,
        profile: profile({ open: { wd: {}, we: {}, all: {} } }),
      });
      // 10:00 → hourly 10 (30), no 09:00 to lean toward.
      expect(valueAt(slots, 600)).toBe(30);
      expect(valueAt(slots, 630)).toBeCloseTo(31.25, 6);
    });

    it("leaves out a slot nothing measured rather than inventing it", () => {
      const { slots } = composeH5Day({
        ...base,
        profile: profile({
          open: { wd: {}, we: {}, all: {} },
          hourly: { wd: {}, we: {}, all: { 11: 40 } },
        }),
      });
      expect(valueAt(slots, 600)).toBeUndefined();
      expect(valueAt(slots, 660)).toBe(40);
    });

    it("aligns nothing to hours nobody published, and scales the first hour", () => {
      const { slots } = composeH5Day({
        ...base,
        alignToSchedule: false,
        level: 60,
        profile: profile(),
      });
      // 10:00 is the wall-clock hour, scaled like any other.
      expect(valueAt(slots, 600)).toBeCloseTo(30 * 1.5, 6);
      // 14:45 is hourly 14 → 20, toward hour 15 which is not measured.
      expect(valueAt(slots, 885)).toBeCloseTo(20 * 1.5, 6);
    });

    it("composes a day that runs past midnight on one ascending axis", () => {
      const night = profile({
        open: { wd: {}, we: {}, all: { 0: 10 } },
        close: { wd: {}, we: {}, all: { 0: 5 } },
        hourly: { wd: {}, we: {}, all: { 18: 30, 23: 40, 24: 20 } },
      });
      const { slots } = composeH5Day({
        ...base,
        openMin: 1080,
        closeMin: 1500,
        profile: night,
      });
      expect(slots[0]).toEqual({ minute: 1080, wait: 10 });
      // 00:00–00:15 of the next morning is minute 1440 and hour 24.
      expect(valueAt(slots, 1440)).toBeCloseTo(20 + 0.375 * 20, 6);
      expect(slots[slots.length - 1]).toEqual({ minute: 1485, wait: 5 });
    });
  });

  it("quantile matches percentile_cont", () => {
    expect(quantile([1, 2, 3, 4], 0.5)).toBe(2.5);
    expect(quantile([10, 20, 30, 40, 50, 60, 70, 80], 0.9)).toBeCloseTo(73, 6);
    expect(quantile([7], 0.9)).toBe(7);
  });
});
