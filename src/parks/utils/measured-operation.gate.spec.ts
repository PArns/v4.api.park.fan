import {
  isMeasuredOperationDay,
  MEASURED_OPERATION_MAX_BLOCK_HOURS,
  MEASURED_OPERATION_MIN_BLOCK_HOURS,
  MEASURED_OPERATION_MIN_DISTINCT_WAITS,
  MeasuredOperationDayStats,
} from "./measured-operation.gate";

/**
 * PAR-697. The gate decides whether the calendar may publish an operating day
 * against the operator's own `CLOSED` entry, so each threshold is here with the
 * production day it was measured to reject.
 *
 * It is a pure function for exactly this reason: until 2026-10-07 the rule was
 * SQL evaluated per read, and the two Walibi Holland days it turns on could
 * only be checked by querying production. The numbers below are those two days.
 */
describe("isMeasuredOperationDay", () => {
  /** A day that clears everything, so each case below changes one thing. */
  const operatingDay: MeasuredOperationDayStats = {
    distinctWaits: 12,
    deadHourReadings: 0,
    blockMinutes: 8 * 60,
  };

  it("opens Walibi Holland's 2026-04-18: 16 rides, 10:00–18:00, varied waits", () => {
    expect(
      isMeasuredOperationDay({
        distinctWaits: 17,
        deadHourReadings: 0,
        blockMinutes: 8 * 60,
      }),
    ).toBe(true);
  });

  it("keeps Walibi Holland's 2026-09-11 shut: a 55-minute burst is not a day", () => {
    // 14 rides reporting the same wait at the same minute, jumping
    // 15 → 45 → 70 → 36 → 16 → 44 → 80 in five-minute steps. The variety alone
    // would pass; the block length is what rejects it.
    expect(
      isMeasuredOperationDay({
        distinctWaits: 7,
        deadHourReadings: 0,
        blockMinutes: 55,
      }),
    ).toBe(false);
  });

  it("keeps a day with a single reading in the dead hours shut", () => {
    // 352 of 551 candidate days carried waits > 0 between 02:00 and 05:00
    // park-local. One such reading is enough: no park runs at four in the
    // morning, so the day is not a day.
    expect(
      isMeasuredOperationDay({ ...operatingDay, deadHourReadings: 1 }),
    ).toBe(false);
  });

  it("rejects a feed reporting one number all day", () => {
    expect(
      isMeasuredOperationDay({
        ...operatingDay,
        distinctWaits: MEASURED_OPERATION_MIN_DISTINCT_WAITS - 1,
      }),
    ).toBe(false);
    expect(
      isMeasuredOperationDay({
        ...operatingDay,
        distinctWaits: MEASURED_OPERATION_MIN_DISTINCT_WAITS,
      }),
    ).toBe(true);
  });

  it("treats both block bounds as inclusive", () => {
    expect(
      isMeasuredOperationDay({
        ...operatingDay,
        blockMinutes: MEASURED_OPERATION_MIN_BLOCK_HOURS * 60,
      }),
    ).toBe(true);
    expect(
      isMeasuredOperationDay({
        ...operatingDay,
        blockMinutes: MEASURED_OPERATION_MIN_BLOCK_HOURS * 60 - 1,
      }),
    ).toBe(false);
    expect(
      isMeasuredOperationDay({
        ...operatingDay,
        blockMinutes: MEASURED_OPERATION_MAX_BLOCK_HOURS * 60,
      }),
    ).toBe(true);
    expect(
      isMeasuredOperationDay({
        ...operatingDay,
        blockMinutes: MEASURED_OPERATION_MAX_BLOCK_HOURS * 60 + 1,
      }),
    ).toBe(false);
  });

  it("refuses a day with no qualifying reading at all", () => {
    // `block_minutes` is null exactly when the aggregate saw no qualifying row.
    // Checked explicitly rather than left to arithmetic: `null >= 240` is false
    // in TypeScript but `Number(null)` is 0, so a coercion anywhere on the way
    // in would turn "no readings" into a real zero-length block.
    expect(
      isMeasuredOperationDay({
        distinctWaits: 0,
        deadHourReadings: 0,
        blockMinutes: null,
      }),
    ).toBe(false);
  });
});
