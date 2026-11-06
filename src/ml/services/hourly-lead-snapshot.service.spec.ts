import { HourlyLeadSnapshotService } from "./hourly-lead-snapshot.service";
import { WaitTimePrediction } from "../entities/wait-time-prediction.entity";

describe("HourlyLeadSnapshotService.pick", () => {
  // A full-hour slot; each case varies the run that predicted it.
  const slot = new Date("2026-11-07T12:00:00Z");
  const before = (minutes: number) =>
    new Date(slot.getTime() - minutes * 60_000);
  const run = before(0);
  const at = (leadMinutes: number, extra: Partial<WaitTimePrediction> = {}) =>
    ({
      attractionId: "a-1",
      createdAt: before(leadMinutes),
      predictedTime: slot,
      predictedWaitTime: 25,
      uncertaintyMinutes: 8,
      modelVersion: "v9",
      predictionType: "hourly",
      ...extra,
    }) as WaitTimePrediction;

  it("keeps only the slots inside a sampling window, each with its bucket", () => {
    const leads = [
      30, 61, 120, 121, 300, 301, 360, 1380, 1440, 2100, 2160, 2161,
    ];
    const rows = HourlyLeadSnapshotService.pick(
      leads.map((l) => at(l)),
      undefined,
      run,
    );
    expect(rows.map((r) => [r.leadMinutes, r.leadBucket])).toEqual([
      [61, "h0-2"],
      [120, "h0-2"],
      [301, "h2-6"],
      [360, "h2-6"],
      [1440, "h6-24"],
      [2160, "h24+"],
    ]);
  });

  it("samples only the slot on the full hour", () => {
    const quarterPast = new Date(slot.getTime() + 15 * 60_000);
    const rows = HourlyLeadSnapshotService.pick(
      [at(360), at(345, { predictedTime: quarterPast })],
      undefined,
      run,
    );
    expect(rows.map((r) => r.targetTime)).toEqual([slot]);
  });

  it("carries the raw answer, the run and the served model of the same index", () => {
    const [row] = HourlyLeadSnapshotService.pick(
      [at(30), at(1400)],
      [null, { wait: 41, source: "pcn" }],
      run,
    );
    expect(row).toMatchObject({
      attractionId: "a-1",
      predictionTime: before(1400),
      leadBucket: "h6-24",
      predictedWaitTime: 25,
      uncertaintyMinutes: 8,
      modelVersion: "v9",
      servedWaitTime: 41,
      servedSource: "pcn",
    });
  });

  it("skips daily rows and measures from `now` when the row has no run time", () => {
    const now = before(360);
    const rows = HourlyLeadSnapshotService.pick(
      [
        at(300, { predictionType: "daily" }),
        at(300, { createdAt: undefined as unknown as Date }),
      ],
      undefined,
      now,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ leadMinutes: 360, servedSource: null });
  });
});
