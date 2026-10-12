import {
  newestPerSlot,
  planHourlyPredictionWrite,
  samePredictionValue,
  type IncomingSlotRow,
  type StoredSlotRow,
} from "./prediction-diff-write.util";

const NOW = new Date("2026-10-12T20:45:00Z");
const RUN = NOW;
const at = (iso: string) => new Date(iso);

const value = {
  predictedWaitTime: 30,
  confidence: 68.2,
  uncertaintyMinutes: 12,
  crowdLevel: "moderate",
  status: "OPERATING",
  baseline: 32.5,
  modelVersion: "v20261010_0600",
};

const stored = (
  attractionId: string,
  slot: string,
  createdAt: string,
  over: Partial<typeof value> = {},
): StoredSlotRow => ({
  attractionId,
  predictedTime: at(slot),
  createdAt: at(createdAt),
  ...value,
  ...over,
});

const incoming = (
  attractionId: string,
  slot: string,
  over: Partial<typeof value> = {},
): IncomingSlotRow => ({
  attractionId,
  predictedTime: at(slot),
  ...value,
  ...over,
});

describe("planHourlyPredictionWrite (PAR-874)", () => {
  it("writes nothing for a slot whose answer did not change", () => {
    const plan = planHourlyPredictionWrite(
      [stored("a1", "2026-10-12T22:00:00Z", "2026-10-12T20:30:00Z")],
      [incoming("a1", "2026-10-12T22:00:00Z")],
      NOW,
      RUN,
    );
    expect(plan.insert).toEqual([]);
    expect(plan.delete).toEqual([]);
    expect(plan.unchanged).toBe(1);
  });

  it("replaces a changed slot: inserts the new row, deletes everything older than this run", () => {
    const row = incoming("a1", "2026-10-12T22:00:00Z", { confidence: 68.9 });
    const plan = planHourlyPredictionWrite(
      [stored("a1", "2026-10-12T22:00:00Z", "2026-10-12T20:30:00Z")],
      [row],
      NOW,
      RUN,
    );
    expect(plan.insert).toEqual([row]);
    expect(plan.delete).toEqual([
      { attractionId: "a1", predictedTime: row.predictedTime, before: RUN },
    ]);
    expect(plan.changed).toBe(1);
  });

  it.each([
    ["predictedWaitTime", { predictedWaitTime: 35 }],
    ["confidence", { confidence: 70 }],
    ["uncertaintyMinutes", { uncertaintyMinutes: null }],
    ["crowdLevel", { crowdLevel: "high" }],
    ["status", { status: "UNKNOWN" }],
    ["baseline", { baseline: 33 }],
    ["modelVersion", { modelVersion: "v20261011_0600" }],
  ])("treats a change in %s as a change", (_col, over) => {
    expect(samePredictionValue(value, { ...value, ...(over as object) })).toBe(
      false,
    );
  });

  it("compares the integer columns as Postgres stores them", () => {
    // 30.4 is stored as 30 — rewriting a 30 for it would be churn for nothing.
    expect(
      samePredictionValue(value, {
        ...value,
        predictedWaitTime: 30.4,
        uncertaintyMinutes: 11.6,
      }),
    ).toBe(true);
    // undefined and null are the same absent value.
    expect(
      samePredictionValue(
        { ...value, confidence: null },
        { ...value, confidence: undefined as unknown as null },
      ),
    ).toBe(true);
  });

  it("inserts a slot nobody stored yet", () => {
    const row = incoming("a1", "2026-10-14T20:30:00Z");
    const plan = planHourlyPredictionWrite([], [row], NOW, RUN);
    expect(plan.insert).toEqual([row]);
    expect(plan.added).toBe(1);
  });

  it("deletes a future slot the new run no longer predicts, as the old delete-first did", () => {
    const plan = planHourlyPredictionWrite(
      [stored("a1", "2026-10-13T09:00:00Z", "2026-10-12T20:30:00Z")],
      [],
      NOW,
      RUN,
    );
    expect(plan.dropped).toBe(1);
    expect(plan.delete).toEqual([
      {
        attractionId: "a1",
        predictedTime: at("2026-10-13T09:00:00Z"),
        before: RUN,
      },
    ]);
  });

  it("never touches a slot that is already under way, and always inserts the run's row for it", () => {
    // The stats/day curve keeps the row written BEFORE the slot began as
    // "what the model said"; the old delete was predictedTime >= now too.
    const underWay = incoming("a1", "2026-10-12T20:30:00Z");
    const plan = planHourlyPredictionWrite(
      [stored("a1", "2026-10-12T20:30:00Z", "2026-10-12T20:15:00Z")],
      [underWay],
      NOW,
      RUN,
    );
    expect(plan.insert).toEqual([underWay]);
    expect(plan.delete).toEqual([]);
    expect(plan.dropped).toBe(0);
  });

  it("compares against the NEWEST stored row and removes older duplicates of an unchanged slot", () => {
    const plan = planHourlyPredictionWrite(
      [
        stored("a1", "2026-10-12T22:00:00Z", "2026-10-12T20:00:00Z", {
          predictedWaitTime: 99,
        }),
        stored("a1", "2026-10-12T22:00:00Z", "2026-10-12T20:30:00Z"),
      ],
      [incoming("a1", "2026-10-12T22:00:00Z")],
      NOW,
      RUN,
    );
    expect(plan.unchanged).toBe(1);
    expect(plan.insert).toEqual([]);
    expect(plan.delete).toEqual([
      {
        attractionId: "a1",
        predictedTime: at("2026-10-12T22:00:00Z"),
        before: at("2026-10-12T20:30:00Z"),
      },
    ]);
  });

  it("keeps rides apart", () => {
    const plan = planHourlyPredictionWrite(
      [
        stored("a1", "2026-10-12T22:00:00Z", "2026-10-12T20:30:00Z"),
        stored("a2", "2026-10-12T22:00:00Z", "2026-10-12T20:30:00Z"),
      ],
      [
        incoming("a1", "2026-10-12T22:00:00Z"),
        incoming("a2", "2026-10-12T22:00:00Z", { predictedWaitTime: 5 }),
      ],
      NOW,
      RUN,
    );
    expect(plan.unchanged).toBe(1);
    expect(plan.changed).toBe(1);
    expect(plan.insert.map((r) => r.attractionId)).toEqual(["a2"]);
  });

  it("ends with the same served set the old delete-and-insert produced", () => {
    // Read-path parity: apply the plan to the stored rows and compare what a
    // reader (newest per slot) sees against the incoming run.
    const before = [
      stored("a1", "2026-10-12T21:00:00Z", "2026-10-12T20:30:00Z"),
      stored("a1", "2026-10-12T21:15:00Z", "2026-10-12T20:30:00Z"),
      stored("a1", "2026-10-12T21:30:00Z", "2026-10-12T20:30:00Z"),
    ];
    const run = [
      incoming("a1", "2026-10-12T21:00:00Z"),
      incoming("a1", "2026-10-12T21:15:00Z", { predictedWaitTime: 40 }),
      incoming("a1", "2026-10-12T21:45:00Z"),
    ];
    const plan = planHourlyPredictionWrite(before, run, NOW, RUN);

    const after = [
      ...before.filter(
        (r) =>
          !plan.delete.some(
            (d) =>
              d.attractionId === r.attractionId &&
              d.predictedTime.getTime() === r.predictedTime.getTime() &&
              r.createdAt.getTime() < d.before.getTime(),
          ),
      ),
      ...plan.insert.map((r) => ({ ...r, createdAt: RUN })),
    ];
    const served = newestPerSlot(after)
      .map((r) => [r.predictedTime.toISOString(), r.predictedWaitTime])
      .sort();
    expect(served).toEqual(
      run.map((r) => [r.predictedTime.toISOString(), r.predictedWaitTime]),
    );
    // …while writing two of the three slots instead of all three, and
    // deleting two rows instead of three.
    expect(plan.insert).toHaveLength(2);
    expect(plan.delete).toHaveLength(2);
  });
});

describe("newestPerSlot", () => {
  it("serves one row per slot, the newest, in first-seen order", () => {
    const rows = [
      stored("a1", "2026-10-12T20:30:00Z", "2026-10-12T20:15:00Z", {
        predictedWaitTime: 10,
      }),
      stored("a1", "2026-10-12T20:30:00Z", "2026-10-12T20:45:00Z", {
        predictedWaitTime: 20,
      }),
      stored("a1", "2026-10-12T20:45:00Z", "2026-10-12T20:15:00Z"),
    ];
    expect(newestPerSlot(rows).map((r) => r.predictedWaitTime)).toEqual([
      20, 30,
    ]);
  });
});
