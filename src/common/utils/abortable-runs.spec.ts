import { AbortableRuns, RunAborted } from "./abortable-runs";

describe("AbortableRuns", () => {
  it("reports a thrown error to onError", async () => {
    const runs = new AbortableRuns();
    const onError = jest.fn();
    const boom = new Error("boom");

    runs.start(() => Promise.reject(boom), onError);
    await runs.abortAndDrain();

    expect(onError).toHaveBeenCalledWith(boom);
  });

  it("ends a run silently at the checkpoint after abortAndDrain started", async () => {
    const runs = new AbortableRuns();
    const onError = jest.fn();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const reached: string[] = [];

    runs.start(async () => {
      reached.push("step 1");
      await gate;
      runs.checkpoint();
      reached.push("step 2");
    }, onError);

    const drained = runs.abortAndDrain();
    release();
    await drained;

    expect(reached).toEqual(["step 1"]);
    expect(onError).not.toHaveBeenCalled();
  });

  it("waits for the step in flight before it resolves", async () => {
    const runs = new AbortableRuns();
    let stepFinished = false;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));

    runs.start(async () => {
      await gate;
      stepFinished = true;
    }, jest.fn());

    let drainedEarly = false;
    const drained = runs.abortAndDrain().then(() => {
      drainedEarly = !stepFinished;
    });
    await Promise.resolve();
    release();
    await drained;

    expect(stepFinished).toBe(true);
    expect(drainedEarly).toBe(false);
  });

  it("starts nothing once abortAndDrain was called", async () => {
    const runs = new AbortableRuns();
    await runs.abortAndDrain();
    const run = jest.fn().mockResolvedValue(undefined);

    runs.start(run, jest.fn());

    expect(run).not.toHaveBeenCalled();
    expect(runs.alive).toBe(false);
    expect(() => runs.checkpoint()).toThrow(RunAborted);
  });

  it("does not throw at a checkpoint while alive", () => {
    expect(() => new AbortableRuns().checkpoint()).not.toThrow();
  });
});
