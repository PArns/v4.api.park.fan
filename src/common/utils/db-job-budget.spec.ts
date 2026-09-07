import { DbJobBudget } from "./db-job-budget";

/** Resolves only when the test says so, so overlap is observable. */
function deferred<T = void>() {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("DbJobBudget", () => {
  it("never runs more than the limit at once", async () => {
    const budget = new DbJobBudget(2);
    let running = 0;
    let peak = 0;
    const gates = [deferred(), deferred(), deferred(), deferred()];

    const started: number[] = [];
    const all = gates.map((g, i) =>
      budget.run(async () => {
        started.push(i);
        running++;
        peak = Math.max(peak, running);
        await g.promise;
        running--;
      }),
    );

    // Only the first two may have entered; the rest wait for a free slot.
    await Promise.resolve();
    expect(started).toEqual([0, 1]);

    gates.forEach((g) => g.resolve());
    await Promise.all(all);

    expect(peak).toBe(2);
    expect(started.sort()).toEqual([0, 1, 2, 3]);
  });

  it("frees the slot when a task throws", async () => {
    // A job that dies must not take a slot with it, or the budget bleeds out
    // and every later job waits forever.
    const budget = new DbJobBudget(1);

    await expect(
      budget.run(() => Promise.reject(new Error("boom"))),
    ).rejects.toThrow("boom");

    await expect(budget.run(async () => "went through")).resolves.toBe(
      "went through",
    );
  });

  // The gap between releasing a slot and the queued task taking it is a window
  // a fresh caller can slip through. Sweeping the microtask offsets pins it
  // down without depending on one exact tick count: if the slot is decremented
  // and re-incremented rather than handed over directly, offset 3 runs two
  // tasks at once — on a connection pool, exactly the overflow this exists to
  // prevent.
  it.each([0, 1, 2, 3, 4])(
    "holds the limit when a newcomer arrives %i microtasks after a slot frees",
    async (ticks) => {
      const budget = new DbJobBudget(1);
      let running = 0;
      let peak = 0;
      const gates = [deferred(), deferred(), deferred()];
      const track = async (g: { promise: Promise<void> }) => {
        running++;
        peak = Math.max(peak, running);
        await g.promise;
        running--;
      };

      const first = budget.run(() => track(gates[0]));
      const queued = budget.run(() => track(gates[1]));

      gates[0].resolve();
      for (let i = 0; i < ticks; i++) await Promise.resolve();
      const newcomer = budget.run(() => track(gates[2]));

      gates[1].resolve();
      gates[2].resolve();
      await Promise.all([first, queued, newcomer]);

      expect(peak).toBe(1);
    },
  );

  it("returns each task's value to its own caller", async () => {
    const budget = new DbJobBudget(2);

    const values = await Promise.all(
      [1, 2, 3, 4, 5].map((n) => budget.run(async () => n * 10)),
    );

    expect(values).toEqual([10, 20, 30, 40, 50]);
  });
});
