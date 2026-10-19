/** Thrown by `AbortableRuns.checkpoint()` once shutdown has begun. */
export class RunAborted extends Error {
  constructor() {
    super("Run aborted by module shutdown");
    this.name = "RunAborted";
  }
}

/**
 * The fire-and-forget chains a service starts in `onModuleInit`, made stoppable.
 *
 * `onModuleDestroy` can cancel a timer that has not fired, but not the `await`
 * chain behind one that has: that chain keeps issuing queries against a
 * DataSource and writing to queues that are already closing, and logs the
 * result as an ERROR that reads like a real defect.
 *
 * Shutdown does both halves. `abortAndDrain()` flags the runs so each one
 * stops at its next `checkpoint()` without a log line, then waits for the step
 * in flight, so the connections close after the last query has returned and
 * not under it. Aborting alone would leave that query running into a closed
 * pool; waiting alone would hold `app.close()` for the whole chain (minutes
 * for the scheduler).
 */
export class AbortableRuns {
  private aborted = false;
  private readonly inFlight = new Set<Promise<void>>();

  /** False once `abortAndDrain()` has been called. */
  get alive(): boolean {
    return !this.aborted;
  }

  /** Call between steps, outside any `try` that logs: throws `RunAborted` after shutdown began. */
  checkpoint(): void {
    if (this.aborted) throw new RunAborted();
  }

  /**
   * Starts `run` without waiting for it. `RunAborted` ends it silently, any
   * other error goes to `onError`. Does nothing after shutdown began.
   */
  start(run: () => Promise<void>, onError: (err: unknown) => void): void {
    if (this.aborted) return;
    const tracked: Promise<void> = run()
      .catch((err: unknown) => {
        if (!(err instanceof RunAborted)) onError(err);
      })
      .finally(() => this.inFlight.delete(tracked));
    this.inFlight.add(tracked);
  }

  /** Stops every run at its next checkpoint and resolves once none is in flight. */
  async abortAndDrain(): Promise<void> {
    this.aborted = true;
    await Promise.all([...this.inFlight]);
  }
}
