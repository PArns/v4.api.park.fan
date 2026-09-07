/**
 * A process-wide ceiling on how much background work may hold database
 * connections at once.
 *
 * Every BullMQ processor already limits its own fan-out (`BATCH_SIZE = 5` and
 * friends), and each one is reasonable on its own. What nothing limited was
 * their *sum*: six processors running five parks each is thirty in-flight
 * queries, which is exactly `DB_POOL_SIZE`. The request path then waits behind
 * them for a connection, and because TypeORM's `maxQueryExecutionTime` starts
 * counting at the `query()` call, that wait is logged as a slow query — a
 * primary-key lookup measured at 4.2 s while postgres executed it in 0.05 ms.
 *
 * The measured signature was a burst of ~30 queries in one second whose
 * durations differed by 2-6 ms: a shared blocker releasing everyone at once.
 * On 2026-08-31 that pattern accounted for 36 % of all logged slow-query time.
 *
 * This budget caps the background half so the request half always has
 * connections left. Jobs may wait — that is what background means.
 *
 * **Use it on the outermost fan-out only.** A task that asks for a second slot
 * while holding one can deadlock against its own siblings.
 */
export class DbJobBudget {
  private inFlight = 0;
  private readonly waiting: (() => void)[] = [];

  constructor(private readonly limit: number) {
    if (limit < 1) {
      throw new Error(`DbJobBudget limit must be at least 1, got ${limit}`);
    }
  }

  async run<T>(task: () => Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await task();
    } finally {
      this.release();
    }
  }

  private async acquire(): Promise<void> {
    if (this.inFlight < this.limit) {
      this.inFlight++;
      return;
    }
    // No counter bump on this side: the releasing task hands its slot over
    // directly, so `inFlight` already includes us by the time we resume.
    await new Promise<void>((resolve) => this.waiting.push(resolve));
  }

  private release(): void {
    // Hand the slot to whoever is queued rather than freeing and re-taking it.
    // Decrementing here would open a window — a few microtasks wide — in which
    // a fresh caller sees room that is already promised to a waiter, and both
    // run. That is one connection over the limit, which is the whole thing we
    // are trying not to do.
    const next = this.waiting.shift();
    if (next) {
      next();
      return;
    }
    this.inFlight--;
  }
}

/**
 * The shared budget. Sized well below `DB_POOL_SIZE` (default 30) so the
 * request path keeps connections no matter how many jobs happen to overlap.
 */
export const dbJobBudget = new DbJobBudget(
  parseInt(process.env.DB_JOB_CONCURRENCY ?? "12", 10),
);

/**
 * `Promise.all(items.map(fn))` for background jobs: same result, but the
 * in-flight count is capped by the shared budget rather than by the caller's
 * own batch size.
 */
export function mapWithDbBudget<T, R>(
  items: readonly T[],
  fn: (item: T) => Promise<R>,
  budget: DbJobBudget = dbJobBudget,
): Promise<R[]> {
  return Promise.all(items.map((item) => budget.run(() => fn(item))));
}
