/**
 * In-process single-flight: dedupes concurrent calls for the same key so an
 * expensive rebuild (e.g. a ~15s ML inference) runs once and every concurrent
 * caller shares its result. Scoped to one Node process — which is exactly the
 * shape of a cache-miss stampede right after a TTL lapse or a warmup eviction,
 * where many requests for the same key arrive within the compute window.
 *
 * The in-flight entry is cleared as soon as the promise settles (success OR
 * failure), so a failed rebuild doesn't get pinned — the next caller retries.
 * Not a cache and not cross-instance: pair it with the normal Redis cache, it
 * only collapses the concurrent recompute.
 */
export class SingleFlight {
  private readonly inFlight = new Map<string, Promise<unknown>>();

  run<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const existing = this.inFlight.get(key) as Promise<T> | undefined;
    if (existing) return existing;

    const promise = (async () => {
      try {
        return await fn();
      } finally {
        this.inFlight.delete(key);
      }
    })();

    this.inFlight.set(key, promise);
    return promise;
  }
}
