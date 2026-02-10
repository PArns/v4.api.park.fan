import { AsyncLocalStorage } from "async_hooks";

interface RequestTimingsStore {
  timings: Record<string, number>;
}

const asyncLocalStorage = new AsyncLocalStorage<RequestTimingsStore>();

/**
 * Run a function inside a request timings context (e.g. from middleware).
 * Timings recorded via recordTiming() will be available via getTimings().
 * Supports async callbacks so the context stays active until the callback settles.
 */
export function runWithTimings<T>(next: () => T | Promise<T>): T | Promise<T> {
  return asyncLocalStorage.run({ timings: {} }, next);
}

/**
 * Record a timing for the current request (e.g. "attraction_phase1_ms" = 500).
 * No-op if called outside a request context (e.g. in a background job).
 */
export function recordTiming(name: string, ms: number): void {
  const store = asyncLocalStorage.getStore();
  if (store) store.timings[name] = ms;
}

/**
 * Get all timings recorded for the current request. Used when writing the slow-request log.
 */
export function getTimings(): Record<string, number> | undefined {
  const store = asyncLocalStorage.getStore();
  return store && Object.keys(store.timings).length > 0
    ? store.timings
    : undefined;
}
