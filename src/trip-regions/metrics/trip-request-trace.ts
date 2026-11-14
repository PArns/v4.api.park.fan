import { AsyncLocalStorage } from "node:async_hooks";

/**
 * What a measured trip-region request learns on its way through the services,
 * for its metric row: which caches answered, and the trip's shape once the
 * query is valid. The interceptor opens a trace around the route
 * (`TripRequestMetricsInterceptor`); the services note into it. Outside a
 * measured request (Parky's tools call the same services) there is no trace
 * and every note is a no-op. docs/frontend/trip-regions.md#what-the-server-records.
 */

/** How a cached read was served: from Redis, by joining a build in flight, or built. */
export type TripCacheState = "hit" | "joined" | "miss";

/** The trip's shape: counts and flags of the normalized query, never a date or a name. */
export interface TripRequestShape {
  /** Trip length in days, arrival and departure included (`/when`: the trip's `days`). */
  tripDays?: number;
  /** Parks planned for: those named, or the headliners. */
  parks?: number;
  /** Whether `parks=` named them; false: the headliners. */
  parksNamed?: boolean;
  /** Park visits, second visits included. */
  visits?: number;
  locks?: number;
  /** One entry per owned ticket: its product's kind (`ticketKindOf`). */
  ticketKinds?: string[];
  /** A hopper anywhere: plan first's resorts or an owned ticket with it. */
  hopper?: boolean;
  /** `/when` and `/best-days`: the days the search or the window spans. */
  rangeDays?: number;
  /** `/when`: kept inside a school-holiday region's breaks. */
  within?: boolean;
}

export interface TripRequestTrace {
  /** The answer's own cache (`/plan`, `/when`). */
  cache?: TripCacheState;
  /** The region calendar the answer was built from. */
  calendarCache?: TripCacheState;
  shape?: TripRequestShape;
}

const storage = new AsyncLocalStorage<TripRequestTrace>();

const RANK: Record<TripCacheState, number> = { hit: 0, joined: 1, miss: 2 };

/** Runs `fn` with `trace` open: what the services note during it lands there. */
export function withTripTrace<T>(trace: TripRequestTrace, fn: () => T): T {
  return storage.run(trace, fn);
}

/**
 * Notes how a cache served this request. The costlier state wins: a build
 * noted after the `joined` that came before the single-flight is a `miss`.
 */
export function noteTripCache(
  which: "cache" | "calendarCache",
  state: TripCacheState,
): void {
  const trace = storage.getStore();
  if (!trace) return;
  const before = trace[which];
  if (!before || RANK[state] > RANK[before]) trace[which] = state;
}

/** Notes the trip's shape, merged into what is already there. */
export function noteTripShape(shape: TripRequestShape): void {
  const trace = storage.getStore();
  if (!trace) return;
  trace.shape = { ...trace.shape, ...shape };
}
