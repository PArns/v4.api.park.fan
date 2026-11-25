import type { Redis } from "ioredis";

/**
 * Redis key for "last seen in any upstream source", one per attraction. The
 * wait-times sync stamps it every time a real feed delivers the ride, changed
 * or not; the hourly heartbeat and reverse-reconciliation never do (PAR-1428).
 */
export const attractionLastSeenKey = (attractionId: string): string =>
  `attraction:last-seen:${attractionId}`;

/** attractionId -> epoch ms; one MGET. Empty on any Redis failure. */
export async function readAttractionLastSeen(
  redis: Redis,
  attractionIds: readonly string[],
): Promise<Map<string, number>> {
  const out = new Map<string, number>();
  if (attractionIds.length === 0) return out;
  try {
    const values = await redis.mget(
      ...attractionIds.map(attractionLastSeenKey),
    );
    values.forEach((raw, i) => {
      const ms = raw ? parseInt(raw, 10) : NaN;
      if (Number.isFinite(ms)) out.set(attractionIds[i], ms);
    });
  } catch {
    // No last-seen is not an error: `lastObserved` falls back to the row.
  }
  return out;
}

/**
 * `queues[].lastObserved`: the last time this ride's wait was read from a
 * source, changed or not. `lastUpdated` stays "when the value last changed".
 *
 * - A ride whose source stopped answering keeps its old last-seen, so it is
 *   never refreshed.
 * - A heartbeat row carries `lastUpdated = now` without any reading, so only
 *   last-seen can speak for it; without one the answer is null.
 * - Never earlier than the row's own `lastUpdated` (last-seen expired or Redis
 *   was flushed).
 */
export function lastObserved(
  qd: {
    lastUpdated?: Date | null;
    timestamp: Date;
    isHeartbeat?: boolean | null;
  },
  seenMs: number | undefined,
): string | null {
  if (qd.isHeartbeat) {
    return seenMs === undefined ? null : new Date(seenMs).toISOString();
  }
  const changedMs = (qd.lastUpdated || qd.timestamp).getTime();
  return new Date(Math.max(changedMs, seenMs ?? 0)).toISOString();
}
