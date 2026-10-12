/**
 * Diff-write for the hourly rows of `wait_time_predictions` (PAR-874).
 *
 * `generate-hourly` runs every 15 minutes and used to delete every future
 * hourly row of a park and insert the whole 48-hour horizon again. Over one
 * 7-day chunk that was ~200 M inserts against ~199 M deletes, and the primary
 * key — which leads with `(attractionId, predictionType, createdAt)` — grew
 * to 15–25× its natural size, because a B-tree page is reused only once it is
 * entirely empty (docs/database/schema.md, "wait_time_predictions write path").
 *
 * The table cannot take an `ON CONFLICT … DO UPDATE` on the slot: it is a
 * hypertable partitioned on `createdAt`, so every unique index has to contain
 * `createdAt`, and two runs never share one. A slot's identity is
 * `(attractionId, predictionType, predictedTime)` and that is not a key
 * Postgres can enforce here. So the comparison happens in the writer instead:
 * read the slots the park currently holds, compare, and only touch the ones
 * that differ.
 *
 * What a reader sees is unchanged: exactly one row per future slot, holding
 * the newest run's value. What changes is the row's `createdAt` — it is now
 * the time the VALUE was written, not the time it was last confirmed. That is
 * why freshness ("has the generator run for this park recently?") moved off
 * the row and onto a marker (`MLService.markHourlyPredictionsFresh`).
 */

/** The columns a reader is served. Two rows equal on all of them are the same answer. */
export interface PredictionValue {
  predictedWaitTime: number;
  confidence: number | null;
  uncertaintyMinutes: number | null;
  crowdLevel: string | null;
  status: string | null;
  baseline: number | null;
  modelVersion: string;
}

export interface StoredSlotRow extends PredictionValue {
  attractionId: string;
  predictedTime: Date;
  createdAt: Date;
}

export interface IncomingSlotRow extends PredictionValue {
  attractionId: string;
  predictedTime: Date;
}

/** Delete the rows of this slot whose `createdAt` is strictly before `before`. */
export interface SlotDelete {
  attractionId: string;
  predictedTime: Date;
  before: Date;
}

export interface HourlyWritePlan<T extends IncomingSlotRow> {
  /** Rows to insert: slots that are new, changed, or not comparable (already under way). */
  insert: T[];
  /** Superseded rows to remove, bounded per slot so the row kept is never hit. */
  delete: SlotDelete[];
  /** Future slots whose stored answer already equals the new one — nothing written. */
  unchanged: number;
  /** Future slots that changed value. */
  changed: number;
  /** Future slots nobody stored before. */
  added: number;
  /** Future slots the new run no longer predicts (e.g. now inside a CLOSED day). */
  dropped: number;
}

const norm = <V>(v: V | null | undefined): V | null =>
  v === undefined ? null : v;

/** The integer columns are rounded by Postgres on the way in; compare what is stored. */
const roundOrNull = (v: number | null | undefined): number | null =>
  v === undefined || v === null ? null : Math.round(v);

/**
 * Whether two rows give a reader the same answer. Exact comparison, on
 * purpose: the stored floats are the doubles this process wrote, read back
 * bit-for-bit, so an epsilon would only hide a real change in `confidence`.
 * The two integer columns are compared rounded, because that is what an
 * insert would have stored.
 */
export function samePredictionValue(
  a: PredictionValue,
  b: PredictionValue,
): boolean {
  return (
    Math.round(a.predictedWaitTime) === Math.round(b.predictedWaitTime) &&
    norm(a.confidence) === norm(b.confidence) &&
    roundOrNull(a.uncertaintyMinutes) === roundOrNull(b.uncertaintyMinutes) &&
    norm(a.crowdLevel) === norm(b.crowdLevel) &&
    norm(a.status) === norm(b.status) &&
    norm(a.baseline) === norm(b.baseline) &&
    a.modelVersion === b.modelVersion
  );
}

const slotKey = (attractionId: string, predictedTime: Date): string =>
  `${attractionId}|${predictedTime.getTime()}`;

/**
 * Decide what one run writes.
 *
 * - `stored` are the park's hourly rows with `predictedTime >= now` (any
 *   `createdAt` that can still hold a future slot). More than one row per slot
 *   is tolerated — the newest is the answer, exactly as every reader takes it,
 *   and the older ones are deleted.
 * - `incoming` are the rows this run would have inserted, after the schedule
 *   filter.
 * - A slot that is already under way (`predictedTime < now`) is outside the
 *   comparison, as it was outside the old delete: its incoming row is inserted
 *   beside whatever is there. The stats/day curve depends on that — it keeps
 *   the last row written BEFORE the slot began as "what the model said".
 * - `runCreatedAt` is the `createdAt` every inserted row carries; a changed or
 *   dropped slot loses every row older than it.
 */
export function planHourlyPredictionWrite<T extends IncomingSlotRow>(
  stored: ReadonlyArray<StoredSlotRow>,
  incoming: ReadonlyArray<T>,
  now: Date,
  runCreatedAt: Date,
): HourlyWritePlan<T> {
  const nowMs = now.getTime();

  // Newest stored row per future slot, and whether older duplicates exist.
  const newest = new Map<string, StoredSlotRow>();
  const duplicated = new Set<string>();
  for (const row of stored) {
    if (row.predictedTime.getTime() < nowMs) continue;
    const key = slotKey(row.attractionId, row.predictedTime);
    const current = newest.get(key);
    if (current) duplicated.add(key);
    if (!current || row.createdAt.getTime() > current.createdAt.getTime()) {
      newest.set(key, row);
    }
  }

  const plan: HourlyWritePlan<T> = {
    insert: [],
    delete: [],
    unchanged: 0,
    changed: 0,
    added: 0,
    dropped: 0,
  };
  const seen = new Set<string>();

  for (const row of incoming) {
    if (row.predictedTime.getTime() < nowMs) {
      plan.insert.push(row);
      continue;
    }
    const key = slotKey(row.attractionId, row.predictedTime);
    if (seen.has(key)) continue; // one answer per slot and run
    seen.add(key);

    const old = newest.get(key);
    if (!old) {
      plan.added++;
      plan.insert.push(row);
    } else if (samePredictionValue(old, row)) {
      plan.unchanged++;
      if (duplicated.has(key)) {
        plan.delete.push({
          attractionId: row.attractionId,
          predictedTime: row.predictedTime,
          before: old.createdAt,
        });
      }
    } else {
      plan.changed++;
      plan.insert.push(row);
      plan.delete.push({
        attractionId: row.attractionId,
        predictedTime: row.predictedTime,
        before: runCreatedAt,
      });
    }
  }

  for (const [key, old] of newest) {
    if (seen.has(key)) continue;
    plan.dropped++;
    plan.delete.push({
      attractionId: old.attractionId,
      predictedTime: old.predictedTime,
      before: runCreatedAt,
    });
  }

  return plan;
}

/**
 * Newest row per `(attractionId, predictedTime)`, in the input's order of first
 * appearance. The read paths apply it so a slot that holds two rows — the one
 * written before it began and the one written while it ran — is served once,
 * with the newest answer.
 */
export function newestPerSlot<
  R extends { attractionId: string; predictedTime: Date; createdAt: Date },
>(rows: ReadonlyArray<R>): R[] {
  const byKey = new Map<string, R>();
  for (const row of rows) {
    const key = slotKey(row.attractionId, row.predictedTime);
    const current = byKey.get(key);
    if (!current || row.createdAt.getTime() > current.createdAt.getTime()) {
      byKey.set(key, row);
    }
  }
  return [...byKey.values()];
}

/**
 * How long a run vouches for a ride's hourly rows: two 15-minute cycles, so one
 * late or failed run does not blank the forecast. This is the window the read
 * paths used to apply to each row's `createdAt`.
 */
export const HOURLY_FRESHNESS_MS = 30 * 60 * 1000;

/**
 * The oldest `createdAt` a FUTURE hourly row can have: the generation horizon
 * (`HOURLY_PREDICTIONS` = 48 h in ml-service/config.py) plus two hours of slack
 * for a slow run. A row created earlier was predicting a slot that has already
 * passed, so a scan for future slots may stop here — which is what keeps it on
 * the one or two newest (uncompressed) chunks. Raise the horizon and this has
 * to grow with it.
 */
export const HOURLY_LOOKBACK_MS = 50 * 60 * 60 * 1000;

/**
 * Redis marker: "the generator wrote or confirmed this ride's hourly forecast
 * within {@link HOURLY_FRESHNESS_MS}". Set by every successful run with that
 * TTL, read by `getStoredPredictions` / `getBatchStoredPredictions`.
 */
export const hourlyPredictionsFreshKey = (attractionId: string): string =>
  `ml:hourly-fresh:${attractionId}`;
