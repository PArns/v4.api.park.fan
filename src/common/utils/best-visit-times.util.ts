export interface BestVisitSlot {
  time: string; // ISO 8601
  predictedWaitTime: number;
  rating: "optimal" | "good";
}

interface PredictionInput {
  predictedTime: string;
  predictedWaitTime: number;
}

const MIN_GAP_MS = 60 * 60 * 1000; // 1 hour minimum between recommendations
const MAX_OPTIMAL = 2;
const MAX_GOOD = 3;
// Predictions use 15-min slot timestamps (e.g. 16:45 represents 16:45–17:00).
export const SLOT_DURATION_MS = 15 * 60 * 1000;

/**
 * Returns the UTC start timestamp (ms) of the currently-active 15-min slot.
 * E.g. at 13:55 → 13:45:00.000Z; at 13:44 → 13:30:00.000Z.
 */
export function currentSlotStartMs(): number {
  return Math.floor(Date.now() / SLOT_DURATION_MS) * SLOT_DURATION_MS;
}

/**
 * Compute best visit time recommendations from 15-min ML predictions.
 *
 * Returns up to 5 slots (≤2 optimal, ≤3 good) sorted by time.
 * Only the currently-active slot and future slots are considered —
 * expired slots (predictedTime < current slot start) are excluded.
 *
 * Strategy: pick the lowest-wait distinct time slots (≥1h apart).
 * "optimal" = absolute minimum wait; "good" = within 25% of minimum.
 *
 * @param predictions  Raw 15-min ML predictions (may include past slots from cache)
 * @param closingTimeIso  Park closing time as ISO string for today, or null
 */
export function computeBestVisitTimes(
  predictions: PredictionInput[],
  closingTimeIso: string | null | undefined,
): BestVisitSlot[] | null {
  // Slot-aligned cutoff: only include the current slot and future slots.
  // floor(now / 15min) * 15min gives the exact start of the active slot,
  // so the previous slot is excluded the moment the new slot begins.
  const cutoff = currentSlotStartMs();
  const closingMs = closingTimeIso ? new Date(closingTimeIso).getTime() : null;

  const future = predictions
    .filter((p) => {
      const ms = new Date(p.predictedTime).getTime();
      if (ms < cutoff) return false;
      if (closingMs !== null && ms > closingMs) return false;
      if (p.predictedWaitTime <= 0) return false;
      return true;
    })
    .sort((a, b) => a.predictedTime.localeCompare(b.predictedTime));

  if (future.length < 2) return null;

  const globalMin = Math.min(...future.map((p) => p.predictedWaitTime));
  // Good threshold: 25% above min, at least 5 min above (prevents rounding noise)
  const goodThreshold = Math.max(globalMin * 1.25, globalMin + 5);

  // Pick top N distinct slots sorted by wait time asc, with ≥1h gap between picks
  const sorted = [...future].sort(
    (a, b) => a.predictedWaitTime - b.predictedWaitTime,
  );

  const results: BestVisitSlot[] = [];
  let optimalCount = 0;
  let goodCount = 0;

  for (const pred of sorted) {
    const rating: "optimal" | "good" | null =
      pred.predictedWaitTime === globalMin
        ? "optimal"
        : pred.predictedWaitTime <= goodThreshold
          ? "good"
          : null;

    if (rating === null) continue;
    if (rating === "optimal" && optimalCount >= MAX_OPTIMAL) continue;
    if (rating === "good" && goodCount >= MAX_GOOD) continue;

    // Enforce minimum separation between selected slots
    const predMs = new Date(pred.predictedTime).getTime();
    const tooClose = results.some(
      (r) => Math.abs(new Date(r.time).getTime() - predMs) < MIN_GAP_MS,
    );
    if (tooClose) continue;

    results.push({
      time: pred.predictedTime,
      predictedWaitTime: pred.predictedWaitTime,
      rating,
    });
    if (rating === "optimal") optimalCount++;
    else goodCount++;

    if (optimalCount >= MAX_OPTIMAL && goodCount >= MAX_GOOD) break;
  }

  return results.length > 0
    ? results.sort((a, b) => a.time.localeCompare(b.time))
    : null;
}
