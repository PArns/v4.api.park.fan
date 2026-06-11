import { RopeDropInfo, RopeDropStored } from "../types/rope-drop.type";

/**
 * Buffer kept before closing when clamping resolved instants — mirrors the
 * compute layer's `closingGuardMinutes` (the pre-closing line drain is never
 * a real recommendation target).
 */
const CLOSING_CLAMP_BUFFER_MINUTES = 30;

/**
 * Resolve a stored rope-drop recommendation into the API shape by turning the
 * opening-relative offsets into concrete UTC instants for a given operating
 * day's opening time.
 *
 * The `*MinutesAfterOpen` offsets are the portable, always-correct values;
 * `rideByUtc`/`bestSlotUtc` are a convenience anchored to `openingUtcIso`
 * (the relevant operating day's opening, a UTC ISO 8601 string). When no
 * opening time is available the UTC fields are null and clients should fall
 * back to the offsets.
 *
 * When `closingUtcIso` is provided, resolved instants are clamped to the
 * operating day (closing minus a guard buffer). The pooled shape offsets come
 * from days of varying length, so on a short day an offset can point past
 * closing — the clamp keeps the convenience values inside today's hours.
 */
export function buildRopeDropInfo(
  stored: RopeDropStored,
  openingUtcIso: string | null | undefined,
  closingUtcIso?: string | null,
): RopeDropInfo {
  const base = openingUtcIso ? new Date(openingUtcIso).getTime() : NaN;
  const close = closingUtcIso ? new Date(closingUtcIso).getTime() : NaN;
  const clampMax = Number.isFinite(close)
    ? Math.max(base, close - CLOSING_CLAMP_BUFFER_MINUTES * 60000)
    : NaN;
  const resolve = (offsetMinutes: number): string | null => {
    if (!Number.isFinite(base)) return null;
    let t = base + offsetMinutes * 60000;
    if (Number.isFinite(clampMax) && t > clampMax) t = clampMax;
    return new Date(t).toISOString();
  };

  return {
    ...stored,
    rideByUtc: resolve(stored.rideByMinutesAfterOpen),
    bestSlotUtc: resolve(stored.bestSlotMinutesAfterOpen),
  };
}
