import { RopeDropInfo, RopeDropStored } from "../types/rope-drop.type";

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
 */
export function buildRopeDropInfo(
  stored: RopeDropStored,
  openingUtcIso: string | null | undefined,
): RopeDropInfo {
  const base = openingUtcIso ? new Date(openingUtcIso).getTime() : NaN;
  const resolve = (offsetMinutes: number): string | null =>
    Number.isFinite(base)
      ? new Date(base + offsetMinutes * 60000).toISOString()
      : null;

  return {
    ...stored,
    rideByUtc: resolve(stored.rideByMinutesAfterOpen),
    bestSlotUtc: resolve(stored.bestSlotMinutesAfterOpen),
  };
}
