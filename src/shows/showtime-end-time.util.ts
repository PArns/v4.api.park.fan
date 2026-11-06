/**
 * Longest span a showtime may cover and still count as one performance. Feeds send an end equal
 * to the start as a placeholder (every Universal park, most of Disney US) and some Disney entries
 * span 3–9 hours, which are windows rather than shows; neither is an end time.
 */
const MAX_SHOW_DURATION_MS = 3 * 60 * 60 * 1000;

/**
 * The end time a public payload may show for one showtime, or null when the feed's `endTime` is
 * missing, unparseable, not after `startTime` or more than three hours later.
 *
 * Pass `projectedStart` when the start was moved onto another day: the end then keeps the feed's
 * duration from that start and is returned in UTC, like `ShowsService.projectShowtimesToToday`.
 */
export function showtimeEndTime(
  showtime: { startTime: string; endTime?: string | null },
  projectedStart: string = showtime.startTime,
): string | null {
  if (!showtime.endTime) return null;
  const durationMs =
    Date.parse(showtime.endTime) - Date.parse(showtime.startTime);
  if (!(durationMs > 0 && durationMs <= MAX_SHOW_DURATION_MS)) return null;
  if (projectedStart === showtime.startTime) return showtime.endTime;
  const start = Date.parse(projectedStart);
  return Number.isNaN(start)
    ? null
    : new Date(start + durationMs).toISOString();
}
