import { formatInTimeZone, fromZonedTime } from "date-fns-tz";

const DAY_MS = 24 * 60 * 60 * 1000;

/**
 * Repairs a closing time whose *date* is wrong while its time-of-day is right.
 *
 * A park's operating day is anchored to one calendar date, so the window
 * between opening and closing must be greater than zero and at most 24 hours.
 * Sources break that in both directions and we used to store it verbatim:
 *
 * - **Closing before opening.** ThemeParks.wiki stamps a past-midnight close
 *   with the day's own date (`open 2026-07-27T12:00+02:00`,
 *   `close 2026-07-27T00:00+02:00`), putting it 12 h *before* opening. The park
 *   then reads CLOSED all day — Parque Warner Madrid did, every day.
 * - **Closing far after opening.** An overshot day or a typo'd year turns one
 *   evening into a 34-hour (SeaWorld San Diego) or 3-year (Busch Gardens
 *   Williamsburg) "operating day", so the park never appears to close.
 *
 * In every observed case the *time-of-day* is correct, so it is re-anchored to
 * the opening's park-local date and rolled forward a day when that lands at or
 * before opening. An equal opening and closing is left alone: rolling it
 * forward would invent a 24-hour operating day out of a source that reported
 * nothing.
 *
 * @param openingTime - Start of the operating window
 * @param closingTime - End of the operating window, possibly misdated
 * @param timezone - Park timezone (IANA), used to read the local time-of-day
 * @returns The closing time, corrected only when the window is impossible
 */
export function normalizeClosingTime(
  openingTime: Date | null | undefined,
  closingTime: Date,
  timezone: string,
): Date;
export function normalizeClosingTime(
  openingTime: Date | null | undefined,
  closingTime: Date | null | undefined,
  timezone: string,
): Date | null;
export function normalizeClosingTime(
  openingTime: Date | null | undefined,
  closingTime: Date | null | undefined,
  timezone: string,
): Date | null {
  if (!openingTime || !closingTime) {
    return closingTime ?? null;
  }

  const span = closingTime.getTime() - openingTime.getTime();
  // Plausible window, or a degenerate zero-length one we must not invent a day for.
  if (span >= 0 && span <= DAY_MS) {
    return closingTime;
  }

  try {
    const openingDate = formatInTimeZone(openingTime, timezone, "yyyy-MM-dd");
    const closingClock = formatInTimeZone(closingTime, timezone, "HH:mm:ss");

    let anchored = fromZonedTime(`${openingDate}T${closingClock}`, timezone);
    if (Number.isNaN(anchored.getTime())) {
      return closingTime;
    }
    if (anchored.getTime() <= openingTime.getTime()) {
      // Past-midnight close: same clock time on the following park-local day.
      // Re-resolved through the timezone so a DST shift in that night keeps the
      // local closing time rather than a fixed 24 h offset.
      const nextDate = formatInTimeZone(
        new Date(anchored.getTime() + DAY_MS),
        timezone,
        "yyyy-MM-dd",
      );
      anchored = fromZonedTime(`${nextDate}T${closingClock}`, timezone);
    }

    return Number.isNaN(anchored.getTime()) ? closingTime : anchored;
  } catch {
    // Unusable timezone — leave the source value untouched rather than guess.
    return closingTime;
  }
}
