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

/**
 * Reads the closing time of a source that publishes a 12-hour clock unlabelled,
 * where a midnight close arrives as `12:00`.
 *
 * This is the one repair `normalizeClosingTime` cannot make. That function
 * trusts the time-of-day and fixes the date; here the time-of-day is the part
 * that is wrong, and the only thing setting it apart from a park that genuinely
 * closes at noon is that the closing falls *before* the opening — which is
 * exactly what the re-anchoring consumes. So it has to run on the raw pair,
 * before any normalization, and only for a park somebody has flagged.
 *
 * Six Flags Qiddiya City is the worked example: `opens 15:00 / closes 12:00`,
 * five days in 2026. Re-anchored it becomes a 21-hour operating day — right
 * during the evening the park is actually open, wrong for the eleven hours
 * afterwards. Read as a 12-hour clock it is `15:00 → 00:00`, a nine-hour
 * evening, which is what the park publishes on its own site.
 *
 * Deliberately narrow. It fires on `12:00:00` and nothing else: a 12-hour clock
 * misprints only the noon/midnight pair, every other hour survives the
 * conversion intact, and widening it to "any closing before opening" would
 * re-decide the past-midnight closes that already work.
 *
 * @param openingTime - Start of the operating window, as the source reported it
 * @param closingTime - End of the window, raw — before `normalizeClosingTime`
 * @param timezone - Park timezone (IANA), used to read the local time-of-day
 * @returns Midnight on the park-local day after opening, or null when this rule
 *   does not apply — in which case the caller falls back to the generic repair
 */
export function correctTwelveHourClockClose(
  openingTime: Date | null | undefined,
  closingTime: Date | null | undefined,
  timezone: string,
): Date | null {
  if (!openingTime || !closingTime) {
    return null;
  }
  // The signal, and the reason this runs before `normalizeClosingTime`: a noon
  // close that sits *after* opening is a real noon close, and stays one.
  if (closingTime.getTime() >= openingTime.getTime()) {
    return null;
  }

  try {
    if (formatInTimeZone(closingTime, timezone, "HH:mm:ss") !== "12:00:00") {
      return null;
    }

    const openingDate = formatInTimeZone(openingTime, timezone, "yyyy-MM-dd");
    // Step to the next calendar date through UTC noon rather than through the
    // park's own zone: adding 24 h to a local midnight lands on the wrong side
    // of a DST boundary, and in the zones that shift *at* midnight that local
    // midnight does not exist at all.
    const nextDate = formatInTimeZone(
      new Date(new Date(`${openingDate}T12:00:00Z`).getTime() + DAY_MS),
      "UTC",
      "yyyy-MM-dd",
    );

    let corrected = fromZonedTime(`${nextDate}T00:00:00`, timezone);
    if (formatInTimeZone(corrected, timezone, "yyyy-MM-dd") !== nextDate) {
      // That midnight does not exist: the zone springs forward across it
      // (Santiago, Havana), and `fromZonedTime` resolves a missing wall-clock
      // time *backwards* — to 23:00 on the day before, an hour before the day
      // it is supposed to start. The day begins at the transition instead, so
      // ask for the first hour that does exist.
      corrected = fromZonedTime(`${nextDate}T01:00:00`, timezone);
      if (formatInTimeZone(corrected, timezone, "yyyy-MM-dd") !== nextDate) {
        // A gap longer than an hour. Rather than keep guessing at the shape of
        // a day nobody has seen, hand it back to the generic repair.
        return null;
      }
    }
    return corrected;
  } catch {
    // Unusable timezone, or an invalid date `formatInTimeZone` refuses —
    // leave it to the generic repair rather than guess.
    return null;
  }
}
