import { formatInTimeZone, fromZonedTime } from "date-fns-tz";
import { unfoldedCloseHour } from "../common/utils/day-shape.util";

/**
 * Parky speaks in the visitor's own clock. The model never converts a time: every instant in a
 * tool result is rewritten here, in code, into the browser's zone before the model reads it.
 */

/** An ISO instant with a zone (`Z` or `±hh:mm`); a bare date or a naive time is left alone. */
const INSTANT =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})$/;

/** The browser's IANA zone when the runtime knows it, else UTC. */
export function userTimeZone(raw: string | undefined): string {
  if (!raw) return "UTC";
  try {
    new Intl.DateTimeFormat("en", { timeZone: raw });
    return raw;
  } catch {
    return "UTC";
  }
}

/** Every instant in `value`, at any depth, as "yyyy-MM-dd HH:mm" in `timeZone`. */
export function localizeTimes(value: unknown, timeZone: string): unknown {
  if (typeof value === "string") {
    if (!INSTANT.test(value)) return value;
    const at = new Date(value);
    return Number.isNaN(at.getTime())
      ? value
      : formatInTimeZone(at, timeZone, "yyyy-MM-dd HH:mm");
  }
  if (Array.isArray(value)) {
    return value.map((item) => localizeTimes(item, timeZone));
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [
        key,
        localizeTimes(item, timeZone),
      ]),
    );
  }
  return value;
}

/**
 * The instant of a park-local hour on a park-local day, so it can be shown in another zone. An
 * hour of 24 or more is past midnight, as plan-day unfolds a day that runs over it.
 */
export function parkHourInstant(
  date: string,
  hour: number,
  parkTimeZone: string,
): string {
  const day = new Date(`${date}T00:00:00Z`);
  day.setUTCDate(day.getUTCDate() + Math.floor(hour / 24));
  const hh = String(hour % 24).padStart(2, "0");
  return fromZonedTime(
    `${day.toISOString().slice(0, 10)}T${hh}:00:00`,
    parkTimeZone,
  ).toISOString();
}

/**
 * The forecast hours worth offering: inside the park's opening on that day and, today, not over
 * yet. `closeHour` is the hour of the closing time (18 for 18:00), lower than `openHour` past
 * midnight; plan-day's curve includes it, but at 18:00 nobody queues for a ride any more.
 */
export function bookableHours<T extends { hour: number }>(
  hours: readonly T[],
  day: { openHour: number | null; closeHour: number | null },
  currentParkHour: number | null,
): T[] {
  const { openHour, closeHour } = day;
  const close =
    openHour !== null && closeHour !== null
      ? unfoldedCloseHour(openHour, closeHour)
      : closeHour;
  return hours.filter(
    (entry) =>
      (openHour === null || entry.hour >= openHour) &&
      (close === null || entry.hour < close) &&
      (currentParkHour === null || entry.hour >= currentParkHour),
  );
}

/** "yyyy-MM-dd HH:mm" now, in the visitor's zone. */
export function nowIn(timeZone: string): string {
  return formatInTimeZone(new Date(), timeZone, "yyyy-MM-dd HH:mm");
}
