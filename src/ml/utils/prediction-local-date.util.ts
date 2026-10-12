import { formatInParkTimezone } from "../../common/utils/date.util";

/**
 * The park-local calendar day a prediction is about (PAR-818).
 *
 * CatBoost's `predictedTime` is a UTC instant (`…+00:00`), whose first ten
 * characters are the UTC date: a 20:00 slot in Los Angeles is 03:00 UTC of
 * the next day, a 08:00 slot in Tokyo 23:00 UTC of the previous one. TFT's is
 * an offset-less `YYYY-MM-DDT12:00:00` that already names the park-local day —
 * parsing it with `new Date()` would read it on the SERVER's clock and, for a
 * park twelve or more hours from UTC, land it on the neighbouring day. An
 * instant is read on the park's clock; anything without an offset is taken at
 * its word.
 *
 * Lives here rather than on `MLService` so services that must not depend on the
 * whole ML client (the lead-time archive) share the same rule.
 */
export function predictionLocalDate(
  predictedTime: string,
  timezone: string,
): string {
  if (!/(Z|[+-]\d{2}:?\d{2})$/.test(predictedTime)) {
    return predictedTime.slice(0, 10);
  }
  return formatInParkTimezone(new Date(predictedTime), timezone || "UTC");
}
