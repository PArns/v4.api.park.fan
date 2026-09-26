/**
 * Raised when a ThemeParks.wiki request could not be sent — or was answered
 * with a 429 — because the upstream budget was exhausted.
 *
 * The type exists so a caller can tell the two shapes of "no data" apart. A
 * throttled fetch means WE did not ask; an empty answer means the source has
 * nothing to publish. Before this distinction existed, `getScheduleExtended`
 * returned an empty list for both and the bulk sync logged the cheerful
 * `📅 Fetched total 0 schedule entries` over 87 of 200 parks it never reached
 * (PAR-480).
 */
export class ThemeParksRateLimitError extends Error {
  constructor(
    message: string,
    /** Seconds the upstream (or our own cooldown) wants us to stay away. */
    readonly retryAfterSeconds: number,
  ) {
    super(message);
    this.name = "ThemeParksRateLimitError";
  }
}
