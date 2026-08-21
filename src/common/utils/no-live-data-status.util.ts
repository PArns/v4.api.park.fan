/**
 * What a ride reads when no live row arrived at all.
 *
 * The park's ride list is optimistic here on purpose: an open park whose feed
 * went quiet for one attraction is far more likely to be a gap in our data than
 * a ride that shut on its own, and the pessimistic reading produced the
 * "Park geöffnet, alle Bahnen zu" page that this fallback was written to stop.
 *
 * It was optimistic about the wrong rides too. Phantasialand's Ice skate hire
 * runs in November, December and January; in August no row arrives because
 * there is nothing to report, and the fallback served it as OPERATING at
 * `very_low` — "geöffnet, sehr wenig los" — on the park page, while the ride's
 * own endpoint, which has no fallback and keeps its CLOSED placeholder, served
 * CLOSED off the very same absence. One ride, one request apart, two answers.
 *
 * So the season gets a say, and only where it has something to say.
 */

/**
 * The status an attraction with no queue rows should carry.
 *
 * `isCurrentlyInSeason` is the API's own resolved answer (see
 * `curated-attraction-facts.util`), and only a hard `false` closes the ride:
 * that is the case where somebody wrote the operating months down, or the
 * detector recorded the last day the ride was seen running. "Seasonal, and
 * nothing else known" stays `null` and keeps the optimism, because it must not
 * hide a ride we have simply not understood yet.
 *
 * Deliberately not reached when a live row exists. A queue row is an
 * observation and the season is a description of past behaviour, so a season
 * that starts a week early is the feed's news to tell — this rule only ever
 * fills a silence.
 */
export function statusWithoutLiveData(
  parkStatus: string | null | undefined,
  isCurrentlyInSeason: boolean | null | undefined,
): "OPERATING" | "CLOSED" {
  if (parkStatus !== "OPERATING") return "CLOSED";
  return isCurrentlyInSeason === false ? "CLOSED" : "OPERATING";
}
