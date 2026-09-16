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
 * How long a park's whole feed must have been silent before its rides stop
 * reading OPERATING off this fallback.
 *
 * The optimism below is written for ONE ride going quiet at a park whose feed
 * works. It has no answer for a park where nothing has arrived at all, and on
 * 2026-09-16 **nine** parks were in that state — the 2, 3 and 4 in the last
 * three columns of the table below. All nine lose the fallback through this
 * constant, whatever their schedule says. `findScheduledButSilentParks` reports
 * the five of them that are also scheduled open, which is a narrower question
 * and a different one.
 *
 * The case that was reported: La Ronde, silent since 2026-06-24 with a schedule
 * running to 2027-08-31, served all 38 of its rides as OPERATING at `very_low`
 * — "geöffnet, sehr wenig los" — while each ride's own page served CLOSED off
 * the same silence.
 *
 * Thirty days, and it is not compared against Busch Gardens Tampa's 65-day
 * recovery in `source-absent-status.util.ts`: that was nine rides of a park
 * whose feed kept working, and a subset is `findSilencedClusters`' subject, not
 * this one. What matters here is the longest gap a WORKING feed leaves, and the
 * answer is that there is no middle ground. Days since the last observed
 * reading, per park with at least one un-retired attraction, measured
 * 2026-09-16:
 *
 * | 0–1 | 2–30 | 31–90 | 90+ | never seen in 400 days |
 * | -- | -- | -- | -- | -- |
 * | 195 | **0** | 2 | 3 | 4 |
 *
 * Every park anyone is still reading answers within 48 hours, and the band
 * between two days and a month is empty. Thirty sits in the middle of that gap
 * at fifteen times the observed maximum, so a seasonal park reopening after the
 * winter clears it with its first poll and no ordinary outage reaches it.
 *
 * `ParkIntegrationService` reads it; the rides of a silent park go to UNKNOWN,
 * the same place a park with no readable source sends them, and never reach the
 * fallback below. Free-flow rides are the exception, as they are everywhere
 * else: a playground opens on a curated flag and the park's schedule, neither
 * of which is a wait time.
 */
export const PARK_FEED_SILENT_DAYS = 30;

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
