/**
 * Queue-Times Exclusions (attractions)
 *
 * Queue-Times mirrors whatever a park's own queue system publishes, and some
 * parks publish things that are not attractions. Those rows still carry a wait
 * time, so without a filter they are ingested as rides and then counted as
 * rides — in the park's attraction total, in occupancy, and in every crowd
 * aggregate built from "all rides in this park".
 *
 * The counterpart for the wiki is `THEMEPARKS_EXCLUSIONS`; this list is keyed
 * the same way, on the external id we mint for the source (`qt-ride-<id>`).
 *
 * Only put an entry here when the row is not an attraction at all. A duplicate
 * of a real ride belongs in a merge, not here — deleting one of a pair loses
 * its history, while a merge keeps it.
 */
export const QUEUE_TIMES_EXCLUSIONS = [
  // ── Energylandia ────────────────────────────────────────────────────────
  // The park publishes turnstile counters alongside its rides. "Licznik" is
  // Polish for counter, and "Zew/Wew" mark the outer and inner gate of the
  // Coloseo theatre and the Egypt area. The Fast Pass rows count the priority
  // gate of a ride that is already in the feed under its own name, so they
  // also double-count the ride they belong to.
  "qt-ride-11250", // 17 Teatr Coloseo Zew Licznik
  "qt-ride-11251", // 18 Teatr Coloseo Wew Licznik
  "qt-ride-11271", // 22 Egipt Zew Licznik
  "qt-ride-11254", // 23 Egipt Wew Licznik
  "qt-ride-11277", // Fast Pass Mayan Kol Licznik
  "qt-ride-11447", // Fast Pass Main Train Licznik
  "qt-ride-14583", // Fast Pass Energus Kol Licznik
  "qt-ride-14584", // Fast Pass Formula Kol Licznik
  "qt-ride-14585", // Fast Pass Anaconda Kol Licznik
  "qt-ride-14586", // Fast Pass Speed Kol Licznik
  "qt-ride-14587", // Fast Pass Tofiffee Kol Licznik
  "qt-ride-14623", // Fast Pass Spacebooster Licznik
  "qt-ride-14624", // Fast Pass Dragon Kol Licznik
  "qt-ride-14625", // Fast Pass Aztecswing Licznik
  "qt-ride-16259", // Fastpass Tsunami Drop Licz
];

const excluded = new Set(QUEUE_TIMES_EXCLUSIONS);

/** True when this Queue-Times ride id must not become an attraction. */
export function isQueueTimesExcluded(externalId: string): boolean {
  return excluded.has(externalId);
}
