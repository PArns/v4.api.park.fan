import { transliterate } from "transliteration";

/**
 * Reduces a ride name to its bare letters and digits.
 *
 * Deliberately stricter than the shared `normalizeForMatching`: sources
 * disagree on separators for the same ride ("Spider-Man" vs "Spider Man"),
 * and that helper also leaves a literal "(r)" behind because it strips
 * trademark symbols only after transliteration has already expanded them.
 */
function normalizeName(name: string): string {
  return transliterate(name.replace(/[®™©℠]/g, ""))
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

export interface AttractionMatchCandidate {
  id: string;
  externalId: string | null;
  slug: string | null;
  name?: string | null;
  queueTimesEntityId?: string | null;
}

export interface IncomingAttraction {
  externalId: string;
  name: string;
  queueTimesEntityId?: string | null;
}

/**
 * Finds the existing row an incoming attraction belongs to.
 *
 * `externalId` is source-scoped — Queue-Times reports "qt-ride-12979" for the
 * same physical ride that ThemeParks.wiki reports as a UUID. Matching on it
 * alone means a ride carried by two sources becomes two rows, the second one
 * taking a "-2" slug. That is the origin of every duplicate attraction pair
 * in the database, and of the duplicate parks at the level above.
 *
 * Order matters: identity first, then the cross-source ID, then the name.
 * The name is the weakest signal — parks legitimately have five rides called
 * "Restroom" — so it is only consulted when no ID lines up.
 */
export function findExistingAttraction(
  incoming: IncomingAttraction,
  candidates: AttractionMatchCandidate[],
): AttractionMatchCandidate | null {
  const byExternalId = candidates.find(
    (c) => c.externalId && c.externalId === incoming.externalId,
  );
  if (byExternalId) return byExternalId;

  if (incoming.queueTimesEntityId) {
    const byQueueTimesId = candidates.find(
      (c) => c.queueTimesEntityId === incoming.queueTimesEntityId,
    );
    if (byQueueTimesId) return byQueueTimesId;
  }

  const normalizedIncoming = normalizeName(incoming.name);
  const byName = candidates.find(
    (c) => c.name && normalizeName(c.name) === normalizedIncoming,
  );
  return byName ?? null;
}
