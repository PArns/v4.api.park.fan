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
 *
 * And even then it must not claim a row that already has an identity from the
 * SAME source. A rename upstream is exactly the case: when Sea World's entity
 * 5a4ad529 changed from "Castaway Bay - Sky Climb" to "Wally the Walrus", the
 * incoming name no longer matched its own row, matched the neighbouring
 * "Wally the Walrus" row instead — and overwrote that neighbour's name with
 * the same string, leaving two rows called "Wally the Walrus" and one ride's
 * identity lost. The same shape produced "Wahoo Racer" twice at Hurricane
 * Harbor Arlington and "Discovery Bay" twice in New Jersey.
 *
 * A row carrying its own wiki UUID has already been claimed by the wiki. Only
 * rows with no id from this source, or none at all, may be matched by name.
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
  const incomingSource = sourceOf(incoming.externalId);
  const byName = candidates.find(
    (c) =>
      c.name &&
      normalizeName(c.name) === normalizedIncoming &&
      // Free to claim only if this row does not already answer to another id
      // from the same source. Otherwise a rename hands one ride's row to its
      // neighbour and both end up with the same name.
      !(c.externalId && sourceOf(c.externalId) === incomingSource),
  );
  return byName ?? null;
}

/** Which upstream issued an id. Queue-Times ids carry a `qt-ride-` prefix. */
function sourceOf(externalId: string): "queue-times" | "wiki" {
  return externalId.startsWith("qt-ride-") ? "queue-times" : "wiki";
}
