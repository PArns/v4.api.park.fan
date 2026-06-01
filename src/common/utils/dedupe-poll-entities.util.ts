import { EntityLiveData } from "../../external-apis/data-sources/interfaces/data-source.interface";

/**
 * Dedup live entities from a SINGLE poll response by `(source, externalId, entityType)`,
 * keeping the LAST occurrence.
 *
 * Why: a flaky upstream (observed on themeparks-wiki) occasionally returns the same entity
 * twice in one park-live response. Both would be written to `queue_data`, doubling that
 * ride's weight in the park aggregates — which once produced a phantom constant 60-61 min
 * for a walk-on ride and made a whole park read "extreme" for a month.
 *
 * What it must NOT collapse (each is a genuinely distinct reading and is preserved):
 * - the same entity reported by DIFFERENT sources (multi-source coverage),
 * - DIFFERENT entities of the same source (different externalId),
 * - the rare case of an attraction and a show/restaurant sharing an externalId
 *   (different entityType).
 *
 * Insertion order is preserved (a deduped key keeps its first-seen position, last value).
 */
export function dedupePollEntities(
  entities: EntityLiveData[] | undefined | null,
): EntityLiveData[] {
  if (!entities || entities.length === 0) return [];
  const byKey = new Map<string, EntityLiveData>();
  for (const e of entities) {
    byKey.set(`${e.source}:${e.externalId}:${e.entityType}`, e);
  }
  return Array.from(byKey.values());
}
