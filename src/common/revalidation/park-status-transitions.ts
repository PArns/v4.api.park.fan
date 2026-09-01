/**
 * Which parks just opened or closed, and the frontend cache tag for each.
 *
 * Pure so it can be tested without Redis or a webhook; the service around it supplies the
 * previous snapshot and posts the tags.
 */

export type ParkOperatingStatus = "OPERATING" | "CLOSED";

/** The park columns a tag is built from. All four are nullable in the schema. */
export interface TaggablePark {
  id: string;
  slug: string | null;
  citySlug: string | null;
  countrySlug: string | null;
  continentSlug: string | null;
}

/**
 * The frontend's cache tag for ONE park's structure fetch.
 *
 * Hand-written twin of `parkCacheTag()` in the frontend (`lib/api/park-live-projection.ts`) — two
 * repos, one string, so change both halves or neither. It is the geo path rather than the slug
 * because slugs are unique per destination and not globally: `disneyland-park` is Anaheim and
 * Paris, and they do not open at the same time.
 *
 * Returns null for a park whose geocoding never completed — it has no frontend URL either, so
 * there is nothing to revalidate.
 */
export function parkCacheTag(park: TaggablePark): string | null {
  const { continentSlug, countrySlug, citySlug, slug } = park;
  if (!continentSlug || !countrySlug || !citySlug || !slug) return null;
  return `park:${continentSlug}/${countrySlug}/${citySlug}/${slug}`;
}

export interface ParkStatusDiff {
  /** Cache tags for the parks whose status changed since the previous snapshot. */
  tags: string[];
  /** The snapshot to persist for the next run. */
  nextSnapshot: Record<string, ParkOperatingStatus>;
}

/**
 * Diff the current park statuses against the previous run's snapshot.
 *
 * A park that is not in `previous` is recorded and NOT reported: an empty snapshot means the
 * first run on a cold Redis, and reading that as "all 213 parks just changed" would post the
 * whole catalogue at every restart. The cost of staying quiet is one missed transition for a park
 * that happened to open during a deploy — against a park page whose cache would otherwise be
 * dropped for no reason on every one.
 */
export function diffParkStatuses(
  parks: TaggablePark[],
  current: Map<string, ParkOperatingStatus>,
  previous: Record<string, ParkOperatingStatus>,
): ParkStatusDiff {
  const tags: string[] = [];
  const nextSnapshot: Record<string, ParkOperatingStatus> = {};

  for (const park of parks) {
    const status = current.get(park.id);
    if (!status) continue; // no reading this cycle — keep the old one rather than inventing one

    nextSnapshot[park.id] = status;

    const before = previous[park.id];
    if (!before || before === status) continue;

    const tag = parkCacheTag(park);
    if (tag) tags.push(tag);
  }

  // A park the status query skipped keeps its previous entry, so a one-cycle gap does not read as
  // a transition on the cycle after it. A park that no longer exists is dropped instead of
  // accumulating in the snapshot forever.
  const known = new Set(parks.map((p) => p.id));
  for (const [parkId, status] of Object.entries(previous)) {
    if (known.has(parkId) && !(parkId in nextSnapshot))
      nextSnapshot[parkId] = status;
  }

  return { tags, nextSnapshot };
}
