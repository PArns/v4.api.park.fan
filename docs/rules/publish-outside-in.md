# Publish a write outside-in

A write to park or attraction data is not published until three caches have let go of the old
payload, and they have to be dealt with from the inside out. Open this before writing a job,
endpoint or one-off that changes what a park or ride page shows.

## The rule

1. **Write** the data (a curated column, never the sync-owned one).
2. **Evict our Redis** for every touched park: `invalidateParkCaches(redis, parkId, attractionIds)`
   (`src/common/cache/park-cache-invalidation.ts`).
3. **Tell the frontend** to revalidate: `RevalidationService.revalidateTags(["parks", "attractions"])`.
4. **Tell it again after `CDN_SETTLE_MS` (16 min)**, because the Cloudflare copy
   (`max-age=300` + `stale-while-revalidate=600`) cannot be purged from this service.

Doing 3 before 2 publishes nothing: the frontend refetches the pre-write payload, still warm in
Redis or at the edge, and pins it in its own data cache for 24 h. That is how a curated ride profile
was written, announced, and still missing from the ride page the next morning. Skipping 4 lets the
re-fetch land on an edge copy cached minutes before the write.

Reference implementations: `CuratedDataProcessor.publish()` and `AdminCurationService`. A write
outside these paths (a raw `UPDATE`, a script) skips all of it: say so, and either evict or state the
TTL until it shows ([production one-offs](automated-retirement-and-identity.md#7-production-one-offs-run-the-deployed-code-from-a-clean-checkout)).

Why the order is what it is, and the park open/close webhook that applies it every five minutes:
[Caching strategy](../architecture/caching-strategy.md#publishing-order).
