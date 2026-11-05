# Identity: the `externalId`, never the slug

Which row is which ride, why a slug that does not match its name is normal, and which row of a name
group the park payload and the sitemap keep. Open this before keying anything on a slug or a name.
The standing rule: [Automated retirement and upstream identity](../../rules/automated-retirement-and-identity.md).

## A slug is a frozen name

When a ride is renamed the sync updates `name` and leaves `slug` alone so the public URL survives:
281 rows carry a slug that no longer matches their name, and that is the system working. On
2026-08-17 three such slugs were taken as the truth (`wahoo-racer-twisted-whizzard` on "Wahoo
Racer"); research proved no "Twisted Whizzard" and no "Discovery Bay - Treehouse" ever existed.

**Resolve identity from `externalId` against the upstream entity.** For a wiki UUID,
`GET https://api.themeparks.wiki/v1/entity/<externalId>` returns the name that row is entitled to.

## The one signature that does mean damage

Not "slug does not match name", but: **this row carries the name another row's slug says it should
have.**

```sql
WITH n AS (
  SELECT a.id, a."parkId", a.name, a.slug, p.name AS park,
         lower(regexp_replace(a.name, '[^A-Za-z0-9]', '', 'g')) AS name_norm,
         replace(regexp_replace(a.slug, '-\d+$', ''), '-', '')  AS slug_norm
    FROM attractions a JOIN parks p ON p.id = a."parkId"
   WHERE a.retired_at IS NULL AND a."externalId" NOT LIKE 'qt-ride-%'
)
SELECT a.park, a.name, a.slug AS its_slug, b.slug AS name_belongs_to
  FROM n a JOIN n b ON b."parkId" = a."parkId" AND b.id <> a.id
 WHERE a.name_norm = b.slug_norm AND a.slug_norm <> a.name_norm;
```

Four rows matched in three parks. The cause was `findExistingAttraction`'s name fallback: after an
upstream rename the new name matched the neighbour holding it, handing one ride's row to another. The
fallback now refuses rows that already answer to another id from the same source
(`attraction-match.util.ts`). The rows already handed over were not repaired; which of them are one
ride is a curation question (PAR-160 / PAR-179 / PAR-205).

## The park payload groups by name — keep it that way

The park payload serves one row per name (`deduplicateEntities` in `park-integration.service.ts`),
which is what hides the damaged rows: they collide on the name because one was handed the other's.
**Do not upgrade the key to the slug or the `externalId`.** Measured over 210 parks / 7,300 rows on
2026-09-20, name plus slug base splits exactly three groups (Hurricane Harbor Arlington "Typhoon
Twister", Sea World "Wally the Walrus", Hurricane Harbor New Jersey "Discovery Bay"), and in each the
ride the odd slug names is already served as its own row — the split only publishes three names
twice. New Jersey's two rows answer to two different upstream ids the feed names alike, so an
`externalId` key splits it too. `park-integration.dedupe.spec.ts` pins the three pairs (PAR-259).

## Which row of a name group survives

The losing row has no page (the frontend resolves a ride page from the park payload), so
`/v1/sitemap/attractions` must lose the same row. Until PAR-498 the payload preferred the OPERATING
row, then the one with coordinates — both move with the feed — and on 2026-09-26 the sitemap
advertised 48 slugs the payload did not serve, exactly the losers of 45 name groups (the frontend's
"~400 hard 404s across 6 locales").

**One rule, read by both** (`outranksNameDuplicate`, `src/common/utils/name-duplicate.util.ts`, called
by `deduplicateEntities` and `SitemapService.oneRowPerName`), using `slug` and `name` only:

1. the slug without a `-N` counter wins (a counter means `generateUniqueSlug` handed that row the
   leftover name) — settles 42 of 45 groups;
2. then the slug `generateSlug(name)` would produce;
3. then the slug ascending, so the order is total.

Both sides build the group key the same way: `resolveAttractionName` (the one `resolveCuratedFacts`
uses, so a curated name cannot split a group on one side) then `nameDuplicateKey`, which trims and
drops blank names. Nine winners changed, seven of them onto the base slug that had been 404ing.

On a change to what the sitemap contains, bump `CacheKeys.sitemapAttractions()` (one flat list, 24 h);
the park payload cache is not versioned and catches up within its own TTL.
