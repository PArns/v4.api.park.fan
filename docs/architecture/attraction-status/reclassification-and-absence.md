# Reclassification, absence and re-issue: how the children sync retires a row

ThemeParks.wiki changes what an entity is, drops it from `/children`, re-issues it under a new id, or
moves it to another park of the resort — and the row we hold has to follow without losing history or
retiring a real ride. Open this before changing `ChildrenMetadataProcessor.handleFetchChildren`,
`syncAttraction`, `syncShow` / `syncRestaurant` or `AttractionRetirementService`. The rules these
mechanisms implement, with the incidents behind them:
[Automated retirement and upstream identity](../../rules/automated-retirement-and-identity.md).

## The undo marker

Every retirement the sync writes carries a reason from `RECLASSIFIED_UPSTREAM_REASONS`
(`RECLASSIFIED_UPSTREAM_REASON`, `ABSENT_UPSTREAM_REASON`; `attraction-retirement.service.ts`), and
`syncAttraction` lifts it the moment the wiki lists the entity as an `ATTRACTION` again. Only those
exact strings are lifted, and only with `retired_kind IS NULL`: a retirement a human entered through
`POST /v1/admin/retire-attractions` survives every run. The reason is user-facing
(`retiredReason` on the public attraction endpoint), so it reads as a sentence with its source, no
issue numbers.

`retired_kind` (PAR-997): `POST /admin/retire-attractions` takes an optional
`kind: "closed" | "reclassified"`. A stated kind wins; null derives the kind from the reason
(`retiredKindOf`), so older rows read as before. The sitemap's SQL filter is the same rule written out
(`retiredKind = 'closed' OR (retiredKind IS NULL AND <reason rule>)`); `findClosedForParkPage` and the
admin list go through `isOnParkPage` / `retiredKindOf`. Retiring again without a kind writes null;
`unretire` clears it; a merge carries the loser's kind only with the loser's `retiredAt`. "Can the
sync undo this?" is asked the same way everywhere — kind null and a sync reason — including
`detect-seasonal` step 2c and the re-issue match. Switching a row to `reclassified` is the same call
with its current `retiredAt` and `reason`.

The 17 Universal Studios Singapore rows retired by hand on 2026-09-15 carry their own reason (with
the entity URL), so the sync treats them as a human retirement and will not lift them.

The protection runs one way: `POST /admin/unretire-attraction/:id` clears the reason, so the next run
retires the row again while the wiki still calls it a show. To override the sync, correct the entity
upstream or add its id to `THEMEPARKS_EXCLUSIONS`.

## An entity changed `entityType`

`handleFetchChildren` fans `/children` out by `entityType` into separate tables; `externalId` is unique
per table, so an entity moved from `ATTRACTION` to `SHOW` got a new `shows` row and kept its
`attractions` row, which then collected reconciliation CLOSED every cycle. On 2026-09-15, in 30 days:
Universal Studios Singapore 17 rows / 11,832 reconciliation rows, Tokyo Disneyland 10, Tokyo DisneySea
5, Animal Kingdom 1, Disneyland Paris 1.

**`ATTRACTION` → `SHOW` / `RESTAURANT`.** `retireReclassifiedAttractions` runs after the show and
restaurant syncs of each park and retires the left-behind attraction row. It skips any row a second
source still fills: a `queue_times_entity_id`, or an `external_entity_mapping` row whose
`external_source` is not `themeparks-wiki` (39 attractions had a wartezeiten mapping and no
Queue-Times id on 2026-09-15). Disneyland Paris' *Mickey's PhilharMagic* is a show to the wiki and a
queueing ride to Queue-Times with live readings; retiring it is a curation decision. The test is
structural — a row's columns say what it is — and deliberately not "a genuine reading in 30 days",
which would delay every fix by a month (after the flip the wiki id resolves to the show, so the
attraction's last reading *is* the reclassification day). Find the case:

```sql
SELECT p.name AS park, count(*) AS total,
       count(*) FILTER (WHERE a.queue_times_entity_id IS NULL)     AS only_wiki,
       count(*) FILTER (WHERE a.queue_times_entity_id IS NOT NULL) AS also_queue_times
FROM attractions a
JOIN shows s ON s."externalId" = a."externalId"
JOIN parks p ON p.id = a."parkId"
WHERE a.retired_at IS NULL
GROUP BY ROLLUP (p.name);
```

**`SHOW` / `RESTAURANT` → `ATTRACTION`.** Both tables carry `retired_at` / `retired_reason` (partial
index on `retired_at IS NULL`); `retireReclassifiedChildEntities` runs straight after its twin and
writes `RECLASSIFIED_AS_ATTRACTION_REASON`, which `syncShow` / `syncRestaurant` lift. Not observed in
production (all 34 collisions on 2026-09-15 ran the other way). Differences: the way back is not
park-scoped (`syncShow` looks up by `externalId` alone, but the row comes back under its **old**
`parkId` unless moved); the second-source guard is inert (zero non-wiki mappings for shows or
restaurants on 2026-09-16, and `show_live_data` has no `data_source`); there is no retirement service,
so eviction and revalidation hang off the write.

**Shared details.** Each direction excludes ids that arrived under the other type in the same response
(`/children` listing one entity twice would otherwise flip the row every run). `WaitTimesProcessor`
builds its entity lookup with shows after attractions and skips retired rows, so an un-retired
attraction is not shadowed by its stale show — except where a second source claims the row
(`withoutForeignSourceMappings`), on purpose. Retired rows receive no readings
(`wait-times.processor.ts` loads attractions with `retiredAt: IsNull()`), which is why
`detect-seasonal` keeps the season of a sync-undoable retirement.

## An entity left `/children` (PAR-621)

Neither reclassification step sees an id that vanished altogether. 515 active rows in 83 parks were in
that state on 2026-10-02; the dead row kept the counter-free slug, won the name group
([identity](identity.md#which-row-of-a-name-group-survives)) and hid the live row (43 rides served
CLOSED, 21 at Walibi Belgium). `retireAbsentAttractions` runs after the two reclassification steps and
retires a row when all hold:

| gate | why |
| --- | --- |
| the park's `/children` call succeeded and listed at least one attraction | our feed going quiet is not the entity going away |
| the `externalId` is in the response under no type, and no row of this run claimed it | absent, not reclassified, not matched under another id |
| `absent_since` older than `ABSENT_UPSTREAM_RETIRE_DAYS` (60) | see the two columns below |
| no `queue_times_entity_id`, no mapping from another source | as above |
| no reading in 7 days other than reconciliation, `system-heartbeat` or carried heartbeats | 77 of the 515 were still measured |
| not a seasonal row (`isSeasonalRow`: `curated_is_seasonal` wins, `false` included) | a maze between two seasons is not gone (PAR-682) |

It writes `ABSENT_UPSTREAM_REASON`, so `retiredKind` is `reclassified`, the sitemap drops the URL,
`detect-seasonal` keeps the season and the sync lifts it if the id returns.

**Two columns speak about the feed, and neither is `updatedAt`.** `last_listed_at` is when the id
was last in `/children`, `absent_since` how long it has been missing; both are written raw by the
children sync alone, after the loop, so they do not move `updatedAt` (PAR-739). The absence is dated
from `last_listed_at`, falling back to `updatedAt` only for a row no sync has stamped yet.
`updatedAt` answers "who last wrote this row", which a curation sweep makes a different question.

**Seasonal rows not yet recognised** are still retired; they are listed for a human
(`findAbsenceRetiredUnreviewed`: `ABSENT_UPSTREAM_REASON`, `curated_is_seasonal IS NULL`,
`NOT is_seasonal`) as `absenceRetiredUnreviewed` on `GET /v1/admin/data-quality`, with one WARN per
park from the nightly data-quality job. Setting `curated_is_seasonal` either way answers it.

## A re-issue is not a rename

The wiki hands seasonal attractions (and once all of Walibi Belgium's rides) a **new entity id**. A
rename keeps the id; a re-issue drops the old one from `/children`. `syncAttraction` therefore passes
the ids the response carried (`listedExternalIds`): a row whose own wiki id is no longer listed may be
claimed by name (`findExistingAttraction`) and moves onto the new `externalId` plus its
`themeparks-wiki` mapping, keeping history and slug. Only active rows or rows with a sync-written
retirement qualify; a row reached through a shared Queue-Times id keeps its wiki id while that id is
listed. A seasonal row is retired anyway when a row this run claimed carries the same name (a twin
from before this change).

Re-issues under a **changed** name (`Hell House` → `Helhuis`) are listed, never merged by a machine:
`findReissueCandidates` pairs every absence-retired row with each younger live row within
`REISSUE_CANDIDATE_METERS` (30), skipping `not_a_duplicate` marks, with a `reissueNamesMatch` hint
(`GET /v1/admin/reissue-candidates`, also on `data-quality`). The admin's duplicates page merges with
`adoptLoserExternalId` (otherwise the survivor keeps its own id and the sync grows the loser back) or
dismisses with a mark. On 2026-10-04: 25 pairs, 9 with matching
names.

## One ride, two upstreams, one row

`externalId` is source-scoped and one column, so the id that loses it lives only in
`external_entity_mapping`. Since PAR-949 `syncAttraction` reads that table before inserting (a listed
id mapped onto a row of the same park stamps that row instead of growing a `-3`). Since PAR-1081 the
wiki path also writes the mapping when its name match lands on a row answering to the other upstream
(`qt-ride-…`); before, `WaitTimesProcessor` resolved such a row as `themeparks-wiki:qt-ride-…` and
dropped every wiki reading — 35 rows in two parks on 2026-10-26, 30 of them at Energylandia with no
mapping at all. The Queue-Times path has written its mapping unconditionally since PAR-311.

## A child moved to another park of the resort (PAR-743)

`attractions.externalId` is unique globally, `slug` only per park. The wiki moves children between the
parks of a resort and keeps the id, so the park-scoped lookup in `syncAttraction` found nothing and the
insert hit `UQ_a94e9dc2762dfca8a463d173657` (`syncShow` / `syncRestaurant`, looking up globally, kept
writing the old park's row). Measured 2026-10-25: 0 of 11,864 entities listed under two parks, 74
listed children whose row lay in another park of the resort. `applyParkMoves` (after the whole run)
moves such a row only within one `destinationId`, only when the row's own park's `/children` from the
same run lacks the id (a park whose fetch failed moves nothing), with a free slug in the new park and
an `attraction_slug_aliases` row for attractions. History stays on the row id; an id listed by two
parks stays put and is logged.

## One child must not cost its park the feed (PAR-714)

Each child of `/children` gets its own `try/catch` in `handleFetchChildren` (all three loops and the
Queue-Times fallback). A per-park catch around the loop ended the park at the first throw, and
everything after the loop — reclassification, absence, mappings, eviction — never ran: Knott's Soak
City wrote 1 of 9 rows per run and Cedar Point Shores 2 of 17 for 71 days, each because of one
unwritable child. A skip is logged with park, child name, upstream id and error, and counted
(`Skipped Children: N`). The park run carries `skippedExternalIds`, so a skipped id never reaches the
reclassification steps' candidate lists (without a replacement row they would retire the entity's
other row).

**Finding such a stall:** one row whose `last_listed_at` stands still among listed rows of the same
park is a skipped child; a whole park frozen is its sync or its stamp failing. Read the stamp, not
`updatedAt`, which no longer moves on an unchanged row (PAR-739).
