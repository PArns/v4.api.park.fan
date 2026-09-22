# Curation

## The rule

**Two writers, no shared cell.** A sync owns its column and overwrites it on
every run. A human owns a parallel column. Reads merge them, curated first.

Put a correction in a synced column and the next job run silently reverts it —
which is not a hypothetical: it is why `curated_may_get_wet` and
`curated_minimum_height` were the first two to exist.

## What is curated

### Attractions

| Curated column            | Corrects          | Written by                            |
| ------------------------- | ----------------- | ------------------------------------- |
| `curated_name`            | `name`            | ThemeParks.wiki sync                  |
| `curated_land_name`       | `land_name`       | Queue-Times sync                      |
| `curated_attraction_type` | `attraction_type` | sync                                  |
| `curated_minimum_height`  | `minimum_height`  | children-metadata / six-flags-heights |
| `curated_maximum_height`  | `maximum_height`  | children-metadata                     |
| `curated_may_get_wet`     | `may_get_wet`     | children-metadata                     |
| `curated_is_seasonal`     | `is_seasonal`     | the nightly `detect-seasonal` job     |
| `curated_season_months`   | `season_months`   | `detect-seasonal`                     |

Human-only, with no sync behind them: `attraction_kind`, `has_single_rider`,
`has_virtual_line`, `open_with_park`, `rcdb_id`, `retired_at` / `retired_reason`, the three
fast-pass columns below, and the whole `attraction_ride_profiles` table except
its `stats` column.

`attraction_kind` is the odd one in that list, because it sits next to a synced
column it does **not** correct. `attraction_type` is the feed's free text —
empty in practice, one value across ~7,400 rows — and `attraction_kind` is a
closed set we decide: `RIDE`, `TRANSPORT`, `SHOW`, `WALKTHROUGH`. Both stay, so
an editor can record that Queue-Times calls something a "Family Ride" and that
it is in fact a railway. It is also the first enum on the attraction half; every
other one describes a park.

#### `has_virtual_line` and its seed

The column answers "does this ride work by return time or boarding group at
all", which is not the same question the `queues` array answers and cannot be
read off it: a virtual line handing out nothing at this moment publishes no
queue, so the ride reads as plain CLOSED. Efteling's Danse Macabre is the
reported case. The live badge built from `RETURN_TIME` / `BOARDING_GROUP` stays
where it is — one is the ride's layout, the other is today's reading of it.

Seeded like `has_single_rider`, from the rides that have ever reported one. This
has not been run yet; it is written down here so that the run is reviewable
rather than reconstructed afterwards.

**It cannot run before the change that adds the column is deployed.** The repo
has no migrations, so `has_virtual_line` appears when TypeORM's `synchronize`
next connects — production runs with `DB_SYNCHRONIZE=true`, confirmed on the
running container on 2026-09-22, despite `docs/deployment/coolify.md` telling
you to set it to `false` (PAR-399). Adding the column by hand instead is not an
option worth taking: a schema change outside the deploy path buys nothing that
the next container start does not.

**Counted against production on 2026-09-22, the three types move 140
attractions** — 139 of them not retired, out of 7,301 that have any `queue_data`
row at all. The whole pass takes **14.5 s**. Two of those numbers are worth
keeping:

- `VIRTUAL_QUEUE` matches **zero** rows in the entire history. Nothing has ever
  been written with it, which is the other half of the dead-branch finding: the
  switch ignored the value and no payload ever carried it. `RETURN_TIME` alone
  carries the seed; `BOARDING_GROUP` matches 2 rides, both of which report
  `RETURN_TIME` as well, so the union stays 140.
- `SINGLE_RIDER` matches **48**, the same count `has_single_rider` was seeded
  from months ago (`docs/changelog.md:3164`). The method reproduces its own
  earlier result.

Efteling's Danse Macabre — the reported case, and the reason to check before
running rather than after — reports `RETURN_TIME` across 36,076 rows and is in
the 140.

```sql
-- One pass over queue_data, not a correlated lookup per attraction. See below.
CREATE TEMP TABLE virtual_line_rides AS
SELECT DISTINCT qd."attractionId" AS id
FROM queue_data qd
WHERE qd."queueType"::text IN ('RETURN_TIME', 'BOARDING_GROUP', 'VIRTUAL_QUEUE');

UPDATE attractions a
SET has_virtual_line = true
WHERE a.has_virtual_line IS NULL
  AND a.id::text IN (SELECT id FROM virtual_line_rides);
```

Four things in it are load-bearing, and the first two were got wrong before they
were got right.

**`::text` on the enum column, not a bare literal.** `queue_data."queueType"` is
a Postgres enum type. `'VIRTUAL_QUEUE'` is a member of the TypeScript enum, but
it only exists in the database type if `synchronize` has run the `ALTER TYPE`
since it was added — and until this change nothing ever wrote that value. On a
database whose type predates it, comparing a bare literal does not merely fail
to match: it aborts the whole statement with
`invalid input value for enum queue_data_queuetype_enum: "VIRTUAL_QUEUE"`.
Casting to text removes the dependency and still matches every real row.
Production is not such a database — checked on 2026-09-22, its enum type already
carries the label, because `synchronize` ran the `ALTER TYPE` when the value was
added to the TypeScript enum, long before anything tried to write it. The cast
stays anyway: it costs nothing and it is what makes the statement safe to paste
into any other instance.

**`has_virtual_line IS NULL`** is what makes the statement safe to run twice.
Without it, a re-run overwrites an editor's hand-written `false` — the ride whose
feed once published a return time and whose park has since stopped running one —
with `true`.

**One pass, not `EXISTS` per row.** `queue_data` is a hypertable that compresses
after 30 days with no `compress_segmentby` (`src/database/hypertables.ts`), and
nothing prunes it, so "has ever reported" reaches back through every compressed
chunk. A correlated `EXISTS` asks that question once per attraction, against an
index that does not exist on a compressed chunk. The `DISTINCT` pass asks it
once in total. Run it in a quiet window either way.

**The quoted identifiers are the physical names.** Neither `attractionId` nor
`queueType` declares a `name:` on the entity, so TypeORM stores them camelCase
while `has_virtual_line` is snake_case; an unquoted identifier folds to lower
case and does not exist.

It seeds `true` only. A ride with no such row is left null — "nobody looked",
which is what the API serves and what the ride page must not render as "no
virtual line".

**`PAID_RETURN_TIME` is deliberately not in the list, and that is a decision
somebody has to confirm.** PAR-385 named it alongside the other three. It is a
return window, so on the wording it belongs; but it is the _paid_ one — Genie+,
Lightning Lane, Express — and that product already has three columns of its own
(`has_fast_pass`, `fast_pass_name`, `fast_pass_price`). Including it would make
the column mean "has a return window of some kind" rather than "you join this
instead of standing in the queue", which is what its own docstring and the ride
page's badge claim. The three above are unambiguous; adding the fourth is one
statement more and is left until the question is answered:

```sql
-- Only if PAID_RETURN_TIME should count. Check the number it would move first.
SELECT count(DISTINCT qd."attractionId")
FROM queue_data qd
WHERE qd."queueType"::text = 'PAID_RETURN_TIME';
```

**Measured on 2026-09-22, that is 55 rides, of which 36 report none of the other
three** — so the fourth type would take the seed from 140 to 176. Every one of
the 36 is in a Disney park (Disneyland Paris 12, Disney Adventure World 8, Tokyo
DisneySea 7, Tokyo Disneyland 3, and one or two each in Magic Kingdom, DCA,
Animal Kingdom, Hollywood Studios and EPCOT); none is at Universal.

The list of names is the argument, not the count. It holds
`"it's a small world"`, `Pirates of the Caribbean`, `Big Thunder Mountain`,
`Peter Pan's Flight`, `Phantom Manor`, `Orbitron®` and `Autopia` — rides with an
ordinary standby queue that additionally sell a return window, not rides you
board by return time. That is the distinction the column is for.

One thing cuts the other way and belongs in the same breath: all 36 have
`has_fast_pass` **NULL**. The fast-pass columns this argument defers to are not
populated for these rides, so leaving `PAID_RETURN_TIME` out moves the fact into
an empty field rather than the right one. That gap is PAR-386's to audit, not
this seed's to paper over.

### Parks

Corrections to a synced column: `curated_name`, `curated_park_type`. Plus
`curated_no_wait_times_reason`, `curated_uses_twelve_hour_clock` and the
internal `curation_note`. Before this there were none at all — the only
park-level curation was a hardcoded list in `live-wait-time-sources.ts`.

**`curated_uses_twelve_hour_clock` is the odd one**, and worth knowing about
before somebody looks for it in a payload: it changes what is _ingested_, not
what is served. A source that publishes a 12-hour clock unlabelled reports a
midnight close as `12:00`, and `normalizeClosingTime` cannot repair that — it
trusts the time-of-day and fixes the date, while here the time-of-day is the
part that is wrong. The only signal separating it from a park that genuinely
closes at noon is the closing falling _before_ the opening, which the
re-anchoring consumes. So the flag runs first, on the raw pair, and only for a
park somebody has written it on: see `correctTwelveHourClockClose`
(`src/common/utils/operating-window.util.ts`).

Set it from the audit query, never on a hunch — a blanket "reinterpret 12:00"
would rewrite every legitimate noon closing in the catalogue, and water parks
and Christmas markets have those:

```sql
SELECT p.name, s.date,
       (s."openingTime" AT TIME ZONE p.timezone)::time AS opens,
       (s."closingTime" AT TIME ZONE p.timezone)::time AS closes
FROM schedule_entries s JOIN parks p ON p.id = s."parkId"
WHERE (s."closingTime" AT TIME ZONE p.timezone)::time = '12:00:00'
  AND (s."closingTime" AT TIME ZONE p.timezone)::date
      > (s."openingTime" AT TIME ZONE p.timezone)::date;
```

Measured against production on 2026-09-11 that returns **five rows in one park**
— Six Flags Qiddiya City, 2026-04-17 through 05-15, stored as a 21-hour day.

**It finds candidates, not cases, and the difference can cost you a curation.**
The query reads _stored_ rows, which are post-normalization, and two different
raw shapes land on an identical stored row:

| What the source sent                    | What `normalizeClosingTime` did              | Stored | Flag fires |
| --------------------------------------- | -------------------------------------------- | ------ | ---------- |
| close `12:00` on the **opening's** date | before opening → re-anchored, rolled forward | 21 h   | **yes**    |
| close `12:00` on the **next** date      | nothing — 21 h is a plausible window         | 21 h   | **no**     |

`correctTwelveHourClockClose` needs the raw closing to precede the raw opening,
and normalization has already overwritten the one value that says whether it
did. Written on the second kind, the column does nothing at all, silently: the
next sync rewrites the same 21-hour day and no log line mentions it.

So before flagging a _new_ park, read the source's own payload for one of the
days (`https://api.themeparks.wiki/v1/entity/{externalId}/schedule`) and look at
the closing's **date**, not only its time. Afterwards, confirm against the next
sync that the row actually moved. For Qiddiya the raw shape is on record —
`opens 15:00 / closes 12:00`, same date, from the 2026-07-27 sweep in `todo.md`
— which is the first kind.

The flag does not rewrite stored rows either way; it changes what the next sync
of that park writes.

### Fast passes, across two rows

Nothing we ingest publishes queue-jump products, so every part is hand-written —
and the parts sit on different rows on purpose.

| Column                            | Row        | Holds                                     |
| --------------------------------- | ---------- | ----------------------------------------- |
| `parks.curated_fast_pass_name`    | park       | The brand: QuickPass, Express Pass        |
| `parks.curated_currency`          | park       | ISO-4217, what the prices are quoted in   |
| `parks.curated_fast_pass_term_id` | park       | The glossary entry explaining the product |
| `attractions.has_fast_pass`       | attraction | Whether this ride sells one               |
| `attractions.fast_pass_name`      | attraction | Override, for the one ride named apart    |
| `attractions.fast_pass_price`     | attraction | What it costs on this ride                |

The name is on the park because it is a brand. Phantasialand sells QuickPass
across the whole park; per ride it would be typed forty times and drift into
"Quick Pass" on the eleventh. The override exists for the resort that really does
sell a differently-named product on one ride — Disney's Lightning Lane Single
Pass beside the Multi Pass — and both still point at one glossary term, because
they are two labels over one idea.

Read through `resolveFastPass(attraction, park)`. It is the only merge that
reaches across two entities, which is exactly why it is a function.

**The price has three states.** Empty is unknown, and stays empty where the
operator prices per day: a frozen Lightning Lane price would be wrong most days,
and the haken without a number is the honest answer there. Zero is **free** — a
claim, not a missing value, the same reading a curated height of 0 gets.
Europa-Park's Virtual Line is the worked example: a queue-jump product included
with admission. A positive price needs a currency and is withheld without one,
because a bare "12" on a ride page is not a price; free needs none.

**A curated `false` never reaches a visitor.** `has_fast_pass = false` means
somebody checked and the park sells none, which is worth recording so the next
editor does not search again — but the payload treats it exactly like `null`.
Publishing "kein QuickPass" would be our own bookkeeping served as the park's
statement, which is the rule in CLAUDE.md §4.

**The term id is checked on write.** It is a frontend glossary id and nothing in
this database can validate one; a wrong id does not error anywhere, the chip just
links nowhere. `GlossaryTermIdsService` fetches the published list and both
curation paths — ride profiles and this one — check against it. An unreachable
frontend skips the check rather than blocking the write.

### Park facts nothing syncs

Eleven columns with one writer and nothing to merge, because no feed states any
of them:

| Group   | Columns                                                                                                                                   |
| ------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| Links   | `curated_website`, `curated_tickets_url`, `curated_wikipedia_url`, `curated_instagram_url`, `curated_facebook_url`, `curated_youtube_url` |
| Contact | `curated_street_address`, `curated_postal_code`, `curated_phone`                                                                          |
| Facts   | `curated_opened_year`, `curated_area_hectares`                                                                                            |

They reach the frontend as one `info` object on the **park detail** payload
(`resolveParkInfo`), and the object is `null` — not an object of nulls — until
somebody has written at least one of them. Never on the listings: the card
overlay re-downloads its fields every five minutes and a postal code has no
business in that budget.

Two decisions worth keeping:

- **One website, not one per locale.** Most parks answer their own domain in
  the visitor's language and the rest are a redirect away; six columns nobody
  fills for 212 parks is a worse trade than one that is occasionally in the
  wrong language.
- **A URL is parsed, not pattern-matched.** `coerce` runs it through `new URL()`
  and accepts `http:` and `https:` only. These values become `href`s on a public
  page, so a stored `javascript:` URL would be cross-site scripting with an
  audit row naming the curator who typed it.

## Reading

Never inline. `resolveCuratedFacts()` for attractions,
`resolveCuratedPark()` for parks. Both rules were already copied into two DTO
mappers once, drifted, and shipped a bug.

Three of the merges are more than a `??`:

**Heights.** `0` means "there is no minimum at all" — a correction overriding
an upstream number with nothing — not a 0 cm limit. Phantasialand's Winni Splash
is the worked example: the wiki publishes 100, while the park's own conditions
say children under 1.00 m may play _when accompanied_, which is no minimum.
Treating 0 as falsy silently restores the wrong upstream number.

**The unit.** `minimum_height_unit` describes a number, so it must never be
emitted next to a null height — a bare "cm" renders on the ride page. When
curation supplies a height the sync never saw, nothing recorded a unit either,
and every curated figure so far is the metric one off the park's own sign, so it
defaults to `cm`.

**Seasonality resolves as a pair.** A curated `false` takes the months down with
it. Otherwise the API serves a ride it just called not-seasonal, carrying a list
of the months it operates in.

## Why seasonality needed a correction at all

`detect-seasonal` runs nightly and is behavioural: it reports what the feed has
been doing. That is the right default and it is wrong in two recurring ways.

A ride closed for a year-long refurbishment looks **exactly** like a seasonal one
from outside. And a genuinely seasonal ride in a park we have watched for under
330 days gets flagged with no months at all — the detector refuses to derive
months from a recording window, a guard that cleaned up 340 attractions and 954
shows on 2026-08-15 and must stay.

Neither could be fixed in the detector's own columns, because it rewrites them
every night.

## Names are display names

`curated_name` does **not** regenerate the slug.

For attractions this is not a preference: their URLs are indexed, linked from
blog posts and stored in the media sidecars, and unlike parks there is no
`attraction_slug_aliases` table. A slug change is a permanent 404 with nothing
recording where the ride went.

For parks, changing the address is `ParkRenameService`'s job — it writes a
`park_slug_aliases` row so the old path keeps redirecting. Renaming for display
and renaming the address are different decisions and stay different operations.

**Timezone is deliberately not curated.** `timezone` is read at 206 call sites —
every schedule, every park-local date, the whole seasonality month derivation. A
curated column resolved only in the DTO would show the right zone on the park
page while the calendar underneath stayed wrong, which is worse than not
offering the correction.

## Writing

Through `AdminCurationService`, which owns the part that is easy to get wrong.
A curation is four steps and the order of the last three is load-bearing:

1. write the curated column (never the sync-owned one),
2. evict our own Redis entries for the park,
3. tell the frontend to revalidate,
4. tell it again after `CDN_SETTLE_MS` (16 min).

Doing 3 before 2 does not publish the change: the frontend refetches the
pre-write payload, still warm in Redis and at the edge, and pins it in its own
data cache for 24 hours. That is how a curated ride profile could be written,
announced, and still missing from the ride page the next morning.

A patch that changes nothing writes nothing — no save, no eviction, no audit
row. A form that PATCHes every field on every blur would otherwise fill the log
with empty edits and bury the real ones.

### Editing a whole park's list at once

`PATCH /v1/admin/content/parks/:id/attractions` takes one entry per ride. It
exists for the fast-pass table: which rides sell a pass is a decision taken
across the whole list, and Phantasialand has forty rides. Forty single PATCHes
would each fire a revalidation webhook at the frontend for one edit.

Only step 4's siblings are hoisted. Each ride keeps its own diff and its own
audit row, so undo still works one ride at a time; the eviction, the
revalidation and the delayed sweep happen once for the batch. A ride from
another park in the list is rejected rather than written — a bulk edit stays
inside the park it was opened from.

### Ride profiles

`attraction_ride_profiles` had no write path at all. Rows were edited with
hand-written SQL, matched on `parks.slug` **and** `attractions.slug` together
(park slugs are not globally unique — `disneyland-park` exists in Anaheim and in
Paris), with `seeded_at = now()` remembered by the person typing it. Forgetting
that column meant the correction was written, correct, and invisible:
`findCuratedSince` uses it as the sole marker for which caches to evict.

Two things move into code. The stamp, so it cannot be forgotten. And the term-id
check, which used to happen the next morning in a nightly audit — the ids are
frontend glossary ids that nothing in this database validates, and a wrong one
does not error, it just makes the ride's layout walkthrough come out shorter.

`elements` is the ride's layout **in ride order**. Repeats are intentional.
Never sort it, never dedupe it.

## Seasons

`park_seasons` is a table because it cannot be derived. `schedule_entries`
already knows when a park is open and is no help: a park running Halloween
Fright Nights and a park having a normal late-closing Saturday produce the same
row. The difference is what the park is _doing_, which exists in no feed we
ingest — until now it existed only in prose, in the Halloween guide, in six
languages.

`dates` sits next to `startDate`/`endDate` because a season is very often not a
range. Walibi Holland's 2026 calendar: Spooky Days on 14, 15, 19, 20 and 21
October; Fright Nights on every weekend between 3 October and 1 November plus
three single dates. Stored as 3 Oct – 1 Nov, that tells a visitor the park is
haunted on a Tuesday.

`dates: null` means "every day between the bounds" and must stay distinguishable
from an empty array, which would mean "runs on no day at all" — a state the
column has no business representing. The endpoint rejects it.

Read publicly at `GET /v1/parks/:continent/:country/:city/:park/seasons`,
deliberately its own request rather than a field on the park payload: that one
is fetched for every park page and re-polled every five minutes, while seasons
change a few times a year.

## Two things the editor has to know about a column

**What "nothing decided" looks like.** Almost every curated column is nullable
and empty means nothing was decided. `open_with_park` is `boolean NOT NULL
DEFAULT false`, so its "nothing decided" is `false` — clearing the field writes
that, not null, and a stored `false` is not an override. Getting the first wrong
is an UPDATE the database rejects; getting the second wrong put a "curated"
badge on every attraction in the catalogue. The field descriptor carries
`defaultValue` so the editor does not have to know which columns are which.

**That a projection must select it.** `resolveCuratedPark` and
`resolveCuratedFacts` read properties off an entity. Handed a row from a query
whose `select` omits the curated columns, they read `undefined`, fall back to
the synced value and report nothing — a silent no-op. Discovery's geo structure,
the country summary, the `/nearby` coordinate index and three of the search
projections each needed the columns added for exactly this reason, and the geo
structure is cached for 24 hours, so the no-op would have outlived several
curation sessions.

## Undo

An undo refuses when the field has moved on. Editor A sets a height null→120,
editor B corrects it 120→140; undoing A's entry without the check writes null
and silently discards B's work, leaving B's entry standing in the log as though
it were current. The check is "is this field still where that entry left it",
which is exactly what an undo is entitled to assume.

## Merges

Every `curated_*` column must be on one of
`AttractionMergeService`'s three inheritance lists, and keep being on it. A merge
deletes the losing row, so a curation that lived only there is gone with no
trace and nothing to notice it by — the value simply reverts to whatever the
sync last wrote, months after anybody remembers deciding otherwise.
`previewMerge` does not cover the gap: it lists `inheritedColumns` and a
`droppedCurations` that reaches `attraction_ride_profiles` and nothing else.

**`INHERITABLE_COLUMNS` is column by column**, and that is the right shape for
most of them: the winner takes whatever it lacks and the loser holds.

**`INHERITABLE_COLUMN_SETS` moves a group together or not at all**, and the
works period is the group it was built for (PAR-297). `curated_out_of_service_from`,
`_to` and `_to_uncertain` are three columns holding one statement, with two
rules guarding it on the way in: `AdminCurationService` refuses an end before
its start, and clears "that end is only an estimate" whenever the end goes away.
Column by column breaks both — a winner holding a start and no end would take
the loser's end and carry a window that ends before it begins, and since PAR-287
that window is served as `worksPeriod`. As a set the winner inherits the columns
the loser holds (an open-ended window is one or two of them) and only when it
holds none of the three itself — all three for a window with both dates and an
estimate flag, one for the ordinary open-ended one.

The window is then asked, on the way out, what the endpoint asks of one being
typed. It has to be: these columns are reachable by hand, and unlike a value the
merge merely reads, this one lands on a row that outlives it. A window stating
nothing — no date, only the estimate flag — stays behind, and so does an
inverted one, which is exactly the state the set exists to prevent and no better
for having arrived whole. A stale estimate flag is dropped instead of sinking
the dates beside it, which is the endpoint's own normalisation: the dates are a
real curation and the only copy of one. `worksPeriodEndsBeforeItBegins` is
shared between the two writers rather than written twice — the `??` rules in
`resolveCuratedFacts` were copied into two DTO mappers once, drifted, and
shipped a bug.

**A window held back is still a silent loss**, and `previewMerge` does not name
it: where the winner already holds part of a window, the loser's disappears
without appearing in `inheritedColumns` or `droppedCurations`. That is PAR-301.
Usually it is the smaller loss, because the survivor keeps a window of its own —
but not always: a winner holding nothing but a stale `to_uncertain` blocks the
set and has no window at all, `resolveWorksPeriod` serving `null` for it.

`curated_is_seasonal` / `curated_season_months` deliberately stay column by
column although `resolveCuratedFacts` resolves them as a pair. A winner curated
seasonal but without months should take the loser's months, and a set would
refuse. The one crossed pair column-by-column can build — a curated `false`
beside inherited months — never reaches a reader, because the resolver drops the
months whenever the resolved seasonality is false.

**`INHERITABLE_DEFAULTED_COLUMNS` is for a column whose "nothing decided" is a
value**, and `open_with_park` is the only one. It is `NOT NULL DEFAULT false`,
so the winner's cell is never empty and the absent-value test the other two
lists rest on can never fire — the column sat off both lists until PAR-300, and
a merge whose loser was marked free-flow and whose winner held the default threw
the statement away silently. The entry names the column and the value that means
nobody said anything, and the rule reads: take the loser's value when the winner
is sitting on that value and the loser is not.

Reading a stored `false` as "nobody said" is not a decision this list invents.
It is the one the editor has run on since the `defaultValue` field existed — see
"Two things the editor has to know about a column" above — so the spec holds the
`unset` here against the descriptor's `defaultValue` rather than letting the two
drift, and refuses a column on this list that has no `defaultValue` at all.

**The rule is one-directional.** A loser's `false` never clears a winner's
`true`: a `false` is the absence of a statement, and an absence may not unset
one ([absent facts](../rules/absent-facts.md)).

Measured against production on 2026-09-18, before the change: of the 47 pairs
`findDuplicatePairs` offers, **none** carries `open_with_park` on either side,
so the automatic path was losing nothing on the day this was built. The pair it
guards is the hand-named one — the merge endpoint takes a `winnerId`/`loserId`
of its caller's choosing and never consults that list. 34 of 7 355 non-retired
attractions are marked free-flow, and 30 of those 34 are fed by a live source,
so a free-flow row is not systematically the loser of `chooseDuplicateWinner`
either.

**A spec holds the descriptors against the lists** (`curated-field.spec-list.spec.ts`),
and it has one exception left. Against the admin's figure it is the bulk-filled
four — `has_single_rider`, `has_virtual_line`, `rcdb_id`, `open_with_park`,
which are not curation.
Against the inheritance lists there is now none: every hand-editable key is
carried. No list derives itself from `ATTRACTION_CURATED_FIELDS`, which is how
the works period's dates sat off all of them from 2026-09-06 to 2026-09-17
without anything failing.

## Related

- `docs/admin/authentication.md` — who is allowed to write any of this
- `docs/architecture/attraction-status-and-seasonality.md` — what the detector does
- frontend: `docs/features/admin.md`
