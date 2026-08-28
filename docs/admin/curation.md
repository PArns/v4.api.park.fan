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

Human-only, with no sync behind them: `has_single_rider`, `open_with_park`,
`rcdb_id`, `retired_at` / `retired_reason`, the three fast-pass columns below,
and the whole `attraction_ride_profiles` table except its `stats` column.

### Parks

Corrections to a synced column: `curated_name`, `curated_park_type`. Plus
`curated_no_wait_times_reason` and the internal `curation_note`. Before this
there were none at all — the only park-level curation was a hardcoded list in
`live-wait-time-sources.ts`.

### Fast passes, across two rows

Nothing we ingest publishes queue-jump products, so every part is hand-written —
and the parts sit on different rows on purpose.

| Column                                  | Row         | Holds                                    |
| --------------------------------------- | ----------- | ---------------------------------------- |
| `parks.curated_fast_pass_name`          | park        | The brand: QuickPass, Express Pass       |
| `parks.curated_currency`                | park        | ISO-4217, what the prices are quoted in  |
| `parks.curated_fast_pass_term_id`       | park        | The glossary entry explaining the product |
| `attractions.has_fast_pass`             | attraction  | Whether this ride sells one              |
| `attractions.fast_pass_name`            | attraction  | Override, for the one ride named apart   |
| `attractions.fast_pass_price`           | attraction  | What it costs on this ride               |

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

`AttractionMergeService.INHERITABLE_COLUMNS` must list **every** `curated_*`
column, and keep listing them. A merge deletes the losing row, so a curation that
lived only there is gone with no trace and nothing to notice it by — the value
simply reverts to whatever the sync last wrote, months after anybody remembers
deciding otherwise.

## Related

- `docs/admin/authentication.md` — who is allowed to write any of this
- `docs/architecture/attraction-status-and-seasonality.md` — what the detector does
- frontend: `docs/features/admin.md`
