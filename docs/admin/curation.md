# Curation

## The rule

**Two writers, no shared cell.** A sync owns its column and overwrites it on
every run. A human owns a parallel column. Reads merge them, curated first.

Put a correction in a synced column and the next job run silently reverts it —
which is not a hypothetical: it is why `curated_may_get_wet` and
`curated_minimum_height` were the first two to exist.

## What is curated

### Attractions

| Curated column | Corrects | Written by |
| --- | --- | --- |
| `curated_name` | `name` | ThemeParks.wiki sync |
| `curated_land_name` | `land_name` | Queue-Times sync |
| `curated_attraction_type` | `attraction_type` | sync |
| `curated_minimum_height` | `minimum_height` | children-metadata / six-flags-heights |
| `curated_maximum_height` | `maximum_height` | children-metadata |
| `curated_may_get_wet` | `may_get_wet` | children-metadata |
| `curated_is_seasonal` | `is_seasonal` | the nightly `detect-seasonal` job |
| `curated_season_months` | `season_months` | `detect-seasonal` |

Human-only, with no sync behind them: `has_single_rider`, `open_with_park`,
`rcdb_id`, `retired_at` / `retired_reason`, and the whole
`attraction_ride_profiles` table except its `stats` column.

### Parks

`curated_name`, `curated_park_type`, `curated_no_wait_times_reason`,
`curation_note`. Before this there were none at all — the only park-level
curation was a hardcoded list in `live-wait-time-sources.ts`.

## Reading

Never inline. `resolveCuratedFacts()` for attractions,
`resolveCuratedPark()` for parks. Both rules were already copied into two DTO
mappers once, drifted, and shipped a bug.

Three of the merges are more than a `??`:

**Heights.** `0` means "there is no minimum at all" — a correction overriding
an upstream number with nothing — not a 0 cm limit. Phantasialand's Winni Splash
is the worked example: the wiki publishes 100, while the park's own conditions
say children under 1.00 m may play *when accompanied*, which is no minimum.
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
row. The difference is what the park is *doing*, which exists in no feed we
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
