# Curation

The hand-written columns on parks and attractions: which exist, how reads merge them with the synced
ones, how the admin editor is generated, how a write is published and undone, and how merges keep them.
Open this before adding a curated column, writing curated data outside the admin, or touching a merge.
Who may write: [authentication](authentication.md). The frontend editor: its `docs/features/admin.md`.

## The rule

**Two writers, no shared cell.** A sync owns its column and overwrites it on every run; a human owns a
parallel `curated_*` column; reads merge them, curated first. A correction in a synced column is silently
reverted by the next run — which is why `curated_may_get_wet` (the wiki fills `may_get_wet` for a few
dozen of ~7,000 rides, sometimes wrongly) and `curated_minimum_height` (the wiki conflates "must be this
tall" with "needs an adult below this") were the first two. The general form:
[absent facts](../rules/absent-facts.md).

## What is curated

### Attractions

| Curated column | Corrects | Synced by |
| --- | --- | --- |
| `curated_name` | `name` | ThemeParks.wiki sync |
| `curated_land_name` | `land_name` | Queue-Times sync |
| `curated_attraction_type` | `attraction_type` | sync |
| `curated_minimum_height`, `curated_maximum_height` | `minimum_height`, `maximum_height` | children metadata / `six-flags-heights` |
| `curated_may_get_wet` | `may_get_wet` | children metadata |
| `curated_is_seasonal`, `curated_season_months` | `is_seasonal`, `season_months` | `detect-seasonal` ([seasonality](../architecture/attraction-status/seasonality.md)) |
| `attraction_ride_profiles.curated_stats` | `attraction_ride_profiles.stats` | `ride-stats` (Wikidata importer) |

Human-only, nothing syncs them: `attraction_kind` ([attraction kind](../frontend/attraction-kind.md)),
`indoor_outdoor` ([indoor/outdoor](../frontend/indoor-outdoor.md)), `has_single_rider`,
`has_virtual_line`, `open_with_park`, `rcdb_id`, the works period `curated_out_of_service_from` / `_to` /
`_to_uncertain` ([works period](../frontend/works-period.md)), `retired_at` / `retired_reason` /
`retired_kind`, the fast-pass columns below, and the rest of `attraction_ride_profiles`
([ride ↔ glossary](../frontend/ride-glossary-link.md)).

`attraction_kind` sits beside `attraction_type` without correcting it: the feed's free text (one value
across ~7,400 rows) against a closed set we decide (`RIDE`, `TRANSPORT`, `SHOW`, `WALKTHROUGH`, `MAZE`).
The one job allowed to write it, into a blank only, is the [maze rule](../architecture/attraction-status/maze-rule.md).

**`has_virtual_line`** means "this ride works by return time or boarding group at all", which the `queues`
array cannot say (a virtual line handing out nothing right now publishes no queue, so Efteling's Danse
Macabre read as plain CLOSED). It was seeded `true` from the rides that ever reported `RETURN_TIME`,
`BOARDING_GROUP` or `VIRTUAL_QUEUE` (140 rides, 2026-09-22; `VIRTUAL_QUEUE` has never been written); a ride
without one stays null — "nobody looked". **`PAID_RETURN_TIME` is not a virtual line:** it is the park's
paid queue-jump product (Genie+, Lightning Lane, Express), recorded in `has_fast_pass` (PAR-386, 36 rides
filled 2026-09-25, no price because Disney prices per day). A ride can rightly carry both (Europa-Park's
free Virtual Line: `has_virtual_line` and `has_fast_pass` with price 0). Open: 19 rides that report both a
paid window and a free type have the paid product recorded nowhere (PAR-539).

### Parks

| Group | Columns |
| --- | --- |
| Corrections of a synced column | `curated_name`, `curated_park_type` |
| Behaviour | `curated_no_wait_times_reason` ([parks we cannot read](../frontend/live-wait-times-availability.md)), `curated_uses_twelve_hour_clock` (below) |
| Early entry | `curated_has_early_entry`, `curated_early_entry_minutes_peak`, `curated_early_entry_minutes_off_peak` |
| Fast pass | `curated_fast_pass_name`, `curated_fast_pass_price_from`, `curated_currency`, `curated_fast_pass_term_id` |
| Links | `curated_website`, `curated_tickets_url`, `curated_wikipedia_url`, `curated_instagram_url`, `curated_facebook_url`, `curated_youtube_url` |
| Contact, facts | `curated_street_address`, `curated_postal_code`, `curated_phone`, `curated_opened_year`, `curated_area_hectares` |
| Grammar | `curated_article_de` (German nominative article; another language gets its own column) |
| Search names | `curated_abbreviations` (comma separated, „EP, PHL“; served as `abbreviations: string[]`, at most five of up to 12 characters, for the park page's H1, its meta description and `alternateName`; the editor refuses anything it would not serve as typed) |
| Internal | `curation_note` |

Human-only columns still carry the `curated_` prefix because the park column list, the merge and the
editor key on it.

- **Early entry** (minutes before opening, on the park's `isHeadliner` rides) reaches
  `/v1/parks/:slug/plan/day` `context` as `hasEarlyEntry`, `earlyEntryMinutesPeak`,
  `earlyEntryMinutesOffPeak`, only when the flag is `true`; `false` (checked, none) and `null` both leave
  the keys out, and non-positive minutes are dropped. Peak/off-peak have no calendar behind them: write
  what holds now (Europa-Park: 45 at an 8:15 opening, 15 at 8:30).
- **Links, contact and facts** reach the park **detail** payload as one `info` object (`resolveParkInfo`),
  `null` until one is written; never the listings (re-downloaded every five minutes). One website, not one
  per locale. A URL is parsed (`new URL()`, `http:`/`https:` only), because a stored `javascript:` URL
  would be cross-site scripting on a public page.
- **Timezone is deliberately not curated**: `timezone` is read at ~200 call sites, and a correction
  resolved only in the DTO would leave the calendar underneath wrong.

**`curated_uses_twelve_hour_clock` changes what is ingested, not what is served.** A source publishing an
unlabelled 12-hour clock reports a midnight close as `12:00`; `normalizeClosingTime` trusts the time and
fixes the date, so only the flag repairs it, on the raw pair, before normalisation
(`correctTwelveHourClockClose`, `src/common/utils/operating-window.util.ts`). Set it from evidence, never
a hunch — a blanket rule would rewrite every real noon closing (water parks, Christmas markets):

```sql
SELECT p.name, s.date,
       (s."openingTime" AT TIME ZONE p.timezone)::time AS opens,
       (s."closingTime" AT TIME ZONE p.timezone)::time AS closes
FROM schedule_entries s JOIN parks p ON p.id = s."parkId"
WHERE (s."closingTime" AT TIME ZONE p.timezone)::time = '12:00:00'
  AND (s."closingTime" AT TIME ZONE p.timezone)::date
      > (s."openingTime" AT TIME ZONE p.timezone)::date;
```

It finds candidates, not cases (2026-09-11: Six Flags Qiddiya City, five 21-hour days). Two raw shapes
store identically: a `12:00` close on the opening's date (the flag fires) and on the next date (it does
nothing, silently). Read the source's own payload
(`https://api.themeparks.wiki/v1/entity/{externalId}/schedule`) and check the closing's **date** before
flagging; confirm the next sync moved the row. Qiddiya's raw shape is on record (`opens 15:00 / closes
12:00`, same date, the 2026-07-27 sweep, PAR-33). The flag changes what the next sync writes, not stored
rows.

### Fast passes, across two rows

No feed publishes queue-jump products, and the parts sit on different rows on purpose:

| Column | Row | Holds |
| --- | --- | --- |
| `parks.curated_fast_pass_name` | park | the brand (QuickPass, Express Pass) |
| `parks.curated_fast_pass_price_from` | park | the floor of a park-wide pass ("ab 25 €") — most parks sell one pass per visit |
| `parks.curated_currency` | park | ISO 4217 |
| `parks.curated_fast_pass_term_id` | park | the glossary entry explaining the product |
| `attractions.has_fast_pass` | attraction | whether this ride sells one |
| `attractions.fast_pass_name` | attraction | override for the one ride named apart (Lightning Lane Single Pass) |
| `attractions.fast_pass_price` | attraction | the per-ride price |

Read through `resolveFastPass(attraction, park)`, the only merge across two entities. **The price has three
states:** empty is unknown (and stays empty where the operator prices per day), **0 is free** (a claim, like
a curated height of 0), a positive price needs a currency and is withheld without one. **A curated `false`
never reaches a visitor:** it records that somebody checked, but the payload treats it like `null`.
**The term id is checked on write** against the frontend's published glossary (`GlossaryTermIdsService`);
an unreachable frontend skips the check rather than blocking the write. The API contract:
[fast pass](../frontend/fast-pass.md). The Disney brands were written with PAR-386: Tokyo parks
**Disney Premier Access** (`premier-access`), the six US parks **Lightning Lane** (`lightning-lane`).

## Reading

Never inline: `resolveCuratedFacts()` for attractions (`attractions/utils/curated-attraction-facts.util.ts`),
`resolveCuratedPark()` for parks (`parks/utils/curated-park-facts.util.ts`). Both rules were once copied into two DTO mappers,
drifted, and shipped a bug. Three merges are more than a `??`:

- **Heights:** `0` means "no minimum at all" (Phantasialand's Winni Splash: the wiki says 100, the park's
  conditions say under 1.00 m only with an adult). Treating 0 as falsy restores the wrong upstream number.
- **The unit:** `minimum_height_unit` is never emitted beside a null height; a curated height the sync never
  saw defaults to `cm`.
- **Seasonality resolves as a pair:** a curated `false` takes the months down with it; non-empty curated
  months alone imply seasonal.

**A projection must select the curated columns.** The resolvers read properties off an entity; a query
whose `select` omits them reads `undefined`, falls back to the synced value and reports nothing.
Discovery's geo structure (cached 24 h), the country summary, the `/nearby` index and three search
projections each needed the columns added for this reason.

**What "nothing decided" looks like** is carried by each field's descriptor (`defaultValue`): almost
every curated column is nullable, but `open_with_park` is `boolean NOT NULL DEFAULT false`, so clearing it
writes `false` and a stored `false` is not an override (getting that wrong put a "curated" badge on every
attraction).

## The editor is generated from descriptors

`GET /v1/admin/content/fields` serves `ATTRACTION_CURATED_FIELDS` and `PARK_CURATED_FIELDS`
(`src/admin/content/curated-field.spec-list.ts`): per field the PATCH key, label, type, the synced key it
corrects (or null), the resolved key it produces, group, options, unit, bounds, hint and `defaultValue`.
Adding a curated column there makes it appear in the admin with no frontend change.
`GET /v1/admin/content/resolve` shows synced, curated and resolved values side by side.

## Writing

Through `AdminCurationService` (`PATCH /v1/admin/content/attractions/:id`, `…/parks/:id`), which publishes
in the [outside-in order](../rules/publish-outside-in.md). A raw `UPDATE` lands in the database and not
on the page, and leaves no audit row.

- **A patch that changes nothing writes nothing** — no save, no eviction, no audit row — so a form that
  PATCHes on every blur does not bury the real edits.
- **Only changed columns are written** (`update`, not `save`): `save()` wrote every loaded column, so two
  editors saving different fields reverted each other.
- **Recording a check that changed nothing** (PAR-601): `POST /v1/admin/content/parks/:id/verify` with
  `fields`, optional `reason` and a required http(s) `sourceUrl` writes one `park.verify` audit row
  (`before` = the fields as they stand, `after` null), saves nothing, evicts nothing, cannot be undone, and
  returns 500 if the audit write fails. History entries carry `kind: "change" | "verification"`; the
  dashboard's curation count (`action LIKE '%.curate'`) leaves verifications out. Attractions have no
  verify action yet.
- **A whole park's list at once:** `PATCH /v1/admin/content/parks/:id/attractions` (the fast-pass table —
  forty single PATCHes would each fire a revalidation). Each ride keeps its own diff and audit row; the
  publish happens once. Every entry is validated before any is written, so a ride from another park or an
  invalid value rejects the batch; a failure partway still publishes what was written.

### Ride profiles

There is no upstream feed and no seed file: **the `attraction_ride_profiles` rows are the source of
truth.** Write them in the admin (`/admin/attractions/<id>`, ride profile editor → `PUT` /
`DELETE /v1/admin/content/attractions/:id/ride-profile`, editor role, audited). The write checks every
term id against park.fan's `/api/glossary-term-ids` and refuses unknown ones, stamps `seeded_at` (the
marker `findCuratedSince` picks the caches to evict by) and publishes like every curated write. Never
touch `stats` / `stats_updated_at`; those belong to the Wikidata importer. What each field means,
`elements` in ride order included: [ride ↔ glossary](../frontend/ride-glossary-link.md).

A row written by hand in SQL must set `seeded_at = now()` (NOT NULL, no default) and then be published
with `POST /v1/admin/publish-ride-profiles { "sinceHours": 24 }`, which evicts and revalidates every
profile stamped in the window; without it a fix surfaces only as TTLs expire (up to 6 h Redis, 900 s
edge, a day in the frontend's data cache). Match such SQL on `parks.slug` **and** `attractions.slug`:
park slugs repeat (`disneyland-park` is Anaheim and Paris) and so do ride names ("Goliath" is six
coasters).

**Term-id audit.** `GET /v1/admin/ride-profile-term-audit` diffs every stored id against park.fan's
`/api/glossary-term-ids` and names the broken ids and the rides they shorten; it aborts rather than
reporting when the glossary answers empty. It also runs daily at 06:30 (`CuratedDataProcessor`,
`ride-profile-term-audit-cron`) and logs a warning without failing the job — the ids only break when
park.fan renames a term. **Still unguarded:** a ride deleted and re-created upstream loses its profile
silently (the FK cascades).

#### Sourcing a ride profile

Ride layouts are facts about a physical object. Assemble them from the park's attraction page, the
manufacturer's project page (Mack, Intamin, Vekoma, B&M and Gerstlauer publish element lists),
Wikipedia ride articles and on-ride footage. RCDB is a link target and a check of
manufacturer/model/year, **never a text source** — its terms permit linking, not reuse. Where sources
disagree on a figure's name, use the manufacturer's name for that installation. Add only an element
you can point at in footage or a published layout: an incomplete list is fine, a wrong one is worse
than none.

Measurements, stricter, because a number looks equally authoritative checked or not:

- Join sources on the **RCDB id**, never the name; confirm the source describes *our* installation
  (our own id pointed at a predecessor ride for four coasters).
- Found by name, the article must name the ride **and one park** — a search for
  "Winja's Force Phantasialand" returns Taron.
- Require two sources within 5 %; where they disagree, write nothing.
- Sanity: no coaster beats `sqrt(2gh)` without a launch or is taller than half its track. The admin
  rejects values outside 0–400 km/h, 0–250 m, 0–10,000 m, 0–3,600 s.

Invariants to check by hand, each a mistake that happened: no unknown term ids; a coaster's list
starts with a lift, launch or drop (Hydra's pre-lift jojo roll is the exception); no ride claims
inversions with no inverting figure in its list; **two rides with a byte-identical element list are a
lead, not a fact** — every time it was chased down, members turned out to be another model (a
"Boomerang" family coaster, an "SLC" that is an MK-900, a B&M floorless filed under a sister ride).
Verify each against its own RCDB id.

```sql
-- identical element lists shared by several rides
SELECT rp.elements, count(*), array_agg(a.slug)
  FROM attraction_ride_profiles rp JOIN attractions a ON a.id = rp."attractionId"
 WHERE jsonb_array_length(rp.elements) > 4
 GROUP BY rp.elements HAVING count(*) > 1 ORDER BY count(*) DESC;

-- rides with an RCDB id and no profile
SELECT p.slug, a.slug, a.rcdb_id
  FROM attractions a JOIN parks p ON p.id = a."parkId"
  LEFT JOIN attraction_ride_profiles rp ON rp."attractionId" = a.id
 WHERE rp."attractionId" IS NULL AND a.rcdb_id IS NOT NULL;
```

### Seasons

`park_seasons` is curated only, because nothing derives a season: a Halloween night and a late Saturday
produce the same schedule row. Editors write it through `POST /v1/admin/content/parks/:id/seasons` and
`PATCH` / `DELETE …/content/seasons/:id`; an empty `dates` list is stored as `null` (every day between
the bounds). The fields and what a client reads: [park seasons](../frontend/park-seasons.md).

## Names are display names

`curated_name` never regenerates the slug: attraction URLs are indexed, linked from posts and stored in the
frontend's media sidecars. A slug changed by hand (only for a real error: wrong language, a leftover `-2`)
needs its `attraction_slug_aliases` row in the same step, or it is a permanent 404 (the merge writes its
own):

```sql
INSERT INTO attraction_slug_aliases ("attractionId", slug)
VALUES ('<attraction id>', '<old slug>') ON CONFLICT DO NOTHING;
UPDATE attractions SET slug = '<new slug>' WHERE id = '<attraction id>';
```

For parks, changing the address is `ParkRenameService`'s job (it writes `park_slug_aliases`). The name
itself comes from the operator, never a translation
([upstream identity rule §4](../rules/automated-retirement-and-identity.md#4-names-come-from-the-operator-never-from-a-translation)).

## Undo

`POST /v1/admin/content/history/:id/undo` refuses when the field has moved on: undoing A's null→120 after
B's 120→140 would write null and silently discard B's work. The check is "is this field still where that
entry left it".

## Merges

Every `curated_*` column must be on one of `AttractionMergeService`'s inheritance lists: a merge deletes
the losing row, and a curation that lived only there reverts silently. `previewMerge` lists
`inheritedColumns` and a `droppedCurations` that covers `attraction_ride_profiles` only.

- **`INHERITABLE_COLUMNS`**, column by column: the winner takes what it lacks.
- **`INHERITABLE_COLUMN_SETS`** move a group together or not at all — built for the works period
  (`curated_out_of_service_from`, `_to`, `_to_uncertain`; PAR-297), whose two input rules
  (`AdminCurationService` refuses an end before its start and clears the estimate flag with the end) a
  column-by-column merge would break. The winner inherits the set only when it holds none of the three,
  and the merged window is checked with the same `worksPeriodEndsBeforeItBegins` the endpoint uses; a stale
  estimate flag is dropped rather than sinking the dates. A window held back is a silent loss
  `previewMerge` does not name (PAR-301).
- **`curated_is_seasonal` / `curated_season_months`** stay column by column on purpose: a winner curated
  seasonal without months should take the loser's months. The crossed pair (curated `false` beside
  inherited months) never reaches a reader, because the resolver drops months when seasonality is false.
- **`INHERITABLE_DEFAULTED_COLUMNS`** is for a column whose "nothing decided" is a value — only
  `open_with_park`: take the loser's value when the winner sits on the default and the loser does not. It
  is one-directional: a loser's `false` never clears a winner's `true` (PAR-300).

`curated-field.spec-list.spec.ts` holds the descriptors against the lists: every hand-editable key must be
carried by a merge list (the works period sat off all of them for eleven days in September 2026 without
anything failing).

## SQL traps when writing these columns by hand

From the `has_virtual_line` seed (2026-09-22), true for any statement against `queue_data`:

- `queue_data."attractionId"` is `uuid` (the entity said `text` until PAR-721); `a.id::text = …` aborts
  with `operator does not exist: text = uuid`.
- `queue_data."queueType"` is a Postgres enum: a bare literal the database type does not carry aborts the
  statement, so `::text` makes a paste-anywhere statement safe — at the cost of the
  `(queueType, status, timestamp)` index.
- Quote the camelCase physical names (`"attractionId"`, `"queueType"`); snake_case columns need no quotes.
- `queue_data` compresses after 30 days with no `segmentby`; "has ever reported" is one `DISTINCT` pass,
  never a correlated `EXISTS` per row.
- Count in one step, write in a separate one after reading the number (a count between `BEGIN` and an
  unconditional `UPDATE` is not a gate), guard the write so a re-run cannot overwrite an editor's value
  (`… IS NULL`), and group by `parks.id`, never the name or slug. Run it through `scripts/prod-psql.sh`
  ([production one-offs](../rules/automated-retirement-and-identity.md#7-production-one-offs-run-the-deployed-code-from-a-clean-checkout)).
