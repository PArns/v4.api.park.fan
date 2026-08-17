# Changelog

Notable changes to the Park Fan API. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Version and date align with releases or significant doc/code milestones.

---

## [Unreleased]

### Fixed — matching names are not enough when one source names two things alike

The auto-merge rule required only that two rows agree on name once punctuation
and a map number are stripped. Heide Park shows why that is not sufficient:
ThemeParks.wiki publishes **three separate attraction entities all called
"PLAYGROUND"**. Their names agree perfectly, so the detector offered
`playground-2` and `playground-3` as *safe* auto-merges — collapsing three real
play areas into one, irreversibly.

A duplicate arises because **two sources** describe one ride: a wiki UUID beside
a Queue-Times id. Two ids issued by the *same* source are that source's own
statement that these are two things. Auto-merge now requires the pair to span
two sources; same-source pairs go to review with a reason that says so.

Found while checking why Heide Park had three rows with one name — the detector
had been about to act on them.

### Added — somewhere to put "a human already checked this"

Duplicate detection and retirement detection are behavioural: they describe
what the feed is doing, not what is true. So every candidate they surface comes
back tomorrow, however carefully it was investigated.

That is not a small annoyance. A research round on 2026-08-16 established that
**57 of 73** silenced attractions are alive — *Jurassic Park - The Ride* runs
under an anniversary overlay name, *Marvel Cave* is central to a 2027 project,
*Shock Wave* is stored rather than scrapped — and that **5 of the remaining
duplicate pairs are genuinely different rides**: Cedar Creek is a lazy river
beside Cedar Creek Mine Ride, KONDAALA is a kids' ride beside the KONDAA
coaster. None of that could be written down anywhere. The same 64 questions
would be re-asked until somebody answered one carelessly, and a merge cannot be
undone.

`attraction_review_marks` holds two kinds:

- **`not_a_duplicate`** — permanent. Two different rides do not become one.
  The pair is stored in a canonical id order, enforced by a CHECK constraint,
  because a pair fact has no direction and storing both ways is how the
  detector came to count 63 rows for 53 real pairs.
- **`not_retired`** — often temporary, with an optional `recheck_after`.
  Shock Wave is the case that argues for it: standing unused since March 2026
  with no announcement either way, so a permanent mark would hide its eventual
  retirement forever.

Consumers: `findDuplicatePairs` excludes marked pairs, and the retirement
candidate query — which until now existed only as documentation in `todo.md` —
became `GET /v1/admin/retirement-candidates`, minus cleared rows. A mark table
nothing reads is a comment.

`detect-seasonal` and reverse-reconciliation deliberately do **not** consult
marks. They describe the feed, and a human's verdict does not change that.

### Fixed — a year in brackets is not a map number

The rule that strips Queue-Times' map numbers from ride names took any trailing
`(digits)`. Six Flags Great America runs `HAUNTED HOUSE: Texas Chainsaw
Massacre (2022)`, and **a 2022 maze is not the 2023 one** — treating the year as
a formatting artefact would have offered two distinct event attractions as a
safe auto-merge.

Now at most three digits. Every real map number in the data sits between 23 and
224, and the only four-digit case in the whole table is that maze.

### Fixed — the duplicate detector counted ten pairs twice

Pairs are found two ways now: a base slug beside a numbered one, and two rows
sharing a Queue-Times id. Many pairs satisfy both — and a `UNION` only removes
exactly equal tuples, so the same two rows arrived twice with their roles
swapped. **63 rows for 53 real pairs.**

Not destructive, but each mirrored twin would have been merged a second time
against a row the first merge had already deleted, producing a spurious failure
in the report. Both branches now key on `LEAST`/`GREATEST` of the ids. Which row
is "base" carries no meaning anyway: `chooseDuplicateWinner` decides the
survivor and is symmetric.

Caught by not believing a number — the planned count jumped from 7 to 41 across
a change that only touched name and slug resolution, which is more than that
change could explain.

### Fixed — a merged ride kept the wrong URL *and* the wrong name

`resolveSurvivingSlug` only knew one rule: if the winner's slug is the loser's
with a number appended, take the loser's. That fits duplicates the sync made by
counting, and nothing else.

Duplicates found by a shared Queue-Times id do not derive from each other at
all. The winner is whichever row ingestion still feeds, and that row's slug can
be an unrelated leftover — the dry run was about to publish **Choco Chip Creek
at `/main-train-2`** and **Mini Track' Tour Ride at `/lolipop-farm`**.

So when neither slug is the other's stem, the survivor now takes whichever slug
the surviving **name** produces. A public URL should read like the ride. Callers
that pass no name keep the old behaviour exactly.

The **name** had the same problem, and it is the one a visitor reads. The
Queue-Times row usually wins a merge — it is the one ingestion still feeds — so
merging Energylandia's duplicates would have settled every ride on its numbered
spelling: `Abyssus (184)`, `Draken (155)`, `Frutti Loop (39)`. When two names
differ *only* by that trailing number, the clean one now wins regardless of
which row it came from. Any other difference is left to the winner; this is not
the place to arbitrate between two genuinely different names.

Scale, measured rather than assumed: Energylandia holds **138** attraction rows
for a park Queue-Times lists 46 rides for, and **31** of those are duplicate
pairs. The visible copy is the Queue-Times one — numbered name, no minimum
height, no RCDB link — while the metadata sits on the `-2` twin beside it. The
merge already inherits whatever the survivor lacks, so collapsing the pairs is
what puts the ride and its facts back together.

### Fixed — a park's map number hid eight duplicates

Queue-Times publishes some parks' own map numbers **inside** the ride name.
Energylandia's feed says `Draken (155)`, `Frutti Loop (39)`, `Abyssus (184)`;
ThemeParks.wiki says `Draken`, `Frutti Loop`, `Abyssus`. So the same ride
arrives twice under names that never match, and the rows end up as `draken`
beside `draken-rc` — eight times in that park alone.

The duplicate detector could not see them: it looked for a base slug next to a
numbered one (`foo` / `foo-2`), and these share no slug stem. It now also pairs
**two rows in one park carrying the same Queue-Times id**, which is not a
resemblance but an identity — the source issues one id per ride. 19 such pairs
exist, across Energylandia, Carowinds, Cedar Point, Kings Island, LEGOLAND New
York, Silver Dollar City, Walibi Belgium and four others.

`normalizeName` now strips a trailing `(number)` before comparing, which is what
lets the two spellings of a ride recognise each other. A year inside a name is
untouched — `Spindeln - Nyhet 2026` is a real ride name, not a map label.

**And the safety rule got stricter, not looser.** A shared Queue-Times id used
to be sufficient on its own to auto-merge. It is not: Carowinds' `Blackbeard's
Revenge - Cannonball Drop & Captain's Curse` and `Blackbeard's Revenge -
Pirate's Plank` both carry id 14744, and their slugs read `tube-slides` and
`drop-slides` — two slide complexes the upstream lumped under one id. Merging
them would destroy one. Kings Island has two stations of a railroad under one
id, Six Flags Great Escape three Sasquatch rows. The id still **finds** the
pair; it no longer **decides** it. That costs a human glance at cases like
`Kiddy Hawk Cove` / `Kiddy Hawk`, which is the right price for a merge nobody
can undo.

### Fixed — a retired attraction is gone, not seasonal

Excluding retired attractions from `detect-seasonal`'s candidate searches stops
them being marked **again**; it does not clear the flag they already carry. And
no reset path can reach them: the recently-operating reset keys off a ride
reporting OPERATING, which a demolished one never will. Verifying the retirement
of the first four found all four still carrying `is_seasonal = true`.

Step 2c clears it, the same shape as the free-flow heal (Step 2b) and for the
same structural reason. "Seasonal" says an attraction closes for part of the
year; that is not what happened to it.

### Added — attractions that no longer exist can finally say so

A demolished ride is not "closed today" and it is not "unknown" either. Both of
those describe a state it could come back from, and the API had no way to say
the third thing. So Disney's **Dino-Sue**, torn down with DinoLand in February
2026, sat in Animal Kingdom's attraction list regardless — as did Animal
Kingdom's *Affection Section* and *Animation Experience*, and Ocean Park's
*North Pole Encounter*.

`attractions.retired_at` + `retired_reason`. The reason carries the **source
URL**: a retirement is a claim about the world, so it travels with its evidence.
No sync writes either column — the same two-writers rule as the curated fields.

**The row and its queue_data stay.** The wait-time history is load-bearing for
baselines and models, and a ride page reading "operated until February 2026" is
worth more than a 404. What retirement changes is visibility: the attraction
leaves the park's list and its operating counts (filtered in `loadParkRelations`
and `findByParkId`, at the source rather than at each DTO site), while its own
detail endpoint keeps answering and now exposes `retiredAt` / `retiredReason`.

Three background jobs were excluded, and the first two matter most:

- **reverse-reconciliation** would write a retired ride a CLOSED heartbeat row
  forever. That is the write half of the bug the UNKNOWN read-path fixed.
- **detect-seasonal** — a retired ride is its perfect candidate: permanently
  CLOSED on every park-open day. It is not seasonal, it is gone.
- the **detail sync**, which spends one rate-limited wiki request per attraction
  and will not learn anything new about a demolished ride's height.

Analytics and ML readers are deliberately untouched: their queries are windowed,
so a ride with no recent data falls out of them already.

Retiring goes through `POST /v1/admin/retire-attractions` rather than a plain
UPDATE, because the write has to drag the park caches and the frontend sitemap
along with it — otherwise the removed slug stays advertised for up to 24h plus
the CDN's stale-while-revalidate window, exactly as the attraction merge found.
`unretire-attraction/:id` undoes it, and `GET /v1/admin/retired-attractions` is
the audit view.

### Added — detectors for the two ways this system went quietly wrong

Both of today's multi-week failures were found **by accident**, while looking at
something else, and neither was visible in any existing check:

- `detect-seasonal` threw a SQL syntax error on every run from 2026-06-03. It
  was scheduled, it ran, it died. 73 days.
- ThemeParks.wiki dropped 44 Europa-Park attractions from its live feed on
  2026-06-07, plus clusters at nine other parks. 10 weeks.

`SystemHealthService.freshness()` could not have caught either — it reads
`MAX(timestamp)` across all of queue_data and the last hour's row count, and
both stayed perfectly healthy while 140 attractions were dead. **An aggregate
cannot see a subset go silent.** The boot-time `hasRepeatableJob` check could
not have caught the first either: it finds jobs that were never scheduled, and
this one was scheduled and running.

Two detectors, daily at 06:45 on the analytics queue, plus
`GET /v1/admin/data-quality` for detail:

**Silenced clusters** — a park where ≥5 attractions stopped reporting on the
same day, while the park's feed demonstrably still works. The window is the
design: the cluster must have fallen silent between 3 and 14 days ago, so the
warning appears within days and then **goes quiet by itself**. No state, no
acknowledgement table, and no nightly-red job that trains people to scroll past
it. `windowDays` is a parameter so history can be inspected on demand.

The park-health gate is an **absolute count of still-live attractions, never a
ratio** — a ratio is self-defeating here, because the very incident being looked
for drags the park below it. Europa-Park at 45% live would have been hidden by a
70% gate.

**Failing jobs** — Bull's failed sets, grouped by job name with the newest
reason. Read straight off the Redis keys rather than by injecting fourteen
queues. Complementary to the boot-time check, not a replacement.

On its first run against production the cluster detector reported four events
inside its default window, none of which anyone knew about: 19 rides at
Schlitterbahn NB, 15 at Six Flags Over Georgia and 13 at Kings Dominion all
silent since 2026-08-09, and 6 at Warner Bros. Movie World since 2026-08-05.
Those names are water-park sections, so this is far more likely to be US parks
closing after the school year than four simultaneous faults — **a hit is an
event, not a verdict**. The point is that until now the event was invisible.

### Fixed — a season cannot be read off less than a year of watching

`detect-seasonal` derived `season_months` from the months an entity had ever
been seen OPERATING. On 2026-08-15 the entire `queue_data` history was **234
days** deep — 2025-12-24 onward — so nothing in the database had been watched
through a full cycle, and every stored month list was a contiguous run anchored
at the start of that window:

    [1, 2, 3, 4, 12]        69 attractions
    [1, 2, 3, 4, 5, 6, 12]  64
    [1, 12]                 18
    [4]                     22

None of those is a season. They are the span in which we happened to be
recording. Europa-Park's 44 feed-dropped rides all carried the identical
`[1,2,3,4,5,6,12]` and read as out of season in August, at a park in peak
season.

Months are now derived only for entities watched for at least
`MIN_OBSERVED_DAYS` (330). The span is measured over **all** rows, not the
OPERATING ones: a genuine winter attraction only ever operates for about forty
days, which says nothing about how long we have been looking at it. Under-
observed entities are excluded from the month query rather than skipped, so the
job writes NULL for them and clears the artefact on every run.

Applied to **shows** as well — 1096 of them were marked from the same 234-day
window and are just as wrong.

`is_seasonal` itself is untouched: "closed on ≥7 consecutive park-open days" is
observationally true whatever the window length. It was the months that lied.

Consequence, and it is the intended one: `isCurrentlyInSeason` goes null almost
everywhere, so out-of-season badges disappear until real evidence exists.
Phantasialand's ice-rink labels were correct and go with it — two right answers
against a hundred and forty wrong ones. Months start flowing again from roughly
**2026-11-19** (2025-12-24 + 330 days), which is about when Wintertraum starts.

### Fixed — "no source reports this ride" was being served as "closed"

Reverse-reconciliation writes a CLOSED `queue_data` row for any attraction no
upstream source has reported in 24 hours, so the seasonal detector has something
to read. The write is deliberate; the status it produces is an assertion we
cannot support. It says the operator closed the ride, when all that happened is
that our data stopped arriving.

On 2026-06-07 ThemeParks.wiki dropped 44 Europa-Park attractions and 18
Rulantica ones from its live feed — every one of them a ride with no
Queue-Times mapping to fall back on. Ten weeks later the park page still showed
a Ball Pool, a London Bus and a Dwarf City as closed, in August, at one of the
busiest parks in Europe. The same signature (one date, a whole cluster of rides)
appears at Universal Studios Singapore, both Wet'n'Wild records, Busch Gardens
Tampa and Ocean Park: roughly 140 attractions across ten parks.

Such an attraction now reads `UNKNOWN` while its park operates, and its queue
rows are dropped — the rule the codebase already applies one level up, where a
park whose wait times we cannot read puts its rides on `UNKNOWN` rather than
guessing. One util, applied on all three surfaces, because this is the third
status rule of its kind and the previous one shipped a bug by being written
three times.

`AttractionStatus` gained `UNKNOWN` — which it had been **serving for months**
via the unreadable-park path while the union claimed four values. The list now
comes from `ATTRACTION_STATUS_VALUES`, the same fix applied to the crowd-level
contract after that exact drift.

Free-flow attractions outrank the new rule: their flag, not the feed, is what
makes them open. In the favorites path that ordering needed an explicit guard,
because the check reads the raw rows rather than the already-overridden ones.

**Not fixed here:** the ~140 attractions keep getting marked seasonal, because
`detect-seasonal` reads the same CLOSED rows — and the season it derives from a
frozen feed is the observation-window artefact, at scale. Tracked in `todo.md`.

### Added — free-flow attractions can now carry a season

`isFreeFlowOpen` said "open whenever the park is", which is not true of every
free-flow area. Europa-Park's two water playgrounds run in summer while the park
itself stays open all winter; Everland's snow playground is the same story
inverted. Those areas could not carry `open_with_park` at all — flagging them
would have reported a snow playground open in July — so they stayed permanently
CLOSED instead.

The gate keys on **`season_months`, not `is_seasonal`**: the flag says an
attraction closes for part of the year without saying which part, and a gate
cannot act on that. Null or empty months mean no restriction, which is what most
free-flow areas are and what the 15 already-flagged ones depend on. Months are
1-based, as `detect-seasonal` writes them, and "which month is it" is asked in
the **park's** timezone — the test that pins this uses an instant where UTC and
park-local disagree, so a UTC implementation fails it.

`isFreeFlowOpen` now takes an options object. Five positional arguments, two of
them optional booleans, is precisely the shape that let the original rule drift
into three copies.

**Ownership boundary, and it is the load-bearing part.** The nightly detector
would have erased every curated season it was given:

- Step 2b (the self-heal added last change) cleared `season_months` on *every*
  free-flow row. It is now scoped to rows where the months are already NULL —
  the mislabel's exact signature, since the detector cannot derive months for an
  attraction it never sees OPERATING. Months on a free-flow row are therefore
  human-written, and this job does not own them.
- Step 2's recently-operating reset now skips free-flow rows outright. Avoras
  emitted 154 OPERATING records while free-flow, so that path would have wiped a
  curated season the first time one appeared in the feed.

Same two-writers rule as `curated_may_get_wet` and `curated_minimum_height`: the
sync owns its cell, the human owns theirs, and neither writes the other's.

No months were curated in this change — the four held attractions need their
operators' season *dates* researched first, and "summer" is not a month list.

### Fixed — a playground is not a season

`detect-seasonal` marks an attraction seasonal when it stays CLOSED across every
park-open day in the lookback window. That is also the permanent state of a
free-flow attraction: a playground has no queue, so its feed reports CLOSED
forever and only the read-time `open_with_park` override says otherwise. The
detector could not tell the two apart, and Step 2's reset keys off queue_data
ever saying OPERATING — which a playground never does — so the mislabel was
permanent.

Phantasialand showed both halves of the damage. Three of its four free-flow
attractions carried `isSeasonal: true` with no months while serving
`status: OPERATING`. And **Avoras** — a walk-in climbing course the park
advertises as open *"ganzjährig im Sommer wie auch im Wintertraum"* — was
reported out of season in August.

Avoras also exposes a second, subtler trap worth naming: its `season_months`
came out as `[1, 12]` because our history for it starts 2025-12-24. December and
January were not its season, they were **our observation window**. The park's
two genuine winter attractions share the same first-seen date and the same
derived months, so the artefact is invisible from the data alone — only the
park's own page separates them. Left in place for now (the ice-rink labels are
correct), recorded in `todo.md`.

Both candidate searches now skip `open_with_park` attractions, and the job
clears the flag on any it had already mislabelled, so it self-heals.

Curated to `open_with_park` after per-attraction research against the operators'
own pages: 14 free-flow attractions across Europa-Park, LEGOLAND Billund /
Deutschland / New York, Walibi Rhône-Alpes, Chessington, Busch Gardens
Williamsburg, Universal Studios Florida, Water Country USA, Hurricane Harbor
Arlington, Familypark and Paradise Country — plus Avoras. Deliberately **not**
flagged: genuinely seasonal free-flow areas at year-round parks (Europa-Park's
two water playgrounds, Everland's snow playground, Bellewaerde's Christmas
playground), because `isFreeFlowOpen` has no season gate and would call them
open in the wrong month.

### Fixed — upstream put the height on the wrong splash attraction

Phantasialand runs two water attractions side by side and ThemeParks.wiki has
their rules swapped: it gives `NEW: Winni Splash` a `minimumHeight` of 100 and
`NEW: Wavy Battle` nothing. The park's own Nutzungsbedingungen say the opposite
— Winni Splash's board reads *"Kinder unter 1,00 m Körpergröße dürfen nur in
Begleitung Erwachsener spielen"* (a supervision threshold, no limit), Wavy
Battle's reads *"Kindern unter 1,00 m Körpergröße ist das Betreten verboten"*
(a real minimum). So a playground that welcomes accompanied toddlers advertised
a 100 cm limit while the one that genuinely turns them away advertised none.
Both soak you; only one said so.

New column `attractions.curated_minimum_height`, under the same rule as
`curated_may_get_wet`: the detail sync overwrites `minimum_height` whenever the
wiki publishes a number, so a correction has to sit **beside** that cell rather
than in it. Always centimetres, `null` means "nothing to correct", and **`0`
means "no minimum height"** — barely a sentinel, since a 0 cm minimum excludes
nobody, and the only way a correction can override an upstream number with
*nothing*.

Both DTO mappers had grown their own copy of `curatedMayGetWet ?? mayGetWet`,
which is precisely how the free-flow status rule drifted into a shipped bug.
Height and wet now resolve through one `resolveCuratedFacts`
(`attractions/utils/curated-attraction-facts.util.ts`), which also closes two
gaps the inline version had: the unit is dropped when the height resolves to
null — otherwise a ride page renders a bare "cm" — and a curated-only height is
labelled cm instead of nothing. `curatedMinimumHeight` also joins the merge's
inheritable columns, or a later merge would silently drop the correction.

Data written to production: Wavy Battle 100 + wet, Winni Splash 0. Mopti's
Monkey Depot is correct as it stands — a dry climbing area with no height rule.

### Fixed — playgrounds read CLOSED on the one page people actually look at

`attractions.open_with_park` marks free-flow attractions — playgrounds, splash
pads, climbing nets. They have no queue, so the upstream feed reports them
CLOSED for the whole day, and the flag exists to overrule that while the park
is open.

The rule was implemented three times and one copy was missing it: the
attraction detail and the favorites list honoured the flag, **the park's ride
list did not** — and that is the surface a visitor sees. Phantasialand's
Mopti's Monkey Depot, Winni Splash and Wavy Battle all carried the flag and all
read CLOSED on an operating park.

The park payload also could not have honoured it as written: it loops over DTOs
and `openWithPark` is only on the entity, so the flag was not even in scope. It
now reads from the park entity it was handed.

The rule is one function (`common/utils/free-flow-status.util.ts`) that all
three call, because "written three times, one drifted" is the actual defect.
Both halves of it are pinned by tests, including the two ways it must NOT fire:
a closed park closes its playgrounds too, and in a park whose wait times we
cannot read nothing below the park may claim to be running.

The override runs before `effectiveStatus` is derived, so these attractions
also count toward the park's operating total instead of being silently missing
from it.

**Not a bug:** these attractions receive data at the same cadence as every
other ride (6 rows in 6 h, against Taron's 8). `queue_data` only writes a row
when something changes; the 5-minute sync still runs.

### Changed — the term audit runs itself, and the processor says what it does

`GET /v1/admin/ride-profile-term-audit` now also runs daily at 06:30. The seed's
CI check went away with the seed, and an endpoint nobody calls catches a renamed
glossary term only when somebody thinks to ask — while the failure is silent by
design, because a ride page drops an unknown term rather than rendering a dead
link.

The job deliberately does **not** fail on a broken id. The ids are correct until
the frontend ships a rename, and a nightly red job teaches people to ignore it;
a warning naming the ids and the rides they shorten is the signal. An
unreachable frontend is logged as exactly that rather than as "the curation is
dead" — there is a test for it. `publish-ride-profiles` stays manual: it belongs
at the end of a curation session, which is a human moment anyway.

`ManualMetadataProcessor` is now `CuratedDataProcessor`. It has not applied a
manual-metadata seed since that seed was deleted; it publishes curated ride
profiles and audits their term ids. **The Bull queue keeps the name
`manual-metadata`** — Bull keys repeatable jobs by queue name in Redis, so
renaming it would strand `ride-profile-term-audit-cron` under the old name,
registered and consumed by nobody. That needs an expand/contract migration,
which is more machinery than a tidier string is worth; the reason is documented
at the decorator.

### Added — `hasSingleRider`, and a curated wet flag that a sync cannot undo

Two curated fields on the attraction, both edited in the database and written
by no sync.

`hasSingleRider` answers "does this ride have a single-rider line at all". It is
deliberately NOT derived from the live `queues` array: a single-rider queue that
happens to be closed, or a park whose wait times we cannot read, would make the
ride look as though it never had one. `queues` keeps answering "is it open and
how long"; this answers "does it exist". Seeded from observation — the 48
attractions that have ever reported a SINGLE_RIDER queue — then extended by
research to 59 yes / 12 no. Three rides were left unset because sources
contradicted each other (Chiapas, River Quest, F.L.Y.).

`curatedMayGetWet` grew from 72 to 130 entries, taking the served coverage from
82 to 140 attractions. Every addition was verified against the park's own page
or Wikipedia, and water play areas are flagged as such rather than as rides.

### Fixed — 42 Magic Kingdom rides were living in Animal Kingdom

Animal Kingdom held 66 attractions, of which 48 came from Queue-Times and only
six were actually its own. Big Thunder Mountain, Haunted Mansion, Space
Mountain, Cinderella Castle and 38 more sat under the wrong park **and were
still being updated there daily**.

The park-level mapping was never wrong: Queue-Times park 6 is Magic Kingdom and
park 8 is Animal Kingdom, exactly as stored. The cause is one level down — the
ingestion resolves an attraction by `externalId` alone, with no park scope
(`children-metadata.processor.ts`), so the Magic Kingdom sync kept writing into
whichever row already carried that id, wrong park and all. There is already a
regression spec naming this exact failure.

Which ride belongs where was decided by Queue-Times' own park listings rather
than by judgement — that is how `meet-moana-at-character-landing` stayed in
Animal Kingdom, where a reasonable guess would have moved it. The 42 were
re-parented, the 35 that collided with an existing Magic Kingdom row were given
the `-2` suffix the duplicate finder looks for, and the existing merge tooling
paired them up inside the park. The wiki row won every pair and inherited the
Queue-Times id, so future syncs resolve to the right row.

All 30 parks that hold Queue-Times rides were checked the same way. **Animal
Kingdom was the only cross-park misassignment** — the 129 other rides that no
longer appear in their park's listing are retired or renamed ids, not
misplaced ones.

Animal Kingdom is now 24 attractions instead of 66. Its headliner set was
already correct, so no baseline was polluted.

### Changed — the curated attraction metadata now lives in the database too

`MANUAL_ATTRACTION_METADATA` and its apply job are gone, the same way the
ride-profile seed went: the 727-entry file, its spec, `ManualMetadataService`,
the `apply-seed` queue handler, `POST /v1/admin/apply-manual-metadata`, and the
call the attraction detail sweep made at the end of every run.

The three things it curated are **not** equally safe without it, and that
difference is the whole design:

- **`rcdb_id`** has no upstream writer at all. It simply stays in the column.
- **`minimum_height`** is overwritten by the detail sync whenever the wiki
  publishes a number — which is the documented intent, the park's own sign
  wins. Curated values keep filling the gaps the wiki leaves.
- **`may_get_wet`** was the problem. The sync overwrites it whenever the wiki
  publishes a differing value, so a correction stored there survives exactly
  until the next run. It now has its own column, `curated_may_get_wet`, on the
  `stats` / `curated_stats` pattern already used by the ride profile: two
  writers, no shared cell, merged on read with curated winning. The 72 curated
  flags were copied into it before the seed was deleted.

### Fixed — two RCDB ids pointed at somebody else's ride

`rcdb_id` was matched to attractions by park proximity and normalised name, and
that has one failure mode: where two parks hold a ride of the *same name*, both
can inherit the one id. Both cases were real. RCDB 3 is the Demon at Six Flags
Great America in Gurnee, and Santa Clara's identical twin — the other half of
the Marriott pair — carried it too. RCDB 9720 is the Magic Kingdom's Seven
Dwarfs Mine Train, and Shanghai's carried it as well.

Both wrong halves are now NULL rather than guessed at: an id that opens a
different ride's page is worse than no link. A duplicate check is documented on
the column.

Three duplicated ids remain and are a **different** problem — the same physical
ride recorded under two park records (Traumatica alongside Europa-Park, and
Universal Studios Florida held once from ThemeParks.wiki and once from
Queue-Times). Those are duplicate parks, not wrong ids.

### Changed — the curated ride profiles now live in the database, not in a seed file

`RIDE_PROFILE_SEED` and its apply job are gone: the 11,409-line seed file, its
spec, the exported term-id allowlist, `RideProfileService.apply()`, the
`apply-ride-profiles` queue handler and `POST /v1/admin/apply-ride-profiles`.
The `attraction_ride_profiles` rows are the source of truth now and are edited
directly. `RideProfileService` keeps every read path, and `RideStatsService`
still writes the Wikidata `stats` column — a different writer for a different
column, as before.

What that trades away is written down in
[the ride ↔ glossary guide](frontend/ride-glossary-link.md): term ids are no
longer validated against the frontend's list, a direct write does not evict
`park:integrated` or ping the frontend to revalidate, and the invariants the
spec used to enforce are now SQL you run by hand. The guide carries those
queries.

### Fixed — 45 curated rides had silently stopped being written, and ten carried the wrong ride

The seed job skips entries whose slugs match no attraction, deliberately, so
one stale line cannot fail a run. The cost showed up when four parks were
renamed upstream — `toverland`, `disney-magic-kingdom`,
`disney-hollywood-studios` and `disney-animal-kingdom` — and 45 curated rides
across them stopped matching. Their rows stayed in the database, frozen at
whatever the last successful run wrote. `MANUAL_ATTRACTION_METADATA` is keyed
the same way and had lost 33 entries the same way: 23 to those renames and ten
to `-2` ride slugs that vanished when duplicate attractions were merged.

Then the profiles themselves. Five B&M floorless coasters carried a
byte-identical element list while publishing 7, 7, 7, 5 and 7 inversions — the
copy-paste pattern that had already produced wrong SLC and Boomerang entries.
Checked one at a time against their own RCDB ids: Dominator does not run the
Medusa layout at all (no dive loop, no zero-g roll), Rougarou is still the 1996
Mantis track and has four inversions rather than five with an inclined loop
rather than a zero-g roll, Vallejo's Medusa has the first sea serpent roll B&M
ever built rather than a cobra roll, Kraken has a second vertical loop and a
flat spin that were missing, and Medusa (Jackson), Scream and Batman: The Dark
Knight were each short the second of two interlocking corkscrews. Hydra opens
with a jojo roll and has no cutback, Hair Raiser's fourth inversion is an
Immelmann and not a corkscrew, and Storm Runner has a cobra loop and a flying
snake dive rather than an Immelmann, a cobra roll and a corkscrew.

Those last three figures had no glossary term, so cobra loop, jojo roll and
flying snake dive were added to the frontend glossary in all six languages.

### Added — profiles for the twenty rides that had an RCDB id and no entry

Each checked against its own id rather than the ride it shares a name with,
which is how the two `THE FLASH: Vertical Velocity` entries turned out to be an
Intamin impulse from 2001 and a Vekoma Super Boomerang from 2025, and Fiesta
Texas's `BATMAN The Ride` an S&S 4D Free Spin rather than the B&M invert the
name promises everywhere else. Where no source states the element order, none
is recorded.

### Fixed — a feed that only ever says CLOSED is not an observation either

The first cut of the unreadable-park treatment keyed off _"no queue row exists"_, on the
assumption that a park we cannot read publishes nothing at all. Hansa-Park does publish:
its upstream carries a row for every one of its 82 attractions, permanently `CLOSED` and
never once carrying a wait time. Whether those rows are present varies through the day —
at 08:00 UTC 80 of 82 were absent, by 11:00 all 82 were there — so the park went from
"82 of 82 running at `very_low`" to **"82 of 82 closed"** at 12:58 on an open summer
Friday, which is the same fabrication wearing the opposite sign.

A row that has only ever said `CLOSED` and never a minute says nothing about whether the
ride is running. For a park on the curated list the rows are now dropped and the ride
reads `UNKNOWN` whenever the park itself is open — the park's own `CLOSED`, which comes
from the schedule and _is_ readable, still closes everything below it. Applied on all
three surfaces that build ride status (park payload, attraction detail, favorites), so a
client cannot read a `waitTime: 0` off a row that means "no data" rather than "walk on".

### Added — a park we cannot read says so, instead of reading as empty

Hansa-Park serves wait times from its own app, only to devices on the park's
WLAN. There is no public endpoint, so the API has never held a single wait time
for it — `/stats` has stood at `totalSampleDays: 0` since ingestion began — while
the catalog carries all 82 attractions and the schedule feed works fine.

Nothing in the response said so, and the live surfaces filled the gap. During
opening hours the park read `OPERATING` with **82 of 82 rides operating**, every
one of them at `crowdLevel: very_low`, Ø 0 min, peak 0 min. That is the optimistic
ride fallback doing its job on a park it was never meant for: it exists so a feed
that drops a row does not report "park open, all rides closed", and it assumes a
missing ride is running. Here every ride is missing, always — so it asserted the
whole park was up, quiet and walk-on, on the strength of the schedule alone. The
ride pages went further and served an ML forecast (10 min at 67 % confidence, every
slot of every day) built from zero observations of this park.

A client cannot tell that apart from a park shut for the night: at 03:00 local
every park in the catalog has zero rides operating and an empty `queues` array.
The difference is knowledge about where a park publishes, which existed nowhere
in the data. It is now curated in `parks/data/live-wait-time-sources.ts` (keyed
by `citySlug` + `parkSlug`, each entry carrying the evidence that put it there)
and published as `liveWaitTimes` on the park detail, the attraction detail's
embedded park, the discovery cards, nearby and favorites:

```ts
liveWaitTimes: { available: false, reason: "in_park_app_only" }
```

`available: false` is a statement about the source, not about right now — a park
whose feed went silent this morning stays `true`, since that is what the
staleness and movement rules already handle.

Everything the API derives from a wait time now follows the house rule and emits
`unknown` rather than a placeholder: the optimistic fallback is skipped so rides
read `UNKNOWN` instead of `OPERATING`, ride and park crowd levels read `unknown`
instead of `very_low`, `peakHour*` and `percentiles` are dropped, and
`hourlyForecast` / `bestVisitTimes` are withheld the same way they are for a
closed park. A `CLOSED` park still closes its rides — that comes from the
schedule, which we can read, and is a fact rather than a gap.

Of 213 parks exactly one is on the list. Frontend contract:
[Parks whose wait times we cannot read](frontend/live-wait-times-availability.md).

### Fixed — a dead schedule feed no longer keeps a running park closed

Energylandia was `OPERATING` on its own park page and `CLOSED` everywhere it was
listed: the country page, the card overlay behind `/v1/discovery/continents/*`,
`/v1/discovery/nearby`, and `/v1/analytics/geo-live`, which left Poland out of the
world map's open counts entirely. The park was open — 45 rides reporting, Ø 45 min,
occupancy 217.

Its schedule sync stopped on 2026-07-24. Everything after that is an `UNKNOWN` row
with no times, which by this API's own contract means "we don't know", not "closed".

`isParkOpen()` handled that correctly because nobody hands it a park's whole history
— `ParkIntegrationService` gives it the next 16 days, `getBatchParkStatus` today and
yesterday — so its "there is a schedule, trust it and ignore the rides" branch means
"hours were published for the days in question". Both SQL re-implementations asked
instead whether the park had **ever** published hours. That is true forever once it
has been true once, so a park whose feed went silent kept a schedule branch it could
no longer satisfy and could never fall back to its live rides again.

The gate is now "did the schedule publish something decisive for today" — an
`OPERATING` or `CLOSED` row, park-level, dated within a day of now to cover every
timezone. `UNKNOWN` no longer silences the ride fallback, while a park that is
genuinely shut still says so with its `CLOSED` row. Shared as
`scheduleRowSpeaksForToday` so the two queries cannot drift apart again.

Two smaller holes in the same queries: neither excluded attraction-level schedule
rows, so one ride publishing its own hours could report the whole park open; and
`parks_with_schedule` matched only `OPERATING` rows, so a park with nothing but
`CLOSED` rows took the ride fallback and a frozen feed could open it.

Of 213 parks exactly one was affected at the time of the fix.

### Fixed — the name you typed outranks the park that happens to be open

Searching `flyer` put _Blue Flyer_ (Blackpool Pleasure Beach, open at the time)
above the ride actually called **Flyer** (Knoebels, closed). Results are ordered
OPERATING-first so people see what they can ride now, and that rule was beating
the match itself.

An exact name match now sorts ahead of everything, including OPERATING-first.
"Exact" ignores punctuation and accents the same way the matcher does, so `fly`
is an exact hit on **F.L.Y.** and `farup` on **Fårup Sommerland**. Deliberately
narrow — prefix and substring hits stay subject to OPERATING-first, so a query
that names no single thing still prefers an open park.

The frontend scored matches on raw strings and had the same blind spot, which is
why `fly` listed _Sky Fly_ above F.L.Y. on park.fan even though the API returned
F.L.Y. first — fixed separately in park.fan#277.

### Fixed — search can reach a park through the town it stands in

Searching a city name could not find the parks in it, and any city spelled with an
accent could not be found at all. `Brühl` reached nothing, `Haßloch` reached nothing,
`orlndo` reached nothing.

Two causes, both in the in-process index that actually serves every query — the SQL
path that does handle city and land has not served a request since the index landed:

- **Accents were shredded, not folded.** Everything downstream splits on
  `[^a-z0-9]`, which treats an accented letter as a separator: `Brühl` arrived as the
  two fragments `br` and `hl`, `Fårup` as `f` and `rup`. Long names survived on
  trigram overlap alone; short ones had no chance. Queries are now folded the same
  way the indexed text is (`Brühl` → `bruhl`), including `ß` → `ss`, which has no
  Unicode decomposition and turned `Haßloch` into `ha` + `loch`.
- **A city was only ever an exact substring.** `park.city.includes(query)` and
  nothing else, so a single typo lost it. The city now gets word-level typo
  tolerance, and an attraction's land does too — `rookburgh` found F.L.Y. but
  `rookhburgh` lost it.

  The secondary fields budget that tolerance against the **shorter** of the two
  words, where the name matcher budgets against the query alone. That difference
  matters: `Ying Tan Shi` holds the word `tan`, two edits from `taron`, and a
  five-letter query is allowed two — so a search for the ride Taron surfaced two
  parks in Yingtan ahead of it. Two edits out of a three-letter word is a different
  word, not a typo.

Ranking is unchanged where it matters: every city and land match is scored strictly
below every name match, so a park named after a town still outranks the parks merely
standing in it. Bounded by the data — the largest city holds 13 parks — and seven
cities go from unreachable to reachable: Brühl, Günzburg, Haßloch, Montréal,
Ciudad de México, Saint-Pourçain-sur-Besbre, San Martín de la Vega.

### Fixed — "open park" is answered the same way everywhere, and a frozen feed no longer counts

At 06:00 UTC on 2026-08-07 the world map showed two parks open in Europe — France
1, Italy 1 — at an hour when nothing in Europe was open. The parks were
Le Parc du Petit Prince and Cinecittà World, and worldwide two more: Adventureland
Resort at 01:20 local and Lake Compounce at 02:20.

All four are fed by queue-times, and queue-times never closes them. It keeps
serving the last snapshot after closing time: the same wait times, the same
`is_open: true` per ride, with a freshly stamped `last_updated`. Cinecittà World
served 29 rides "OPERATING @ 15 min" **unchanged for 24 hours straight** — verified
against the upstream endpoint, so this is an upstream artefact and not an ingestion
fault. Any rule of the shape "a ride is OPERATING and has a plausible wait" calls
that park open at 3 a.m., and the analytics queries used exactly that rule.

Neither of the two obvious guards separates a frozen feed from a real park: the
placeholder waits sit well above any wait-time floor (15 min, 45 min), and the row
timestamps are current, so a staleness check passes too. What does separate them is
**movement** — a live feed's numbers change, a frozen snapshot's do not. The ride
fallback now requires at least one ride to have reported more than one distinct
`(waitTime, status)` within the movement window.

The freshness window stays at 2 h ("is this park still reporting"); the movement
window is 4 h, because a real park's queues can settle for an hour or two near
closing and a 2 h window drops the last hours of the day.

Replaying the rule hourly over 7 days against production, in park-hours judged open
per local hour of day:

| local hour | 00  | 02  | 04  | 06  | 08  | 10  | 12  | 14  | 16  | 18  | 20  | 22  |
| ---------- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| before     | 21  | 22  | 20  | 20  | 20  | 18  | 49  | 60  | 60  | 55  | 40  | 23  |
| after      | 1   | 0   | 0   | 1   | 2   | 4   | 41  | 53  | 50  | 43  | 27  | 4   |

Before, the count sits flat at ~20 through the whole night — that is the bug. After,
the night empties and what remains is an opening-hours curve peaking at 14:00–16:00
local. Per park over those 7 days: **Cinecittà World 169 h → 4 h** and Le Parc du
Petit Prince 22 → 7 (the two frozen feeds), Adventureland 152 → 71, Lake Compounce
132 → 65, Le Pal 91 → 61. Parks that were already reported correctly lose **nothing**:
Gröna Lund 63 → 63, Beto Carrero 43 → 43, Hellendoorn 32 → 32, Fårup 27 → 27,
Universal Studios Orlando 75 → 75.

A "no park is open at 04:00 local" clock guard was measured and rejected: over the
same 7 days it removed 2 park-hours, both at 05:00–06:00 local. A magic constant
worth 2 park-hours a week is not worth the maintenance question it raises.

The same four CTEs had been copy-pasted across `getGlobalRealtimeStats` (twice),
`getTickerData` and `getGeoLiveStats`. They still agreed with each other, but four
copies of a rule is how they stop agreeing — they now come from one place,
`analytics/utils/open-parks.sql.ts`, which documents why the rule looks the way it
does. `test/e2e/open-park-openness.e2e-spec.ts` pins both halves against a real
database: a frozen feed must not count, a moving one and a settled-but-recently-moved
one must, and all three call sites must return the same set.

Not part of this change, but found while confirming it: because the park detail
endpoint uses a hard 30-minute recency window (`isParkOpen`) while these feeds are
polled hourly at night, a fallback park's own `status` flips between OPERATING and
CLOSED every hour. That is why the map and the park page disagreed at 06:00 but
agreed at 06:18.

### Changed — the measurements say who to credit, instead of leaving it to be worked out

`rideProfile.stats` shipped `source` and `sourceId` and left every client to
rebuild the citation rule from them: that a curated ride owes nobody a credit,
and that a `sourceId` is a Wikidata entity whose page lives at
`wikidata.org/wiki/{id}`. Both are facts about the data. The frontend got the
rule wrong, which is what this is fixed from — it rendered the credit whenever
`stats` existed at all, so 26 of the 27 rides that state a measurement today
named a source none of their numbers came from and linked to `/undefined`.

- **`stats.attribution`** — `{ label, url }`, or **null when every surviving
  number is hand-curated**. Null is the whole rule: render it when it is there,
  show nothing when it is not, and the citation cannot come out wrong. It is
  also null when a ride _has_ an entity but the curation outvoted it on every
  field, because naming a source none of the displayed values came from is a
  false citation either way.
- **`source` and `sourceId` stay** — they are provenance, and honest data. They
  are no longer the thing a client has to interpret to draw a link.

### Fixed — `stats` was served as an undeclared object

The DTO typed the field off the entity interface, so Swagger emitted
`stats: object` with no properties at all — a prose description and an example
were the entire contract. Consumers hand-maintained a mirror of a shape nothing
could check, which is how the frontend's copy drifted from `source: 'rcdb'` (a
source that no longer exists) without a single failing build on either side.

`RideStatsDto` and `RideStatsAttributionDto` now declare every field, so `source`
publishes its `curated | wikidata | mixed` enum and the shape is checkable by
anyone generating a client from the spec.

### Fixed — a park merge could wire a park to another park's feed

A merge fills the winner's empty upstream ids from the loser, which is how a
park keeps its feeds when the row carrying them disappears. But an upstream id
is not a fact about the loser — it is a claim about which park the _source_ is
describing, and nothing checked that the claim still held for the winner.

Islands of Adventure in Orlando spent three days carrying `qt-park-97`, which
Queue-Times uses for **Adventure Island in Tampa**, a hundred kilometres away.
It arrived from a merge with a row that sat in Tampa. The damage was not the
wrong wait times it would have served once that water park reopened — it was
that the sync could no longer find the real Islands of Adventure under its own
id (`qt-park-64`) and created a **second park row** for it two days later.
That duplicate is what surfaced the whole thing.

`consolidateEntityIds` now asks `canInheritSourceIds` first: when both rows
state coordinates and those coordinates describe different places (>10 km), the
ids are not inherited, a warning names which feeds were left behind, and
`MergeResult.skippedSourceIds` carries them out to the caller.

The threshold is deliberately loose. This is not a duplicate test — the
detector already decided these are the same park, and it works to a kilometre.
This is the last check before one row's identity is copied onto another, so it
only has to recognise "not the same place at all". Missing coordinates, and the
`0,0` of a failed geocode, are not evidence of anything and do not block
inheritance.

Refusing costs a park a feed until someone looks, which the next sync makes
loudly visible by creating a row. Accepting costs a park quietly serving
another park's data. Refusing is the cheaper mistake.

Repaired in production alongside the fix: the two Islands of Adventure rows
merged back into one (queue_data 185,337 + 2,335 → 187,672 exactly, zero
orphans), the surviving row now carries the correct `qt-park-64`, and the
loser's path redirects instead of 404ing. Park count 210 → 209, duplicates 0.

### Added — curated ride measurements, because Wikidata alone leaves the headliners blank

The Wikidata import below is the right automatic source and it is not enough:
of the 452 coasters this repo curates, it states a **top speed for 84**. The
gap is not random — it is worst exactly where people look. Fury 325, Steel
Vengeance, Millennium Force, Hyperia, Eejanaika all have a Wikidata entity with
a height and a length and no speed at all.

So the seed can now state measurements too, and 472 rides do.

- `RideProfileSeedEntry.stats` (`topSpeedKmh`, `heightM`, `lengthM`,
  `durationSeconds`, all optional, all metric) is written to a **separate**
  column, `attraction_ride_profiles.curated_stats`. Same reason `stats` is its
  own column: two writers, no shared cell, so neither the import nor a seed
  re-run can undo the other's work.
- `mergeRideStats` merges the two **field by field** with curated winning, and
  reports `source: "curated" | "wikidata" | "mixed"` for the result. `sourceId`
  is dropped unless an imported value survived — citing a Wikidata entity none
  of the served numbers came from would be a false citation. Nothing changes for
  a consumer that already reads `rideProfile.stats`.
- Coverage after this change: the API serves a top speed for **458 of the 710
  curated rides, up from 88** — 403 of 452 coasters. The seed states 456 speeds,
  450 heights, 452 lengths and 370 durations; the import fills the rest.

Assembled the way the rest of this seed is, and cross-checked rather than
copied: each ride's figures were read from its park page and its Wikipedia
articles in three languages, joined on the RCDB id we already store — so a
"Goliath" can never pick up another "Goliath"'s numbers — then required to
agree within 5% before being written. What the sources disagreed about was left
out rather than averaged: 37 fields are still blank for that reason.

The 233 rides with no RCDB id — nearly every Disney and Universal attraction —
needed a second pass that finds the ride by name, which means it has to prove
it found the right one. Two more gates do that, and both earned their keep:
searching "Winja's Force Phantasialand" returns the article about Taron, which
is in the right park and states 117 km/h, so the article must also carry a
distinctive word from the **ride's** name; and an infobox that lists three
parks states one speed for whichever installation somebody typed in, so
multi-park articles are dropped whole. That is why Tokyo's Big Thunder Mountain
still has no speed here and Radiator Springs Racers, VelociCoaster, Rock 'n'
Roller Coaster, all three Revenge of the Mummys, Expedition Everest and
Journey to the Center of the Earth do.

What is left is left on purpose. Universal publishes no specification for
Mine-Cart Madness and the enthusiast estimate of "around 30 mph" is not a
measurement, so that field stays empty. Where the sources disagreed a third one
was asked before anything was written: Big Thunder Mountain Railroad reads 35
mph in every English source against one German 45 km/h, Divertical is the
world's tallest water coaster at 60 m and 110 km/h rather than the 50 m the
German article states, and Stampida's 74 km/h carries two sources against one.
Heide-Park's Big Loop is quoted at 60, 63 and 70 km/h by three sources, so it
keeps its height and length and no speed. Chessington's Dragon's Fury keeps a
height and a length and no speed for the plainer reason that nobody publishes
one.

The physics check paid for itself twice more. It flagged four terrain coasters
that legitimately drop below their own station — Oblivion falls into a hole in
the ground — and, in the previous round, one entry that was simply another
ride's.

Three checks earned their place by catching real errors:

- **Identity.** The English infobox states its own RCDB id, and 8 rides came
  back with one that was not ours. Six were our bug, now fixed in
  `MANUAL_ATTRACTION_METADATA`: Steel Vengeance pointed at Mean Streak,
  Wildcat's Revenge at the Wildcat it replaced, Twisted Cyclone at the Georgia
  Cyclone, Canada's Wonderland's Backlot Stunt Coaster at the Kings Island
  installation, Magic Mountain's Road Runner Express at a Six Flags New Orleans
  ride, and SeaWorld San Diego's Journey to Atlantis at the San Antonio one
  (whose own id was missing and is now filled in). Each verified against the
  RCDB page title — the link target, not its content.
- **Park.** An infobox that names a different park is about a different
  installation. This is what kept Magic Kingdom's Seven Dwarfs Mine Train
  figures off Shanghai's.
- **Physics.** A ride cannot be faster than `sqrt(2gh)` unless something
  launched it. Five rides tripped this: four are terrain coasters that drop
  below their own station, and the fifth exposed a wrong seed entry — Le PAL's
  Yukon Quad was filed as a 2015 Mack coaster and is Intamin's 2018 Family
  Launch Coaster.

Two invariants in `ride-profile-seed.spec.ts` now hold the line: every curated
measurement must be inside what a ride can physically be, and no ride may be
taller than half its own track — the shape a foot/metre slip takes.

### Fixed — Dwervelwind was three years and one building off

Toverland's Dwervelwind was seeded as a 2015 **indoor** dark ride. It opened on
**29 September 2012** — a five-week autumn preview as _D'Wervelwind_, before it
reopened in spring 2013 with the rest of the Magische Vallei — and it is
outdoors: the queue winds between the coaster's own ponds and boulders, and the
station is an open-air thatched structure. Toverland's own page, Wikidata
(P1619) and both Wikipedia articles agree on the date.

Now `spinning-coaster` + `steel-coaster` + `terrain-coaster`, with the horseshoe
its layout actually starts with, and the park's figures: 75 km/h, 21 m, 462 m,
1:52.

The same audit compared every seeded `openedYear` against the sources: **51
disagreed, 39 are corrected here**, and the way the last 26 were settled is the
point. An infobox year cannot decide these on its own, because "opened" means
the year the ride opened _here_ and an article about a relocated or rebuilt ride
answers a different question. The article's opening sentence does say which case
it is, so all 36 were read.

That reading corrected Pantherian (1993→2010 — it is Intimidator 305), Nessie
(1987→1980), Big Loop (1989→1983), Shaman (1990→1985), Tornado Madrid
(2003→1999), Flying Eagle (2010→2018) and twenty more. It also confirmed seven
seed values the infoboxes contradicted: Magic Kingdom's Big Thunder Mountain
Railroad really is 1980 (Disneyland's is the 1979 one), TRON is 2023 in Florida
and 2016 in Shanghai, and Michigan's Adventure's Thunderhawk opened there in
2008 however much of it was Serial Thriller in 1998.

Nine are still open, two of them for want of a sentence that states a year and
seven because they are the same multi-park ambiguity, already resolved in our
favour.

### Added — ride measurements from Wikidata

Nothing in this system knew how fast, how tall or how long a ride is:
ThemeParks.wiki carries rider heights and stops there.

Wikidata does, it is **CC0** so the values can be stored and served, and it
already anchors this catalogue — the `rcdbId` on every attraction came from
Wikidata property P2751, which is exactly the key the import joins on. (The
roller-coaster database those ids point at permits the link, not the data; see
the note on `Attraction.rcdbId`.)

- `WikidataClient` + `parseStatsResults` (`src/external-apis/wikidata/`) fetch
  speed, height, length and duration. Every measurement is read through `psn:`
  — the **normalised** value — so Wikidata converts to SI before we see it: a
  ride entered in mph and one entered in km/h both arrive as metres per second.
  That is why there is no unit table here and none to get wrong. Black Mamba's
  22.222222222222222224 m/s comes home as exactly 80 km/h.
- One SPARQL request answers for 200 rides, so the whole catalogue is three
  queries rather than one request per ride.
- **Coverage is thin and that is the source, not a bug.** Of ~1740 Wikidata
  items carrying an RCDB id, ~146 state a speed, ~208 a height, ~222 a length.
  A ride Wikidata says nothing about is left unstamped, so a later run picks it
  up if somebody fills the values in.
- `RideStatsService.import()` writes to `attraction_ride_profiles.stats` (jsonb,
  with `source`/`sourceId` provenance) and stamps `stats_updated_at`; rows
  imported in the last 90 days are skipped. Queued on `ride-stats`, triggered by
  `POST /v1/admin/import-ride-stats` (`limit` for a trial run), and it publishes
  through the same cache order as the curated seed: our caches, then the
  frontend, then once more after the CDN window.

### Fixed — a seed write was announced before it was visible

Applying the curated seeds (`apply-ride-profiles`, `apply-seed`) told the
frontend to drop its `parks` cache but never evicted our own. The frontend
refetches the moment it is told, so it read the pre-seed payload back out of
`park:integrated:{parkId}` (up to 6 h old for a closed park) or off the
Cloudflare edge — and pinned that in its own data cache **for 24 h**. A
curated ride profile could be written, announced, and still be missing from
the ride page the next morning, for some regions and not others: Cloudflare
caches per PoP and the frontend's data cache is per region.

`ManualMetadataProcessor` now publishes a write in the order that works —
`invalidateParkCaches()` for every touched park first, `revalidateTags` second,
and a second revalidation `CDN_SETTLE_MS` (16 min) later, because the edge copy
lives up to `max-age 300 + stale-while-revalidate 600` and this service has no
way to purge it. The delayed sweep carries a fixed job id, so iterating on a
seed leaves one pending sweep rather than a pile.

Both seed services now report which parks they wrote into
(`RideProfileSeedResult.touchedParks`, `ManualMetadataResult.touchedParks`);
the metadata seed reports only rows that actually changed, so a no-op run
evicts nothing. See
[Caching Strategy](architecture/caching-strategy.md#1-integrated-responses).

### Added — Hersheypark ride heights

`MANUAL_ATTRACTION_METADATA` gains a minimum rider height for **46
Hersheypark rides**, a park where ThemeParks.wiki carries none at all: all 72
of its rides showed no height requirement.

Hersheypark does not print inches. It sorts rides into candy categories
(Hershey's Miniatures 0-36", Kisses 36-42", Reese's 42-48", Hershey's 48-54",
Twizzlers 54-60", Jolly Rancher 60"+), and the per-ride assignment appears
only in the park's own Rider Safety & Accessibility Guide — as a row of candy
logos. The badges are images, so neither the ride pages, the ride listing, the
FAQ nor the PDF's text layer carries them; the guide's pages had to be
rendered and read. A ride's minimum is the floor of the smallest badge shown.

Stored as centimetres with `minimumHeightUnit: "in"`, following the existing
convention: 36"=91, 42"=107, 48"=122, 54"=137.

Rides whose badge row starts at Miniatures have no minimum at all and are
deliberately left out — NULL means "unknown" in this column and there is no
way to express "none", so a 0 would be an invented number.

### Added — Ride ↔ Glossary link (`rideProfile`) (2026-07-28)

Rides now carry a curated profile that connects them to the frontend glossary:
the named track figures in ride order, what kind of ride it is, who built it
and when it opened. Everything is stored as **glossary term ids**, so the link
works in both directions off one table — a ride page renders "Zero-G Roll" as
a link into the glossary entry that explains and animates it, and that entry
lists the other rides that have one.

- New `attraction_ride_profiles` table (`elements` / `types` jsonb + GIN
  containment indexes, manufacturer name _and_ optional term id, model,
  opening year, inversions).
- `RIDE_PROFILE_SEED`: **701 rides across 100 parks** — every Disney and
  Universal park, Phantasialand, Toverland, Europa-Park, Movie Park, all three
  Walibis, plus the major European, North American, Asian and Australian
  coaster parks. All 701 resolve against the live database and they now cover
  **every one of the 474 rides that carry an RCDB id**, plus 227 without one
  (dark rides, flats).
  - 629 are hand-curated with full layouts, assembled from park and
    manufacturer pages, Wikipedia and on-ride footage. RCDB is used only as a
    link target and to confirm manufacturer/model/year, exactly as it already
    is for `rcdbId`.
  - 72 carry **builder and opening year only** — the RCDB-carrying rides the
    curated passes never reached. Those two fields are facts from Wikidata
    (P176 / P571 / P729 / P1619, CC0), joined on the RCDB id both sides
    already hold. They deliberately have no `elements`: nobody has walked
    those layouts, and an invented figure is worse than none.
- Served on the attraction detail response _and_ embedded in the park response
  (one batched read per park — the frontend ride page renders from the park
  payload).
- Reverse lookup: `GET /v1/glossary/terms/:termId/attractions` and
  `GET /v1/glossary/terms/counts`. Matches the term as a track figure, a ride
  type _or_ a manufacturer in one indexed query.
- Applied by `POST /v1/admin/apply-ride-profiles` (own queue, seconds,
  idempotent). There is no upstream feed: updates happen by editing the seed
  file and re-running the job.
- `ride-profile-seed.spec.ts` guards the data against a mirrored allowlist of
  frontend term ids plus four curation invariants. The "no inversions claimed
  without an inverting figure" check caught four wrong entries on its first run
  (Maverick, Pantheon, Hyperia, Cheetah Hunt).
- The apply job now **names** the entries it skipped instead of only counting
  them. Skipping a slug that matches nothing is deliberate — ride slugs drift
  and one stale line must not fail the run — but a bare count hid twelve dead
  entries, among them Universal Studios Japan's whole coaster line-up. CI
  cannot catch this (no database), so the log is the only signal.
- `MANUAL_ATTRACTION_METADATA`: **20 more `rcdbId`s**. The first Wikidata pass
  compared names literally, so every ride whose name carries licensor styling
  was missed — we store "BATMAN™ The Ride" and "THE RIDDLER™'s Revenge" where
  Wikidata has "Batman: The Ride" and "The Riddler's Revenge". Re-matched on
  park geo-proximity plus a name comparison that strips ™/® and case, then
  each id confirmed against the ride and park name on its RCDB page. That
  confirmation step is not ceremony: the geo match proposed 4327 for Universal
  Orlando's Revenge of the Mummy, which is Singapore's ride (Orlando is 2232).

→ [Ride ↔ Glossary link](docs/frontend/ride-glossary-link.md)

### Fixed — Misdated closing times no longer make a park read CLOSED while it is open (2026-07-27)

A park's operating day is anchored to one calendar date, so the window between
`openingTime` and `closingTime` must be greater than zero and at most 24 h.
Sources break that in both directions and `saveScheduleData` stored it verbatim:

- **Closing before opening.** ThemeParks.wiki stamps a past-midnight close with
  the day's _own_ date — Parque Warner Madrid publishes
  `open 2026-07-27T12:00+02:00` / `close 2026-07-27T00:00+02:00`, putting the
  close 12 h _before_ the open. `isParkOpen` then reads CLOSED for the whole
  day, every day: the park badge, `effectiveStatus` and every derived crowd
  level said "closed" while rides reported 60-minute queues. 179 rows, 15 parks,
  concentrated in September/October (Halloween events with post-midnight hours:
  Cedar Point, Six Flags, SeaWorld, Carowinds, Kings Dominion/Island).
- **Closing far after opening.** An overshot day or a typo'd year turned one
  evening into a 34-hour (SeaWorld San Diego) or 3-year (Busch Gardens
  Williamsburg) "operating day", so those days never closed.

The time-of-day is the trustworthy part in every observed case, so
`normalizeClosingTime` re-anchors it to the opening's park-local date and rolls
it forward a day when that lands at or before opening (DST-safe: the local
closing time is preserved, not a fixed 24 h offset). An _equal_ opening and
closing is left alone — rolling it forward would invent a 24-hour operating day
out of a source that reported nothing.

The Wartezeiten sync used to **drop** any day whose closing was at or before its
opening, costing those parks their schedule; it now only skips the equal case
and lets the normalizer repair the rest.

Rows written before this fix were repaired in place on 2026-07-27 (178 windows
re-anchored). Days in the past are never re-synced, so they would have stayed
broken; `npm run repair:schedule-dates` (local script) reports and repairs both
this and the older `date`-vs-`openingTime` mismatch.

Two upstream problems this deliberately does **not** invent a fix for:

- 7 rows read `opens 15:00 / closes 12:00` (Six Flags Qiddiya City, Kings
  Dominion's September Haunt evenings) — almost certainly a 12-hour-clock error
  at the source, where 12:00 means midnight. Re-anchoring makes them 18–21 h
  days: right during the event hours, wrong overnight. A curated override would
  be the real fix.
- One row with an equal opening and closing (Universal Volcano Bay, dated
  1970-01-01) is left untouched by design.

### Fixed — Favorites: a closed park's rides no longer report OPEN and CLOSED at once (2026-07-27)

`GET /v1/favorites` could describe the same ride two contradictory ways, which the
frontend rendered as a green **OPEN** badge next to a red **CLOSED** one:

- **`effectiveStatus` was stripped from the payload** (to save bytes), so the card
  had no park-aware status left — only the raw `status`/`queues[].status`, which are
  a _last-known_ value. Sources stop publishing at closing time and `queue_data` only
  gets a row on change, so a ride's newest row keeps saying `OPERATING` for hours
  after the park shuts (measured: 132 rides across 9 closed parks at once). The
  `crowdLevel`, however, _is_ park-aware and already read `"closed"` → the two badges
  disagreed. `effectiveStatus` is now part of the response again.
- **The cache-miss path never assigned `status` at all**, leaving the hardcoded
  `"CLOSED"` placeholder from `AttractionResponseDto.fromEntity` (attractions carry no
  status column) next to live `OPERATING` queue data — visible even in _open_ parks.
  It now derives `status` from the live STANDBY queue, sets `effectiveStatus` from the
  park status, gates `crowdLevel` to `"closed"` for a closed park, and mirrors the
  integrated path's free-flow (`openWithPark`) override so a cache miss can't flip a
  playground's status.

Raw `status` and `queues[]` stay untouched (honest `lastUpdated`, honest history);
`effectiveStatus` is the field a card should render.

### Fixed — Close the remaining "invented rating" paths and the Swagger contract (2026-07-27)

Review follow-up to the two entries below. The rule they established ("anything
that cannot rate against a real baseline emits `unknown`") was stated more
broadly in the docs than the code delivered.

- **`statistics.crowdLevel` could still publish a fabricated `very_low`.**
  `calculateParkOccupancy`'s no-live-data branch returned an `OccupancyDto`
  with no `crowdLevel` at all, so the `occupancy.crowdLevel ?? getParkCrowdLevel(occupancy.current)`
  fallback recomputed from `current = 0` → `very_low`, for a park we had heard
  nothing from. That branch now states `crowdLevel: "unknown"` itself.
- **A walk-on read as "no data".** `getAttractionCrowdLevel` short-circuited on
  `waitTime === 0` _before_ looking at the baseline, so a ride reporting 0 min
  against a real P50 returned null → `"unknown"` — while `getLoadRating` rated
  the identical pair `very_low`. The attraction payload therefore carried
  `crowdLevel: "unknown"` next to `comparison: "much_lower"`. Only an absent
  wait (`null`/`undefined`) is missing data now.
- **`/v1/search` still invented ratings.** `getCrowdLevelForSearch` substituted
  `"moderate"` for a null rating, and `getBatchLoadLevels` re-derived the tier
  from `occupancy.current` instead of reading the gated `occupancy.crowdLevel`,
  bypassing the ratability gate that every other surface honours.
- **The published OpenAPI contract omitted `unknown`.** Several DTOs hand-wrote
  a six-tier `enum:` while the API had been sending `unknown` for months, and
  three declarations shared `enumName: "CrowdLevel"` with differing member
  lists. All crowd-level enums now derive from `CROWD_LEVEL_VALUES` /
  `CROWD_LEVEL_WITH_CLOSED_VALUES` in `common/types/crowd-level.type.ts`, so the
  schema cannot drift from the TS union again.

Also swept up: an orphaned JSDoc left over the wrong method, and two comments
that the rename in the entry below had made false (`ratioP90` no longer exists;
`getCurrentOccupancy` was described as using the formula it deliberately does
_not_ use — it stays park-wide `avg ÷ park-P50` because the trained models
depend on that feature distribution).

### Fixed — Live park load measures the park, not its second-busiest ride (2026-07-27)

The live park crowd level was the **P90 across the per-headliner ratios**
(`latest_wait ÷ that ride's own P50`). With the headliner set capped at 10
(`MAX_TIER1_HEADLINERS`) the P90 index `(n-1) × 0.9` lands on the
second-highest ratio, so the park rating was an extreme-value estimator over
at most ten rides — one-sided (an outlier could only push it up, never down)
and dominated by whichever ride had the smallest baseline to divide by.

Phantasialand read `high` (123%) on an afternoon when Taron sat at 20/45 min
and F.L.Y. at 20/40; both were at the bottom of the sorted list and
contributed nothing, while Crazy Bats (45/30) and Wakobato (40/30) set the
level.

- **Now a baseline-weighted mean** (`getHeadlinerLoad`): `Σ current headliner
waits ÷ Σ those rides' P50 baselines`. Same afternoon → 240/290 = 83% =
  `low`. Each ride carries the weight of the queue it represents; only rides
  that reported enter both sums, so a closed headliner leaves numerator and
  denominator together. The threshold ladder is unchanged.
- **No wait floor on this query.** The 10-minute `MIN_WAIT_TIME_THRESHOLD`
  would delete the quietest queues from a weighted mean and bias the park
  upward on exactly the days that should read low.
- **`breakdown.typicalAvgWait`** is now the baseline of exactly the rides in
  `currentAvgWait`, so the pair the park page renders divides out to the
  percentage beside it. It previously carried the park-wide P50, which is how
  a payload could show "25 min now / 30 min typical" next to `+23%`.
- **`avgWaitToday` / `peakWaitToday`** come from one per-headliner query
  (each ride's AVG and MAX today, meaned across rides). The "average" used to
  be `ParkDailyStats.p90WaitTime` — a P90 pooled over _all_ attractions —
  while the "peak" was headliner-scoped, so nothing ordered them and the page
  served `avgWaitToday: 45` beside `peakWaitToday: 40`. Per ride AVG ≤ MAX,
  so the pair is now ordered by construction.
- **Four invented ratings became `unknown`.** `statistics.crowdLevel` — the
  field the park page actually renders — re-derived the rating from the raw
  percentage and bypassed the ratability gate. Likewise `getLoadRating`
  without a baseline, the calendar's hourly predictions (rated against a
  hardcoded 25-minute reference), and the attraction live crowd level. A
  0-minute wait against a _real_ baseline is still rated: that is a walk-on,
  not missing data, so it reads `very_low`.

Docs: [Crowd Levels](analytics/crowd-levels.md) (§1, §4, §6).

### Fixed — Calendar compares past and future days on the same statistic (2026-07-27)

`headlinerForecast.avgWait` carried two different measurements depending on
which side of today a date fell. Future days come from
`getServingDailyPredictions`, where `predict.py` collapses the peak-window
hours to a per-day MAX — a day-peak proxy. Past days came from a plain AVG
over the day's readings. Same field, same UI slot, systematic step up at the
today/tomorrow seam: Phantasialand rendered ~40 min for last week beside
~65 min for next week, much of which was the statistic changing rather than
the park getting busier.

The historical side is now a per-ride **day-P90** (`getHeadlinerDailyPeaks`,
renamed from `getHeadlinerDailyAverages`), matching both the forecast side
and the numerator `calculateCrowdLevelForDate` already rates each day with.
The response field keeps its `avgWait` name — it is still the mean across
headliners. **Crowd levels were never affected**; both sides already divided
a peak by the typical-day-peak baseline.

### Changed — Calendar payload diet: influencingHolidays off by default (SEO/perf P1) (2026-07-14)

`GET /v1/parks/{path}/calendar` no longer returns per-day `influencingHolidays`
by default. That field was ~98% of the ~2.25 MB body and **no consumer of this
endpoint reads it** (the header holiday panel reads
`schedule[].influencingHolidays` from the **park** endpoint) — so the default
calendar response (grid tab + per-month client fetches) shrinks ~50×, and Next's
2 MB fetch-cache cap stops being a design constraint.

- Opt back in with **`?include=influencingHolidays`** (comma-separated list for
  future sections). Shape is otherwise unchanged.
- Stripped at the response layer, so the per-month Redis cache stays a single
  (full) variant — the win is the over-the-wire payload, and new day objects are
  returned so the shared cache entry is never mutated.

### Added — Precomputed best-days endpoint (SEO/perf P0) (2026-07-14)

New lean, precomputed endpoint that replaces the frontend's derivation of best-days /
crowd-FAQ / header-forecast content from the ~2.25 MB `/calendar` response (of which
~98 % is unused per-day `influencingHolidays`) plus its 10–20 s cold ML compute:

- **`GET /v1/parks/{continent}/{country}/{city}/{parkSlug}/best-days`** — rolling
  today → +90 days in the park timezone (optional `from`/`to`, capped at 90 days).
  Returns only the projection the frontend keeps (`date`, `status`, `crowdLevel`,
  `predictedCrowdLevel?`, `isHoliday`, `isSchoolVacation`, `isBridgeDay`) plus an
  optional stats-quality `byDayOfWeek` aggregate. Target ≤ 15 KB.
  ([DTO](../src/parks/dto/best-days-calendar.dto.ts) · [service](../src/parks/services/best-days.service.ts))
- **Materialized, never lazy.** Served from a Redis snapshot (`best-days:<parkId>`,
  26 h TTL) that the 12 h calendar warmup refreshes for every park — reusing the
  already-warm month caches, so the request path is a single GET (p99 < 300 ms cold
  and warm, no ML on request). Cache miss ⇒ `200` + empty `days` (frontend degrades);
  unknown park ⇒ `404`. This is what lets the frontend drop its SSR seed-timeout guard.
- **HTTP caching:** `public, max-age=3600, s-maxage=3600, stale-while-revalidate=86400`
  (Express weak ETag + 304 native), no auth variance → CDN-absorbed.
- **On-change revalidation webhook:** after each calendar-warmup batch the backend fires
  one batched `POST {REVALIDATE_URL} {tags:["best-days:<slug>", …]}` so the frontend
  drops its day-long cache immediately. No-op unless `REVALIDATE_SECRET` is set (keeps
  dev/test/CI from pinging production). ([service](../src/common/revalidation/revalidation.service.ts))
- `byDayOfWeek` is populated best-effort from the 24 h `/stats` cache (read-only —
  never triggers the slow 2-year aggregate); omitted when that cache is cold.

### Fixed/Changed — PCN intraday review fixes (serving, scorers, ops) (2026-07-02)

Implements priorities 1–5 of [docs/ml/pcn-intraday-review.md](ml/pcn-intraday-review.md)
(§8 has the full status table + the one-time board-reset SQL):

- **Crowd-level quantile fix (serving):** the PCN champion-swap override now derives
  `crowdLevel` from PCN **q0.8** (display stays q0.5) — restoring the documented
  per-purpose quantile split; pcn-service enforces quantile monotonicity in
  `predict_quantiles` (mirrors CatBoost's non-crossing fix).
- **5-minute wait steps restored (serving):** the override served PCN's raw q0.5 via
  `Math.round` → users saw 1-minute waits (e.g. "23 min") since the swap went live.
  `roundServedWait` now mirrors ml-service `round_to_nearest_5` + the operating
  min-10 rule at every serving boundary (read paths, park curve, deviation compare),
  and the shadow scorer quantizes PCN's column the same way so the board compares
  served-vs-served. `pcn_forecasts` stays raw.
- **Shadow scorers (pcn + shape): full-day contract.** Board rows are only (re)written
  from windows that fully cover their target_date (`full_day_window`, lookback 48h/96h,
  current partial slot excluded) — fixes the rolling-window overwrite that degraded
  matured days to their last hour (visible as lead-bucket N sums > "all" N).
  **One-time board reset required after deploy** (SQL in the review doc §8).
- **Retention + index:** `pcn_forecasts` (14d) / `shape_forecasts` (30d) pruned in the
  score jobs; `created_at` index supports the serving staleness filter + the DELETE.
- **Forecast tick efficiency:** inference fetches `PCN_FORECAST_WINDOW_DAYS=7` instead
  of the 548-day training window (fallback to full window for thin grids), and a cheap
  bounded EXISTS pre-check skips stale parks BEFORE the panel fetch.
- **Serving consistency:** the park-level hourly curve + calendar (`getParkPredictions`)
  now apply the PCN override after the cache read (cache stays CatBoost-pure);
  the deviation service measures against the actually-served PCN wait (shared
  `pcn-serving.constants.ts`); admin board `LIMIT 400` → day-proportional.
- **Model plumbing (no behavior flip):** channel-evolution contract (checkpoints store
  their trained channel list; predict selects by name → tensor channels are append-only
  and deploy-safe), new `dow_sin/dow_cos/is_weekend` tensor channels (picked up by the
  next nightly retrain), `PCN_GWN_LAYERS` env + `run_bakeoff.py --layers` for the
  receptive-field bake-off (served default stays 2 until a busy-segment win).

### Added — PCN intraday model review (docs only) (2026-07-02)

Full review of the new intraday stack (pcn-service, champion-swap serving path,
shadow scorers, boards): [docs/ml/pcn-intraday-review.md](ml/pcn-intraday-review.md).
Key findings — P0: crowd-level recomputed from PCN q0.5 instead of q0.8 in the
serving override; rolling-window scorer overwrites matured board days (pcn + shape,
visible as lead-bucket N > "all" N); park-curve/calendar/deviation consumers bypass
the PCN override. P1: `pcn_forecasts` has no retention while serving reads it per
request; every 15-min forecast tick rebuilds the full 548-day tensor per park.
P2: served GraphWaveNet receptive field is 4 slots (1 h) at layers=2 — the 192-slot
context is architecturally unreachable; DOW/holiday/schedule/weather channels from
the design doc are not in the tensor yet. No production code changed.

### Added — severe-weather warnings (MeteoGate → DWD/MeteoAlarm) (2026-06-19)

Official severe-weather warnings on the weather response. Open-Meteo provides
forecast data only (our existing "nowcast alert" is self-derived), so warnings
come from a new `WeatherWarningSource` (MeteoGate `api.meteogate.eu/warnings`,
EUMETNET → MeteoAlarm/national services; DWD for Germany), ~40 European
countries, auth via `METEOALARM_API_TOKEN`.

- `MeteoGateWarningsClient`: per-country EDR `locations` query (trailing-24h
  `datetime`, filters by `sent`), dedupe the CAP index by `alertId`, fetch the
  CAP as **JSON** (no XML), parse de/en `info`; Redis cache + circuit-breaker +
  singleflight, fail-soft.
- `weather_warnings` entity (per park × alert) + `WeatherWarningsService`:
  group parks by country, `expires > now` filter, park↔area matching (bbox →
  exact point-in-polygon), atomic per-country replace.
- `weather-warnings` cron every 15 min.
- Exposed as `warnings: WeatherWarningDto[]` on the embedded park `weather`
  object **and** the live `/weather/nowcast` (German + English).
- Frontend guide: [docs/frontend/weather-warnings.md](frontend/weather-warnings.md).

Production needs `METEOALARM_API_TOKEN` in the Coolify env.

**Follow-up (same day):** MeteoGate's German feed was found to lag badly (it
served only expired DE alerts while DWD had an active EXTREME-HEAT warning for
Brühl/Phantasialand). German parks now use **Bright Sky (DWD direct, no key,
point-based)** — warnings come pre-matched to a park's warncell (no
point-in-polygon); MeteoGate stays for the rest of Europe. Also added **segment
de-duplication**: services that slice one warning into many hourly CAP alerts
(same event/area/severity, walking `expires`) are collapsed to one row spanning
the full window (e.g. Toverland 34 → one per distinct event).

### Docs — frontend guide for the ride P50/P90 stats (`typicalWaits`) (2026-06-19)

Added [docs/frontend/ride-typical-waits.md](frontend/ride-typical-waits.md): how
to consume the typical-vs-busy peak-wait stats on the attraction detail endpoint
(`GET …/attractions/:attractionSlug` → `typicalWaits`). Documents the shape
(`weekday`/`weekend` buckets = P50/P90 of daily peaks, `byDayOfWeek`, record
`peak`), the `displayable` gate (≥ 20 operating days — render on this, not a
client threshold), country-aware weekends, the 365-day window, and 24h
freshness. The feature itself shipped 2026-06-18 (per-weekday + typical-vs-busy
peak waits on the ride route).

### Added — gate "thin-data" parks out of crowd levels, ML training & MAE (2026-06-18)

Parks with **< 30 operating days** of valid headliner data were emitting a
confident `moderate` crowd level from a median over a handful of days (Sesame
Place from 1 day, Knoebels 3, Nigloland 3, …) and polluting ML training + the
reported MAE. Now a park is "ratable" only with **≥ 30 operating days**; below
that:

- **Single source of truth:** `calculateTypicalDayPeak` returns the operating-day
  count alongside the median; `calculateP50Baseline` forces `typicalDayPeak = 0`
  (→ NULL column + no Redis key) below the threshold. Ratable ≡
  `park_p50_baselines."typicalDayPeak" IS NOT NULL`.
- **New `unknown` crowd level** ("keine Prognose"): every derived rating surface
  reads `unknown` for a non-ratable park — calendar prognosis, yearly,
  historical-stats, per-attraction, and the live/"today" rating. New helpers
  `rateOrUnknown` (typical-day-peak surfaces) and `AnalyticsService.isParkRatable`
  / `getRatableParkIds` (live/occupancy surfaces); `OccupancyDto` now carries a
  gated `crowdLevel`. Raw wait-time minutes and the numeric occupancy %
  (ML feature) are unchanged — only the rating string flips.
- **ML training** (`ml-service/db.py`) and the **reported aggregate MAE/accuracy**
  (`prediction-accuracy`, `ml-drift-monitoring`) `INNER JOIN park_p50_baselines …
typicalDayPeak IS NOT NULL`, excluding thin parks from both.
- Frontend must render the new `unknown` enum value as
  "Keine Prognose / noch nicht genug Daten".

No schema change (`typicalDayPeak` is already nullable). The gate takes effect on
the next `calculate-park-baselines` cron; ML exclusion on the next nightly train.

### Fixed — green up the unit suite (stale ML specs) (2026-06-17)

18 pre-existing failures in three ML specs were all **stale tests, not code bugs**
(no service code changed; verified against the real wiring / recalibration commits):

- `ml-dashboard.service.spec`: the Redis mock's `set: jest.fn()` returned
  `undefined`, so `redis.set(...).catch(...)` in `getDashboard` threw. Real
  `ioredis.set` returns a Promise → mock now `mockResolvedValue("OK")`.
- `ml-alert.service.spec`: the MAE→severity ladder was recalibrated for the
  q0.8-quantile era (commit on 2026-06-13; thresholds 8→**13/17/22**) but the spec
  still asserted the RMSE-era values. Spec updated to the current thresholds.
- `ml.service.spec` (`storePredictions`): the chunked save
  `save(rows, { chunk: 1000 })` (stays under Postgres' 65535 bind-param limit)
  made `toHaveBeenCalledWith(array)` fail on the extra arg. Assertion updated.

Unit suite is now fully green (476 passed, 0 failed). Note: CI only runs CodeQL,
so these never gated — but the suite was misleading.

### Fixed — per-attraction & historical-stats crowd-level calibration (2026-06-17)

- **Per-attraction calendar divided a day's peak by the attraction P50 (median)**
  (`attraction-integration.service.ts`), i.e. peak-vs-median — the same structural
  miscalibration the park calendar already fixed: a day's peak is ~1.5-2× the
  median, so a _normal_ day read elevated. It now divides by a new per-attraction
  **typical-day-peak** baseline (`AnalyticsService.getAttractionTypicalDayPeak` —
  median over operating days of the day's peak, computed on-demand from
  `queue_data_aggregates`, cached in Redis `attraction:typicalpeak:{id}`), so a
  normal day ≈ 100% = `moderate`. No P50 fallback. The previously-uncalled
  per-attraction branch of `calculateCrowdLevelForDate` was aligned to the same
  baseline (no longer P90→P50). Migrated attractions' key is evicted on merge.
- **`/historical-stats` used all-attraction `park_daily_stats`**, a different
  baseline definition than the calendar, so the same day could read a different
  crowd level on the two surfaces. It now computes **headliner-only** day-values
  from `queue_data_aggregates` and a self-consistent typical-day-peak (median of
  those day-values), matching the calendar's semantic (typical day ≈ 100%).
- **Doc reconciliation** (`crowd-levels.md`): the park calendar `day_value` is the
  raw-`queue_data` daily P90 (not hourly-slot P90s); documented the new
  per-attraction typical-day-peak.
- **New docs**: [`ml/quantile-serving-and-calibration.md`](ml/quantile-serving-and-calibration.md)
  (the full quantile→display/crowd mapping + the monotonic / `predicted_peak` /
  single-flight fixes) and
  [`development/full-db-validation-checklist.md`](development/full-db-validation-checklist.md)
  (the calibration invariants + SQL to verify against real parks, since the
  dev/CI container has no DB).
- _(Deferred: switching `stats.service.ts` percentiles from nearest-rank to linear
  is now moot for historical-stats — it no longer reads `park_daily_stats` — and
  is entangled with the outlier-cap heuristic + its unit tests, so it's left as an
  optional follow-up for the raw `/stats` endpoints.)_

### Fixed — P50/P90 consistency, cache invalidation, doc alignment (2026-06-17)

- **Yearly predictions used the abandoned peak-vs-median regime**
  (`park-integration.service.ts` `aggregateDailyPredictions`): P90-of-predicted-headliner-waits
  ÷ the **P50** baseline, while the monthly calendar's future path uses AVG-of-headliners ÷
  **typical-day-peak**. The same future date therefore read systematically busier on the yearly
  view (and for ≤10 headliners `floor(n*0.9)` was effectively the max). Now mirrors the calendar
  (AVG ÷ typical-day-peak; missing baseline → 'moderate', no P50/P90 fallback) so yearly and
  monthly agree.
- **Park merge/repair left analytics & attraction caches stale.** `invalidateParkCaches`
  (`park-cache-invalidation.ts`, keys centralized in `cache-keys.ts`) now also evicts
  `park:statistics`, `park:p50/p90`, `park:typicalpeak`, `analytics:headliners`,
  `analytics:crowdlevel:*`, `park:historical-stats`, `park:derivedHours`, `ml:park:*`, the migrated
  attractions' `attraction:integrated`/baseline caches, and the `discovery:geo:structure` skeleton.
- **Discovery geo-structure was never invalidated** (its invalidator had zero callers) — merge/repair
  now bust it.
- **Yearly ML cache was force-evicted by warmup but never re-warmed** (`cache-warmup.service.ts`),
  guaranteeing a ~15 s cold path twice a day. Warmup no longer evicts it (TTL-refreshed instead).
- **Baseline recompute didn't evict derived caches** (`analytics.service.ts` `saveP50Baselines`) —
  now evicts `park:statistics` + cached crowd levels.
- **ML dashboard "last accuracy check" was always fabricated** — `compareWithActuals` now persists
  the `ml:last-accuracy-check` marker the dashboard reads.
- **`/nearby` re-loaded every park from the DB on each request** (two full-table scans). It now
  reuses a shared, user-INDEPENDENT park-coordinate index cached in Redis (`location:parkcoords:v1`);
  the per-user distance computation and the nearby response itself stay per-request (never cached).
  Busted on merge/repair + admin flush.
- **No stampede guard on the ~15 s ML serving rebuild** — `getServingDailyPredictions` now
  single-flights concurrent cold rebuilds (calendar + yearly share one compute) via a new
  `SingleFlight` util. Also extended to discovery `getGeoStructure` (geo-skeleton build) and
  `getLiveStats` (the heavy live multi-CTE), each via an extracted `build*` method with an
  in-flight cache re-check. (Remaining follow-up: `getParkPredictions("daily")`, which needs a
  larger method extraction. Single-flight is in-process per instance — not cross-instance.)
- **`getCountrySummary` 404 path hit the DB on every probe** — now negative-cached, so crawlers
  probing bogus continent/country slugs no longer re-query.
- **Admin cache flush missed `ml:dashboard:*` and `location:*`** — added to the flush patterns.
- **Non-crossing MultiQuantile** (`ml-service/model.py`): per-row quantiles are now sorted
  monotonically so the crowd signal (q0.8) can't fall below the displayed median (q0.5) and the
  uncertainty band can't silently collapse; `nf-service` `predicted_peak` semantics documented
  (= E[daily-P90], a median forecast of a P90 target).
- **Documentation realigned** to the two-regime model (live = ratio-vs-P50, calendar =
  typical-day-peak; P90 no longer "primary"): schema, caching-strategy, system-overview,
  headliner-logic, crowd-levels, model-overview, feature-engineering-concepts, common-issues,
  neuralforecast-tft-evaluation. Fixed dead doc links, the nonexistent `OPEN_WEATHER_API_KEY`
  (Open-Meteo needs no key), `inference.py`→`main.py`, npm→pnpm, and removed the unused
  `@types/luxon`.
- **Removed dead code** `getCrowdLevelTrainingData` and corrected stale "P90 baseline /
  peak-vs-median" comments across analytics/calendar/location/park-integration.

### Added — TFT per-attraction board + MultiQuantile per-purpose serving (2026-06-13)

- **TFT best/worst board** (`ml-monitoring.controller.ts` `GET /v1/ml/monitoring/tft/performers`,
  `prediction-accuracy.service.ts` `getTftTopBottomPerformers`, frontend
  `app/admin/ml/page.tsx`). TFT daily-peak forecasts scored against realised daily P90 per
  attraction, same board hygiene as CatBoost (stddev≥8 floor). New "Per-attraction accuracy
  (TFT daily)" section in /admin/ml next to the (renamed) CatBoost hourly board.
- **MultiQuantile per-purpose serving** (`ml-service/config.py`, `model.py`, `predict.py`).
  Loss → `MultiQuantile:alpha=0.5,0.8,0.95`; serving picks the quantile per purpose: the
  wait DISPLAY uses the median (q0.5), the crowd-level uses q0.8. Backtest (21d, 392k slots):
  median cut overall MAE −24% / quiet −41% and removed the +3.4-min quiet over-read; q0.8 is
  busy-optimal (q0.95 over-shoots). Backward-compatible (falls back to the legacy path until a
  MultiQuantile model is trained). **Finding: the busy-tail "worst predictions" are inherent
  variance, not quantile- or ensemble-fixable** — q0.95 and a CatBoost+TFT daily mean both
  measured worse than the status quo. See docs/ml/tft-vs-catboost-clean-comparison.md §6.6.

### Fixed — ML dashboard hygiene: best-board, MAE alert, daily breakdown, anomalies (2026-06-13)

- **Anomaly board de-noised** (`ml-anomaly-detection.service.ts`). `unexpected_closure` was 89%
  of all anomalies (534/602 live), and ~90% of those were genuine ride closures during opening
  hours (CLOSED/DOWN/REFURBISHMENT) — operational reality, not a model defect (the model
  correctly predicted a wait for a ride that should have been running). It buried the ~68 real
  model anomalies (large_error / extreme_value on genuine rides). Removed closure detection from
  model-quality anomaly monitoring (enum kept for history); purged the 5883 existing closure
  rows. Board now shows 68 actionable anomalies instead of 602.

- **"Best predictions" board de-polluted** (`prediction-accuracy.service.ts`
  `getTopBottomPerformers`). It was dominated by 0.0-MAE non-rides — shows, walk-on/kiddie
  rides and transport mis-ingested as attractions (Hall of Presidents, Mickey's PhilharMagic,
  PEANUTS kiddie rides): their wait never varies (a 4D film "queues" a constant ~15 min), so
  the model predicts the constant perfectly. Added a stddev floor (≥8) — real rides swing
  widely (Taron 14.6, Manta 18.2, Wrath of Rakshasa 29.1) vs shows at 0-7 (Hall of Presidents
  1.9, Magiezijn 0.0). Verified live: board now shows real rides at ~3.6-4.2 MAE. Display
  filter only, no data deleted.
- **Accuracy-degradation alert recalibrated** (`ml-alert.service.ts`). Threshold 8 min
  (severity ladder 7/10/15) dated from the RMSE-loss era (MAE ~4-5); the current
  Quantile(0.8) loss predicts the upper conditional quantile by design, so live MAE sits at a
  structural ~10-12 and the alert fired permanently. Now 13 (severity 13/17/22) — catches a
  real climb, not the q0.8 baseline. Honest fix for the quiet over-read remains MultiQuantile
  serving.
- **Daily breakdown shows n/a, not 0%** (`prediction-accuracy.service.ts`, `ml-dashboard.dto.ts`).
  Per-type breakdown reported DAILY as 0% coverage / 0 MAE (reads as broken); daily predictions
  are intentionally never scored against actuals. Now `mae`/`coveragePercent` are null with an
  explicit `tracked: false` flag so the UI renders "n/a".

### Fixed — daily-prediction coverage + verified-coverage metric (2026-06-13)

Follow-up to the 2026-06-10 generate-daily fix; coverage had plateaued at ~110/160.

- **Daily park selection mirrored the hourly 3-stage net** (`prediction-generator.processor.ts`).
  `handleGenerateDaily` filtered parks with `isParkOperatingToday()` only, while the hourly
  generator also force-includes parks with recent ride activity. ~14 demonstrably-open parks
  (Energylandia, Beto Carrero, Grona Lund, Universal Studios Orlando, Chimelong, Warner Bros.
  Movie World, …) report no schedule (UNKNOWN), so the daily cron — running once at 01:00 UTC,
  local night for many — read them as closed and gave them thousands of HOURLY but ZERO daily
  predictions. Daily now uses the same net with a 24h activity window. Result: park selection
  134 → 151, successful 110 → **125 parks / 4068 attractions**.
- **Season-end filter loosened** (`ml.service.ts` storePredictions #4): only skips after the
  last OPERATING schedule day when that day is genuinely in the past (real off-season), not when
  it merely equals today (schedule-sync horizon). Energylandia (open daily, 0 future OPERATING
  entries) was discarding its whole future calendar.
- **"Verified coverage" metric corrected** (`prediction-accuracy.service.ts`). The homepage
  widget read 54% (tripping the <80% alert); the real rate is ~80-96%. `coveragePercent` reused
  the MAE-eligible count, which excludes ride closures and sub-5-min waits — conflating "did we
  check this against reality?" with "was the actual a non-trivial wait?". Measured live: 95% of
  the "uncovered" slots are rides closed/quiet _during opening hours_ (the operating-hours filter
  already works). Coverage is now `COMPLETED / total`, separate from the strict MAE filter.
- **Open-but-null rides no longer counted as unplanned closures** (`prediction-accuracy.service.ts`).
  Status-only parks (Chimelong = #1 most-popular, many Asian/water parks) report status=OPERATING
  with waitTime=null; these were scored as closures with full error, inflating MAE and dragging
  coverage down (~37% of all "unplanned closures"). They are now left uncompared (PENDING → MISSED).

### Fixed — generate-daily silently failing for ~87 of 139 live parks (2026-06-10)

Live diagnosis (injected one-off `generate-daily` Bull job, watched the logs): two
independent bugs starved most parks of fresh CatBoost daily predictions — Magic Kingdom
had none since 2026-04-10, a large cluster (HK Disneyland, Universal Singapore, Kings
Dominion, …) since ~2026-05-24. Only ~53 parks/night succeeded.

- **`storePredictions` bind-parameter overflow** (`ml.service.ts`): one multi-row INSERT
  for a big park (60 attractions × 365 days × 11 columns ≈ 240k params) exceeds the
  Postgres wire-protocol limit of 65535 bind parameters → driver fails with
  `bind message has N parameter formats but 0 parameters` (N = total mod 65536).
  Fix: `repository.save(entities, { chunk: 1000 })`.
- **`deduplicatePredictions` TimescaleDB decompression abort** (`ml.service.ts`): the
  hypertable is partitioned on `createdAt` (compress_after 14d) but the dedupe DELETE
  filtered only on `predictedTime`, so it scanned every chunk and died with
  `tuple decompression limit exceeded` (100k tuples/transaction) on parks whose stale
  forward rows had been compressed — a permanent failure loop (the stale rows could
  never be deleted, so every night failed again, before storePredictions even ran).
  Fix: dedupe scoped to `createdAt >= now() - 13 days` (uncompressed chunks only;
  compressed-batch min/max metadata on predictedTime+createdAt skips the rest).
  `deleteOldPredictions` now lifts the limit via
  `SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0` (bounded
  nightly cleanup; native 90d retention policy exists as well).
- **One-time remediation executed on the live DB**: 4.46M superseded compressed forward
  rows purged in 4 batched transactions (~30s total), so the nightly jobs start clean.

### Changed — TFT daily horizon 30 → 45 days (2026-06-10 re-eval)

Scheduled re-evaluation (due ~2026-06-14) run early; all gates passed decisively:

- **Live matched scoreboard (14d, strict lead-1)**: TFT beats CatBoost on every segment —
  ALL 8.1/−0.1 vs 11.9/−4.1, busy(P90≥40) 16.1/−7.1 vs 27.4/−25.2, headliner 11.6/−0.1
  vs 17.3/−9.0 (MAE/bias).
- **Lead degradation is shallow**: TFT MAE 8.2 (lead 1-2) → 9.3 (lead 10-13); TFT at
  lead 10-13 still beats CatBoost at lead 1-2.
- **Horizon backtests** (`nf-service/backtest_horizon.py`, headliners): h=45
  (BASE 2026-04-26) lead 31-45 ALL 15.3/−3.2, busy≥40 20.8/−11.9 — better than CatBoost
  at lead 1. h=60 (BASE 2026-04-11) lead 46-60 ALL 17.6/+7.5, busy≥40 19.2/+1.0 — also
  viable, deferred until ~8 months of history (overall bias from the thinner window).
- Series maturity: headliner median 168 operating-day points (was 72 at the h=30 gate).
- Changes: `NF_HORIZON=45` (nf-service config), `getTftDailyPredictions`/
  `getServingDailyPredictions` defaults 30 → 45 (`ml.service.ts`). CatBoost now serves
  only day 46-365 + intraday. Intraday re-run (Shanghai) confirms TFT still has no busy
  edge there (busy≥60 22.9/−14.1 vs persistence 19.7/−0.5) — intraday stays CatBoost.

### Changed — Peak-vs-median crowd level (corrects PR #46)

- **Crowd-level semantic switched again — now peak-vs-median** (`analytics.service.ts`, `attraction-integration.service.ts`, `park-integration.service.ts`, `calendar.service.ts`). Baseline is now **P50** (median, "typical wait") instead of P90; current value is **P90 of a short window** (20 min live, P90-of-slot-P90s for calendar days). The previous P90-vs-P90 design (PR #46) was mathematically apples-to-apples but methodologically off: P90 baseline is "an exceptionally busy day in the last 18 months", so most days landed in "very_low" / "low" because they didn't touch that ceiling. Peak-vs-median reads 100% when the current peak matches a typical wait, 150%+ when the queue is materially above typical — matches user intuition. Threshold ladder is unchanged. See [Crowd Levels](analytics/crowd-levels.md) for the full design.
- **Live park current value**: window shrunk from 60 min to **20 min** (`getCurrentParkPeakWait`). With 5-min sampling that's ~4 samples per ride, so the MAX-then-avg reads as a recent P90. Window auto-expands to 60 → 240 min only when the 20-min window has no qualifying data (source lag, sparse-reporting ride).
- **Calendar daily value**: from "weighted avg of hourly P90s" to **P90 of in-hours slot P90s** (`attraction-integration.service.ts`). MAX would be too fragile against single outlier slots; P90 is robust and still represents the day's actual peak hour. Filters slots by the park's opening/closing schedule so off-hours samples don't pollute the reading.
- **`attraction_hourly_history` backfill expanded** from rolling 7 days to the full data window (2025-12-24 onward). Before the backfill, days without a row were misinterpreted as "Ganztägig geschlossen" by the frontend even when raw `queue_data` had operating samples. The backfill jobs are idempotent and re-runnable per date range.
- **`p90-crowd-levels.md` → `crowd-levels.md`** (rename + rewrite). The new doc describes the corrected peak-vs-median architecture; the P90 baseline tables and Redis keys are retained as the fallback path.

### Changed — Peak-vs-peak crowd level (PR #46, since corrected)

- **Crowd-level semantic across every user-facing surface switched from P50-vs-P50 (avg vs typical avg) to P90-vs-P90 (peak vs typical peak)** (`analytics.service.ts`, `attraction-integration.service.ts`, `park-integration.service.ts`, `location.service.ts`, `search.service.ts`, `calendar.service.ts`, `ml.service.ts`). "How busy was today" is what users remember as the peak headliner experience, not the day's median — apples-to-apples math now matches that intuition. P50 stays available as a fallback for brand-new entities without a P90 row yet (still apples-to-apples, just an avg-vs-avg reading until the next cron). The threshold table (very_low / low / moderate / high / very_high / extreme) is unchanged, so the labels keep their meaning. See [Crowd Levels](analytics/crowd-levels.md) for the full architecture (this doc was renamed from `analytics/p90-crowd-levels.md` and rewritten for the current two-regime model).

- **Park live occupancy now reads per-headliner MAX in the last 60 min, averaged across headliners** (`analytics.service.ts:getCurrentParkPeakWait`) — the live counterpart to the 548-day P90 baseline, with the same shape on both sides of the comparison.

- **Calendar `peakLoad` mixed-percentile bug fixed** (`calendar.service.ts:buildPredictedCrowdLevels`): the ML-prediction path used to divide the predicted P90 by the P50 baseline, which systematically inflated peakLoad readings. Now uses P90 baseline; peakLoad and crowdLevel are both peak-vs-peak.

- **ML pipeline forwards both baselines** (`ml.service.ts`, `prediction-request.dto.ts`): the Python service now receives both `p50Baseline` and `p90Baseline` in every prediction request. Training labels in `getCrowdLevelTrainingData` switched to day-P90 ÷ P90 baseline — models recalibrate within ~1 daily cycle.

- **Attraction history utilization fixed** (`attraction-integration.service.ts`): the per-day "utilization" badge on the attraction history chart used to compare the day's weighted avg wait against either P50 baseline or today's intra-day P90 — a mixed-percentile reading that drifted between days. Now uses weighted avg of hourly P90s ÷ attraction P90 baseline (peak-vs-peak), matching every other surface.

### Added — P90 baseline infrastructure (PR #46)

- **`park_p90_baselines`, `attraction_p90_baselines` tables** populated by the existing P50 cron at 3 AM / 4 AM. PostgreSQL produces both percentiles from one PERCENTILE_CONT sort, so the additional rows cost nothing on top of the existing P50 scan. Cached in Redis (`park:p90:{id}`, `attraction:p90:{id}`, 24 h TTL).

- **Read API**: `AnalyticsService.getP90BaselineFromCache`, `getAttractionP90BaselineFromCache`, `getBatchAttractionP90Baselines` (MGET + DB hydrate + pipeline writeback). Replaces the live-aggregation `getBatchAttractionP90s` on every hot path.

### Added — Pre-aggregated hourly history (PR #46)

- **`attraction_hourly_history` table + `AttractionHourlyHistoryProcessor`** (daily 04:30): pre-aggregates yesterday's per-attraction 15-min-slot P90/avg/sampleCount breakdown into one JSONB blob per `(attractionId, date)`. The attraction history endpoint now reads past days from this table (one indexed SELECT) and computes only today's slots live — replaces the previous always-live PERCENTILE_CONT scan of the entire window (typically 30 days × ~96 slots = ~2,880 percentile groupings per cold hit, per attraction).

### Removed (PR #46)

- **`OccupancyCalculationProcessor` + `occupancy-calculation` queue + `precompute-p90-sliding-window` job**: the precompute wrote a Redis cache (`analytics:percentile:sliding:*`) that nobody reads any more — the P90 baseline lives in the new tables. ~10 k heavy queries per night eliminated; the orphaned Redis keys TTL out within 24 h of deploy.

- **`AnalyticsService.get90thPercentileWithConfidence`, `get90thPercentileSlidingWindow`, `getBatchAttractionP90s`**: live 548-day PERCENTILE_CONT methods, fully replaced by the cache-table-backed read API.

- **`AttractionsMetadataProcessor`, `ShowsMetadataProcessor`, `RestaurantsMetadataProcessor` + their Bull queues** (`attractions-metadata`, `shows-metadata`, `restaurants-metadata`): all three were marked `@deprecated Phase 6.2` but still instantiated by Nest. `ChildrenMetadataProcessor` has covered their work since the combined sync landed.

### Performance (PR #46)

- **Wait-times processor `processLandData` N+1** (`wait-times.processor.ts`): the every-5-min land-info sync did a SELECT + 2 UPDATEs per attraction (~3,000 queries per run for a 100-attraction park). Now bulk-fetches current land/queueTimesEntityId in the same SELECT used for IDs, diffs in memory, and groups UPDATEs by target land. Steady-state: 0 UPDATEs.

- **`/v1/analytics/realtime` LATERAL subquery** (`analytics.service.ts:getGlobalRealtimeStats`): two correlated subqueries per park (a COUNT + a per-attraction latest-status LATERAL JOIN) replaced with a single `attraction_counts` CTE that LEFT JOINs the already-computed `latest_updates` and aggregates via FILTER. Cache-miss latency falls from seconds to ms.

- **ML alert auto-resolve N+1** (`ml-alert.service.ts`): per-alert `find()` + N×`save()` replaced with one UPDATE matching the same WHERE filter.

### Added

- **Full statistics on `/v1/analytics/realtime`** (`analytics.service.ts`, `global-stats.dto.ts`): `longestWaitRide` and `shortestWaitRide` now expose today's full statistics — `avgWaitToday`, `minWaitToday`, `peakWaitToday`, `peakWaitTimestamp`, `typicalWaitThisHour`, `currentVsTypical` — in addition to the existing `sparkline` field. Values are fetched via `getAttractionStatistics`, the same method used by the attraction detail endpoint, so they are always consistent. Frontend no longer needs to fill these fields with `null`.

- **Sparklines on `/v1/analytics/realtime`** (`analytics.service.ts`, `global-stats.dto.ts`): `longestWaitRide` and `shortestWaitRide` now include a `sparkline` field — an array of `{ timestamp, waitTime }` pairs covering today's operating window. Each ride uses its own park's timezone and schedule opening time as the window start (identical to the park controller), so rides from e.g. Tokyo and Orlando both show the correct local-day history.

- **`getAttractionSparklinesBatch`** (`analytics.service.ts`): New helper on `AnalyticsService` for fetching sparklines when attractions may span multiple parks. Groups by `parkId`, calls `getEffectiveStartTime` once per park, batches `getBatchAttractionWaitTimeHistory` per group, and merges results into a single `Map<attractionId, SparklinePoint[]>`. Use this for any multi-park context (global stats, recommendations, …); use `getBatchAttractionWaitTimeHistory` directly when you already hold a shared `startTime` for a single park. See [Sparklines](analytics/sparklines.md).

### Added

- **`crowdLevel` on `ParkReference.analytics.statistics`** (`discovery.service.ts`, `geo-structure.dto.ts`): The `/v1/discovery/geo`, `/v1/discovery/continents`, `/v1/discovery/continents/:continent`, and `/v1/discovery/continents/:continent/:country` endpoints now expose the park's current live crowd level inside `analytics.statistics` alongside `avgWaitTime`. Previously the only source of live crowd level on these endpoints was `currentLoad.crowdLevel`, which could be `null` even when wait-time statistics were available — causing the frontend's Popular Parks section to render wait times without a crowd badge. `analytics.statistics.crowdLevel` is now co-present with `avgWaitTime` whenever the park has a valid P50 baseline. The other discovery routes already expose live crowd level via their existing shapes: `/v1/discovery/:continent/:country` (`ParkResponseDto.analytics.statistics.crowdLevel`) and `/v1/discovery/nearby` (`analytics.crowdLevel` per park/ride).

### Changed

- **Shared crowd-level utility** (`common/utils/crowd-level.util.ts`): Extracted the P50-relative occupancy → CrowdLevel threshold ladder (very_low/low/moderate/high/very_high/extreme) into a single reusable function. `AnalyticsService.determineCrowdLevel` now delegates to it (all ~20 existing call sites unchanged), and `DiscoveryService.hydrateStructure` uses it directly. Thresholds now exist in exactly one place.

### Added

- **Smart Gaps: Historical Hour Reconstruction** (`docs/analytics/smart-gaps.md`): Automatically reconstructs park opening/closing hours for past days using a 15-minute sliding window and 10% attraction activity threshold (rides with waitTime >= 5 min only). Includes rounding to nearest full hour and strict exclusion of service points (bars, snacks) via name-based blacklist.
- **`isEstimated` flag for Calendar API**: New per-day flag in `CalendarDay` to signal reconstructed historical data.
- **`hasOperatingSchedule` flag for Parks API**: New per-park flag to signal if a park provides an official API calendar (true) or relies on inference/estimates (false). Added to all park-related DTOs and Nearby responses.
- **Automated Seasonal Detection**: Logic to identify "Seasonal Parks" (winter gaps > 21 days) to suppress crowd predictions during off-season while allowing them for year-round parks with UNKNOWN schedule.

### Changed

- **Optimized Seasonal Check**: Accelerated `isParkSeasonal` query by 120x (from 72ms to 0.6ms) using SQL Window Functions (`LEAD`).
- **ML Feature Context Alignment**: ML service now receives real-time reconstructed opening hours instead of static 9/10 AM fallbacks, improving prediction accuracy for "No-Schedule" parks.
- **Batch Processing for DTO Enrichment**: Introduced `getBatchHasOperatingSchedule` to prevent N+1 queries when listing parks.

### Added

- **Training roadmap doc** (`docs/ml/training-roadmap.md`): Tracks known ML issues, data quality analysis, and next steps for training improvements including UNKNOWN park inclusion strategy.
- **Reverse reconciliation for stale attractions** (`wait-times.processor.ts`, docs: `docs/architecture/reverse-reconciliation.md`): Attractions that disappear from every upstream source (ThemeParks.wiki, Queue-Times, Wartezeiten) for >24h are now auto-closed. A Redis `attraction:last-seen:{id}` key is touched only by real source sightings (never by the heartbeat). After each park's 5-minute sync the processor diffs seen vs. known attractions and writes a `status=CLOSED` `queue_data` entry for any attraction stale for >24h. Grace period of 24h protects newly created rides from premature closure, and the safety guard `seenAttractionIds.size > 0` prevents mass-close during upstream outages. The hourly heartbeat now also skips stale attractions instead of preserving their last `OPERATING` status. Fixes Movie Park Germany's Halloween mazes (e.g. _A Quiet Place_) showing "open, 0 min" year-round.
- **`POST /admin/detect-seasonal`** (`admin.controller.ts`, `admin.module.ts`): Manual trigger for the `detect-seasonal` analytics job (normally daily at 2:30 am). Intended to re-evaluate seasonal flags after deploying the reverse-reconciliation fix so newly `CLOSED` attractions get `isSeasonal=true` + `seasonMonths` populated without waiting for the cron.

### Fixed

- **Stale "open with 0 min" status for disappeared attractions** (`wait-times.processor.ts`): Previously `writeHourlyHeartbeats` re-stamped `lastUpdated=now` with the previous `status` every hour for any attraction missing from the feed, so seasonal Halloween mazes and silently-removed rides remained `OPERATING` forever. The heartbeat now reads `attraction:last-seen:{id}` and skips attractions not seen in any source for >24h; the reverse-reconciliation step has already written `CLOSED` for them. Root cause was the missing counter-signal: no upstream source ever reports "this attraction no longer exists".
- **P50 baselines missing for UNKNOWN parks** (`analytics.service.ts`): `identifyHeadliners` and `calculateAttractionP50` both filtered `scheduleType = 'OPERATING'`. Parks with UNKNOWN schedule entries (USJ, Universal Studios, Warner Bros Movie World, Blackpool etc.) had no headliners identified → no P50 baseline → `getCurrentOccupancy` returned hardcoded 100 → `park_occupancy_pct = 100` flat for all UNKNOWN parks at inference (feature useless for 22+ parks). Fixed: changed both queries to `IN ('OPERATING', 'UNKNOWN')`. Safe because both queries already filter `qd.status = 'OPERATING'` AND `qd.waitTime >= 10` — truly closed parks produce 0 qualifying rows regardless of schedule type.

---

### ML: ride-based park open/closed detection

- **ML: ride-based park open/closed detection** (`parks.service.ts`): `getBatchParkStatus` and `isParkOperatingToday` now derive open/closed status from live ride data when no confirmed schedule exists. Threshold: ≥3 attractions with recent data AND ≥25% reporting `waitTime ≥ 5 min`. Window: 2h for real-time status, park-local today for daily planning. Parks with explicit `CLOSED` schedule today are excluded from the heuristic.
- **ML: `parkLiveStatus` feature context** (`ml.service.ts`, `predict.py`, `feature-context.type.ts`): NestJS now passes `featureContext.parkLiveStatus` to the Python ML service. In `predict.py`, UNKNOWN-schedule rows are corrected to `is_park_open=1` when the park is confirmed OPERATING via ride data. Explicit CLOSED entries are never overridden. Fixes predictions for parks like Six Flags, Universal, and other parks that report UNKNOWN schedule but are genuinely open.
- **ML dashboard: model metrics history endpoint** (`GET /v1/ml/models/metrics-history?limit=50`): Returns MAE, RMSE, MAPE, R² per trained model ordered oldest→newest for sparkline charts. See integration guide for frontend.

### Fixed

- **UNKNOWN schedule parks excluded from prediction generation** (`parks.service.ts` `isParkOperatingToday`): Parks with `scheduleType=UNKNOWN` (e.g. Six Flags, Universal Hollywood, 66 parks affected) were treated the same as CLOSED — no predictions generated. Fixed: UNKNOWN falls through to ride-data check; if no data, defaults to `true` (conservative).
- **`getBatchParkStatus` heuristic over-filtered** (`parks.service.ts`): Previous filter excluded parks that ever had any OPERATING schedule entry, making the heuristic dead code for most UNKNOWN parks. New filter: only exclude parks with explicit CLOSED schedule today (park-local timezone via `AT TIME ZONE` join). Threshold raised from `waitTime > 0` (any single ride) to ≥25% with `waitTime ≥ 5`.
- **`CURRENT_DATE` UTC vs. park-local** (`parks.service.ts`): CLOSED-schedule exclusion query used `date = CURRENT_DATE` (UTC), which could match wrong date for UTC+ parks at night. Fixed with `(CURRENT_TIMESTAMP AT TIME ZONE p.timezone)::date` via JOIN on parks.
- **Daily predictions: `parkLiveStatus` always "CLOSED" at night** (`prediction-generator.processor.ts`): Daily prediction generator called `getBatchParkStatus` at runtime (e.g. 02:00 UTC), getting `"CLOSED"` for parks outside operating hours → UNKNOWN override never fired. Fixed: parks that pass `isParkOperatingToday` now receive `liveStatus="OPERATING"` explicitly.
- **ML training UNKNOWN filter reverted** (`ml-service/db.py`): Including UNKNOWN-schedule parks in training data caused MAE to jump from ~5.9 → 14.4 min and R² to drop from 0.86 → 0.37. Root cause: UNKNOWN days include closed parks still sending 5-min sentinel values — the filter can't distinguish real operating data from sentinel data. Reverted to `scheduleType = 'OPERATING'` only. The training/inference asymmetry for UNKNOWN parks is accepted; `parkLiveStatus` correctly handles them at inference time without needing training examples.
- **`park_has_operating` UUID type mismatch** (`ml-service/predict.py`): Dict key built from `schedules_df["parkId"]` could be a UUID object while `row["parkId"]` was a string → silent dict miss → UNKNOWN override never fired. Fixed: `astype(str)` on groupby key + `str(row["parkId"])` at lookup.
- **Dead code in `features.py`** (`ml-service/features.py`): `parkLiveStatus` override block in `add_park_schedule_features` was unreachable (only called during training where `feature_context=None`). Removed; the authoritative override is in `predict.py`.

### Weather forecast in integrated park response

### Fixed (weather)

- **Weather DATE timezone off-by-one** (`weather.service.ts`): Two bugs caused non-UTC parks to show wrong weather. (1) Save used `fromZonedTime(midnight, tz)` → east-of-UTC parks (e.g. `Europe/Berlin`) stored dates shifted -1 day (March 31 saved as March 30). (2) Query used `DATE(weather.date AT TIME ZONE :tz)` — PostgreSQL casts DATE to midnight-UTC timestamptz first, then shifts back to local time, which for west-of-UTC parks (e.g. `America/New_York`) moves today's date to yesterday → `current` always null. Fixed: save uses noon-UTC (`new Date(\`${date}T12:00:00Z\`)`), query uses direct date-string comparison (`weather.date >= :start`).
- **Weather empty for US parks** (root cause above): Parks like "Universal's Epic Universe" returned `weather: { current: null, forecast: [] }`. The park has coordinates and Open-Meteo data; the off-by-one query excluded today's DB record. (`park-integration.service.ts`, `park-with-attractions.dto.ts`): The integrated park endpoint now returns `weather.forecast` (next 6 days) in addition to `weather.current`. Previously `getCurrentAndForecast()` fetched 16 days from DB but only `current` was mapped into the response. The API now exposes today + 6 forecast days (7 total).
- **Weather architecture doc** (`docs/architecture/weather.md`): Documents Open-Meteo sync strategy, storage schema, BullMQ jobs, timezone handling, DATE timezone bug pattern, and why parks may have empty weather (missing lat/lng coordinates).
- **Weather cache TTL extended** (`weather.service.ts`): Increased from 30 minutes to 2 hours. Weather data changes at most twice a day (sync at 00:00 and 12:00 UTC); frequent cache misses caused unnecessary DB load.

### Fixed

- **P50/headliner: `waitTime >= 10` filter** (`analytics.service.ts`, `calendar.service.ts`, `stats.service.ts`, `attraction-integration.service.ts`): All historical wait-time aggregations (headliner identification, P50 baseline calculation, weekday averages, percentiles, longest waits) used `waitTime > 0`, while the real-time path used `minWaitTime=5`. Queue-Times API reports `waitTime=1` as a walk-on/no-queue placeholder (common for water parks, e.g. Rulantica slides). This caused ~40–65% of water-park samples to be 1-minute placeholders, depressing P50 baselines and causing "Extreme" crowd level while individual rides showed normal waits. Fixed by aligning all historical queries to `waitTime >= 10`. The existence check `hasQueueDataInWindow` is intentionally kept at `> 0`.
- **P50/headliner: schedule-based closed-day exclusion** (`analytics.service.ts`, `calendar.service.ts`): Seasonal parks (Kennywood, Canada's Wonderland) accumulate queue data during off-season months. Without filtering, closed-day data drags P50 baselines down (e.g., Kennywood: 31 raw data days → 7 OPERATING days). Fixed by adding a `LEFT JOIN schedule_entries` (park-level, `attractionId IS NULL`) to all historical queries, using `DATE(qd.timestamp AT TIME ZONE <park_tz>)` for correct local-date matching. Days with no schedule entry are included; days with `OPERATING` are included; any other type is excluded.
- **ML training: same `>= 5` and schedule filters** (`ml-service/db.py`): Training data extraction used `waitTime >= 0` and had no schedule filter. Now applies `waitTime >= 10` and the same schedule JOIN (with `JOIN parks p` for timezone). Requires retraining to take effect.
- **ML training: `fetch_recent_wait_times` `>= 5` filter** (`ml-service/predict.py`): Inference recent-wait lookup also aligned to `waitTime >= 10` + schedule JOIN.
- **ML: historical occupancy DOW×hour timezone bug** (`ml-service/db.py`): `fetch_historical_park_occupancy` built the (DOW, hour) occupancy profile using `EXTRACT(DOW/HOUR FROM qd.timestamp)` (UTC), but inference looked up with local park time → systematic 1–2 hour shift for all non-UTC parks. Fixed by joining `parks` and using `AT TIME ZONE p.timezone` in the GROUP BY. Since `queue_data` has no `parkId`, the join path is `queue_data → attractions → parks`.

### Changed

- **DB indexes: remove unused** (`ml-prediction-request-log.entity.ts`, `park-p50-baseline.entity.ts`, `attraction-p50-baseline.entity.ts`, `attraction.entity.ts`, `park.entity.ts`, `ml-model.entity.ts`): Removed ~182 MB of unused indexes from `ml_prediction_request_log` (6 indexes with 0–2 scans) and 6 further duplicate/zero-scan indexes across other entities. TypeORM `synchronize: true` creates new indexes but does not drop removed ones; `scripts/drop-unused-indexes.sql` must be run once on production.
- **DB index: new partial index for schedule JOIN** (`schedule-entry.entity.ts`): Added `idx_schedule_park_date_no_attraction` — partial index on `(parkId, date) WHERE "attractionId" IS NULL`. Covers the `schedule_entries` lookup in all analytics and ML historical queries without touching attraction-level schedule rows.

- **ML: 5-minute prediction bug** (`model.py` `predict_with_uncertainty`): `virtual_ensembles_predict` was called with `prediction_type="TotalUncertainty"`, which returns uncertainty scalars `[knowledge_unc, data_unc]` (shape `(n, 2)`), not per-ensemble predictions. `np.mean(axis=1)` averaged the two ~2.77 values → `round_to_nearest_5` → **5 min** for all predictions. Fixed by switching to `prediction_type="VirtEnsembles"` (shape `(n, 10, 1)`), squeezing to `(n, 10)`, and taking `median ± std` instead of `p5/p95` (more stable at n=10).
- **ML: NoneType crash in `fetch_holidays`** (`db.py`): `sorted(country_codes)` failed when the list contained `None` (parks with missing country metadata). Fixed by filtering: `country_codes = [c for c in country_codes if c is not None]`.
- **ML: Weekend underprediction** (`features.py`, `predict.py`, `config.py`): `volatility_7d` dominated feature importance at 32.91% while `is_weekend` was 0.01% and `avg_wait_last_1h` was 0.00%. The model could not distinguish weekday vs weekend crowd levels. Fixed by:
  - Splitting `volatility_7d` into `volatility_weekday` + `volatility_weekend` in training pipeline (`calculate_trend_volatility`)
  - Adding `rolling_avg_weekday` + `rolling_avg_weekend` via SQL window functions in `fetch_recent_wait_times`
  - Adding `avg_wait_same_dow_4w` (mean of last 4 same-day-of-week observations) for a stable historical reference
  - Lowering `VOLATILITY_CAP_STD_MINUTES` from 40 → 15 to reduce volatility dominance
  - All new features propagated to inference in `predict.py`
  - Detailed analysis: [Prediction Quality Issues](ml/prediction-quality-issues.md)

- **ML: Flat future predictions / hour importance 0.84%** (`features.py`, `db.py`, `predict.py`): `park_occupancy_pct` (15% importance) was broadcast from the current real-time value to ALL prediction rows — including rows 24h or 14 days in the future — causing flat, hour-invariant predictions. Fixed in two stages:
  - **Inference fix** (`db.py`, `features.py`): `fetch_historical_park_occupancy()` computes expected park occupancy by (DOW, hour) over the last 8 weeks (via `attractions` JOIN, since `queue_data` has no `parkId`). `add_park_occupancy_feature` now applies real-time occupancy only to rows within ±2h of base_time; future rows use the DOW×hour historical profile.
  - **Training fix** (`features.py`, `config.py`): Occupancy Dropout — 30% of training rows have their actual `park_occupancy_pct` replaced with the DOW×hour mean from the same park's training data (`OCCUPANCY_DROPOUT_RATE=0.30`). This teaches the model to rely on `hour`/`day_of_week` when occupancy is approximate, closing the gap for future predictions.

- **Schedule date-shift bug** (`saveScheduleData`): ThemeParks.wiki returns dates as date-only strings (`"YYYY-MM-DD"`). These were passed to `new Date()`, producing midnight UTC, which `formatInParkTimezone` then shifted back by one day for parks west of UTC (e.g. a park with `date:"2026-03-02"` was stored as `2026-03-01` in America/New_York). Fix: detect date-only strings via regex and use them directly without timezone conversion. Full ISO timestamps (from wartezeiten/queue-times processors) still go through `formatInParkTimezone`. (Bug: today's schedule entry stored under yesterday's DB date; opening hours were 1–2 days off in live DB for US parks.)
- **Holiday date range in `saveScheduleData`**: Date range for holiday pre-fetch was built from `new Date(e.date)` (midnight UTC), causing `formatInParkTimezone` to shift the range back 1 day for US parks. Fixed: use noon-UTC timestamps (`${dateStr}T12:00:00Z`) consistent with the rest of `saveScheduleData`.
- **Weather service date filter** (`weather.service.ts`): `allWeather.find()` and `.filter()` used `formatInParkTimezone(new Date(w.date), tz)` on a TypeORM DATE column (midnight UTC). For US parks this shifts midnight UTC to the previous calendar day, causing today's weather entry to be lost (not matched as "current" and excluded from "forecast"). Fixed: extract date string via `w.date.toISOString().split("T")[0]`, which is always correct because midnight UTC IS the calendar date stored in the DB.
- **Schedule response missing today's entry** (`buildIntegratedResponse`): Added filter `date >= todayInParkTz` to trim past entries (DB query fetches from -2 days), and a synthetic OPERATING entry for today if the park is operating but its schedule row is missing.
- **`peakHour` timezone ambiguity** (`analytics.service.ts`): Changed from returning `"HH:mm"` (plain string, interpreted as UTC by frontend) to a full ISO-8601 datetime with timezone offset (`"2026-03-02T11:00:00-05:00"`), eliminating frontend UTC misinterpretation.
- **Cache invalidation on INSERT** (`saveScheduleData`): `invalidateScheduleCache` was only called after UPDATE, not after INSERT. New entries would remain stale for up to 1 hour. Fixed: call `invalidateScheduleCache` after INSERT too.

### Changed

- **Calendar API:** UNKNOWN→OPERATING upgrade only for parks **without** OPERATING entries in `schedule_entries`. Parks with schedule integration keep UNKNOWN for days without schedule (DB-check via `hasOperatingSchedule`). Fixes Phantasialand Jan 26–31 incorrectly showing OPERATING.
- **Gap-fill** (`fillScheduleGaps`): Look-back added. Range: (today - 182 days) through (today + 182 days). Past gaps (e.g. winter closure Jan–Mar) are re-evaluated when new OPERATING (e.g. March 28) arrives, so UNKNOWN→CLOSED is promoted correctly.

### Performance

#### Schedule Sync Optimizations (NestJS)

- **Schedule sync (`saveScheduleData`)**: Batch DELETE operations for cleanup placeholders (UNKNOWN/CLOSED removed when API provides real data) reduced from ~300 individual queries to **3 batch queries** (99% reduction). Code deduplication: normalize scheduleType once instead of 3× redundant iterations.
- **Gap-fill (`fillScheduleGaps`)**: Batch INSERT/UPDATE operations for gap-filled entries and status changes reduced from ~364 individual queries to **~5 batch queries** (98.6% reduction). All iterations collect entries/updates in-memory, then execute bulk operations using `createQueryBuilder().insert()` and `whereInIds()`.
- **Duplicate cleanup (`cleanupDuplicateScheduleEntries`)**: SQL window functions and CTEs replace N+1 queries; same-type and cross-type duplicate detection reduced from ~160 queries to **2 queries** (98.8% reduction). Uses PostgreSQL `ROW_NUMBER()` OVER (PARTITION BY) for efficient deduplication.
- **Per-park cleanup**: New `cleanupDuplicateScheduleEntriesForPark()` method called before gap-fill to prevent duplicates from parallel schedule syncs (runs targeted cleanup for single park instead of waiting for daily global cleanup).
- **Operating date range extraction**: New `getOperatingDateRange()` helper extracts min/max OPERATING date logic into reusable function (used by gap-fill classification and calendar fallback).

**Schedule sync impact**: Typical schedule sync reduced from ~924 database queries to ~12 queries (**98.7% reduction**), estimated duration improvement from ~92 seconds to ~1.2 seconds.

#### ML Service Optimizations (Python) – 2026-02-15

- **Database query caching**: Added in-memory caching for holidays (1h TTL), schedules (5min TTL), recent wait times (2min TTL), and weather historical data (1h TTL). Reduces repeated queries for unchanged data.
- **Query optimization with window functions**: `fetch_recent_wait_times` now pre-computes `rolling_avg_7d` and `rolling_std_7d` using PostgreSQL window functions instead of Python aggregation. Reduces data transfer and eliminates expensive Python loops.
- **Holiday lookup vectorization**: Replaced loop over 1000+ prediction rows with pandas `.map()` operations. Pre-processes park metadata once instead of per-row. Eliminates JSON parsing in loop.
- **Historical features optimization**: Uses pre-computed rolling averages and standard deviations directly from database instead of Python calculations.

**ML service impact**:

- First request (cold cache): **40-50% faster**
- Cached requests (warm cache): **70-85% faster**
- Daily predictions (365 days): up to **90% faster**
- Database query reduction for repeated requests: **80-90%** fewer queries

**Documentation**: [ML Performance Optimizations](ml/performance-optimizations.md)

---

## [4.6.2] – 2026-02-08

### Changed

- **Schedule sync / Gap-fill**
  - **Doc:** "When gap-fill runs (DB updates are automatic)" in [Schedule Sync & Calendar](architecture/schedule-sync-and-calendar.md): gap-fill runs after every schedule sync (sync-all-parks, sync-schedules-only, sync-park-schedule); optional job `fill-all-gaps` for all parks. No one-off DB correction needed when using park-timezone range.
  - **lookAheadDays:** Default increased from 90 to **120 days** so the DB is filled further ahead (typical 4‑month planning).
- **Calendar warmup:** Range extended from "current + 2 months" to **-1 to +3 months** (last month through 3 months ahead, park timezone) so the typical user range (recap + planning) is cache-hot after daily warmup.

---

## [4.6.1] – 2026-02-08

### Added

- **Calendar, Schedule & ML rules doc** (`docs/architecture/calendar-schedule-and-ml-rules.md`): Single source of truth for status (OPERATING/CLOSED/UNKNOWN), crowd level, schedule sync, next schedule, and ML alignment.
- **Frontend doc** (`docs/frontend/calendar-schedule-status.md`): How to display calendar status (UNKNOWN vs CLOSED) in the UI. Linked from CLAUDE.md.
- **Changelog** (`docs/changelog.md`): This file; linked from CLAUDE.md.
- **Timezone Audit** (`docs/development/timezone-audit.md`): Audit of all time operations against park timezone. Linked from CLAUDE.md.

### Changed

- **Calendar API**
  - Status is derived only from schedule (and the rule "past/today + crowd level = OPEN"); crowd level no longer overrides status.
  - Past and today: if no schedule but we have a (non-closed) crowd level → treat as OPERATING so we can show data.
  - Future: use schedule (OPEN/CLOSED/UNKNOWN); future days without schedule stay UNKNOWN but get a crowd prediction (ML or fallback "moderate"), not "closed".
- **Schedule sync**
  - `saveScheduleData`: API type "Closed"/"CLOSED" (case-insensitive) is normalised to `ScheduleType.CLOSED` so off-season (e.g. Phantasialand February) is stored when the API provides it. When saving OPERATING, any gap-fill CLOSED for that date is now deleted so the API entry takes precedence.
  - **Gap-fill** (`fillScheduleGaps`): Missing days are now classified as CLOSED or UNKNOWN:
    - **CLOSED** if there is at least one OPERATING day before and one after the gap (strictly between min/max OPERATING dates).
    - **UNKNOWN** if the park has no OPERATING entries, or the gap is before the first OPERATING date (e.g. before we have data), or on/after the last OPERATING date (schedule not yet published).
    - Existing UNKNOWN entries can be updated to CLOSED when re-running gap-fill if they are now "in the middle". OPERATING and API-provided CLOSED are never overwritten.
  - Gap-fill range uses **park timezone** (`getStartOfDayInTimezone`, `addDays`) so the filled range is always "today" through "today + 90" in the park's calendar.
- **Docs**
  - All relevant docs translated to English (frontend calendar status, review, troubleshooting peak hour section, calendar-schedule-and-ml-rules).
  - Schedule sync & calendar doc: new "Gap-fill rules" section; UNKNOWN vs CLOSED and Gaps sections updated.
  - CLAUDE.md: added Frontend section and link to Calendar, Schedule & ML Rules; Critical Rules strengthened (park timezone for all time operations); link to this changelog and Timezone Audit.

### Fixed

- Calendar no longer shows "Öffnungszeiten noch nicht verfügbar" for days that are known to be closed (gap-fill and API CLOSED now set status CLOSED where appropriate).
- When the API provides OPERATING for a date that had a gap-fill CLOSED, the calendar could show CLOSED (because `getSchedule` orders by scheduleType ASC). `saveScheduleData` now deletes any CLOSED row for that date when saving OPERATING.
- **Timezone audit:** All time operations now use park timezone. Fixed: `getUpcomingSchedule` (range in park TZ), `weather.service` fallback + `markPastDataAsHistorical` (per-park), `getBatchParkHours` (per-park today), `getParkPercentilesToday` / `getAttractionPercentilesToday` (startOfDay in park TZ), `tomorrowInParkTz` (getTomorrowDateInTimezone), `isParkCurrentlyOpen` / `isParkOperatingToday` (getCurrentDateInTimezone).

---

## [Older versions]

Older changes were not recorded in this changelog. From this version onward, notable changes will be listed here with version and date.

---

(Compare URLs can be added when using a Git remote, e.g. `[4.6.1]: https://github.com/owner/repo/compare/v4.5.0...v4.6.1`.)
