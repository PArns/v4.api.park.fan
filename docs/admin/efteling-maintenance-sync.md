# Efteling maintenance calendar → works periods (PAR-1245)

Once a week the API reads Efteling's own maintenance calendar,
<https://www.efteling.com/en/park/information/in-maintenance>, and keeps the park's rides'
[works periods](../frontend/works-period.md) in line with it. Everything else about works periods
is still curated by hand ([curation](curation.md)).

- **Job:** `manual-metadata` / `sync-efteling-maintenance`, Monday 10:20 UTC
  ([job queues](../architecture/job-queues.md)); by hand `POST /v1/admin/sync-efteling-maintenance`
  (editor), body `{ dryRun? }`. The job's return value (Bull's `returnvalue`, kept for the last 100
  runs) is the per-ride diff: `added`, `updated`, `removed`, `skipped` with a reason, plus
  `unmatched` names and `unparsed` rows.
- **Code:** parser `src/external-apis/efteling/efteling-maintenance.parser.ts` (fixture of the
  2026-11-06 page beside it), sync `src/admin/content/efteling-maintenance-sync.processor.ts`.
- **One request** per run, with the browser headers; nothing is fetched per ride.

## Reading the page

Each row is a ride link and a date text. `November 9 until December 4, 2026 inclusive` is a window;
`December 8, 2026` is one day. A start without a year takes the end's year, or the year before when
its month comes after the end's. `until November 6, 2026` (no start) is a window that began before
the sync saw it: it is stored with `from` = the park-local day the sync **first** saw it, and later
runs keep that day. Never an open start, which would cover the ride's whole past and hide its outage
history. A row in any other form is reported as `unparsed`.

**A page without the "Closed" list, or one whose list parses to no window, fails the job and writes
nothing**: that is a layout change, and the failure shows on the admin's queue card and in the
nightly failing-jobs report.

## Matching

Park by slug `efteling` (exactly one, or the job fails). Each name to a non-retired ride of that
park, exactly on `name`, then on `curatedName`, ignoring case, diacritics and whitespace. No match,
or two, is `unmatched`, logged and returned; nothing is guessed. A missing ride is fixed by giving
it the page's name as `curatedName`.

## Which windows are the sync's

Every window it writes carries `source: "efteling-maintenance-calendar"`; a window without `source`
is hand-written. The marker is stored in `curated_out_of_service_periods`, survives the entity
transformer and `resolveWorksPeriods`, is shown in the admin's field value, and is **not** served
on public payloads (`publicWorksPeriod`). An admin save sends windows without `source`; a window it
sends with unchanged `from` and `to` keeps the stored `source` (`keepWindowSources`), and one whose
dates the editor changed becomes hand-written.

Per ride, per run:

- a listed window the ride lacks is **added** (`toUncertain: false`, the park published the dates);
- the sync's own window that overlaps a listed one is **updated** to the listed dates;
- the sync's own window the page no longer lists is **removed only if it has not started**
  (park-local today); one that started stays as the ride's history;
- a hand-written window is never touched, and a listed window that would overlap one is
  **skipped** and reported, as is one whose "until" day is already past.

Writes go through `AdminCurationService.curateAttractionsBulk` as the system actor
`maintenance-sync@park.fan` (like the fun-fact translator): one audit row per ride, with the counts as
reason and the page as `sourceUrl`, so each ride's change can be undone in the admin; then one
Redis eviction and the two frontend revalidations ([publish outside-in](../rules/publish-outside-in.md)).

## Error logging

A failed run (fetch, layout change, no park, a write) is logged at error level and written to the
job-failure log with `logJobFailure` (`/data/parkfan/logs/` on the host), then rethrown, so the job
still shows as failed on the queue card. A failed fetch is also written to `external-api-errors`
(`logExternalApiError("efteling", "maintenance-calendar", …)`; timeouts are left out there, as for
every source). Names that match no ride and rows with unreadable dates do not fail the run, since the
rest still applies, but they are logged at error level and written to `external-api-errors` too:
those are windows that did not reach the site.

## First run

The only destructive step removes the sync's own not-yet-started windows, so on the first run, when
no window carries the source, it removes nothing. Against the 2026-11-06 page it adds 12 windows to 9
rides (Droomvlucht, De Oude Tufferbaan and Baron 1898 get two each) and touches nothing else. To
watch it before it writes, trigger it with `{ "dryRun": true }` and read the job's result.
