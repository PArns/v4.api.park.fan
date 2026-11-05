# Calendar status for clients (`UNKNOWN` vs `CLOSED`, `scheduleCoverage`)

What a client may conclude from a calendar day's `status`, `crowdLevel` and
`isEstimated`, and from `meta.hasOperatingSchedule` and `scheduleCoverage` — the
field that says how far a park's published schedule reaches. Open it before
rendering opening hours or closures. How the server decides each status is in
[calendar, schedule & ML rules](../architecture/calendar-schedule-and-ml-rules.md);
reconstructed past days in [smart gaps](../analytics/smart-gaps.md).

```
GET /v1/parks/:continent/:country/:city/:parkSlug/calendar?from=&to=
→ { meta, days[]: { date, status, isEstimated?, hours?, crowdLevel, … } }
```

## Status

| status      | means                                                       | crowd level                                   | show                                                          |
| ----------- | ----------------------------------------------------------- | --------------------------------------------- | ------------------------------------------------------------- |
| `OPERATING` | the park is open                                            | yes                                           | the hours; with `isEstimated: true`, mark them reconstructed  |
| `CLOSED`    | the park is **definitively closed** (official or seasonal gap) | `"closed"`, no prediction                  | "Closed"                                                      |
| `UNKNOWN`   | no official hours yet — **the park may be open**            | today and future: the forecast or `"unknown"`; a strictly past day: `"closed"` | "opening hours not yet published", with the crowd level when there is one |

- **`UNKNOWN` is not closed.** Show a crowd prediction for an `UNKNOWN` day when one
  exists; that is what trip planning needs. The crowd level never changes the
  status.
- **`isEstimated: true`** only on past days whose status and hours were
  reconstructed from ride activity.
- **`crowdLevel: "unknown"` is a real value** — "no forecast", never "moderate". A
  day reads it when there is nothing to rate it by (PAR-1028); the same holds for
  `peakLoad` and for `/predictions/yearly`.
- **The same rule governs the live `status` in listings.** A park whose schedule
  publishes only `UNKNOWN` for today is judged from its live ride feed, so
  `/v1/discovery/continents/*`, `/v1/discovery/nearby` and `/v1/analytics/geo-live`
  agree with the park detail; a `CLOSED` row for today still wins.

## `hasOperatingSchedule`

On the calendar's `meta` and the park detail. `true`: the park has ever published
official hours. `false`: it never has, so future hours are null and past ones
reconstructed — show a disclaimer that hours are estimated from ride activity. It
says nothing about today.

## `scheduleCoverage`: how far the schedule reaches

`{ from, to }` on the calendar's `meta` **and on the park detail payload**, so a
client can decide which months to publish for 212 parks without 212 calendar calls
(the park listing does not carry it). `from`/`to` are the first and last dates with
a park-level `OPERATING` entry (`YYYY-MM-DD`, park tz), `null` at both ends for a
park with none.

```jsonc
"meta": { "slug": "phantasialand", "timezone": "Europe/Berlin", "hasOperatingSchedule": true,
          "scheduleCoverage": { "from": "2025-12-26", "to": "2027-01-06" } }
```

**Read `to` before trusting a future `status`.** Inside the window a status is a
statement about the park; past `to` it is a statement about our sync — no schedule
row exists, and for a park with seasonal closures the gap-fill resolves that to
`CLOSED`. On 2026-08-28 the calendar answered `CLOSED` for all of July 2027 at
Phantasialand and Europa-Park simply because their 2027 hours were not published;
nothing in `days[]` tells that apart from a real closure. So:

- **stop at `to`** when paginating months, building a month index or emitting
  sitemap URLs;
- **do not render `CLOSED` past `to` as "closed"** — treat it like `UNKNOWN`;
- expect `to` to move forward by itself as parks publish (it is derived from rows);
- read it as optional (`scheduleCoverage?.to`) and treat absence like `null`:
  coverage unknown.
