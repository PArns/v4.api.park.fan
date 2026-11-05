# Precomputed best days (`/best-days`)

The contract for the lean today → +90 days projection of a park's calendar
(status, crowd level, holiday flags, optional weekday ranking) that park.fan's
best-days, crowd-FAQ and header-forecast blocks read instead of the full
`/calendar`. Open it when reading that endpoint or changing how it is refreshed.

```
GET /v1/parks/{continent}/{country}/{city}/{parkSlug}/best-days[?from=YYYY-MM-DD&to=YYYY-MM-DD]
```

Code: `BestDaysService` (`src/parks/services/best-days.service.ts`).

## Window

Rolling **today → +90 days** in the park's timezone, no parameters needed. Optional
`from`/`to` (park-local) slice the stored snapshot; a range over **90 days** or a bad
date is a 400. Days outside the stored window are simply not returned.

## Response

```jsonc
{
  "meta": {
    "slug": "phantasialand",
    "timezone": "Europe/Berlin",
    "hasOperatingSchedule": true,
    "computedAt": "2026-07-14T03:10:00.000Z",
    "windowFrom": "2026-07-14",
    "windowTo": "2026-10-12"
  },
  "days": [
    {
      "date": "2026-07-14",          // park tz
      "status": "OPERATING",          // OPERATING | CLOSED | UNKNOWN
      "crowdLevel": "low",            // very_low … extreme | unknown | closed
      "predictedCrowdLevel": "low",   // ML forward level; absent when unratable
      "isHoliday": false,
      "isSchoolVacation": true,
      "isBridgeDay": false
    }
  ],
  "byDayOfWeek": [{ "dayOfWeek": 1, "avgCrowdScore": 2.1, "sampleDays": 98 }] // optional
}
```

- **Excluded on purpose** (vs `/calendar`): `influencingHolidays`, weather, hourly
  arrays, events, hours, `recommendation`, `peakLoad`. There is no `isToday` —
  derive it from `date` + `meta.timezone`, so a cached copy never goes stale.
- **`crowdLevel` vs `predictedCrowdLevel`**: equal on future days. On today
  `crowdLevel` is the level as of `computedAt`, `predictedCrowdLevel` the pure
  forecast; a live "now" level belongs to the park page, not this planning surface.
- **`byDayOfWeek`** is read best-effort from the 24 h `/stats` cache
  (`getCachedByDayOfWeek`, read-only — it never triggers the stats scan) and is
  omitted while that cache is cold; fall back to [`/stats`](./park-stats-and-peak-time.md).
- Unknown park: **404**. A snapshot that cannot be built: **200** with `"days": []`.

## How it stays fast

Served from a materialized Redis snapshot (`best-days:<parkId>`), never computed per
request from ML:

- The calendar warmup (`warmup-calendar-daily`, [twice a day](../architecture/job-queues.md#schedule-utc))
  refreshes each park's calendar months and projects the snapshot from those warm
  month caches (`BestDaysService.precomputeForPark`) — no extra ML cost.
- **The snapshot expires with the month caches it is built from**
  (`CALENDAR_WARMUP_MONTH_TTL`; [TTLs](../architecture/caching-strategy.md#keys-and-ttls)):
  a projection may not outlive its source. At 26 h, a 24.8-hour-old snapshot said
  "low" for three October Saturdays the calendar and `/plan/day` rated `very_high`.
  The current month's caches expire sooner, so for up to 11 h a current-month day can
  lag the calendar.
- **A miss rebuilds on demand**, single-flighted, rather than answering empty (an
  empty 200 would sit at the edge for an hour and blank "Prognose heute"). Only a
  failed rebuild answers `days: []`.
- `Cache-Control: public, max-age=3600, s-maxage=3600, stale-while-revalidate=86400`;
  Express answers `If-None-Match` with 304.

## On-change revalidation

After each warmup batch the API fires one batched `POST {REVALIDATE_URL}` with
`{ "tags": ["best-days:<slug>", …] }` for every recomputed park, so park.fan drops
its day-long best-days cache at once (`RevalidationService`).

| Env                 | Default                           | Notes                                                                 |
| ------------------- | --------------------------------- | --------------------------------------------------------------------- |
| `REVALIDATE_URL`    | `https://park.fan/api/revalidate` | park.fan's endpoint                                                   |
| `REVALIDATE_SECRET` | unset                             | sent as `Authorization: Bearer …`; **empty disables the webhook**     |

The webhook is best-effort: a failed POST is logged and never fails the warmup. The
order in which the API evicts and announces is in
[caching strategy](../architecture/caching-strategy.md).
