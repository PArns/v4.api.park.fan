# Precomputed Best-Days Endpoint (`/best-days`)

**Added 2026-07-14.** A lean, precomputed projection of the calendar for the
frontend's best-days / crowd-FAQ / header-forecast blocks — so those surfaces
stop pulling the ~2.25 MB `/calendar` response (≈98 % of which is per-day
`influencingHolidays` they never read) through the `/api` proxy, and stop
paying its 10–20 s cold ML compute.

```
GET /v1/parks/{continent}/{country}/{city}/{parkSlug}/best-days
```

## Window

Rolling **today → +90 days** in the **park's timezone**, no params required.
Optional `from` / `to` (YYYY-MM-DD, park timezone) slice the materialized
snapshot; the range is capped at **90 days** (`400` otherwise). Days outside the
stored today → +90 d window are simply not returned.

## Response (target ≤ 15 KB uncompressed)

```jsonc
{
  "meta": {
    "slug": "phantasialand",
    "timezone": "Europe/Berlin",
    "hasOperatingSchedule": true,
    "computedAt": "2026-07-14T03:10:00.000Z", // when the batch produced this
    "windowFrom": "2026-07-14",
    "windowTo": "2026-10-12"
  },
  "days": [
    {
      "date": "2026-07-14",          // YYYY-MM-DD, park tz
      "status": "OPERATING",          // ParkStatus: OPERATING | CLOSED | UNKNOWN
      "crowdLevel": "low",            // very_low|low|moderate|high|very_high|extreme|unknown|closed
      "predictedCrowdLevel": "low",   // optional: ML forward level; absent when unratable
      "isHoliday": false,
      "isSchoolVacation": true,
      "isBridgeDay": false
    }
  ],
  // OPTIONAL: stats-quality weekday ranking (same source as /stats) so the FE
  // can render it without the (also slow) /stats aggregate. Omitted when cold.
  "byDayOfWeek": [{ "dayOfWeek": 1, "avgCrowdScore": 2.1, "sampleDays": 98 }]
}
```

**Deliberately excluded** (vs `/calendar`): `influencingHolidays`, weather,
hourly arrays, events, schedule/hours detail, `recommendation`, `peakLoad`.
`isToday` is **not** baked in — derive it from `date` + `meta.timezone` so it
never goes stale in a cache. The calendar **grid tab** keeps using the full
`/calendar` endpoint (it needs hours/weather per day).

`crowdLevel` vs `predictedCrowdLevel`: on future days they're equal (the ML
forecast). On **today** `crowdLevel` carries the level as of `computedAt` while
`predictedCrowdLevel` is the pure ML forward — a live "now" number is patched
separately on the park page, not here (this is a planning surface).

## Errors

- `404` — unknown park.
- `200` with `"days": []` — no snapshot materialized yet (brand-new park, or
  cold Redis before the first warmup). The frontend degrades gracefully.

## How it stays fast (the SLO)

Served from a **materialized Redis snapshot** (`best-days:<parkId>`, 26 h TTL),
never a lazy compute:

- The 12 h **calendar warmup** (`warmup-calendar-daily`, 08:00 + 20:00 UTC —
  after the nightly forecast) force-refreshes each park's calendar and then
  projects the today → +90 d snapshot from the **already-warm month caches**
  (`BestDaysService.precomputeForPark`). No extra ML cost.
- The request path is a single Redis GET + in-memory slice ⇒ **p99 < 300 ms,
  cold and warm**. This is what lets the frontend drop its `BEST_DAYS_SEED_TIMEOUT_MS`
  guard — the seed can't be slow, so the timeout becomes a formality.

`byDayOfWeek` is read **best-effort** from the 24 h `/stats` cache
(`getCachedByDayOfWeek`, read-only — it never triggers the slow 2-year
percentile scan). It's omitted until that cache is warm; the frontend can fall
back to `/stats` when absent.

## HTTP caching

```
Cache-Control: public, max-age=3600, s-maxage=3600, stale-while-revalidate=86400
```

No auth variance → the CDN absorbs repeat traffic. Express emits a weak `ETag`
and answers `If-None-Match` with `304` natively.

## On-change revalidation

After each calendar-warmup batch, the backend fires **one batched**
`POST {REVALIDATE_URL}` with `{ "tags": ["best-days:<slug>", …] }` for every
park it recomputed, so the frontend drops its (day-long) best-days cache
immediately instead of waiting out the TTL.

Config (see `.env.example`):

| Env | Default | Notes |
|-----|---------|-------|
| `REVALIDATE_URL` | `https://park.fan/api/revalidate` | Frontend endpoint. |
| `REVALIDATE_SECRET` | _(unset)_ | Sent as `Authorization: Bearer <secret>`. **Empty ⇒ webhook disabled** (dev/test/CI never ping production). |

The webhook is best-effort — a failed POST is logged and never fails the warmup
batch.
