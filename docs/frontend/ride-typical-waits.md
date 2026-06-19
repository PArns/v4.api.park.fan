# Ride P50/P90 Statistics — `typicalWaits` (frontend guide)

> What the frontend needs to render the "typical vs busy peak wait" stats on a
> ride/attraction page. Shipped 2026-06-18.

## Where it lives

Field **`typicalWaits`** on the attraction detail response:

```
GET /v1/parks/:continent/:country/:city/:parkSlug/attractions/:attractionSlug
→ AttractionResponseDto.typicalWaits: TypicalWaitsDto | null
```

It is only populated on the **single-attraction detail** endpoint (not on the
park's `/attractions` list). `null` (or omitted) ⇒ no data; render nothing.

## What the numbers mean

Everything is derived from the distribution of **daily peak waits** over a
**365-day** look-back window. A "daily peak" = that operating day's highest
hourly P90 wait. Over the set of daily peaks in a bucket:

- **`typical` = P50** (median) of the daily peaks → *a normal day's peak wait*
- **`busy` = P90** of the daily peaks → *a busy day's peak wait*

All values are **whole minutes** (or `null` when the bucket has no data). They
describe the **peak** of a day, not the all-day average — phrase the UI as
"peak wait" (e.g. "Normal: ~35 min · Busy: ~60 min at peak").

## Shape (`TypicalWaitsDto`)

```ts
interface TypicalWaitBucket {
  typical: number | null; // P50 of daily peaks (min)
  busy: number | null;    // P90 of daily peaks (min)
  sampleDays: number;     // operating days with data in this bucket
}

interface TypicalWaits {
  weekday: TypicalWaitBucket;          // Mon–Fri (country-aware)
  weekend: TypicalWaitBucket;          // Sat/Sun, or Fri+Sat in Gulf states
  byDayOfWeek: Array<                  // only days that have data
    TypicalWaitBucket & { dayOfWeek: number; isWeekend: boolean }
  >;                                   // dayOfWeek: 0=Sun … 6=Sat
  peak: { value: number; date: string } | null; // record peak + YYYY-MM-DD
  windowDays: number;                  // 365
  dataFrom: string;                    // window start, YYYY-MM-DD (park tz)
  dataTo: string;                      // window end, YYYY-MM-DD (park tz)
  displayable: boolean;                // see below
  generatedAt: string;                 // ISO 8601 UTC
}
```

## Rendering rules

- **Gate on `displayable`, not your own threshold.** It is `true` only when the
  total sample is large enough to be meaningful (≥ 20 operating days of data).
  When `false`, hide the whole section — the buckets may still hold noisy
  numbers that shouldn't be shown.
- **Weekday vs weekend** is the headline split (`weekday` / `weekend` buckets).
  `byDayOfWeek` powers an optional per-day breakdown (a 7-bar chart); it only
  contains days that actually have data, so don't assume 7 entries — map by
  `dayOfWeek` and leave gaps for missing days.
- **`isWeekend`** is country-aware (don't hardcode Sat/Sun) — use it to colour
  weekend bars.
- **`peak`** is the single highest daily peak in the window with its date —
  good for a "record: 120 min on 2025-08-09" line. `null` ⇒ omit.
- Treat any `typical`/`busy` of `null` as "no data" for that bucket/day.
- `dataFrom`/`dataTo`/`windowDays` document the window (e.g. "based on the last
  365 days") if you want a caption.

## Freshness

The aggregate is **cached 24h** and recomputed daily (the inputs — the hourly
percentile aggregates — only change once a day), so `generatedAt` updates about
once per day. No need to poll.

## Relation to crowd levels

This is **per-ride wait minutes**, a different surface from the park/ride
**crowd level** (`very_low`…`extreme`/`unknown`). Don't derive one from the
other — render them as separate UI elements.
