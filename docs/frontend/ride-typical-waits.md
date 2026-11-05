# A ride's typical and busy peak wait (`typicalWaits`)

The contract for the "normal vs busy day" peak-wait figures of one ride: where the
object appears, what P50/P90 of daily peaks mean, its shape and the `displayable`
gate. Open it when rendering a ride's typical waits. This is per-ride minutes, a
different surface from the crowd level — never derive one from the other.

## Where it lives

- **Attraction detail**: `GET /v1/parks/…/attractions/:attractionSlug` →
  `typicalWaits: TypicalWaitsDto | null`, with `displayable: false` below the floor.
- **Park payload**: on every attraction with at least 20 measured operating days
  (the `displayable` floor; all rides since PAR-908, not only headliners), so the SSR
  ride page needs no second call. Below the floor the field is omitted.
- Not on the 5-minute live poll (`LiveParkSnapshot`).

The park copy is precomputed nightly at 05:30 UTC (`calculate-typical-waits`, table
`attraction_typical_waits`) by one grouped scan of `queue_data_aggregates` per park,
and the job primes the detail endpoint's cache, so both serve identical numbers.
`byDayOfWeek` stays in the park copy because park.fan's component maps over it;
going from 10 to 84 rides cost Europa-Park +58.7 KB raw, +1.05 KB brotli
(2026-10-17).

## What the numbers mean

Over a **365-day** window (`TYPICAL_WAITS_WINDOW_DAYS`), each operating day's peak is
its highest hourly P90 wait. Over the daily peaks in a bucket:

- **`typical`** = P50 — a normal day's peak wait;
- **`busy`** = P90 — a busy day's peak wait.

Whole minutes, or `null` when the bucket has no data. They describe the **peak** of a
day, not the all-day average: phrase the UI as "peak wait".

## Shape

```ts
interface TypicalWaitBucket { typical: number | null; busy: number | null; sampleDays: number }

interface TypicalWaits {
  weekday: TypicalWaitBucket;   // country-aware weekdays
  weekend: TypicalWaitBucket;   // Sat/Sun, or Fri+Sat in Gulf states
  byDayOfWeek: Array<TypicalWaitBucket & { dayOfWeek: number /* 0 = Sun */; isWeekend: boolean }>;
  peak: { value: number; date: string } | null; // the record daily peak
  windowDays: number;           // 365
  dataFrom: string; dataTo: string; // YYYY-MM-DD, park tz
  displayable: boolean;
  generatedAt: string;
}
```

- **Gate on `displayable`** (≥ 20 operating days with data,
  `TYPICAL_WAITS_MIN_TOTAL_SAMPLE_DAYS`), never on your own threshold; when `false`
  hide the section even if buckets hold numbers.
- **`byDayOfWeek` lists only days with data** — map by `dayOfWeek`, do not assume
  seven. Use `isWeekend` (country-aware) rather than hardcoding Saturday/Sunday.
- `peak` is the single highest daily peak with its date; `null` → omit.
- Refreshed daily and cached 24 h: `generatedAt` moves about once a day. Do not poll.

Code: `src/analytics/utils/typical-waits.util.ts`.
