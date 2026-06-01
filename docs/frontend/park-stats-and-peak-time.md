# Park Stats & Peak Time — Frontend Integration (API v2)

Guide for the frontend on the **delivered** backend contract for historical
park statistics and the "peak time today" feature. This is the as-built
counterpart to the planning doc in `park.fan` — it notes where the backend
intentionally deviated and lists exactly which client-side logic can now be
removed.

**Principle:** the backend is the single source of truth. The frontend
**renders**, it does **not** classify crowd levels or guess time formats.

All fields below are **additive and backwards-compatible** — every previously
existing field is still present, so you can migrate gradually behind a fallback.

---

## 1. Crowd level — the backend now classifies

### What changed

The `/stats` endpoint previously returned only a numeric `avgCrowdScore`
(1.0–5.0) and the frontend mapped it to a label with its own threshold table.
The backend now also returns the **label directly** as `avgCrowdLevel`, and it
is computed with the **same logic as the live `crowdLevel`** on the park
endpoint — so historical and live readings finally mean the same thing.

> **Why the old client mapping was wrong:** `avgCrowdScore` is clamped to a max
> of 5.0, but the client's `extreme` tier started at `> 5.5` → `extreme` was
> unreachable. And the score is *absolute minutes*, whereas the live crowd
> level is *occupancy relative to the park's own baseline*. A structurally quiet
> park therefore always read "low" historically even on its busiest days. The
> new `avgCrowdLevel` fixes both.

### Canonical `CrowdLevel`

```ts
type CrowdLevel =
  | "very_low" | "low" | "moderate"
  | "high" | "very_high" | "extreme";
```

**The thresholds live exclusively in the backend.** They are occupancy-relative
(a period's peak ÷ the park's typical-day-peak baseline; 100% ≈ a statistically
typical day) and are documented backend-side. The frontend must not re-derive
them anywhere.

### Frontend cleanup

- ❌ Delete `CROWD_SCORE_TO_LEVEL` in `park-stats-section.tsx` — use
  `avgCrowdLevel` from each `byMonth` / `byDayOfWeek` row directly.
- ✅ Keep reading `avgCrowdScore` only if you still need it for sorting/tooltips;
  it remains for backwards compatibility.

---

## 2. `GET /v1/parks/{continent}/{country}/{city}/{parkSlug}/stats`

Historical aggregates. **`schemaVersion: 2`.**

### Query parameters

| Name            | Type        | Default | Description |
| --------------- | ----------- | ------- | ----------- |
| `years`         | int (1..5)  | 2       | Look-back window size |
| `topN`          | int (1..50) | 10      | Number of top attractions |
| `minSampleDays` | int (≥0)    | 30      | Below this, `meta.displayable` becomes `false` |

### Response

```ts
{
  byMonth: {
    month: number;            // 1..12
    avgCrowdScore: number;    // 1.0–5.0 (kept for back-compat / sorting)
    avgCrowdLevel: CrowdLevel; // NEW — render this, do not re-classify
    avgWaitP50: number;       // minutes
    avgWaitP90: number;       // minutes
    sampleDays: number;
  }[];
  byDayOfWeek: {
    dayOfWeek: number;        // 0 = Sunday … 6 = Saturday (ISO DOW)
    avgCrowdScore: number;
    avgCrowdLevel: CrowdLevel; // NEW
    avgWaitP50: number;
    avgWaitP90: number;
    sampleDays: number;
  }[];
  topAttractions: {
    attractionSlug: string;
    attractionName: string;   // canonical name (frontend does not translate)
    avgWaitP50: number;
    avgWaitP90: number;
    sampleDays: number;
    rank: number;             // NEW — explicit 1-based rank (not array index)
  }[];
  meta: {
    parkSlug: string;
    dataFrom: string;         // "YYYY-MM-DD", park timezone
    dataTo: string;           // "YYYY-MM-DD", park timezone
    totalSampleDays: number;
    windowYears: number;      // NEW — use for the "Last X years" subtitle
    displayable: boolean;     // NEW — render gate (totalSampleDays >= minSampleDays)
    generatedAt: string;      // NEW — ISO 8601 UTC, when the aggregate was computed
    schemaVersion: 2;         // NEW
  };
}
```

### Frontend cleanup

- ❌ Replace the `years = round((dataTo - dataFrom) / 1yr)` math with
  `meta.windowYears`.
- ❌ Replace the hardcoded render gate `meta.totalSampleDays < 30` with
  `!meta.displayable`.
- ✅ Use `topAttractions[].rank` instead of the array index.

### Caching

Unchanged behaviour: the endpoint is cached server-side for 24 h and is backed
by nightly snapshot tables, so cold-cache responses stay fast. Keep your
existing `revalidate: 86400`. (CDN `Cache-Control` headers are a possible
future addition — not yet emitted.)

---

## 3. `GET /v1/parks/{...}` — peak time today

There is **no dedicated peak endpoint** — the peak fields stay on the park
object under `analytics.statistics`, because they are always rendered together
with the other live stats.

### Fields (under `analytics.statistics`)

```ts
{
  // existing
  avgWaitTime: number;
  avgWaitToday: number;
  peakWaitToday: number;     // minutes
  crowdLevel: CrowdLevel;    // live, occupancy-based
  totalAttractions: number;
  operatingAttractions: number;
  closedAttractions: number;
  timestamp: string;

  // peak hour
  peakHour: string | null;        // ALWAYS ISO 8601 with offset, e.g.
                                  // "2026-06-01T14:00:00+02:00" — never bare HH:MM
  peakHourLocal: string | null;   // NEW — "14:00" already in park TZ, display as-is
  peakHourConfidence: number;     // NEW — 0..1 (0 = no statement possible)
  peakHourSource:                 // NEW — provenance of the value
    | "observed_today"            //   today's peak already happened → real value
    | "prediction"                //   live data exists but peak is still ahead → forecast
    | "historical_fallback"       //   no usable data today → typical-peak fallback
    | null;                       //   no peak applies (e.g. after closing)
}
```

### Confidence tiers

| `peakHourSource`      | `peakHourConfidence` | Meaning |
| --------------------- | -------------------- | ------- |
| `observed_today`      | 0.9                  | Peak already occurred today — most reliable |
| `prediction`          | 0.6                  | Live signal today, peak still ahead — forecast |
| `historical_fallback` | 0.4                  | No data today — typical-peak estimate |
| `null`                | 0                    | No peak (e.g. closed / after closing time) |

### Frontend cleanup

- ❌ Delete the `peakHour.includes('T')` heuristic and the manual
  `fromZonedTime` conversion in `park-status.tsx`. `peakHour` is always ISO;
  feed it straight to `<LocalTime>`. For a no-conversion park-local label, use
  `peakHourLocal`.
- ✅ `PeakHourBadge` can use `peakHourSource` / `peakHourConfidence` to render
  e.g. "≈ 14:00" for forecasts vs. a firm "14:00" for an observed peak.

---

## 4. Migration plan

1. Backend ships the new fields additively (done — old fields stay).
2. Frontend reads the new fields, keeping fallbacks to the old ones for one
   release.
3. Once the backend is live, remove the client-side mappings:
   - `CROWD_SCORE_TO_LEVEL` (use `avgCrowdLevel`)
   - the `years` math (use `meta.windowYears`)
   - the `peakHour.includes('T')` heuristic (always ISO now)
   - the hardcoded `< 30` render gate (use `meta.displayable`)
4. Optionally drop the back-compat fallbacks after a bake-in period.

---

## 5. Notes / deviations from the planning doc

- **`peakHour` format** was flagged as "inconsistent HH:MM or ISO" — in this API
  it is already consistently ISO 8601 with offset. Only the extra fields were
  missing.
- **Materialized view for `/stats`** is effectively already in place
  (`park_daily_stats` nightly snapshot + `queue_data_aggregates` daily batch +
  24 h Redis). A dedicated view would add little and was not built.
- **`avgCrowdLevel`** is intentionally occupancy-relative (consistent with the
  live endpoint), not a 1:1 mapping of the absolute `avgCrowdScore`.
- Not yet done (optional follow-ups): CDN `Cache-Control` / `ETag` headers on
  `/stats`, and a lightweight `/live` sub-object for peak-only polling.
