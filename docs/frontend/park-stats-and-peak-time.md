# Park history (`/stats`) and today's peak hour

The contract for a park's historical aggregates (`GET …/:parkSlug/stats`: by month,
by weekday, busiest/quietest days, top attractions) and for the peak-hour fields on
the park payload's `analytics.statistics`. Open it when rendering a park's history or
its "peak today" line. The rule for both: **the backend classifies, the frontend
renders** — never re-derive a crowd level from a score.

## `GET /v1/parks/{continent}/{country}/{city}/{parkSlug}/stats`

Code: `ParkHistoricalStatsService` (`src/analytics/park-historical-stats.service.ts`),
DTO `src/analytics/dto/park-historical-stats.dto.ts`. `meta.schemaVersion: 4`.

| Query               | Default | Notes                                                                                                                         |
| ------------------- | ------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `years`             | 2       | clamped to 1–5                                                                                                                |
| `topN`              | 10      | clamped to 1–50; every depth is a slice of one cached 50-ride list, so two depths agree per ride (PAR-983)                     |
| `minSampleDays`     | 30      | below it `meta.displayable` is `false`                                                                                        |
| `minAttractionDays` | 20      | measured days a ride needs to enter `topAttractions`. `0` or a negative value (clamped to 0) **removes the floor** — the one-measured-day ranking the default exists to prevent |

A non-integer parameter is a 400 (`QueryInt`). Cached 24 h in Redis and at the edge
(`HttpCacheInterceptor`, `Cache-Control: public, max-age=86400, …`); the inputs are
the nightly `queue_data_aggregates` rollup, so `generatedAt` moves about once a day.

```ts
{
  byMonth: Array<{ month: number /* 1..12 */ } & Row>;
  byDayOfWeek: Array<{ dayOfWeek: number /* 0 = Sunday … 6 */ } & Row>;
  busiestDays: DayRow[];   // up to 5, fullest first
  quietestDays: DayRow[];  // up to 5, quietest first
  topAttractions: Array<{
    attractionSlug: string; attractionName: string;  // curated name wins
    avgWaitP50: number; avgWaitP90: number; sampleDays: number;
    rank: number;                  // 1-based, by avg P90 — use it, not the array index
    land: string | null;           // curated wins; key always present
    attractionType: string | null; // the feed's free text, not a glossary id or `attractionKind`
  }>;
  meta: {
    parkSlug: string; dataFrom: string; dataTo: string; // YYYY-MM-DD, park tz
    totalSampleDays: number;
    windowYears: number;     // for the "last X years" caption
    displayable: boolean;    // the render gate: totalSampleDays >= minSampleDays
    generatedAt: string; schemaVersion: 4; minAttractionDays: number;
  };
}
type Row = { avgCrowdScore: number; avgCrowdLevel: CrowdLevel; avgWaitP50: number; avgWaitP90: number; sampleDays: number };
type DayRow = { date: string; avgCrowdScore: number; avgCrowdLevel: CrowdLevel; avgWaitP50: number; avgWaitP90: number };
```

- **Render `avgCrowdLevel`; do not classify `avgCrowdScore`.** The level is
  occupancy-relative — a period's headliner peak ÷ the park's typical-day-peak — on
  the same thresholds as the live level ([crowd levels](../rules/crowd-levels.md)).
  `avgCrowdScore` (absolute minutes mapped to 1.0–5.0) stays for sorting only: a
  structurally quiet park would read "low" on its busiest days.
- **Headliners only** for the month, weekday and day rows (the calendar's semantic);
  a day the schedule calls CLOSED is neither a value nor a sample day (PAR-692).
- **`busiestDays` / `quietestDays`** rank measured days with at least six measured
  hours, cover the measurement window rather than all time, and are both empty below
  ten such days (`MIN_EXTREME_DAYS`).
- Gate on `meta.displayable`, not on a threshold of your own; the caption comes from
  `meta.windowYears`.

The hour-by-hour shape of the same window is a separate endpoint:
[`/stats/hourly`](./park-hourly-profile.md).

## Peak hour today (`analytics.statistics` on the park payload)

No endpoint of its own — the fields live beside the other live stats:

```ts
{
  peakWaitToday: number;
  peakHour: string | null;        // always ISO 8601 with offset, e.g. "2026-06-01T14:00:00+02:00"
  peakHourLocal: string | null;   // "14:00" in park time, display as-is
  peakHourConfidence: number;     // 0..1
  peakHourSource: "observed_today" | "prediction" | "historical_fallback" | null;
}
```

| `peakHourSource`      | `peakHourConfidence` | Meaning                                           |
| --------------------- | -------------------- | ------------------------------------------------- |
| `observed_today`      | 0.9                  | the peak already happened today                   |
| `prediction`          | 0.6                  | live data today, the peak is still ahead — a forecast |
| `historical_fallback` | 0.4                  | no usable data today — the typical peak           |
| `null`                | 0                    | no peak applies (closed, after closing)           |

The mapping is `peakHourConfidence()` (`src/analytics/utils/peak-hour.util.ts`).
Qualify anything that is not `observed_today` as an estimate ("≈ 14:00"). For a park
whose wait times cannot be read these fields are withheld — see
[live-wait-times-availability](./live-wait-times-availability.md).
