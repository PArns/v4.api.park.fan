# Common Issues & Troubleshooting

Short guide for frequent problems and how to fix them.

## Stale cache (e.g. peakWaitToday wrong)

**Symptom**: API returns `peakWaitToday: 450` (or another implausible value) while DB or live data shows ~45.

**Cause**: Redis cache key `park:statistics:{parkId}` still holds an old value (e.g. from bad data before outlier cap).

**Fix**:

1. Invalidate the key (Redis CLI or script):
   ```bash
   redis-cli DEL "park:statistics:<parkId>"
   ```
2. Or use a script that clears park caches (see [Scripts](development/scripts.md)).
3. Stats are recomputed on next request; CacheWarmupProcessor also refreshes periodically.

**Prevention**: `StatsService` caps `maxWaitTime` at `min(rawMax, max(120, p90*3))` so bad values no longer enter `park_daily_stats` / cache.

---

## Occupancy always 100%

**Symptom**: `occupancy.current` is 100% even when the park is quiet.

**Cause**: Occupancy uses **(current median / P50 baseline)**. If the baseline was wrong (e.g. “all attractions” P50 instead of headliner P50), the denominator is too low and occupancy is inflated.

**Fix**: Ensure the API uses `getP50BaselineFromCache(parkId)` for park occupancy (headliner P50). No manual fix needed if code is correct; if you see 100% on a quiet day, check that P50 baseline job has run and `park_p50_baselines` has a sane value for that park.

**See**: [P50 Crowd Levels](../analytics/p50-crowd-levels.md), [Caching Strategy](../architecture/caching-strategy.md).

---

## Wrong “today” / schedule for a park

**Symptom**: Schedule or “today” data is for the wrong calendar day (e.g. US park when server is in Europe).

**Cause**: Using server date (`new Date()`) instead of the park’s timezone for “today”.

**Fix**: Use `getCurrentDateInTimezone(park.timezone)` for “today” and `getTomorrowDateInTimezone(park.timezone)` for “tomorrow”. Schedule queries should use date-string equality (`schedule.date = :todayStr`) where possible.

**See**: [Date & Time Handling](development/datetime-handling.md).

---

## Holidays / schedules: date shift

**Symptom**: A holiday or schedule for “2025-12-25” appears as 24th or 26th.

**Cause**: Treating date-only values as timestamps; timezone conversion shifts the calendar day.

**Fix**: Store and compare holidays/schedules as **date-only strings** (`YYYY-MM-DD`). Use `getCurrentDateInTimezone(park.timezone)` when comparing with “today”. Never use `new Date("2025-12-25")` without explicit noon-UTC or timezone handling.

**See**: [Date & Time Handling](development/datetime-handling.md) (§ Strict Date-Only Handling).

---

## ML predictions missing or wrong scale

**Symptom**: Predictions fail or crowd level from ML doesn’t match API.

**Checks**:

1. **ML service up**: `GET http://localhost:8000/health` (or your ML base URL).
2. **Occupancy feature**: At inference, `park_occupancy_pct` comes from the API (P50-based). If it was trained with P90, retrain with P50 (see [Model Overview](../ml/model-overview.md)).
3. **P50 baseline**: API passes `p50Baseline` for crowd level; Python uses it so labels match TypeScript.

**See**: [Model Overview](../ml/model-overview.md) (§ Alignment with API).

---

## Peak hour / park peak outside operating hours

**Symptom:** Dashboard shows e.g. peak hour 22:00 or park peak 500 min, even though the park is only open until 19:00 today.

**Cause:** The “typical” peak hour comes from 60 days of history (often longer summer hours). Park peak could come from daily stats that were not limited to today’s operating hours.

**Fix (implemented):**

1. **Peak hour (peakHour):** If the displayed peak hour in the park’s timezone is **after** today’s closing time, it is not shown (`peakHour: null`).
2. **Park peak (peakWaitToday):** Computed **only from headliner attractions** (same rides as P50/crowd level). **Average of peaks:** per headliner MAX(wait time today), then sum / number of headliners. This is intentional – it describes typical peak load across headliners and is not dominated by a single outlier ride. Without headliner fallback: max over all attractions.
3. **Trend (occupancy.trend):** Computed **only from headliner attractions**. Per headliner: average wait time in the last 1h and 1h–2h; then **average** = sum of these per-headliner averages / number of headliners (not sum over all data points). Current wait time for occupancy (`getCurrentSpotWaitTime`) is also headliner-only when headliners exist.

**Relevant code:** `AnalyticsService.getParkStatistics` (peak hour, park peak), `AnalyticsService.calculateParkOccupancy` (trend, current), `getCurrentSpotWaitTime(…, headlinerIds)`.

---

## Duplicate or conflicting schedule entries

**Symptom**: Calendar shows conflicting opening hours (e.g. both OPERATING and CLOSED for the same date), or schedule queries return unexpected multiple entries for a single day.

**Cause**: Parallel schedule syncs or interrupted processes may create duplicate entries with different schedule types for the same `(parkId, date)`. This can happen when:
- Multiple schedule sync jobs run simultaneously for the same park (rare, but possible with manual triggers)
- Race condition between `saveScheduleData` and `fillScheduleGaps` when running concurrently
- API provides conflicting data that gets saved before cleanup runs

**Automatic fixes**:

1. **Per-park cleanup (immediate)**: `fillScheduleGaps(parkId)` automatically calls `cleanupDuplicateScheduleEntriesForPark(parkId)` **before** gap-filling. This removes duplicates for that specific park using SQL window functions:
   - **Same-type duplicates**: Multiple entries with identical `(parkId, date, scheduleType)` → keeps most recent by `updatedAt`
   - **Cross-type conflicts**: Multiple entries for same `(parkId, date)` with different scheduleTypes → applies priority (OPERATING > API-provided CLOSED > Gap-filled CLOSED > UNKNOWN)

2. **Global cleanup (daily)**: `cleanupDuplicateScheduleEntries()` runs as part of `fillAllParksGaps()` job, processing all parks. Uses optimized SQL (2 queries total instead of N+1 pattern).

**Manual fix** (if needed before automatic cleanup):

```bash
# Run gap-fill for a specific park (triggers per-park cleanup)
npm run script:fill-gaps -- --parkId=<park-uuid>

# Or run global cleanup for all parks
npm run script:fill-all-gaps
```

**Prevention**:
- Ensure only one schedule sync job runs per park at a time (BullMQ queue handles this automatically for cron jobs)
- Avoid concurrent manual API calls to `saveScheduleData` for the same park
- The batch DELETE operations in `saveScheduleData` now clean up cross-type conflicts automatically (OPERATING deletes CLOSED, CLOSED deletes OPERATING, etc.)

**Technical details**: See [Schedule Sync & Calendar](../architecture/schedule-sync-and-calendar.md) (§ Performance optimization, § Persistence).
