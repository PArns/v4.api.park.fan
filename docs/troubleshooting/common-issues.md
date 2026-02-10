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

## Slow API requests (where to look)

**Symptom**: Some requests take many seconds (e.g. 30s+) and are easy to miss in the main log stream.

**Where they are stored**: Slow requests (>3s) are written to a **dedicated log file** (JSON Lines) so they are not lost in the main logs. Default path: `logs/slow-requests.log` (overridable with `SLOW_REQUEST_LOG_PATH`). One short line is still printed to the main stream: `Slow request (see slow-request log): GET /v1/... 30179ms`.

**How to use**:
- Tail the file: `tail -f logs/slow-requests.log`
- In production, ship this file separately (e.g. different log panel or alert when new lines appear)
- Each line is JSON: `ts`, `method`, `url`, `endpoint`, `query`, `statusCode`, `responseTimeMs`, `ip`, and **`breakdown`** (where time was spent), e.g. `{"attraction_phase1_ms":5200,"attraction_phase2_ms":800}` or `{"park_phase1_ms":1200,"park_phase2_ms":3100}`.
- **Breakdown keys**: `attraction_phase1_ms` (total), `attraction_phase1_queue_ms`, `attraction_phase1_park_status_ms`, `attraction_phase1_forecasts_ms`, `attraction_phase1_park_url_ms`, `attraction_phase1_ml_ms`, `attraction_phase1_p50_ms`, `attraction_phase1_p90_ms`, `attraction_phase2_ms`; `park_phase1_ms` (total), `park_phase1_weather_ms`, `park_phase1_schedule_ms`, `park_phase1_queue_ms`, `park_phase1_ml_ms`, `park_phase1_next_schedule_ms`, `park_phase2_ms`; `calendar_phase1_ms`, `calendar_phase2_ms`, `calendar_phase3_ms`. Use sub-keys to see which dependency is slow.
- Group by `endpoint`: `jq -r '.endpoint' logs/slow-requests.log | sort | uniq -c`
- Inspect breakdown: `jq '.breakdown' logs/slow-requests.log`

**Phase1 optimizations (no feature loss)**:
- **Park status fallback**: When schedule says CLOSED we used to call Queue-Times/ThemeParks/Wartezeiten on every request. We now cache the result in `park:status_live:{parkId}` (2 min when OPERATING, 1 min when CLOSED) so repeat requests skip the external API. Status and behaviour unchanged.
- Weather, schedule, ML predictions, and park status (`getBatchParkStatus`) are already cached (see [Caching Strategy](../architecture/caching-strategy.md)). Cold cache is still slow; warmup and TTLs reduce how often phase1 runs.

**See**: [Caching Strategy](../architecture/caching-strategy.md), parallelisation in `attraction-integration.service` and `park-integration.service`.

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
