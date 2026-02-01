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

## Stoßzeit / Park-Höchststand außerhalb Öffnungszeiten

**Symptom**: Dashboard zeigt z. B. Stoßzeit 22:00 oder Park-Höchststand 500 Min, obwohl der Park heute nur bis 19:00 geöffnet hat.

**Ursache**: Die „typische“ Stoßzeit stammt aus 60 Tagen Historie (oft längere Sommer-Öffnungszeiten). Park-Höchststand konnte aus Tagesstatistiken kommen, die nicht auf die heutige Betriebszeit begrenzt waren.

**Fix (implementiert)**:

1. **Stoßzeit (peakHour)**: Liegt die angezeigte Stoßstunde in der Park-Zeitzone **nach** der heutigen Schließzeit, wird sie nicht angezeigt (`peakHour: null`).
2. **Park-Höchststand (peakWaitToday)**: Wird **nur aus Headliner-Attraktionen** berechnet (gleiche Rides wie P50/Crowd Level). **Durchschnitt der Spitzen**: Pro Headliner MAX(Wartezeit heute), dann Summe / Anzahl Headliner. Das ist bewusst so gewählt – es beschreibt die typische Spitzenlast über die Headliner hinweg und wird nicht von einem einzelnen Ausreißer-Ride dominiert. Ohne Headliner-Fallback: Max über alle Attraktionen.

3. **Trend (occupancy.trend)**: Wird **nur aus Headliner-Attraktionen** berechnet. Pro Headliner: Durchschnitt Wartezeit in der letzten 1h bzw. 1h–2h; dann **Durchschnitt** = Summe dieser Pro-Headliner-Durchschnitte / Anzahl Headliner (keine Summe über alle Datenpunkte). Aktuelle Wartezeit für Occupancy (`getCurrentSpotWaitTime`) ist ebenfalls Headliner-only, wenn Headliner vorhanden sind.

**Relevanter Code**: `AnalyticsService.getParkStatistics` (Stoßzeit, Park-Höchststand), `AnalyticsService.calculateParkOccupancy` (Trend, current), `getCurrentSpotWaitTime(…, headlinerIds)`.
