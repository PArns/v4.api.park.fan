# Common issues

Symptoms seen before, their cause and the fix, each with a link to the page that owns the mechanism.
Open it when an API answer looks wrong. Database slowness: [DB health runbook](db-health-runbook.md).

## An implausible statistic (e.g. `peakWaitToday: 450`)

**Cause:** an old value in Redis, here `park:statistics:{parkId}`, computed from bad data before the outlier
cap. **Fix:** `redis-cli DEL "park:statistics:<parkId>"`, or `POST /v1/admin/flush-cache` for every park
cache ([recalculating aggregates](../analytics/data-recalculation.md#caches)); it is rebuilt on the next
request. **Prevention:** `StatsService` caps a day's maximum at `max(120, 3 × P90)` with 20 or more samples,
`max(120, 5 × P50)` below that, so outliers no longer reach `park_daily_stats`.

## A park's occupancy or crowd level looks wrong

Check `occupancy.breakdown`: `currentAvgWait / typicalAvgWait` must divide out to `current`. A park with no
P50 baseline answers `current: 50`, `crowdLevel: "unknown"`, `confidence: "low"`: the 50 is a placeholder,
so a client that shows `current` instead of `crowdLevel` shows a made-up number. Then check the headliners
and baselines ([when a crowd level looks wrong](../analytics/headliner-logic.md#when-a-crowd-level-looks-wrong))
and that the 03:00 UTC `calculate-park-baselines` job ran. Formulas: [crowd levels](../analytics/crowd-levels.md).

## Wrong "today" for a park, or a date one day off

**Cause:** the server's date instead of the park's (`new Date()`, `toISOString().slice(0, 10)`), or a
date-only value turned into an instant (`new Date("2025-12-25")` is 24 December in New York). **Fix:** the
helpers in `src/common/utils/date.util.ts` and date-only values kept as `YYYY-MM-DD` strings
([park-timezone rule](../rules/park-timezone.md); [date-only values and the two US-park anti-patterns](../development/datetime-handling.md#6-date-only-values-holidays-schedules-events)).

## A peak hour after closing time

The typical peak hour comes from 60 days of history, often longer summer days. `getParkStatistics` returns
`peakHour: null` when the hour, in the park's timezone, is at or after today's closing hour.

## ML predictions missing or on the wrong scale

1. Is the service up? `GET http://ml-service:8000/health` inside the network (`/v1/health` on the API
   reports it too).
2. The crowd level from ML divides by the `typicalDayPeakBaseline` the API sends
   ([crowd levels: ML integration](../analytics/crowd-levels.md#ml-integration)); a park without one gets
   `unknown`, by design.

## ML service: `ECONNRESET` or "socket hang up"

The API's ML client (`MLService`, `src/ml/ml.service.ts`) has a 120 s timeout and retries `ECONNRESET`,
`ETIMEDOUT`, `ECONNABORTED` and "socket hang up" twice, after 1 s and 2 s (log: `retry N/2`). ml-service runs
gunicorn with `keepalive = 120`, `timeout = 120`, `graceful_timeout = 30` (`ml-service/gunicorn.conf.py`), so
an idle keep-alive connection is not closed under a long prediction. Errors that survive the retries point at
the service itself: memory, worker recycling (`max_requests`), or a restart.

## Conflicting schedule rows for one day

**Cause:** parallel syncs or a sync racing `fillScheduleGaps`. **Automatic fix:** `fillScheduleGaps(parkId)`
first runs `cleanupDuplicateScheduleEntriesForPark`, and the daily global gap fill runs
`cleanupDuplicateScheduleEntries()`. Only the day-status types are arbitrated (OPERATING > CLOSED from the
API > gap-filled CLOSED > UNKNOWN); event types such as `TICKETED_EVENT` stay beside them, so a reader picks
the day's status by type, never the first row. **By hand:** `POST /v1/admin/fill-schedule-gaps` (queues `fill-all-gaps` for every park). Mechanism:
[schedule sync and calendar](../architecture/schedule-sync-and-calendar.md).

## A cron's outcome cannot be found

Bull keeps the last 100 completed and 500 failed jobs per queue (`defaultJobOptions` in
`src/queues/queues.module.ts`), but some crons are added with `removeOnComplete: true` and
`removeOnFail: true` (`audit-ride-profile-terms`, `monitor-data-quality`): a finished run and a failed one
then leave the same trace, none. The repeat entry proves the cron fires, not that its handler succeeded, and
the container log starts at the last deploy. Read the job's own log line after a run, or call the handler
through its admin endpoint where one exists.
