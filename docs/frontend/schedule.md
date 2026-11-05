# Park schedule rows (`/schedule`)

The contract for a park's raw opening-hours rows over a date range: what a row is, which days are
filled in, and which holiday fields this endpoint cannot be trusted for. Open it before reading
`/schedule` instead of the calendar. What `OPERATING`, `CLOSED` and `UNKNOWN` mean to a visitor, and
`hasOperatingSchedule`: [calendar status](./calendar-schedule-status.md). For a page, the calendar
(`/calendar`) is usually the better source: it fills gaps, rates crowds and computes holidays.

```jsonc
// GET /v1/parks/:continent/:country/:city/:park/schedule?from=2026-10-27&to=2026-11-02
{ "park": { "id": "…", "name": "Phantasialand", "slug": "phantasialand", "timezone": "Europe/Berlin" },
  "schedule": [
    { "date": "2026-10-27", "scheduleType": "OPERATING",
      "openingTime": "2026-10-27T08:00:00.000Z", "closingTime": "2026-10-27T17:00:00.000Z",
      "isHoliday": false, "isPublicHoliday": false, "isSchoolHoliday": false, "isBridgeDay": false,
      "isEstimated": false, "isInferred": false, "influencingHolidays": [] } ] }
```

## The rows

- `from` and `to` are park-local `YYYY-MM-DD`; `from` defaults to today and `to` to `from` + 30 days.
  More than 90 days, `to` before `from`, or a day the month does not have answer 400.
- **One row per stored `schedule_entries` row at park level**, ordered by date. A day can have
  several (an `OPERATING` row and a `TICKETED_EVENT` or `EXTRA_HOURS` row beside it), and
  `scheduleType` can be any of `OPERATING`, `CLOSED`, `UNKNOWN`, `TICKETED_EVENT`, `PRIVATE_EVENT`,
  `EXTRA_HOURS`, `INFO`, `MAINTENANCE`, not only the two Swagger lists. Which row states the day:
  `pickDayStatusEntry` (`src/common/utils/schedule.util.ts`), `OPERATING` over `CLOSED` over `UNKNOWN`.
- **A day without a row is missing, not closed.** Unlike the calendar, nothing fills the gap.
- `openingTime` / `closingTime` are UTC instants; absent on a row without hours. `purchases` is what
  the source sent (paid queue products and the like), absent when none.

## Reconstructed days

Only for a park that has never published hours (`hasOperatingSchedule` false), and only for an
`UNKNOWN` row on or before today:

| Ride activity that day | Row becomes |
| --- | --- |
| yes | `OPERATING` with hours derived from the rides, `isEstimated: true`, `isInferred: true` |
| none, strictly past day | `CLOSED`, `isEstimated: true` |

A park that publishes hours gets its rows as stored.

## Holiday fields

- `isHoliday`, `holidayName` and `isBridgeDay` were stamped on the row when the schedule was synced, so
  they can lag the holiday tables.
- **`isPublicHoliday` and `isSchoolHoliday` are always `false` here** and `holidayType` is never sent:
  no type is stored with a row, and these two are derived from it. `influencingHolidays` is always
  `[]`. Read holidays from the calendar, which computes them at request time.

Cached 1 h at the edge.
