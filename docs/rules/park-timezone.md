# Park timezone, always

A park's "today", its day boundaries and every date range belong to the park's own `timezone`
(`parks.timezone`), never to the server clock. Open this page before writing anything that turns
a time into a date; the full guide with the traps is
[Date & time handling](../development/datetime-handling.md).

## The rule

- **Never `new Date()` for business logic.** The container runs in UTC, so `new Date()` and
  `toISOString().slice(0, 10)` give UTC's date: a US park's evening becomes tomorrow, an Asian
  park's morning yesterday. Schedules, the calendar, gap-fill, holidays, ML and analytics all
  ask for the park's day.
- **Use the helpers in `src/common/utils/date.util.ts`:** `getCurrentDateInTimezone`,
  `getTomorrowDateInTimezone`, `getYesterdayDateInTimezone`, `getStartOfDayInTimezone`,
  `getCurrentTimeInTimezone`, `isSameDayInTimezone`, `formatInParkTimezone`, `addIsoDays`.
- **A date-only value is a calendar day, not an instant.** A holiday date, a schedule date or a
  Postgres `DATE` column must not go through `new Date(…)` and back through a timezone formatter;
  that shifts it by a day on one side of UTC
  ([§6–§7 of the guide](../development/datetime-handling.md)).
- **Python:** one pandas column carries one timezone, so park-local time is stored naive in
  `local_timestamp` and never compared against a `TIMESTAMPTZ`
  ([§8 of the guide](../development/datetime-handling.md)).
- **Not a park's day: a reader's.** The [site statistic](../frontend/site-analytics.md) and the
  [page-view rankings](../frontend/page-views.md) count per UTC day (`utcDay` in
  `src/common/utils/reader-hash.util.ts`): a page view belongs to a reader, and readers sit in
  every zone.
