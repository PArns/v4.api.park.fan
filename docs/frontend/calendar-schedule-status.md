# Calendar: status (ParkStatus) – UNKNOWN vs CLOSED

Short guide for the frontend: how to display **opening hours** in the calendar and how to tell **UNKNOWN** from **CLOSED**.

## Field: `status` (ParkStatus)

Each calendar day has **one** field:

- **`status`**: `ParkStatus` = `"OPERATING"` | `"CLOSED"` | `"UNKNOWN"`

From this you can tell both “park open/closed” and “do we have schedule data at all?”.

---

## Meanings

| status      | Meaning | Display recommendation |
|------------|---------|--------------------------|
| **OPERATING** | Park has opening hours (from source). | Show opening and closing times (e.g. from `hours.openingTime` / `hours.closingTime`). |
| **CLOSED**    | Park is **confirmed closed** on this day (source returns “Closed”). | e.g. “Closed” – no time range. |
| **UNKNOWN**   | **No opening hours from source yet** (month not yet published or placeholder). | e.g. “Opening hours not yet available” or “Not yet published” – **not** “Closed”. |

---

## Important

- **UNKNOWN ≠ closed.** UNKNOWN means: we don’t have real schedule data for this day yet (e.g. May 2026 until the park publishes that month).
- Use **CLOSED** only when the park is explicitly reported as closed for that day.
- When `status === "UNKNOWN"`: don’t show opening hours and make it clear that the info is still missing (not that the park is closed).

---

## Example (TypeScript)

```ts
function getScheduleLabel(day: CalendarDay): string {
  switch (day.status) {
    case "OPERATING":
      return day.hours
        ? `${formatTime(day.hours.openingTime)} – ${formatTime(day.hours.closingTime)}`
        : "Open";
    case "CLOSED":
      return "Closed";
    case "UNKNOWN":
    default:
      return "Opening hours not yet available";
  }
}
```

---

## API reference

- **Endpoint:** `GET /v1/parks/:continent/:country/:city/:parkSlug/calendar?from=&to=`
- **Response:** `days[]` with each `date`, **`status`** (ParkStatus: OPERATING | CLOSED | UNKNOWN), `hours?`, …
- Backend details: [Schedule Sync & Calendar](../architecture/schedule-sync-and-calendar.md)
