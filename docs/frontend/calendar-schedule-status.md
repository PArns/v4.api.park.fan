# Calendar: status (ParkStatus) – UNKNOWN vs CLOSED

Short guide for the frontend: how to display **opening hours** and **crowd predictions** in the calendar and how to tell **UNKNOWN** from **CLOSED**.

## Key Fields

### 1. `status` (ParkStatus)
Each calendar day has a **status** field:
- **`status`**: `ParkStatus` = `"OPERATING"` | `"CLOSED"` | `"UNKNOWN"`

### 2. `isEstimated` (boolean) - NEW
- `true`: The day's status and hours were **reconstructed from ride activity** (Smart Gaps).
- `false` or `undefined`: Data is from an official source or the day is confirmed closed.
- **Application**: Only used for **historical dates**.

### 3. `hasOperatingSchedule` (boolean) - NEW
Found in the `meta` object of the calendar response and the park detail response.
- `true`: The park provides official opening hours.
- `false`: The park does not provide official hours. Opening times are either null (future) or reconstructed (past).

---

## Meanings & Display logic

| status      | Meaning | Crowd Prediction | Display recommendation |
|------------|---------|------------------|--------------------------|
| **OPERATING** | Park is open. | **YES** | Show times. If `isEstimated: true`, add a "reconstructed" hint. |
| **CLOSED**    | Park is **definitively closed** (official or seasonal gap). | **NO** | e.g. "Closed" – tag is typically greyed out. |
| **UNKNOWN**   | No official hours yet, but **might be open** (trip planning). | **YES** | Show crowd level. For hours show "TBA" or "Not yet known". |

---

## Important Rules

- **UNKNOWN ≠ closed.** UNKNOWN means we don’t have official hours, but the park could be open. **Always show crowd predictions** for UNKNOWN days if they are available.
- **CLOSED = no predictions.** When a park is closed, the API suppresses ML predictions to prevent "ghost" wait times.
- **Disclaimer**: If `hasOperatingSchedule` is `false`, the park page should show a disclaimer: *"Official hours are not available for this park. Data is estimated based on attraction activity."*

---

## Example (TypeScript)

```ts
function getScheduleLabel(day: CalendarDay, parkMeta: CalendarMeta): string {
  if (day.status === "OPERATING") {
    const timeStr = day.hours
      ? `${formatTime(day.hours.openingTime)} – ${formatTime(day.hours.closingTime)}`
      : "Open";
    return day.isEstimated ? `${timeStr} (Estimated)` : timeStr;
  }
  
  if (day.status === "CLOSED") {
    return "Closed";
  }

  // UNKNOWN
  return "Times TBA";
}

function shouldShowCrowdLevel(day: CalendarDay): boolean {
  // Show crowd levels even if times are not known yet (trip planning)
  // but never show them for definitively closed days.
  return day.status === "OPERATING" || day.status === "UNKNOWN";
}
```

---

## API reference

- **Endpoint:** `GET /v1/parks/:continent/:country/:city/:parkSlug/calendar?from=&to=`
- **Response:** `meta`, `days[]` with `date`, `status`, `isEstimated`, `hours?`, `crowdLevel`, ...
- Technical details: [Smart Gaps Documentation](../analytics/smart-gaps.md)
