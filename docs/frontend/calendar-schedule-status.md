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

### 4. `scheduleCoverage` (`{ from, to }`) — how far the schedule reaches

On **`meta`** of the calendar response and on the **park detail payload**, so a client can read it
without asking for a calendar. That matters at scale: deciding which calendar months to publish
for 212 parks is one field on a payload you already fetch, not 212 calendar calls.

`from` and `to` are the first and last dates with a park-level `OPERATING` entry (`YYYY-MM-DD`,
park timezone), or `null` at both ends when the park has none — the same park
`hasOperatingSchedule: false` describes. The park **listing** deliberately does not carry it: it
is a per-park fact that would be paid for on every row of every list.

**Read `to` before you trust a future `status`.** Inside the window a status is a statement about
the park. Past `to` it is a statement about our sync: no schedule row exists, and for a park with
a history of seasonal closures the gap-fill resolves that to `CLOSED`. That is right shortly after
a season ends and wrong far beyond it — on 2026-08-28 this endpoint answered `CLOSED` for every
day of **July 2027** at Phantasialand and Europa-Park, which are open in July, simply because
their 2027 hours had not been published. Toverland and Disneyland Paris answered the same range
with real crowd levels. Nothing in `days[]` tells the two apart; `scheduleCoverage.to` does.

So a client should:

- **Stop at `to`** when paginating months, building a month index, or emitting sitemap URLs. A
  month entirely past `to` is not a page worth publishing — it renders a full grid of confident
  closures for a park that will very likely be open.
- **Not render `CLOSED` as "closed" past `to`.** Treat it as unknown, the way `UNKNOWN` is treated:
  "opening hours not yet available".
- Expect `to` to move forward on its own as parks publish. It is derived from the rows, not
  configured, so nothing needs editing when a park releases its next season.
- **Guard the field itself for one cache generation after the deploy that adds it.** The park
  payload is cached in Redis for 3 min (open) to 6 h (closed), and those entries survive a deploy,
  so a payload built before it carries no `scheduleCoverage` at all. Read it as
  `scheduleCoverage?.to` and treat "absent" the same as `null`: coverage unknown, fall back to
  whatever the client did before. The cache key was deliberately **not** versioned to force this
  out — `park:integrated` also backs the nearby and favorites MGET fast paths, and cold-starting
  those to shorten a one-time six-hour window is the worse trade.

```jsonc
"meta": {
  "slug": "phantasialand",
  "timezone": "Europe/Berlin",
  "hasOperatingSchedule": true,
  "scheduleCoverage": { "from": "2025-12-26", "to": "2027-01-06" }
}
```

---

## Meanings & Display logic

| status        | Meaning                                                       | Crowd Prediction | Display recommendation                                          |
| ------------- | ------------------------------------------------------------- | ---------------- | --------------------------------------------------------------- |
| **OPERATING** | Park is open.                                                 | **YES**          | Show times. If `isEstimated: true`, add a "reconstructed" hint. |
| **CLOSED**    | Park is **definitively closed** (official or seasonal gap).   | **NO**           | e.g. "Closed" – tag is typically greyed out.                    |
| **UNKNOWN**   | No official hours yet, but **might be open** (trip planning). | **YES**          | Show crowd level. For hours show "TBA" or "Not yet known".      |

---

## Important Rules

- **UNKNOWN ≠ closed.** UNKNOWN means we don’t have official hours, but the park could be open. **Always show crowd predictions** for UNKNOWN days if they are available.
- **The same rule governs the live `status` in listings.** A park whose schedule
  publishes only UNKNOWN rows for today is judged from its live ride feed, not called
  closed — so `/v1/discovery/continents/*`, `/v1/discovery/nearby` and
  `/v1/analytics/geo-live` agree with the park detail response. A `CLOSED` row for
  today still wins over the ride feed. Note that `hasOperatingSchedule` keeps its
  wider meaning below (_has this park ever published hours_), so it is not a signal
  about today.
- **CLOSED = no predictions.** When a park is closed, the API suppresses ML predictions to prevent "ghost" wait times.
- **Disclaimer**: If `hasOperatingSchedule` is `false`, the park page should show a disclaimer: _"Official hours are not available for this park. Data is estimated based on attraction activity."_

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
