# Calendar, Schedule & ML Rules

Single source of truth for how **status** (OPERATING / CLOSED / UNKNOWN), **crowd level**, and **schedule sync** behave in the API and how the **ML service** aligns with these rules.

Related: [Schedule Sync & Calendar](schedule-sync-and-calendar.md), [ML Model Overview](../ml/model-overview.md), [Frontend: Calendar status](../frontend/calendar-schedule-status.md).

---

## 1. Calendar API Rules (Park has schedule in general)

**Endpoint:** `GET /v1/parks/:continent/:country/:city/:parkSlug/calendar?from=&to=`

**Source:** `CalendarService.buildCalendarResponse()` → `buildCalendarDay()` in `src/parks/services/calendar.service.ts`.

### 1.1 Status (ParkStatus)

| Rule | Implementation |
|------|----------------|
| **Future:** OPEN/CLOSED from schedule | `status` comes from `schedule?.scheduleType`: OPERATING → `"OPERATING"`, CLOSED → `"CLOSED"`, UNKNOWN or no entry → `"UNKNOWN"`. |
| **Future:** Days without schedule = UNKNOWN, but with crowd prediction | Future days without schedule keep `status: "UNKNOWN"`. They still get a **crowdLevel** (ML prediction or fallback `"moderate"`), not `"closed"`. |
| **Past + Today:** With crowd level treated as OPEN | Only for **past + today** (`dateStr <= today`): if `status === "UNKNOWN"` and computed crowd level ≠ `"closed"` → `status = "OPERATING"` so we have something to show. Future UNKNOWN are **not** upgraded to OPERATING. |

### 1.2 Crowd Level

- **OPERATING:** `crowdLevel = inferredCrowdLevel` (historical: Redis/Stats/Queue/Analytics; future: ML).
- **UNKNOWN + future:** `crowdLevel = inferredCrowdLevel` (ML prediction or fallback `"moderate"`) – **not** `"closed"`.
- **CLOSED** or past without opening data: `crowdLevel = "closed"`.

### 1.3 Important

- **Crowd level does not influence status** (e.g. no override UNKNOWN → CLOSED just because crowdLevel is "closed"). Status comes from schedule or from the rule “past/today + crowd level = OPEN”.
- **UNKNOWN** = “Opening hours not yet available” (frontend); **CLOSED** = “Closed”.

---

## 2. Schedule Sync

**Jobs:** `sync-park-schedule` (on-demand), `sync-schedules-only` (daily 15:00), `sync-all-parks` (daily 03:00).

- **ThemeParks API:** The previous month plus 12 months ahead are requested (`getScheduleExtended`). All returned entries (OPERATING, CLOSED, …) are persisted.
- **Normalisation:** In `ParksService.saveScheduleData()`, `entry.type` for **CLOSED** is normalised: `"Closed"` / `"CLOSED"` (case-insensitive) → `ScheduleType.CLOSED`, so off-season (e.g. Phantasialand February) is stored as CLOSED when the API provides it.
- **Cleanup when saving from API:** Bidirectional: when we save **OPERATING** for a date, we delete any **CLOSED** row; when we save **CLOSED** for a date, we delete any **OPERATING** row. This ensures one source of truth per date.
- **Gaps:** `fillScheduleGaps(parkId)` fills **missing** days (from today up to 120 days ahead, park timezone) with holiday/bridge metadata and either **CLOSED** or **UNKNOWN**:
  - **CLOSED:** There is at least one OPERATING day before and one after this date (in stored schedule) → gap is "in the middle", so we treat it as closed.
  - **UNKNOWN:** No OPERATING for the park, or this date is before the first OPERATING (e.g. before we have data) or on/after the last OPERATING (schedule not yet published). So we keep "opening hours not yet available".
  - **Demotion:** Gap-fill CLOSED (no opening/closing times) that is now after the last OPERATING gets demoted to UNKNOWN.

Details: [Schedule Sync & Calendar](schedule-sync-and-calendar.md).

---

## 3. Next Schedule (Park & Location APIs)

**Used by:** Park integrated response (`/v1/parks/.../integrated`), location/nearby, favorites.

- **Source:** `ParksService.getNextSchedule(parkId)` (and batch `getBatchSchedules`).
- **Behaviour:** Returns the **next OPERATING** day only: query filters by `scheduleType = OPERATING`, `openingTime IS NOT NULL`, `closingTime IS NOT NULL`, from park’s “tomorrow” up to 365 days ahead. **CLOSED** and **UNKNOWN** days are ignored.
- **Result:** `dto.nextSchedule` (openingTime, closingTime, scheduleType) is always an operating day when present; no UNKNOWN/CLOSED in next schedule. TTL and dynamic TTL for closed parks (e.g. expire before next opening) use this next OPERATING entry.

---

## 4. ML Service Rules & Alignment

### 4.1 Where schedule matters

| Component | File | Behaviour |
|-----------|------|-----------|
| **Schedule filter (after prediction)** | `ml-service/schedule_filter.py` | Filters predictions by `schedule_entries`. |
| **Inference (features + output)** | `ml-service/predict.py` | Sets `is_park_open`, `status`; skips model inference for CLOSED/UNKNOWN rows (reduces load). |

### 4.2 filter_predictions_by_schedule (schedule_filter.py)

- **Daily predictions:** Only days with an **OPERATING** entry are kept. Days with only CLOSED or UNKNOWN (no OPERATING on that day) are **removed**.
- **Fallback:** If the park has **no** schedule integration (no OPERATING entry in the query), **all** daily predictions are kept (“no schedule” → keep all).
- **Hourly predictions:** Only times **within** operating hours (openingTime–closingTime) are kept; times outside or days without OPERATING with times are filtered out.

**Effect on calendar:** For **future UNKNOWN days** the ML API returns **no** daily prediction (because the day is not OPERATING). The calendar then uses the fallback `mlPrediction?.crowdLevel || "moderate"` for those days – they get a display crowd level (e.g. “moderate”) but not a real ML prediction for that day. To show real ML predictions for UNKNOWN days, the ML filter would need to be extended to optionally return predictions for UNKNOWN days (for parks with schedule).

### 4.3 predict.py (Inference)

- **Schedule features:** From `schedule_entries`, OPERATING/CLOSED/UNKNOWN are evaluated per park/date. A park with no OPERATING entry at all is treated as “no schedule” → predictions are kept.
- **Inference skip for CLOSED/UNKNOWN:** Rows with `status === "CLOSED"` or `"UNKNOWN"` are **excluded before** model.predict. No inference is run for definitively closed days (reduces system load; those predictions would be filtered out anyway).

### 4.4 Summary: ML ↔ Calendar

| Aspect | Calendar (API) | ML Service |
|--------|----------------|------------|
| CLOSED day | `status: "CLOSED"`, `crowdLevel: "closed"` | No inference run (predict.py skips CLOSED rows); filter_predictions_by_schedule would remove anyway. |
| UNKNOWN day (future) | `status: "UNKNOWN"`, `crowdLevel` = ML or fallback “moderate” | Daily prediction is **not** returned (only OPERATING days); calendar uses fallback “moderate”. |
| Past/today without schedule | With crowd level → `status: "OPERATING"` | Independent; calendar derives status from crowd data. |

---

## 5. Implementation checklist

- [x] Calendar: Status only from schedule + rule “past/today + crowd level = OPEN”; no status override by crowdLevel.
- [x] Calendar: Future UNKNOWN days always have a crowdLevel (ML or “moderate”), never blanket “closed”.
- [x] Schedule sync: API type “Closed”/“CLOSED” is stored as `ScheduleType.CLOSED`.
- [x] ML: Daily predictions only for OPERATING days (filter_predictions_by_schedule); fallback “no schedule integration” → keep all.
- [x] ML: CLOSED/UNKNOWN in predict.py → `crowdLevel: "closed"` in response.
