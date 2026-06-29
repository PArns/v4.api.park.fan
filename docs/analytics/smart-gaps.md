# Smart Gaps: Operating Hours & Status Inference

The "Smart Gaps" system is a core part of the park.fan API intelligence. It allows the platform to provide a rich user experience even for parks that do not provide official operating hours via an API (e.g., Efteling, Hellendoorn) and ensures accurate data across seasonal closures.

## 1. The Problem
Many theme parks only provide real-time wait times but no digital calendar for their opening hours. Without this data:
- Future planning is difficult (users don't know if the park might be open).
- Historical analytics are messy (no clear "opening" or "closing" time to anchor wait-time curves).
- Seasonal closures look like "data gaps" rather than planned shutdowns.

## 2. Historical Hour Reconstruction (Smart Gaps)

For past days without official schedule data, the system reconstructs opening hours from raw attraction activity.

### The Algorithm
The reconstruction works in 15-minute sliding windows:

1.  **Gastro/Service Filtering:** Attractions with names containing keywords like *bar, snack, restaurant, shop, cafe, corner, hire* are excluded from activity measurements to prevent 24h false-positives from service points.
2.  **Activity Threshold (10%):** A park is considered "active" in a window only if at least **10% of its rides** (minimum 2, maximum 10) simultaneously show a wait time of **≥ 5 minutes**. This filters out early-morning test runs and maintenance.
3.  **Opening Time:** The start of the first 15-minute window that reaches the threshold, rounded **down** to the nearest full hour (e.g., 10:15 AM → 10:00 AM).
4.  **Closing Time:** The end of the last 15-minute window that reaches the threshold, rounded **up** to the nearest full hour (e.g., 5:15 PM → 6:00 PM).
5.  **Status Validation:** If the threshold is never reached for a day, the day is marked as `CLOSED`, and all crowd/analytics data is suppressed to prevent "ghost" records.

## 3. Seasonal Detection

The system automatically distinguishes between year-round and seasonal parks via the `isParkSeasonal` check.

- **Logic:** It scans the database for historical gaps of more than 21 days between `OPERATING` entries.
- **Seasonal Parks:** Future dates outside the known operating range are automatically marked as `CLOSED`. This prevents showing crowd predictions for the middle of winter in a park like Hansa-Park.
- **Year-Round Parks:** Future dates without hours remain as `UNKNOWN`. This allows showing ML-powered crowd and ride-time predictions for trip planning, even if official hours aren't published yet.

## 4. API Flags & Signals

The API provides three main signals to the frontend to communicate data reliability:

| Flag | Scope | Meaning |
|------|-------|---------|
| `hasOperatingSchedule` | Park | `false` if the park doesn't provide an official API calendar. Signals to the frontend to show a "Best Effort" disclaimer. |
| `isEstimated` | Day | `true` if the status or hours for this day were reconstructed from activity. |
| `isInferred` | Hours | `true` if the specific start/end times are derived/rounded. |

## 5. ML Integration

The reconstruction logic is mirrored in the Machine Learning `buildFeatureContext`. 

Instead of using static assumptions (like "Parks usually open at 9 AM"), the ML service receives the **actual detected opening time** for the current day. If the 10% threshold hasn't been met yet, the model receives `null` for the opening time, allowing it to correctly predict "Closed/0min" during the early morning hours.

## 6. Implementation References
- **SQL Logic:** `ParksService.getDerivedHistoricalHours` (uses Window Functions for performance).
- **Inference Logic:** `CalendarService.buildCalendarDay`.
- **Batch Processing:** `ParksService.getBatchHasOperatingSchedule`.
