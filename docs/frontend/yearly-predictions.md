# Yearly predictions (`/predictions/yearly`)

The contract for a park's day-by-day crowd forecast over the next months: what each day carries, how
`recommendation` follows from the crowd level, and why an empty list is not "no crowds". Open it when
rendering a long-range calendar or a best-day list. How closed days and `holidays` are decided:
[calendar rules §1.3](../architecture/calendar-schedule-and-ml-rules.md#13-yearly-predictions-use-the-same-closed-rule).
The precomputed best days of the next 90 days: [best days](./best-days-endpoint.md).

```jsonc
// GET /v1/parks/:continent/:country/:city/:park/predictions/yearly
{ "park": { "id": "…", "name": "Phantasialand", "slug": "phantasialand" },
  "predictions": [
    { "date": "2026-10-28", "crowdLevel": "high", "confidencePercentage": 81.4,
      "recommendation": "neutral", "source": "ml", "avgWaitTime": 37 },
    { "date": "2026-11-02", "crowdLevel": "closed", "confidencePercentage": 76, "recommendation": "closed", "source": "ml" }
  ],
  "holidays": [{ "date": "2026-11-01", "isHoliday": true, "isSchoolVacation": false,
                 "isBridgeDay": false, "holidayName": "All Saints' Day" }],
  "generatedAt": "2026-10-27T09:12:44.000Z" }
```

Built by `ParkIntegrationService.aggregateDailyPredictions` from `MLService.getParkPredictionsYearly`,
the same daily forecast the calendar serves.

## A day

- **Dates run from the park's today to about 182 days out, with gaps.** A date the model left out is
  missing; a missing date is not a closed day.
- **`crowdLevel`** is the mean predicted wait of the park's headliners (of all rides when it has none)
  against its typical-day peak, the calendar's rule for a future day ([crowd levels](../rules/crowd-levels.md)).
  `closed` follows the schedule as the calendar does. `unknown` means no typical-day peak or no
  predicted wait to rate.
- **`avgWaitTime`** is that mean rounded to the minute, not to 5; round it before display. Absent on a
  `closed` day and on an `unknown` day without waits.
- **`recommendation`** depends on the crowd level alone:

  | `crowdLevel` | `recommendation` |
  | --- | --- |
  | `very_low`, `low` | `highly_recommended` |
  | `moderate` | `recommended` |
  | `high` | `neutral` |
  | `very_high` | `avoid` |
  | `extreme` | `strongly_avoid` |
  | `closed` | `closed` |
  | `unknown` | absent |

- **`confidencePercentage`** is the unrounded mean of the rides' `confidence`, a 30–100 score of how
  far ahead the forecast reaches, not of how accurate it is; a ride without one counts as 0.
- `holidays` covers the park's region from today through 364 days ahead, independent of the forecast,
  and is absent when the holiday table could not be read.

## What the response does not say

- **`generatedAt` is when this response was built**, not when the forecast was computed.
- **An empty `predictions` can be an outage.** When the forecast cannot be read the endpoint answers
  `predictions: []`, and the edge keeps that for 24 hours like any other answer (the route has no
  degraded-body check). Redis keeps only a real forecast, for 13 hours per park and park-local day.
