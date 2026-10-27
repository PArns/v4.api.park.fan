# A ride's `status` and `effectiveStatus`

Every ride on the park payload (`GET /v1/parks/…/{park}`, `attractions[]`) and on the attraction
detail carries two statuses. Read **`effectiveStatus`** to decide whether a visitor can queue for
the ride now; `status` is the ride's own reading before the park is taken into account. The
catalog list `…/{park}/attractions` carries neither.

## How each is derived

`status` (park payload: `ParkIntegrationService`; detail: `AttractionIntegrationService`):

| Case | `status` |
| --- | --- |
| The park has no readable wait-time source ([live wait times](./live-wait-times-availability.md)) | `UNKNOWN` while the park is open, `CLOSED` when it is not; queue rows dropped |
| Every row was written by reverse reconciliation, not a source (`readsUnknownFromAbsentSource`) | `UNKNOWN`, rows dropped |
| Live queue rows exist | the first row's status (`OPERATING`, `DOWN`, `CLOSED`, `REFURBISHMENT`) |
| No live row (`statusWithoutLiveData`) | `CLOSED` when the park is not `OPERATING` or the ride is out of season (`isCurrentlyInSeason === false`), otherwise `OPERATING` (the optimistic fallback, PAR-1100) |
| Free-flow ride (`open_with_park`, `isFreeFlowOpen`) | `OPERATING` with a synthetic 0-minute queue while the park is open, readable and inside the ride's season months |

`effectiveStatus`:

- **`CLOSED` whenever the park is `CLOSED`**, whatever the ride reported.
- Otherwise `status`. The detail endpoint applies the readable-source gate itself: an open park
  without a readable source gives `UNKNOWN`, matching the park payload.

## What reads it

- **Counts.** `operatingAttractions` and the total count use `effectiveStatus`; a ride closed by
  its season counts in neither unless it reads `OPERATING` ([seasonality](./seasonality.md)).
- **Crowd level.** A ride whose `effectiveStatus` is `CLOSED` gets `crowdLevel: "closed"`; the
  live level is rated only for a running ride ([ride live crowd level](./ride-live-crowd-level.md)).
- **`bestVisitTimes`** keeps the current slot only while `effectiveStatus` is `OPERATING`.
- **Reopen alerts** apply the same park rule: in a closed park every ride reads as not operating,
  even where a source keeps reporting `OPERATING` (`RideAlertsService.diffAndPersistReopen`).

## What a client may not conclude

- `UNKNOWN` is an absence of information, never a closure; render it apart from `CLOSED`.
- `OPERATING` from the no-data fallback is a guess about a ride no source has mentioned recently,
  not an observation (PAR-1100 tracks the ride no source has ever reported).
- Neither field says anything about a works period: read [`worksPeriod`](./works-period.md).
