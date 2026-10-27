# A ride's `status` and `effectiveStatus`

Every ride on the park payload (`GET /v1/parks/…/{park}`, `attractions[]`) and on the attraction
detail carries two statuses. Read **`effectiveStatus`** to decide whether a visitor can queue for
the ride now; `status` is the ride's own reading before the park is taken into account. The
catalog list `…/{park}/attractions` carries neither.

## How each is derived

The two endpoints do not run the same chain: the park payload (`ParkIntegrationService`) has the
optimistic fallback and the silent-park gate, the detail (`AttractionIntegrationService`) has
neither, so one ride can read differently on the two. Why each case exists:
[status regimes](../architecture/attraction-status/status-regimes.md) (the silent park:
[§5](../architecture/attraction-status/status-regimes.md#5-a-whole-park-gone-silent)).

| Case | park payload `status` | detail `status` |
| --- | --- | --- |
| The park has no readable wait-time source ([live wait times](./live-wait-times-availability.md)) | `UNKNOWN` while the park is open, `CLOSED` when it is not; queue rows dropped | decided by the rows below; queue rows dropped |
| The park's whole feed has been silent for `PARK_FEED_SILENT_DAYS` (30) | as the row above | not gated: decided by the rows below |
| Park open, every row written by reverse reconciliation (`readsUnknownFromAbsentSource`) | `UNKNOWN`, rows dropped | `UNKNOWN`, rows dropped |
| Live queue rows exist | the first row's status (`OPERATING`, `DOWN`, `CLOSED`, `REFURBISHMENT`) | the same |
| No live row | `statusWithoutLiveData`: `CLOSED` when the park is not `OPERATING` or the ride is out of season (`isCurrentlyInSeason === false`), otherwise `OPERATING` (the optimistic fallback) | `CLOSED`, the placeholder from `AttractionResponseDto.livePlaceholders()` |
| Free-flow ride (`open_with_park`, `isFreeFlowOpen`) | `OPERATING` with a synthetic 0-minute queue while the park is open, readable and inside the ride's season months | the same, without the readable check |

`effectiveStatus`:

- **`CLOSED` whenever the park is `CLOSED`**, on both endpoints, whatever the ride reported.
- Otherwise `status` on the park payload. The detail gives `status` while the park's wait times
  are readable and `UNKNOWN` when they are not, whatever `status` says.

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
- `OPERATING` from the no-data fallback (park payload only) is a guess about a ride no source has
  mentioned recently, not an observation (PAR-1100 tracks the ride no source has ever reported).
- Neither field says anything about a works period: read [`worksPeriod`](./works-period.md).
