# Ride downtime

How often and for how long rides are reported down, what we publish about it and what we refuse to.
Start here, then open the page for the part you are changing.

| Question                            | Answer                                                                                                                                                                                       |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Is this ride down right now?        | **Yes**: the live `outage` line, from the feed's `DOWN` or, in parks whose feed never says `DOWN`, from a closure inside opening hours.                                                       |
| How often, how long (last 90 days)? | **Yes, gated**: a count of _reported_ outages, the empirical median, the longest and `downShare`. Never a rate.                                                                              |
| It is down now. How much longer?    | **Yes**: the measured recovery curve, read at the outage's elapsed operating minutes ([recovery estimate](recovery-estimate.md)).                                                            |
| When will it break next?            | **No.** Not as a number, a sentence or a softer rewording ([refused](refused.md)).                                                                                                           |

## Pages

| Page                                            | Covers                                                                                                                         |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| [reconstruction.md](reconstruction.md)          | The outage interval, the exposure denominator, the five tables, the nightly job and its deadlines, the staged fill, the traps. |
| [publishing.md](publishing.md)                  | The park regimes, the ride gates and withheld reasons, the live line, the non-negotiables.                                     |
| [closure-gap-signal.md](closure-gap-signal.md)  | The second signal (`closed_gap`) for parks whose feed never emits `DOWN`: filters, standing closures, the window rules.         |
| [recovery-estimate.md](recovery-estimate.md)    | How long a running outage still has to go: the curve, its calibration, `recoveryWindow`.                                       |
| [refused.md](refused.md)                        | Why there is no forecast of the next outage, the erasure measurement, and the list of rejected ideas.                          |

## What is built

| Part                                                    | Where                                                                                                                                                                                    |
| ------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Live line `outage` on the park and attraction payloads  | `AttractionOutageService` (`src/attractions/services/attraction-outage.service.ts`), row filter `src/common/utils/outage-rows.sql.ts`, closure statement `src/common/utils/closure-gap.sql.ts` |
| Nightly reconstruction, `downtime` queue, 05:00 UTC     | `DowntimeReconstructionProcessor` (`src/queues/processors/downtime-reconstruction.processor.ts`), SQL in `src/analytics/utils/outage-reconstruction.sql.ts`                              |
| Historical figures, `downtime` on the attraction detail | `DowntimeProfileService` (gates), `toDowntimeBlock` (`src/attractions/dto/downtime-reliability.dto.ts`); rendered by the frontend's `AttractionDowntimeSection`                          |
| Recovery estimate, `outage.estimate`                    | `DowntimeRecoveryService`, `src/analytics/utils/downtime-estimate.util.ts`, `operating-clock.util.ts`                                                                                    |
| Write-path columns `is_heartbeat`, `raw_status`, `last_merged_at` | Applied by `synchronize` at boot on 2026-09-07                                                                                                                                 |
| Erasure measurement                                     | `GET /v1/admin/downtime-erasure` (`src/admin/downtime-measurement.service.ts`); measured 2026-10-22, see [refused.md](refused.md#the-erasure-measured)                                   |
| Event census                                            | `GET /v1/admin/downtime-measurement`                                                                                                                                                     |

The plan required a hand check of twenty publishable rides against the parks' own channels before
historical figures went out. The figures are now served; no record of that check exists in either repo.

## Wording

Every figure says the outage was **reported** (`gemeldet` / `reported` / `signalée` / `gemeld` /
`segnalata` / `notificada`), never that the ride was broken. The copy lives in the frontend's
`attractions.downtime` messages namespace.
