# Ride downtime: what is published

The live `outage` line, the per-park regime, the per-ride gates that decide whether a historical figure
is shown, and the rules no surface may break. Open it before changing `DowntimeProfileService`,
`toDowntimeBlock`, `AttractionOutageService` or a client that renders them. Index: [README](README.md).

## The live line (`outage`)

`AttractionOutageService.getCurrentOutages` attaches `outage` to rides on the park payload
(`park-integration.service`) and the attraction detail (`attraction-integration.service`), from one
service so the two status chains cannot disagree. Fields (`src/attractions/dto/attraction-outage.dto.ts`):

- `startedAt`, and `startObserved: false` when the run reaches past the 7-day lookback
  (`TRAILING_OUTAGE_LOOKBACK_DAYS`) — then a client names the day, not a clock time.
- `signal`: `down` (the park's feed said `DOWN`, only possible in a `wiki_entity_id` park) or
  `closed_gap` (we saw the ride stop inside opening hours and nobody reported it; see
  [closure-gap signal](closure-gap-signal.md)). The wording differs: „Störung gemeldet seit 14:20 Uhr"
  against „Steht seit 14:20 Uhr still".
- `estimate`: the [recovery estimate](recovery-estimate.md), absent when no curve covers it.

The service never asks about a ride the caller already resolved to `OPERATING` (the line follows the
badge it sits under), nor inside a curated works period (`isCuratedOutOfService()`). It is a clock time,
never an elapsed counter, and the frontend marks it `data-nosnippet`.

Proposed in the plan and **not built**: `outage.minutesToday` and a park-level `ridesDownNow`.

## Park regimes

`park_downtime_coverage.regime`, rebuilt nightly for every park by `DowntimeProfileService.rebuildCoverage`,
decided in this order:

| Regime           | Condition                                                                                                         |
| ---------------- | ----------------------------------------------------------------------------------------------------------------- |
| `not_capable`    | No `parks.wiki_entity_id`: no source there can emit `DOWN`. Read from configuration, never from the outcome.      |
| `no_schedule`    | No usable published `OPERATING` window at all; any denominator would be circular.                                 |
| `never_reports`  | Listed at ThemeParks.wiki, has never emitted `DOWN`, and has at least `MIN_BLIND_EVIDENCE_HOURS` (1,500) observed operating hours. |
| `artefact`       | The feed is hourly: one minute-of-hour value holds ≥ 50 % of run edges (`artefactOnTheHourShare`, ≥ 20 edges). |
| `outside_window` | Publishes hours, none inside the 90-day window. The only regime that ends on its own.                             |
| `reports`        | Everything else.                                                                                                  |

**Why `never_reports` exists.** `wiki_entity_id` says a `DOWN` is possible, not that the park's feed
carries it. Measured 2026-09-07: 102 of 182 scheduled parks had no `DOWN` row in 180 days, among them
Phantasialand, Energylandia, Hersheypark, Alton Towers and PortAventura, and together they had more
observed operating time than the reporting parks (736,738 h against 583,782 h). Read as `reports`, every
ride there would have said "no outage reported in 90 days": our blindness printed as the operator's
clean record. It is the one place capability is read from the outcome, and the evidence floor licenses
it: 1,500 hours at the reporting parks' 10th-percentile rate (3.46 per 1,000 h) predicts 5.2 events, so
silence has probability 0.0056 for a park that reports. Counts after the 270-day fill (2026-09-11):
97 `reports`, 94 `never_reports`, 16 `not_capable`, 6 `no_schedule`.

**Why the artefact test measures resolution.** The first test (70 % of runs resting on one reading)
correlated with "share of outages under 60 minutes" at r = 0.996 and marked 47 of 78 parks unreadable,
including EPCOT, which a second source cannot even erode. `queue_data` is a change log, so a short
outage writes exactly one row. The minute-of-hour test is timezone-independent (an hourly feed at :30
piles up on :30); its highest production value is 0.236 (Lotte World), so **no park is in this regime**.

## Ride gates

`decideProfile()` in `src/analytics/downtime-profile.service.ts` returns figures or a withheld reason. The
order is the design: the ride's own obstacle, then the park's capability, then what makes a number
meaningless, then what makes it imprecise, homogeneity last.

| Reason                                                                                     | When                                                                                                         |
| ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| `free_flow`                                                                                | `open_with_park` ride: no queue to show an outage in.                                                        |
| `not_down_capable`, `park_never_reports`, `no_schedule`, `outside_window`, `artefact_regime` | From the park regime. Also `not_down_capable` when no profile row exists.                                  |
| `recently_merged`                                                                          | `last_merged_at` inside the window.                                                                          |
| `new_ride`                                                                                 | Fewer than 30 lifetime observed days.                                                                        |
| `thin_events`                                                                              | Fewer than 24 outages or fewer than 12 with an observed end.                                                 |
| `thin_exposure`                                                                            | Fewer than 40 observed operating days or 150 operating hours.                                                |
| `heavily_censored`                                                                         | More than 25 % of outages censored. Seasonal by design (89.8 % of raw runs in March, 20.5 % in September).   |
| `inhomogeneous`                                                                            | Both window halves have ≥ 6 events and differ more than 3×.                                                  |
| `stale_data`                                                                               | Set at read time when the profile is older than 2 days (`MAX_PROFILE_AGE_DAYS`); a permanent reason wins over it. |

All thresholds are `DOWNTIME_GATES`. The event floor of 24 was confirmed on 2026-09-06 (90 days, 150,132
events over 2,228 rides in 78 parks; 1,324 rides clear 24). It applies to every derived figure, not only
to a rate.

**What publishes** (`DowntimeFiguresDto`): `outages` in `windowDays` (90), `observedDays`, `medianMinutes`
(the empirical median of outages with an observed end, rounded to five, over `usableDurations` — never a
Kaplan-Meier median, so the sentence beside it is checkable), `longestMinutes`, and `downShare`, the only
denominator on the block. A withheld block carries the reason and the count, never the inputs, so no
client can re-derive a different verdict.

`PERMANENT_REASONS` (`not_down_capable`, `park_never_reports`, `artefact_regime`, `no_schedule`,
`free_flow`) are never replaced by `stale_data`: telling a blind park its numbers are "not current"
would promise figures that cannot arrive. `outside_window` is deliberately not permanent.

## Non-negotiables

1. No cross-park comparison, ranking, reliability score, listing badge, sort or filter by outages. The
   difference between two parks is a difference between two feeds.
2. None of these numbers in structured data: no `aggregateRating`, no review, no JSON-LD property.
3. The event floor applies to every derived quantity.
4. No downtime field in the 5-minute live projection or the park shell: the park page renders none of
   the historical numbers, and the live object is absent for 99.7 % of rides.
5. On a ride page the historical block is a chapter only where figures publish, one inline sentence
   otherwise (frontend `AttractionDowntimeSection`).
