# Ride downtime: what is refused, and why

Why there is no probability of a ride's next outage, the measurement that confirmed it, and the ideas
already rejected so none of them is proposed again. Open it before adding any downtime figure that is
not already published. Index: [README](README.md).

## No forecast of the next outage

This is about "when will this ride break next, and for how long". A ride that is already down is
answered by the [recovery estimate](recovery-estimate.md); the difference is whether the event is
forecast or observed. Four reasons, each sufficient:

1. **The arithmetic.** At the observed magnitudes, _p_ for the next three operating hours is 0.005–0.01.
   Separating a calibrated 0.006 from 0.012 at 80 % power needs about `(2.8)²/p` ≈ 1,300 events per bin:
   several hundred ride-years.
2. **The numerator is not stationary.** It is not "the ride broke" but "ThemeParks.wiki reported `DOWN`
   and `ConflictResolverService` did not overwrite it". The second half changes when a source mapping is
   added or a name match starts working, with no deploy and no trace in the data.
3. **Censoring depends on the outcome.** The resolver rewrites `DOWN` (and `CLOSED`) to `OPERATING`
   whenever a second source reports `OPERATING` with a wait ≥ 5. A queue drains over 15–30 minutes after
   a ride stops, so what disappears is the short outages of the popular rides on dual-sourced parks.
4. **Left truncation at an unknown point.** Writes are delta-only, so an outage that starts and ends
   between two polls writes no row, and nobody knows what share that is.

No softer rewording either: no „rechnerisch etwa eine Störung an einem Tag mit neun Betriebsstunden",
no „eher unzuverlässig", no conversion of a rate onto a visit, an hour or a day.

### The erasure, measured

`queue_data.raw_status` keeps the status the resolver overrode. Read with
`DowntimeMeasurementService.measureErasure` against a fixed clock over 2026-09-22 17:15 – 2026-10-22
17:15 UTC (PAR-155; `GET /v1/admin/downtime-erasure` gives a moving window instead):

- 3,485 `DOWN` readings erased (31,054 minutes) and 35,218 `CLOSED` readings, across 94 parks.
- 4.6 % of all `DOWN` rows overall, but 36–43 % in the Disney parks and 64 % at Universal Studios Japan,
  where a second source reports waits. The six Disney parks carry 78 % of the erased `DOWN` rows.
- In minutes the erased share is 0.8 %, a lower bound (the kept `DOWN` minutes include time after close).
- The weekly volume (764–964 `DOWN` rows, up from 91 a day in September) shows reason 2.

Reason 3 is measured, not argued. **The refusal stands.**

## Rejected, with reasons

1. **Any probability of a future outage, MTBF, MTTR**, and every softer rewording. See above.
2. **A Kaplan-Meier median as the published duration.** Under censoring it is not "half of the observed
   outages", so the sentence beside it would be false against its own counts. The empirical median of
   outages with an observed end is published instead.
3. **Quasi-Poisson dispersion with a `sqrt(phi)` widening.** At expected daily counts well under one the
   Pearson statistic is dominated by tiny-exposure days; the widening would be a random number.
4. **A ride compared against its park's median.** At a few hundred published rides and nominal 5 %, about
   fifteen rides would read as worse than their park by chance. Deleted, not corrected.
5. **`MAX(source_count) = 1` as a merge filter.** `data_source` is not provenance (the resolver never
   rewrites it on merge and the heartbeat copies it). Replaced by `attractions.last_merged_at`.
6. **A poll log derived from `SELECT DISTINCT timestamp`.** A poll that changed nothing writes no row, so
   both censoring flags would fire on healthy data, correlated with park size.
7. **`attractionIsOutOfSeason()` in the historical reconstruction.** It computes against `NOW()`.
8. **A rendered "closed every Sunday" claim.** Six observed Sundays are not an operating plan.
9. **An outage derived from `CLOSED` during opening hours in every park.** 534 of 602 `unexpected_closure`
   anomalies were genuine closures. The [closure-gap signal](closure-gap-signal.md) is the narrow
   exception: blind parks only, filtered, and worded as an observation.
10. **A `schedule_entries` row per attraction.** It would be a second schedule writer with no owner.
11. **A downtime field in the 5-minute projection or the park shell.** The park page renders none of the
    historical numbers.
12. **A chapter heading on every ride page.** On most of ~7,000 rides it would promise outages over a
    sentence taking it back.
13. **A rate per 100 operating hours** (fitted internally as hierarchical gamma-Poisson) and **outage starts
    by hour of day**: the hour shape of a flag the resolver erases on popular rides shows queue drainage,
    not failures.
14. **A change to `compress_segmentby`** as part of this work: that is its own measurement.

## Open work

Who answers a park that disputes a published figure, and whether its figures go offline meanwhile, is
undecided: PAR-1145.
