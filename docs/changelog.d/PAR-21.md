### Changed — the hourly forecast reaches 48 hours instead of 24

`HOURLY_PREDICTIONS` goes from 24 to 48, so `/plan/day` answers tomorrow end to
end from the model's own hourly rows rather than composing its evening from a
daily level and a historical shape. Measured against production before the
change: the stored hourly horizon was 23.9 hours across 3,268 rides.

The cost is not where it looks. Of the 2,645,535 stored hourly rows only 123,722
are future ones, and only those scale with the horizon — the other 2.52 million
are past targets inside the seven-day retention, and how many of those
accumulate depends on the 15-minute generation cadence, not on how far each run
reaches. So the horizon doubles for about 30 MB, 0.3 % of a 10.2 GB table. What
does double exactly is the write volume per run: 96 slots per ride become 192,
in `wait_time_predictions` and in the `prediction_accuracy` upsert beside it.

Three places were coupled to the number and two had to move with it.
`PlanDayService.HOURLY_HORIZON_DAYS` goes 1 → 2, the gate that decides whether
the measured hours are fetched at all; left at one day it would have dropped the
model's own answer for a day it had answered and served the composed curve in
its place, which is invisible because the two draw identically. And the
`cleanup-old` cutoff goes from `now-8d` to `now-9d`: the purge prunes on
`createdAt` because that is the partition key, so its slack has to cover the
horizon — at 48 hours with the old one-day slack it would have deleted every
target in the `now-7d`..`now-6d` day, inside the retention it is meant to keep.
The dedup window in `deduplicatePredictions` stays at 48 hours, but it is no
longer the generous over-reach it was: it now exactly equals the horizon.
