### Fixed — a park whose season starts after the window no longer reads as a park we cannot see

`park_downtime_coverage.regime` gains `outside_window`, and the ride-level
refusal gains a reason of the same name: the park publishes hours, none of them
fall in the measured 90 days, so there is no operating time to divide by. Before
this such a park read `reports`, its rides reached neither the `ex` nor the
`sched` CTE in `rebuildProfiles`, got no profile row at all, and the read path
answered `not_down_capable` — a statement about the park's source, on a park
whose source is delivering. Measured against production on 2026-09-21:
Traumatica, 12 rides, 25 028 `queue_data` rows in the 90 days it was shut, and
23 OPERATING rows that all start on 2026-09-23 or later. It is the only park in
the catalogue that changes; 209 of 210 keep the regime they had.

The test is hung on the schedule and not on "has no exposure day in the window",
which would be shorter and false: 7 of the 14 parks with no exposure day that
day have no rides at all, and one of them published 91 operating rows inside the
same window.

`outside_window` is the one park-level refusal that is not permanent to the read
path, because it is the one that ends by itself — a stalled row ages into
`stale_data` („these numbers are not current"), which stays true after the park
opens.
