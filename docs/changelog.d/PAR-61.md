### Changed — free-flow rides get their own downtime reason, retired rides leave the profile table

The downtime rebuild skips free-flow rides (`open_with_park`): their source reports them CLOSED all day, so they have no exposure day and no profile row. The ride page then fell back to `not_down_capable`, a claim about the park's source, on a park whose source works. A new reason `free_flow` is written for every live free-flow ride in every park, ahead of the park's own reason, and it counts as permanent for staleness. Its row is deleted once the ride stops being free-flow.

A retired ride also leaves the `ex` population on the day it retires. Its exposure days stay in the 90-day window, so before this its profile was rewritten every night with a fresh `generatedAt` for three months and never turned `stale_data`.

The frontend needs the matching `withheld.free_flow` string before the first rebuild after deploy.
