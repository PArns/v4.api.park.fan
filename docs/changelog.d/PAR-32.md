### Fixed — a ride the feed stopped reporting is no longer read as seasonal

Reverse-reconciliation writes a `CLOSED` row for any ride no source has
mentioned in 24 hours, and the hourly heartbeat carries the previous row's
status and `data_source` forward. The seasonal detector read both as feed
evidence, which is the exact signature it looks for: closed on every park-open
day while the park was open. All 44 Europa-Park rides that ThemeParks.wiki
dropped on 2026-06-07 came out with the identical month list `[1,2,3,4,5,6,12]`
— every month before the feed went silent, presented as a season.

The evidence queries now read observed rows only, through the
`observedReadingsSql()` predicate the closure-gap statements already use: the
current-status and operating-history gates, the `attraction_day_operating`
rollup that also supplies the show search, and the watched span and month scan
behind `seasonMonths`. A ride in this state has no current status at all, so it
drops out of the candidates instead of being handed a season.
