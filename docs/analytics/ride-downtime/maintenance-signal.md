# The maintenance signal

A ride the feed reports as under maintenance (`REFURBISHMENT`: wartezeiten.app „Wartung",
ThemeParks.wiki `REFURBISHMENT`) reads like an outage: since when, and when such a stop usually
ends. It is stored as its own signal, `maintenance`, beside `down` and `closed_gap` in
`attraction_outages` (PAR-1219).

## Why its own signal

Most REFURBISHMENT runs are short stops: over 30 days Europa-Park had 289 that ended in a recovery,
at a median of 15 operating minutes (measured 2026-11-02). Some are real refurbishments lasting
months (Typhoon Lagoon, Fuji-Q). Merged into `down`, the long ones would distort the fault curve
and the published counts, so maintenance gets a recovery curve of its own and no published figure.

## How it is built

- **Nightly:** `MAINTENANCE_INTERVALS_SQL` is `OUTAGE_INTERVALS_SQL` over REFURBISHMENT runs, using
  the same run rules (`outageRunBreaks(alias, "REFURBISHMENT")`), stitching, heartbeat caps and
  curated-works-period filter. A run longer than `MAX_OUTAGE_DAYS` is a works period and leaves the
  curve. Only `recovered` is an observed end; a run that turns into DOWN ends `closed`, censored.
  If the statement fails, that night keeps the stored `maintenance` rows and the DOWN rows are
  still written.
- **Curve:** `DowntimeRecoveryService` partitions by signal, so the `maintenance` curve comes from
  the same statement.
- **Live:** `AttractionOutageService.getCurrentOutages` asks the trailing-run query
  (`trailingOutageWithElapsedSql("REFURBISHMENT")`) for rides whose `effectiveStatus` is
  `REFURBISHMENT`, and reads the estimate from the `maintenance` curve. Like `down`, it applies only
  to parks with a `wiki_entity_id`, and never inside a curated works period.

## What it never feeds

The downtime profile (counts, median, longest, `downShare`), the exposure table's `outageStarts`
and the scan census all filter on `signal = 'down'`. A maintenance stop is never counted as a
reported fault.

## Wording

The frontend says „In Wartung seit …", never „Störung gemeldet" and never „steht still".
