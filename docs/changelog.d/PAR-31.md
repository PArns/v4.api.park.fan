### Fixed — the park merge guard now lists the five ride-downtime tables, and `attraction_outages` loses a duplicate index

`PARK_REFERENCING_TABLES` in `merge-dependencies.spec.ts` was missing five
tables that carry a park id: `attraction_outages`, `attraction_exposure_days`,
`attraction_downtime_profiles`, `park_downtime_coverage` and
`downtime_recovery_curves`. The strategies were already declared in
`PARK_DEPENDENCIES` (`move` for the first three, `discard` for the other two),
but the guard compares against the snapshot, so it could not notice a table
that stopped being declared. The snapshot now has 29 entries instead of 24.

`idx_attraction_outages_ride` on `(attractionId, started_at)` repeated the
table's own primary key column for column. TypeORM `synchronize` created it on
every deploy and no query could prefer it, so the `@Index` is removed.
