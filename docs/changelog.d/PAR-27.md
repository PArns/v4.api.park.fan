### Changed — headliner and park-merge raw-SQL rows are typed

`AnalyticsService.identifyHeadliners` declares `HeadlinerRow` for both of its
raw queries, and `ParkMergeService.migrateEntities` declares `EntityRow` for its
`SELECT id, slug, name` lookups; the merge helpers take an `EntityManager`
instead of `any`. NUMERIC and COUNT columns stay `string`, as pg returns them.
Types only, no behaviour change.
