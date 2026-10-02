### Changed — the downtime measurement counts scheduled parks the way the regime does

`parksWithSchedule` in `GET /v1/admin/downtime-measurement` asked whether a park
has any park-level `OPERATING` row. The regime in
`DowntimeProfileService.rebuildCoverage` asks for a row an exposure day can use:
a park timezone, both times set, and a window still positive after
`normalizedClosingSql()`. Both now call one helper,
`usableOperatingScheduleRowSql()` in `src/common/utils/park-open-window.sql.ts`,
so changing the condition changes both. Measured against production on
2026-10-02: 192 parks before and after, no park differs.
