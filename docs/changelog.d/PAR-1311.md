### Added — the trip planner's usage and server metrics in the admin MCP

Two MCP tools look at the multi-day trip planner
([admin MCP](admin/mcp.md#the-trip-planners-tools)). `trip_planner_report`
reads the frontend's 22 `trip_*` events and `ticket_link_clicked` per region:
the funnel from a start to days saved, an `.ics` or a copied link with its
drop-off, the dates and parks read off the requests, find dates, ticket
answers and modes, hopper, views, the errors shown, and the purchase links by
seller kind and position, each attributed to its region by the product's
catalog. Small values fold and visitor strings come under `visitorData`, as in
`planner_report`.

`trip_planner_ops` reads a new record the server keeps of every answer of
`/plan`, `/when` and `/best-days`: latency percentiles (all answers and the
built ones), the answer's and the calendar's cache, whether the search was
proven, the loosened rules, the visits left out and why, the error codes, the
owned tickets by kind, and the slowest shapes by trip length, parks and bases
([what the server records](frontend/trip-regions.md#what-the-server-records)).
A row holds the UTC hour, the API's own codes and the trip in counts: no date,
name, message, address or visitor id.

**Schema:** a new table, `trip_request_metrics`, created by `synchronize` at
the deploy; nothing else changes. Rows are kept 90 days and deleted nightly at
01:25 UTC by the new `trip-metrics` / `prune` job.
