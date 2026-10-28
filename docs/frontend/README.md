# API contracts for clients

The contracts park.fan (and any other client) reads: what a field means, when it is
absent, and what a client may not conclude from it. Each page owns one surface; the
frontend's own handling lives in park.fan's `docs/api/` and links back here. Open the
page for the field or endpoint you are reading or changing.

## Conventions every page assumes

- **Null keys are stripped.** The global `ExcludeNullInterceptor` (`src/main.ts`)
  removes every null-valued key outside `/v1/admin/*`; `?debug=true` keeps them. A
  field documented as `null` arrives **absent** — read every nullable field as
  optional.
- **Absent is not false.** A missing curated fact is "nobody checked", never the
  negative statement ([absent facts](../rules/absent-facts.md)).
- **`unknown` is a crowd level, not a tier.** Never render it as `moderate`; never
  re-derive a level a payload already carries ([crowd levels](../rules/crowd-levels.md)).
- **Dates are park-local** `YYYY-MM-DD`; compare them against the park's `timezone`,
  never the reader's clock.

## Pages

| Page | Covers |
| --- | --- |
| [plan-day/](./plan-day/README.md) | `/plan/day`: the trip planner's per-ride series for one date |
| [schedule](./schedule.md) | `/schedule`: a park's opening-hours range |
| [yearly-predictions](./yearly-predictions.md) | `/predictions/yearly`: crowd outlook per day of the year |
| [park-seasons](./park-seasons.md) | `/seasons`: a park's named seasons and events |
| [shows](./shows.md) | shows on the park payload: status, today's showtimes, season |
| [weather](./weather.md) | `/weather`, `/weather/forecast`, `/weather/nowcast` |
| [trips](./trips.md) | `/v1/trips`: stored plans, the id as credential, write limits |
| [push](./push.md) | `/v1/push*`: subscriptions, topics, ride alerts, followed shows, quiet hours |
| [favorites](./favorites.md) | `/v1/favorites`: favourites with full entities and distances |
| [discovery](./discovery.md) | `/v1/discovery/*`: geo tree, continents, countries, `nearby` |
| [search](./search.md) | `/v1/search`: the response a client renders |
| [analytics](./analytics.md) | `/v1/analytics/*`: realtime, geo-live, ticker, best times |
| [best-days-endpoint](./best-days-endpoint.md) | `/best-days`: the precomputed today → +90 d projection |
| [calendar-schedule-status](./calendar-schedule-status.md) | calendar `UNKNOWN` vs `CLOSED`, `hasOperatingSchedule`, `scheduleCoverage` |
| [park-stats-and-peak-time](./park-stats-and-peak-time.md) | `/stats` history and the peak-hour fields |
| [park-hourly-profile](./park-hourly-profile.md) | `/stats/hourly`: the park's day shape |
| [ride-day-curve](./ride-day-curve.md) | `/stats/day`: one ride's day curve |
| [ride-typical-waits](./ride-typical-waits.md) | `typicalWaits`: a ride's typical and busy peak wait |
| [effective-status](./effective-status.md) | a ride's `status` vs `effectiveStatus`: which to read, how each is derived |
| [ride-live-crowd-level](./ride-live-crowd-level.md) | a ride's live `crowdLevel` and `bestVisitTimes` |
| [live-wait-times-availability](./live-wait-times-availability.md) | `liveWaitTimes`: parks we cannot read, single `UNKNOWN` rides |
| [seasonality](./seasonality.md) | `isSeasonal`, `seasonMonths`, `isCurrentlyInSeason` |
| [works-period](./works-period.md) | `worksPeriod`: a curated rebuild window |
| [not-run-today](./not-run-today.md) | `notRunToday`: a ride that has not run yet today |
| [retired-parks](./retired-parks.md) | `retiredAt` on a park |
| [attraction-kind](./attraction-kind.md) | `attractionKind` |
| [indoor-outdoor](./indoor-outdoor.md) | `indoorOutdoor` |
| [fast-pass](./fast-pass.md) | `fastPass` |
| [ride-glossary-link](./ride-glossary-link.md) | `rideProfile` and the glossary → rides lookup |
| [weather-warnings](./weather-warnings.md) | `weather.warnings` |
| [page-views](./page-views.md) | `/v1/page-views`: the most-read counter |
| [site-analytics](./site-analytics.md) | `/v1/analytics/collect` and `/v1/admin/analytics/*`: the site statistic, its events and reports |
| [search-console](./search-console.md) | `/v1/admin/search-console/*`: Google Search Console's queries, pages and series, joined with the site statistic |
