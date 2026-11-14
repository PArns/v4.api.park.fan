# Admin MCP server

An MCP server at `https://api.park.fan/v1/mcp` that lets Claude (Claude Code, claude.ai, any MCP
client) read the admin's data directly: parks and rides with their live state, popularity, the
site statistic and its SEO reports, the trip planner's usage and its server metrics, Parky's tool calls and his site events, Google Search Console, system and ML health, job queues, data-quality findings,
duplicate and retirement candidates, outage statistics, the audit log, and Parky's chats and the feedback visitors send. Read-only in v1. A client signs in through an OAuth 2.1 flow whose
login and consent happen in park.fan's own admin, or presents a personal access token. Open this
page to connect a client, add a tool, or change anything on `/v1/mcp`, `/v1/oauth/*` or
`/.well-known/oauth-*`. The admin login itself: [authentication](authentication.md).

## Connecting

**Claude Code**

```bash
claude mcp add --transport http parkfan-admin https://api.park.fan/v1/mcp
```

Then `/mcp` in Claude Code, pick `parkfan-admin`, _Authenticate_. The browser opens park.fan's
consent page (behind the admin login), shows the client and where it will be sent, and _Approve_
hands Claude Code its tokens. It refreshes them itself; a grant unused for 30 days expires.

**claude.ai** — Settings → Connectors → _Add custom connector_, URL
`https://api.park.fan/v1/mcp`, no client id or secret. _Connect_ runs the same consent.

**A personal access token instead of OAuth** — create one on the admin account page ("API access /
MCP", shown once; it asks for your password, and your two-factor code if you use one), then:

```bash
claude mcp add --transport http parkfan-admin https://api.park.fan/v1/mcp \
  --header "Authorization: Bearer pfa_…"
```

**MCP Inspector** — `npx @modelcontextprotocol/inspector`, transport _Streamable HTTP_, the same
URL; its OAuth flow works from the browser (see [CORS](#edge-cors-caching-and-throttling)).

## How a login works

```
MCP client               api.park.fan                         park.fan (admin)
    │ POST /v1/mcp              │                                     │
    │──────────────────────────►│ 401 WWW-Authenticate: Bearer        │
    │◄──────────────────────────│   resource_metadata=".../.well-known/oauth-protected-resource/v1/mcp"
    │ GET /.well-known/oauth-protected-resource/v1/mcp, …/oauth-authorization-server
    │ POST /v1/oauth/register   │ (RFC 7591, public client)           │
    │ browser → GET /v1/oauth/authorize?…&code_challenge=…(S256)      │
    │                           │ validate, park request in Redis 10 min
    │                           │ 302 → /admin/oauth/consent?request=<id>
    │                           │                                     │ admin login if needed
    │                           │◄── GET  /v1/admin/oauth/requests/:id (session, via proxy)
    │                           │◄── POST /v1/admin/oauth/requests/:id/approve
    │                           │──► { redirectTo: <redirect_uri>?code=…&state=…&iss=… }
    │◄──────────── browser follows redirectTo ────────────────────────│
    │ POST /v1/oauth/token (code + code_verifier)                     │
    │◄── access pfo_… (1 h) + refresh pfr_… (30 days, rotating)       │
    │ POST /v1/mcp  Authorization: Bearer pfo_…                       │
```

The admin never logs in on api.park.fan: the consent page runs behind the existing admin login,
with its Turnstile, lockout and second factor, and the API trusts the session the admin proxy sends.

## Credentials

Both kinds live in Postgres, never in clear: `admin_api_tokens` holds one row per PAT or OAuth grant
(what the account page lists and revokes), `admin_api_token_secrets` the SHA-256 of each secret
(a PAT's one, a grant's access and refresh tokens). Every bearer lookup joins `admin_users`, so:

- **Role = the lesser of the account's role and the token's cap.** v1 has one scope, `admin:read`,
  capped at `viewer`; an owner's token is a viewer.
- **A deactivated account is refused at its next request**, revoked or not.
- **Whatever ends an account's sessions ends its tokens:** deactivation, a password change or reset,
  a second-factor reset, _sign out everywhere_ (`AdminAuthService.signOutEverywhere`). It first sets
  `admin_users.tokens_revoked_at`; a token created before that instant is refused at every request
  even if its row escaped the sweep, and a code approved before it is refused at the exchange.
  Tokens are issued under a lock on the account's row, so issuing and signing out never interleave.
- **Tokens reach `/v1/mcp` and nothing else.** `AdminAuthGuard` accepts them only on an endpoint
  marked `@AdminTokenOnly()`, and such an endpoint accepts neither a session nor the shared pass.
  So a token cannot list, mint or extend tokens, approve a consent or call an admin REST route.

|            | PAT `pfa_…`                                                          | OAuth access `pfo_…`         | OAuth refresh `pfr_…`              |
| ---------- | -------------------------------------------------------------------- | ---------------------------- | ---------------------------------- |
| Lifetime   | 30, 90, 365 days or none                                             | 1 hour                       | 30 days, renewed with each refresh |
| Made by    | `POST /v1/admin/auth/tokens` (session and password)                  | the code or refresh exchange | the same                           |
| Revoked by | `DELETE /v1/admin/auth/tokens/:id`, an owner, the account's sign-out | revoking the grant           | revoking the grant; reuse          |

- **Codes** are single-use, live 60 s, are stored hashed in Redis and bound to the client, the
  redirect URI and the PKCE challenge. A code presented twice revokes the grant it bought.
- **Refresh tokens rotate.** Each refresh issues a new pair; presenting a used one more than 10 s
  after its use revokes the whole grant (the thief and the client both hold it, and the server cannot
  tell which is which). Within those 10 s it is a race between two processes of one client and only
  answers `invalid_grant`.
- **Limits:** 25 active PATs per account, counted under the account's row lock; a new grant beyond
  20 active ones revokes the oldest.
- `last_used_at` is written at most once a minute per token.
- **Audit rows:** `auth.token.create`, `auth.token.revoke`, `auth.oauth.grant` (at the approval,
  with the redirect host, the scope and the request's `startedFrom`). A reuse or code replay revokes
  and logs a warning; it has no actor.

## Tools

One tool, one file in `src/mcp/tools/`, registered by one line in `src/mcp/tools/index.ts`. A tool
is a name, a description written for the model, a zod input shape whose every field is described
and bounded, and a handler that calls an existing service through `ctx.get(Service)`. Answers are
compact JSON, lists cut to the limit the input allows, the whole answer cut at 100 000 characters.
A handler throws `McpToolError` for a failure the model should read; any other error reaches the
model as a generic failure and its message only goes to the log. `index.spec.ts` checks every tool
against these rules.

| Tool                         | Answers                                                                                                                                                                                 | Wraps                                            |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| `whoami`                     | account, effective role, credential kind and client                                                                                                                                     | the principal                                    |
| `find_parks`                 | parks by name, city or country, with status, crowd level, today's hours                                                                                                                 | `SearchService`                                  |
| `get_park`                   | live state: schedule, crowd, weather, rides by status, longest waits, rides down (reported or inferred), in maintenance (with the curated window), maintenance ahead, not run today     | `ParkIntegrationService`                         |
| `park_forecast`              | days ahead as the calendar shows them: hours, crowd level, expected peak wait, weather, holidays, rides in maintenance (`PlanDayService`); today's hourly curve and headliners; models  | `CalendarService`, `MlOverviewService`           |
| `find_attractions`           | rides by name, optionally in one park, with status and wait                                                                                                                             | `SearchService`                                  |
| `get_attraction`             | one ride: queues, crowd level, outage, curated facts, ride profile, typical waits, downtime, retirement, optional history                                                               | `AttractionIntegrationService`                   |
| `page_views`                 | reads per kind, language and page                                                                                                                                                       | `PageViewsService.adminOverview`                 |
| `system_health`              | host, Postgres, Redis, data freshness, the site statistic's flush backlog and drops; or the ML half, whose PCN boards bring their day rows only with `boardRows` (14 days of them run past the answer limit) | `SystemHealthService`                            |
| `queue_status`               | Bull counts per queue, recent failures of one                                                                                                                                           | `AdminQueueStatusService`                        |
| `ml_overview`                | forecast components, decisions, comparison summary, drift                                                                                                                               | `MlOverviewService`                              |
| `model_comparison`           | the comparison matrix, question × model against what serves: state, note, switch checks, live paired difference with CI, offline verdict; per-lead rows for one `question` only | `ModelComparisonService`                         |
| `model_comparison_day`       | one ride-day's archived curves against the 15-min truth with each model's MAE; without park, ride and date the dates, parks and rides that have curves | `ModelComparisonService`                         |
| `ml_accuracy`                | where the hourly forecasts miss: MAE, bias and baseline per park or per ride, against the window before, ranked                                                                         | `ForecastErrorBreakdownService`                  |
| `data_quality`               | silenced clusters, failing jobs, unreviewed absence retirements, re-issue candidates                                                                                                    | `DataQualityMonitorService.report`               |
| `duplicates`                 | duplicate attraction and park pairs with the merge winner                                                                                                                               | `AttractionMergeService`, `ParkValidatorService` |
| `retirement_candidates`      | rides whose source went quiet                                                                                                                                                           | `AttractionReviewService`                        |
| `audit_log`                  | admin actions, filterable                                                                                                                                                               | `AdminAuditService.list`                         |
| `assistant_conversations`    | Parky's chats, newest first: language, messages, tokens, cost, rating, prompt version, start page and first question; filter by rating, prompt version or a word                        | `AssistantRecordsService.listConversations`      |
| `assistant_conversation`     | one chat in full: questions, answers, the tools Parky called with their input, tokens, cost, prompt version, ratings, its feedback                                                      | `AssistantRecordsService.conversation`           |
| `site_feedback`              | feedback from the form or filed by Parky from a chat (`source`, `kind`), newest first, by status; whether the visitor wants a reply, never the address                                  | `AssistantRecordsService.listFeedback`           |
| `assistant_tools_report`     | Parky's tool calls across chats: per tool, region and prompt version the calls and errors by closed code; from the kept chats the refused paths, dropped keys, `change_trip`'s keys, the interview's ops and completion | `AssistantRecordsService.toolStats`, `.toolInputStats` |
| `assistant_report`           | Parky from the site statistic: openings by source, questions by kind, reply chips shown, drawn and pressed, the interview per kind with its completion, plan changes, undos, ratings | `SiteAnalyticsAssistantService.report`           |
| `downtime`                   | outage events per park and ride over a window                                                                                                                                           | `DowntimeMeasurementService.measure`             |
| `analytics_overview`         | page views, visits, visitors, events, views per visit, bounce, average visit and time on page; the window before and the change; realtime; optional series with every figure per period | `SiteAnalyticsService.overview`                  |
| `analytics_live`             | the last half hour minute by minute: active visitors, pages, countries, channels, referrers, devices, events                                                                            | `SiteAnalyticsLiveService.live`                  |
| `analytics_breakdown`        | views, visits or exits (`measure`) per value of one dimension, share, time on page, exits on a path, bounce and average visit when counting visits                                      | `SiteAnalyticsService.breakdown`                 |
| `analytics_traffic_crosstab` | page views or visits by two dimensions, as a `counts[row][col]` matrix, small values folded                                                                                             | `SiteAnalyticsService.trafficCrosstab`           |
| `analytics_events`           | every event name with count, previous, change, blocked                                                                                                                                  | `SiteAnalyticsService.events`                    |
| `analytics_event`            | one event: props with top values, combinations, page types, devices, entities                                                                                                           | `SiteAnalyticsService.event`                     |
| `analytics_crosstab`         | one event by two sides, as a `counts[row][col]` matrix, small values folded                                                                                                             | `SiteAnalyticsService.crosstab`                  |
| `search_report`              | the site search per surface: openings, typed, clicks, dead ends, rates; what was clicked where; the parks and rides opened most                                                         | `SiteAnalyticsService.search`                    |
| `planner_report`             | the trip planner: funnel with rates, ways in with the days they led to, wizard steps, blocks by kind and source, tools, sharing, push, the parks and rides planned most                 | `SiteAnalyticsService.planner`                   |
| `trip_planner_report`        | the multi-day trip planner per region: the dates-first funnel (started → requested → shown → changed → tickets → kept), the optional steps as shares of the plans shown, split at the new planner's first day, find dates, edits and options, parks left out, tickets, hopper, views, purchase links by seller kind and position, errors shown, the page types it was used from | `SiteAnalyticsTripService.report`, `TicketCatalogService` |
| `trip_planner_ops`           | the server's record of `/plan`, `/when`, `/best-days` and of Parky's trip tools (`caller`): latency percentiles (all and built), caches, exact share, relaxation, unplaced, left-out and error codes, owned tickets by kind, move buffer and gateways, the slowest shapes | `TripRequestMetricsService.ops`                  |
| `seo_landing_pages`          | the pages visits of one channel (search by default) entered on, with change, bounce, sources                                                                                            | `SiteAnalyticsSeoService.landingPages`           |
| `seo_trends`                 | landing pages rising and falling against the window before                                                                                                                              | `SiteAnalyticsSeoService.trends`                 |
| `seo_not_found`              | paths that showed the not-found page, with the referrers of the visits that entered on them                                                                                             | `SiteAnalyticsSeoService.notFound`               |
| `seo_uncovered`              | parks or rides no search visit entered on                                                                                                                                               | `SiteAnalyticsSeoService.uncovered`              |
| `gsc_status`                 | whether Google Search Console is connected, when it last imported, the days covered                                                                                                     | `SearchConsoleService.status`                    |
| `gsc_queries`                | the Google queries the site was found with: clicks, impressions, ctr, position, change; filter by page, country, device; `type` web, image, news, discover                              | `SearchConsoleService.queries`                   |
| `gsc_pages`                  | the pages found in Google, with the site's own visits, bounce and visit length from search                                                                                              | `SearchConsoleService.pages`                     |
| `gsc_series`                 | clicks, impressions, ctr and position per day, week or month                                                                                                                            | `SearchConsoleService.series`                    |
| `gsc_inspect_url`            | Google's verdict on up to 10 park.fan pages (URL Inspection): indexed or why not, canonical, last crawl; cached 6 h, 1,800 a day                                                        | `SearchConsoleInspectionService.inspect`         |

The site-statistic tools share `tools/site-analytics.inputs.ts`: `days` (1–365, 30 when absent;
1–180 for `seo_trends`, which needs the window before) or `from` and `to` (`YYYY-MM-DD`), `interval`
(`hour`, `day`, `week`, `month`), `locale`, and `filters` as `[{ dim, value }]`, which become the
reports' `filter=<dim>:<value>`, and `datacenter` (`exclude` by default, `include`, `only`;
[data centres](../frontend/site-analytics.md#data-centres)). The filter's and the breakdown's help
lists every page type the frontend sends (`SITE_PAGE_TYPES`), among them a trip region's hub,
month and best-time pages, `trip_hub`, `trip_month` and `trip_best_time` since release 2.21.0
(they were `other` before), so `page_type:trip_hub` reads the hub's numbers. The tools over events (`analytics_events`,
`analytics_event`, `analytics_crosstab`, `search_report`, `planner_report`, `trip_planner_report`,
`assistant_report`) take only the event's dimensions (`eventWindowInput`), and
`seo_not_found` only the page view's (`pageWindowInput`, with `browser_language`): their rows carry
no visit dimension, so the schema does not offer one rather than the report refusing it. Their
descriptions carry what a model must know to read the numbers: rows are UTC hours, visitors are
daily uniques summed over the window and `null` under any filter but the language, bounce is
`1 − engaged / visits`, the times are measured foreground seconds, a visit filter makes page views
and events `null`, `previous` ends at the current hour and is `null` past 180 days, a series period
the window cuts is `partial`, reports lag the site by up to six minutes, and Do Not Track, GPC and
opted-out readers are missing
([site statistic](../frontend/site-analytics.md)). Rates are rounded to three places, lists cut to
the `limit` each takes, series only on request. Strings the collector took from visitors' browsers come
back under `visitorData` with a `visitorDataNotice` (`tools/visitor-data.ts`), parks and rides by
the name park.fan's tables give them ([security](#security)).

Reads made through the tools do not count as popularity hits (`countHit = false`). A tool that
writes would set `minRole` and needs a scope above `admin:read`; v1 issues none.

### Small values

The admin's reports and its UI show every number. The tools, whose answers a model reads and may repeat
elsewhere, do not show a value counted fewer than **5** times (`MCP_MIN_CELL` in `tools/small-cells.ts`).
One helper, `foldSmallRows`, does it for every list; each tool names, per list, the keys that name a row,
the count that decides, and the measures that add up.

- **Lists fold.** Every row below 5 goes into one row at the end whose names are `(other)`: the label the
  collector's day guard already uses for values past its limit, so a row that is already `(other)` joins
  it and there is one. The count that decides is the row's `visitors` when it has them, otherwise its
  `views`, `visits`, `count` or the like (a landing page's visits in the window, a surface's openings).
  The additive measures are summed onto the `(other)` row (views, visits, events, counts, exits, the
  previous window's count, the share, and the visitors of a breakdown by country, device, browser, OS or
  channel); everything else is `null` there: bounce rate, averages, durations,
  rates, and a field that names the row (a page type, an entity's id and path, a page's sources, a
  series). Nothing is added to `(other)` that a tool did not list as additive, so a new field never
  carries one row's value onto the others'. A list without a small row comes back as it is. Totals are
  not folded and still count every value: `total`, a report's `totals`, `previous`; but under a
  filter, a language or `datacenter=only` a total or previous count of 1 to 4 is null (with the rates computed from it,
  and the `change` that needs it) and `smallValuesHidden` says so.
- **Where.** `analytics_breakdown`, `analytics_events`, `analytics_event` (property values, combinations,
  page types, devices, entities), `analytics_live` (pages, countries, channels, referrers, devices,
  events), `search_report` (surfaces, openings by page type and device, result types, positions, browse
  items, the parks and rides opened most, an id no table knows left out and counted in
  `unresolvedEntities`), `planner_report` (ways in, wizard steps, blocks, tools, outcomes, openings by page type and
  device, the parks and rides planned most, an id no table knows any more left out and counted in
  `unresolvedEntities`), `trip_planner_report` (the regions side by side, the page types, ways in and the tile,
  what the requests asked, answers by mode and outcome, loosened rules, parks left out and their
  fixes, errors by status, find dates, edits and options, tickets, hopper, views, days opened, what
  was kept, and the purchase links by kind and position, surface, mode, seller and product),
  `assistant_report` (openings, questions, chips by position and context, the interview's starts,
  steps and ends, plan changes, undos, answers, ratings), `assistant_tools_report` (tools, regions,
  prompt versions, refused paths, dropped keys, `change_trip`'s keys),
  `seo_landing_pages` (pages, and the sources of each), `seo_trends`, `seo_not_found` (paths, and the
  referrers of each) and `page_views` (the top parks, rides, posts and news, and each page's reads per
  language). `seo_uncovered` lists our own parks and rides that had no search visit at all, so there is no
  count in it to fold; `gsc_*` (Google drops rare queries itself) and `audit_log` are as they were.
- **Crosstabs.** A side value whose cells add up to fewer than 5 goes under `(other)` on that side (a `day`
  side keeps every period, `__rest__` and `(none)` are values like the others); then every cell below 5
  is `null` in `counts`, and `other: { cells, count }` holds how many there are and their sum, `null`
  when none. The numbers in `counts` and in `other` add up to `total`.
- **Series.** A series of the whole site's totals (the overview's, the live view's minutes) is as counted.
  A series under a filter or a language, and one per event name or search surface, has every value from 1
  to 4 as `null`, and so every rate and average computed from one (an overview point with 3 visits has
  no bounce rate), and the answer carries `smallValuesHidden`: whether any value was hidden. A zero is a
  zero. `page_views` hides the days of one `kind` the same way.
- **The descriptions say so.** Each tool's description carries one sentence for the list rows
  (`SMALL_CELLS_DESCRIPTION`), the crosstabs another and the tools with series a third, next to the
  `visitorData` sentence; the strings stay under `visitorData`, the label `(other)` among them.

The answer also carries `hourlyFrom`, the day from which a figure that needed the hourly rows is complete
(null when none was cut): the hourly rows hold 90 to 97 days, the daily roll-ups 400
([retention](../frontend/site-analytics.md#retention)).

### The trip planner's tools

Two tools look at the multi-day trip planner (the region hubs, [trip regions](../frontend/trip-regions.md)),
one from each side; `planner_report` is the day planner's.

- **`trip_planner_report`** reads the frontend's `trip_*` events and `ticket_link_clicked`
  (the list: the frontend's `docs/features/orlando-planner.md#analytics`) through
  `SiteAnalyticsTripService` (`site-analytics-trip.ts` holds the figures, pure; `TRIP_EVENTS` names
  every event it reads, and a new `trip_*` event goes there too). Every `trip_*` event carries
  `region`; a value `regions.ts` does not have is `(other)`. A purchase link carries no region (its
  eight properties are the statistic's limit), so the tool reads every region's ticket catalog,
  archived products included, and attributes a click to the region whose catalog has its product;
  a product no catalog has is counted in `unattributedTicketLinks` and listed only with every
  region.
- **The funnel** is the dates-first planner's (release 2.21.0, PAR-1309; the frontend's
  `docs/features/orlando-planner.md#the-funnel`): `started` (`trip_planner_started`, source `hero`,
  `button`, `link` or `dates`) → `requested` (`trip_plan_requested`) → `shown` (`trip_plan_shown`)
  → `changed` (`trip_edit_opened` and the visitor's `trip_option_changed`; Parky's are counted
  beside it) → `tickets` (`trip_tickets_opened`) → `kept` (`trip_days_saved`, `trip_ics_exported`,
  `trip_link_copied`). Every step has `ofStarted`. `requested` and `shown` have `fromPrevious` and
  `dropOff`. `changed` and `tickets` are optional, a plan that suits needs neither, so they come as
  `ofShown`, a share of the plans shown, and never as a drop-off; `kept` follows `shown`, with
  `fromPrevious` and `dropOff` against it, and lists the day planner opened from the trip planner
  (`planner_opened` with the source `<region>-trip-day`, `<region>-trip-first` or `<region>-hub`;
  the prefix names the region).
- **A window across the deploy.** `newFunnelFrom` is the first UTC day any of the new planner's own
  events (`trip_edit_opened`, `trip_option_changed`, `trip_tickets_opened`) was counted
  (`firstDayOf`, the hourly rows and the daily roll-up of names). Each step counts its days before
  it (`before`) by the old planner's events, a yes of `trip_tickets_answered` for `tickets` (no
  longer sent, so it counts only there) and nothing for `changed`, which the old planner did not
  send, and its days from it on (`since`) by the new ones. A rate is taken over the days both steps
  count, so `changed` is a share of the answers of the new planner's days only. Until the frontend
  sends the new events, every day is `before`.
- **Counts are events, not visitors.** An answer fires once per distinct request, so a step can
  outnumber the one before it; `dropOff` is then null.
- **The tile as a way in.** The planner tile on a region park's pages (PAR-1317) is read beside
  the ways in (`source`, `via`): `tile.shown` counts its `section_seen` of section `trip_tile`,
  which names no region, so the park in the page's path does (`parkKeyOf`, the region parks of
  `regions.ts`; a page of another park is `(other)`), and `tile.clicked` its `trip_tile_clicked`
  by link (`plan` or `calendar`), with `clickRate`, all of it per region under `visitorData`.
  The views need the page, which only the hourly rows keep, so a window past them says so in
  `hourlyFrom`.
- **Beside the funnel:** the requests (mode, length
  bucket, parks with 0 for the headliners, tickets, hopper, blocks, rest), answers by mode and
  outcome with the loosened rule, the error's status and the parks left out with the fix chosen,
  the cities of a route, find dates (toggles, periods, windows found and planned), edits by panel and
  where they were opened, options changed by option, value and who, the stay suggestion, ticket
  openings and entries, hopper, the views switched to, the date grid's resort mode, days opened,
  pins, the accuracy note, what was kept, the purchase links, the regions side by side and
  `pageTypes`: the starts, requests and answers by the page they came from (`trip_hub`,
  `trip_month` and `trip_best_time` from the deploy that sends them on, `other` before it and on
  every other page). An event the frontend does not send yet lists
  nothing. The small-value and `visitorData` rules are the other event tools': lists fold, a region
  narrows like a filter, so one region's counts from 1 to 4 are null, per era too, and so is every
  rate computed from one.
- **`trip_planner_ops`** reads the server's own row per answer of `/plan`, `/when` and `/best-days`
  and per plan of Parky's trip tools
  ([what the server records](../frontend/trip-regions.md#what-the-server-records)) through
  `TripRequestMetricsService.ops`: per region, route and caller (`site`, `parky`) the requests,
  errors, latency p50/p90/p99 and max over every 200 and over the built ones, the cache and the
  calendar's, the exact share, the shares that loosened a rule, left a visit out, left a chosen park
  out, used a hopper or owned tickets, and in a region with bases the share that suggested other
  stays; the relaxation codes, unplaced reasons, error codes and ticket kinds; the move buffers and
  fixed ends of the routes (`stays`); the built answers' latency by shape (`slowest`, by trip days
  × parks × bases, and `byLength`, `byParks`, `byBases`); a series per UTC day on request. UTC
  days, `days` up to 90 (the retention), 7 by default, `caller` to read one side. Its lists are not
  folded and it has no `visitorData`: every value is the server's own measurement or code, no row
  stands for a reader, and the rare long trip is exactly the shape the optimizer has to be tuned
  for. The rows reach the table once a minute per process, so the last minute is missing.

### Parky's tools

Two tools look at Parky, park.fan's chat assistant ([assistant](../frontend/assistant.md)), beside
`assistant_conversations` and `assistant_conversation`, which read the chats one by one.

- **`assistant_tools_report`** sums his tool calls across chats over a window of UTC days (up to
  90, 7 by default; `tool` and `promptVersion` narrow it): per tool, per trip region an input named
  (one `regions.ts` has) and per prompt version the calls, errors, error rate and the calls per
  outcome. A failed call records why as a closed code (`errorKind`, `ASSISTANT_TOOL_ERROR_KINDS`):
  `unknown_tool`, `invalid_input` (the schema refused the input), `refused` (the tool said no: no
  such park, the plan route's 400) or `failed` (an error of ours); a failure stored before the
  codes is `error`. The chats are kept 7 days; the nightly prune adds each batch's calls to
  `assistant_tool_day_stats` in the transaction that deletes it (per day, prompt version, tool,
  outcome and region, only the number of calls), and the report sums those with the kept chats', so
  it reads the same before and after a prune. What only the kept chats can say comes under `kept`
  (from `keptFrom`): answers, those with tool calls, calls per answer; the interview's ops (start,
  answered, skip, ask, end with done or left) with `completion` = ended done / started; and under
  `visitorData` with the chat notice the paths a schema refused (`issues`), the keys it dropped
  without a word (`ignoredKeys`: a parameter the tool does not have, such as `bases` on
  `change_trip`) and which keys `change_trip` was given. Keys and paths only, never a value; a key
  that is not an identifier is left out. Lists fold calls counted fewer than 5 times.
- **`assistant_report`** reads his events in the site statistic (`SiteAnalyticsAssistantService`,
  the figures in `site-analytics-assistant.ts`): openings by source, questions by kind, the reply
  chips (`shown`: times chips were drawn under an answer, `drawn`: how many, `pressed`: questions
  of kind chip, `pressRate`), the interview per kind (trip or day) from its start to its end (done
  or left) with `completion` and `leftRate`, steps by step, plan changes by planner and kind, undos,
  answers and ratings. A chip's position and context, the interview's region, a step's outcome
  (answered or skipped) and the end's `plan` (the tracking audit's T22 and T23) are listed as soon as
  the frontend sends them. The small-value and `visitorData` rules are the other event tools'.

## Endpoints

| Method      | Path                                             | Who                                      | Does                                                                                                                                                  |
| ----------- | ------------------------------------------------ | ---------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| POST        | `/v1/mcp`                                        | PAT or OAuth access token                | MCP JSON-RPC, Streamable HTTP, stateless, JSON answers                                                                                                |
| GET, DELETE | `/v1/mcp`                                        | the same                                 | 405: no server stream, no session                                                                                                                     |
| GET         | `/.well-known/oauth-protected-resource[/v1/mcp]` | public                                   | RFC 9728 metadata                                                                                                                                     |
| GET         | `/.well-known/oauth-authorization-server`        | public                                   | RFC 8414 metadata                                                                                                                                     |
| POST        | `/v1/oauth/register`                             | public                                   | RFC 7591 registration, public clients only                                                                                                            |
| GET         | `/v1/oauth/authorize`                            | public (browser)                         | validates, parks the request, 302 to the consent page                                                                                                 |
| POST        | `/v1/oauth/token`                                | public client                            | `authorization_code` (PKCE S256), `refresh_token`                                                                                                     |
| POST        | `/v1/oauth/revoke`                               | public client                            | RFC 7009; revokes the whole grant                                                                                                                     |
| GET         | `/v1/admin/oauth/requests/:id`                   | session, any role                        | what the consent page shows: client, redirect host, scope, `createdAt` (its age; a request lives 10 minutes), `expiresAt`, `startedFrom` and `viewer` |
| POST        | `/v1/admin/oauth/requests/:id/approve`           | session + body `{ password, totpCode? }` | `{ redirectTo }` with the code; 401 `invalid-password`, `totp-required`, `invalid-totp` or 429 `rate-limited`, and the request stays pending          |
| POST        | `/v1/admin/oauth/requests/:id/deny`              | session, any role                        | `{ redirectTo }` with `access_denied`                                                                                                                 |
| GET         | `/v1/admin/auth/tokens[?userId=]`                | session; `userId` owner only             | active PATs and grants                                                                                                                                |
| POST        | `/v1/admin/auth/tokens`                          | session + password (+ `totpCode`)        | `{ token, item }`, the token shown once                                                                                                               |
| DELETE      | `/v1/admin/auth/tokens/:id`                      | session; others' owner only              | revokes a PAT or a grant                                                                                                                              |

The shared pass is refused on every row. The response shapes the frontend reads are in
[endpoints](endpoints.md#api-tokens-and-the-admin-mcp).

Redirect URIs: `https://` on a host in `OAUTH_REDIRECT_HOSTS` (default `claude.ai`, `claude.com`)
with a path in `OAUTH_REDIRECT_PATHS` (default `/api/mcp/auth_callback`, claude.ai's connector
callback) and no query or port, or `http://127.0.0.1|localhost|[::1]` with any port and path.
Matched exactly, except that a loopback URI may name another port than it registered (RFC 8252
§7.3). An unknown client or an unregistered redirect
URI gets an HTML error page from `/v1/oauth/authorize`, never a redirect.

## Security

What was checked when this was built, and where it is enforced:

- **PKCE:** S256 only, `plain` and a missing challenge refused at the authorize step; the verifier
  compared in constant time; the code is consumed (`GETDEL`) before the verifier is checked, so a
  wrong guess burns it.
- **Redirect URIs:** validated at registration and again at authorize (the allowed hosts may have
  narrowed), exact match, no fragment or user info, and on an allowed https host only the pinned
  callback path: any other page of claude.ai that echoes a query parameter would otherwise receive
  codes. Errors before the redirect URI is trusted are a
  page here, so `/v1/oauth/authorize` is no open redirect; `redirectTo` is always a registered URI.
- **Replay:** codes single-use and 60 s, a replayed code revokes its grant; refresh tokens rotate
  with reuse detection (conditional `UPDATE`, so of two parallel refreshes exactly one wins).
- **Leakage:** secrets stored only as SHA-256 (pending requests and codes in Redis too); tokens only
  in the `Authorization` header, never in a URL; `code`, `code_verifier`, `access_token`,
  `refresh_token`, `client_secret` and `state` are redacted from logged URLs (`redactUrl`), and so is
  the pending request id in `/v1/admin/oauth/requests/:id`; nothing
  logs headers or OAuth bodies; every OAuth answer is `no-store`, the authorize redirect too, and
  API answers carry `Referrer-Policy: no-referrer`.
- **Consent CSRF:** the approve call reaches the API only through park.fan's admin proxy with the
  session as a bearer header, which no other site can attach; the proxy's cookie is `SameSite=Strict`
  ([the frontend's admin docs](https://github.com/PArns/park.fan/blob/main/docs/features/admin.md)).
  The pending request id is 256 random bits, stored hashed, and answered once. An approval also asks
  for the account password (and the two-factor code, when on) again, like creating a token, so a
  stolen session cannot mint a refreshable grant.
- **Consent phishing:** anyone may register a client called "Claude", so the consent page shows the
  redirect host, and https redirects are limited to `OAUTH_REDIRECT_HOSTS`. A grant obtained through
  a phished approval lands in the approving admin's own claude.ai session or on their own machine.
  The page also warns when the request was started from another country or browser family than the
  admin's: `/v1/oauth/authorize` parks the GeoIP country and the browser family of the browser that
  started it (`request-origin.ts`, never the address or the User-Agent; no User-Agent counts as
  another browser), the consent read returns them beside the same two facts of the admin's address
  and the User-Agent the session signed in with, and the page compares them. Whoever starts the
  request chooses both facts (a VPN exit, a copied User-Agent), so the warning catches a careless
  phish and proves nothing about a careful one; the redirect limits above are the control.
- **DCR abuse:** 10 registrations per address bucket (IPv4, IPv6 /64) and hour (Redis) plus 10 a
  minute (throttler), names stripped of control, bidi, zero-width, separator, filler and tag
  characters (`stripInvisibleCharacters`) and cut at 100, at most 10 redirect URIs. Registrations
  never used for a grant are deleted after a week. A bucket keeps at most 20 unused ones, its own
  oldest going first; beyond 1000 overall the oldest older than an hour give way, so a flood cannot
  push out a client that registered minutes ago; only past 5000, all younger than an hour, are new
  ones refused.
- **Authorize from strangers:** a pending request is written to Redis before anybody signs in, so at
  most 30 per address bucket and 30 per client live in any 10 minutes (an error page, or
  `temporarily_unavailable` at the client's redirect).
- **Prompt injection through the statistic:** paths, referrers, event names and property
  values reach the public collector from anybody's browser. Every tool returns them under
  `visitorData`, beside a `visitorDataNotice` saying they are data and never instructions, and its
  description says the same; parks and rides come by the name in park.fan's own tables, and keys that
  no longer resolve are left out. OAuth client names are self-chosen too: `whoami` and `audit_log`
  say so.
- **Prompt injection through Parky's chats and the feedback:** the `assistant_*` and `site_feedback`
  tools return what visitors typed, and Parky's answers with it, since a visitor can steer those,
  under `visitorData` with the chat notice (`CHAT_DATA_NOTICE`). `assistant_tools_report` hands
  the keys and paths of the model's inputs the same way, identifiers only, since the model wrote
  them. The feedback's email address and the hashes of visitor and session stay out: a model gets
  `wantsReply`, a person the address in the admin through `adminUrl`.
- **Amplification:** JSON-RPC batches are refused (one tool call per request, which the throttler
  counts), and one token runs at most 10 requests at once per API instance.
- **Role escalation:** the token cap, the token-only endpoint, and the session-only token and
  consent endpoints above; the tools a role may not use are not even listed, and are refused again
  at call time.
- **The shared pass:** refused on the MCP endpoint, the token endpoints and the consent endpoints.
- **Timing:** secrets are looked up by hash, never compared in JS; the PKCE comparison is
  constant-time. A client id is not a secret.
- **DNS rebinding / `Origin`:** the MCP endpoint accepts any origin, because no credential is ambient
  (no cookie; a bearer token must be attached explicitly).

## Edge: CORS, caching and throttling

- **Cloudflare** must let these through to the origin, without a challenge page (MCP clients and
  claude.ai's servers cannot solve one): `/v1/mcp`, `/v1/oauth/*`, `/.well-known/oauth-*`. The consent
  endpoints are under `/v1/admin` and arrive through the frontend proxy like every admin call.
- **CORS:** `buildCorsDelegate` answers `Access-Control-Allow-Origin: *` without credentials on
  `/v1/mcp`, `/v1/oauth/register|token|revoke` and the discovery documents, and exposes
  `WWW-Authenticate` ([CORS](../deployment/cors.md#the-admin-mcps-public-surface)).
- **Caching:** `/v1/mcp` is `NoCdnCacheInterceptor`; the OAuth answers set `no-store` themselves;
  the discovery documents are public for 5 minutes like any GET.
- **Throttling:** the global limiter applies (none of this comes through the frontend's bypass key),
  with `@Throttle()` per minute: register 10, authorize 30, token 60, revoke 30. Registration and
  authorize also count per address bucket in Redis (above).

## Environment

| Variable               | Meaning                                                                                            |
| ---------------------- | -------------------------------------------------------------------------------------------------- |
| `API_PUBLIC_URL`       | this API's public origin: issuer and resource base; default `https://api.park.fan`                 |
| `FRONTEND_URL`         | where `/admin/oauth/consent` is; default the origin of `REVALIDATE_URL`                            |
| `OAUTH_REDIRECT_HOSTS` | https redirect hosts, comma-separated, `*` for any; default `claude.ai,claude.com`                 |
| `OAUTH_REDIRECT_PATHS` | the paths an https redirect may have on those hosts, `*` for any; default `/api/mcp/auth_callback` |

## Known limits

- **SDK line.** This uses `@modelcontextprotocol/sdk` 1.x (spec 2025-11-25). Its README calls 1.x
  the maintenance line, with security fixes until at least 2027-01-27; v2 ships as
  `@modelcontextprotocol/server`. The SDK is used in `mcp-server.factory.ts` and `mcp.controller.ts`
  only.
- **Consent cookie.** The admin cookie is `SameSite=Strict`, so the first page load of a consent link
  that started on claude.ai carries no cookie; the consent page has to read the session from the
  browser, not from that first request.
- A PAT is created with the password (and the two-factor code), but a stolen session plus the
  password can still mint a read-only token that outlives the session, until the account signs out
  everywhere.

### `ml_accuracy` reads `prediction_accuracy` only

`ForecastErrorBreakdownService` runs the drift watch's statement (`MlDecisionWatchService.computeDrift`:
`scoredSlotSql`, the weekday-median baseline of the same ride and hour over the 4 weeks before) with a
grouping key, so its totals equal the ML overview's drift figures. It reads the `(comparison_status,
target_time)` index for `days` + `days` + 28 rows' worth of time, never `wait_time_predictions` or
`queue_data`. `COMPLETED` rows are kept 90 days, so `days` is at most 28. The statement runs under a 45 s
`statement_timeout` and the answer is cached 30 minutes per hour-aligned window; with `park` set the read
narrows to that park's rides. Models are `catboost` (raw hourly), `pcn` and `chronos` (as served); TFT's
daily level is scored in the forward archive, not per park.
