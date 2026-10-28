# Admin MCP server

An MCP server at `https://api.park.fan/v1/mcp` that lets Claude (Claude Code, claude.ai, any MCP
client) read the admin's data directly: parks and rides with their live state, popularity, the
site statistic and its SEO reports, Google Search Console, system and ML health, job queues, data-quality findings,
duplicate and retirement candidates, outage statistics and the audit log. Read-only in v1. A client signs in through an OAuth 2.1 flow whose
login and consent happen in park.fan's own admin, or presents a personal access token. Open this
page to connect a client, add a tool, or change anything on `/v1/mcp`, `/v1/oauth/*` or
`/.well-known/oauth-*`. The admin login itself: [authentication](authentication.md).

## Connecting

**Claude Code**

```bash
claude mcp add --transport http parkfan-admin https://api.park.fan/v1/mcp
```

Then `/mcp` in Claude Code, pick `parkfan-admin`, *Authenticate*. The browser opens park.fan's
consent page (behind the admin login), shows the client and where it will be sent, and *Approve*
hands Claude Code its tokens. It refreshes them itself; a grant unused for 30 days expires.

**claude.ai** — Settings → Connectors → *Add custom connector*, URL
`https://api.park.fan/v1/mcp`, no client id or secret. *Connect* runs the same consent.

**A personal access token instead of OAuth** — create one on the admin account page ("API access /
MCP", shown once; it asks for your password, and your two-factor code if you use one), then:

```bash
claude mcp add --transport http parkfan-admin https://api.park.fan/v1/mcp \
  --header "Authorization: Bearer pfa_…"
```

**MCP Inspector** — `npx @modelcontextprotocol/inspector`, transport *Streamable HTTP*, the same
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
  a second-factor reset, *sign out everywhere* (`AdminAuthService.signOutEverywhere`). It first sets
  `admin_users.tokens_revoked_at`; a token created before that instant is refused at every request
  even if its row escaped the sweep, and a code approved before it is refused at the exchange.
  Tokens are issued under a lock on the account's row, so issuing and signing out never interleave.
- **Tokens reach `/v1/mcp` and nothing else.** `AdminAuthGuard` accepts them only on an endpoint
  marked `@AdminTokenOnly()`, and such an endpoint accepts neither a session nor the shared pass.
  So a token cannot list, mint or extend tokens, approve a consent or call an admin REST route.

| | PAT `pfa_…` | OAuth access `pfo_…` | OAuth refresh `pfr_…` |
| --- | --- | --- | --- |
| Lifetime | 30, 90, 365 days or none | 1 hour | 30 days, renewed with each refresh |
| Made by | `POST /v1/admin/auth/tokens` (session and password) | the code or refresh exchange | the same |
| Revoked by | `DELETE /v1/admin/auth/tokens/:id`, an owner, the account's sign-out | revoking the grant | revoking the grant; reuse |

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
  with the redirect host). A reuse or code replay revokes and logs a warning; it has no actor.

## Tools

One tool, one file in `src/mcp/tools/`, registered by one line in `src/mcp/tools/index.ts`. A tool
is a name, a description written for the model, a zod input shape whose every field is described
and bounded, and a handler that calls an existing service through `ctx.get(Service)`. Answers are
compact JSON, lists cut to the limit the input allows, the whole answer cut at 100 000 characters.
A handler throws `McpToolError` for a failure the model should read; any other error reaches the
model as a generic failure and its message only goes to the log. `index.spec.ts` checks every tool
against these rules.

| Tool | Answers | Wraps |
| --- | --- | --- |
| `whoami` | account, effective role, credential kind and client | the principal |
| `find_parks` | parks by name, city or country, with status, crowd level, today's hours | `SearchService` |
| `get_park` | live state: schedule, crowd, weather, rides by status, longest waits, rides down (reported or inferred), not run today | `ParkIntegrationService` |
| `find_attractions` | rides by name, optionally in one park, with status and wait | `SearchService` |
| `get_attraction` | one ride: queues, crowd level, outage, curated facts, typical waits, downtime, retirement, optional history | `AttractionIntegrationService` |
| `page_views` | reads per kind, language and page | `PageViewsService.adminOverview` |
| `system_health` | host, Postgres, Redis, data freshness, the site statistic's flush backlog and drops; or the ML half | `SystemHealthService` |
| `queue_status` | Bull counts per queue, recent failures of one | `AdminQueueStatusService` |
| `ml_overview` | forecast components, decisions, comparison summary, drift | `MlOverviewService` |
| `data_quality` | silenced clusters, failing jobs, unreviewed absence retirements, re-issue candidates | `DataQualityMonitorService.report` |
| `duplicates` | duplicate attraction and park pairs with the merge winner | `AttractionMergeService`, `ParkValidatorService` |
| `retirement_candidates` | rides whose source went quiet | `AttractionReviewService` |
| `audit_log` | admin actions, filterable | `AdminAuditService.list` |
| `downtime` | outage events per park and ride over a window | `DowntimeMeasurementService.measure` |
| `analytics_overview` | page views, visits, visitors, events, views per visit, bounce, average visit and time on page; the window before and the change; realtime; optional series with every figure per period | `SiteAnalyticsService.overview` |
| `analytics_live` | the last half hour minute by minute: active visitors, pages, countries, channels, referrers, devices, events | `SiteAnalyticsLiveService.live` |
| `analytics_breakdown` | views, visits or exits (`measure`) per value of one dimension, share, time on page, exits on a path, bounce and average visit when counting visits | `SiteAnalyticsService.breakdown` |
| `analytics_traffic_crosstab` | page views or visits by two dimensions, as a `counts[row][col]` matrix, small values folded | `SiteAnalyticsService.trafficCrosstab` |
| `analytics_events` | every event name with count, previous, change, blocked | `SiteAnalyticsService.events` |
| `analytics_event` | one event: props with top values, combinations, page types, devices, entities | `SiteAnalyticsService.event` |
| `analytics_crosstab` | one event by two sides, as a `counts[row][col]` matrix, small values folded | `SiteAnalyticsService.crosstab` |
| `search_report` | the site search per surface: openings, typed, clicks, dead ends, rates; what was clicked where | `SiteAnalyticsService.search` |
| `seo_landing_pages` | the pages visits of one channel (search by default) entered on, with change, bounce, sources | `SiteAnalyticsSeoService.landingPages` |
| `seo_trends` | landing pages rising and falling against the window before | `SiteAnalyticsSeoService.trends` |
| `seo_not_found` | paths that showed the not-found page, with the referrers of the visits that entered on them | `SiteAnalyticsSeoService.notFound` |
| `seo_uncovered` | parks or rides no search visit entered on | `SiteAnalyticsSeoService.uncovered` |
| `gsc_status` | whether Google Search Console is connected, when it last imported, the days covered | `SearchConsoleService.status` |
| `gsc_queries` | the Google queries the site was found with: clicks, impressions, ctr, position, change; filter by page, country, device | `SearchConsoleService.queries` |
| `gsc_pages` | the pages found in Google, with the site's own visits, bounce and visit length from search | `SearchConsoleService.pages` |
| `gsc_series` | clicks, impressions, ctr and position per day, week or month | `SearchConsoleService.series` |

The site-statistic tools share `tools/site-analytics.inputs.ts`: `days` (1–365, 30 when absent;
1–180 for `seo_trends`, which needs the window before) or `from` and `to` (`YYYY-MM-DD`), `interval`
(`hour`, `day`, `week`, `month`), `locale`, and `filters` as `[{ dim, value }]`, which become the
reports' `filter=<dim>:<value>`. The tools over events (`analytics_events`, `analytics_event`,
`analytics_crosstab`, `search_report`) take only the event's dimensions (`eventWindowInput`), and
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
  not folded and still count every value: `total`, a report's `totals`, `previous`.
- **Where.** `analytics_breakdown`, `analytics_events`, `analytics_event` (property values, combinations,
  page types, devices, entities), `analytics_live` (pages, countries, channels, referrers, devices,
  events), `search_report` (surfaces, openings by page type and device, result types, positions, browse
  items), `seo_landing_pages` (pages, and the sources of each), `seo_trends`, `seo_not_found` (paths, and the
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

## Endpoints

| Method | Path | Who | Does |
| --- | --- | --- | --- |
| POST | `/v1/mcp` | PAT or OAuth access token | MCP JSON-RPC, Streamable HTTP, stateless, JSON answers |
| GET, DELETE | `/v1/mcp` | the same | 405: no server stream, no session |
| GET | `/.well-known/oauth-protected-resource[/v1/mcp]` | public | RFC 9728 metadata |
| GET | `/.well-known/oauth-authorization-server` | public | RFC 8414 metadata |
| POST | `/v1/oauth/register` | public | RFC 7591 registration, public clients only |
| GET | `/v1/oauth/authorize` | public (browser) | validates, parks the request, 302 to the consent page |
| POST | `/v1/oauth/token` | public client | `authorization_code` (PKCE S256), `refresh_token` |
| POST | `/v1/oauth/revoke` | public client | RFC 7009; revokes the whole grant |
| GET | `/v1/admin/oauth/requests/:id` | session, any role | what the consent page shows: client, redirect host, scope, `createdAt` (its age; a request lives 10 minutes) and `expiresAt` |
| POST | `/v1/admin/oauth/requests/:id/approve` | session, any role | `{ redirectTo }` with the code |
| POST | `/v1/admin/oauth/requests/:id/deny` | session, any role | `{ redirectTo }` with `access_denied` |
| GET | `/v1/admin/auth/tokens[?userId=]` | session; `userId` owner only | active PATs and grants |
| POST | `/v1/admin/auth/tokens` | session + password (+ `totpCode`) | `{ token, item }`, the token shown once |
| DELETE | `/v1/admin/auth/tokens/:id` | session; others' owner only | revokes a PAT or a grant |

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
  The pending request id is 256 random bits, stored hashed, and answered once.
- **Consent phishing:** anyone may register a client called "Claude", so the consent page shows the
  redirect host, and https redirects are limited to `OAUTH_REDIRECT_HOSTS`. A grant obtained through
  a phished approval lands in the approving admin's own claude.ai session or on their own machine.
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
- **Amplification:** JSON-RPC batches are refused (one tool call per request, which the throttler
  counts), and one token runs at most 4 requests at once per API instance.
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

| Variable | Meaning |
| --- | --- |
| `API_PUBLIC_URL` | this API's public origin: issuer and resource base; default `https://api.park.fan` |
| `FRONTEND_URL` | where `/admin/oauth/consent` is; default the origin of `REVALIDATE_URL` |
| `OAUTH_REDIRECT_HOSTS` | https redirect hosts, comma-separated, `*` for any; default `claude.ai,claude.com` |
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
