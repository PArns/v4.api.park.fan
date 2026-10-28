# Admin MCP server

An MCP server at `https://api.park.fan/v1/mcp` that lets Claude (Claude Code, claude.ai, any MCP
client) read the admin's data directly: parks and rides with their live state, popularity, system
and ML health, job queues, data-quality findings, duplicate and retirement candidates, outage
statistics and the audit log. Read-only in v1. A client signs in through an OAuth 2.1 flow whose
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
MCP", shown once), then:

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
  a second-factor reset, *sign out everywhere* (`AdminAuthService.signOutEverywhere`).
- **Tokens reach `/v1/mcp` and nothing else.** `AdminAuthGuard` accepts them only on an endpoint
  marked `@AdminTokenOnly()`, and such an endpoint accepts neither a session nor the shared pass.
  So a token cannot list, mint or extend tokens, approve a consent or call an admin REST route.

| | PAT `pfa_…` | OAuth access `pfo_…` | OAuth refresh `pfr_…` |
| --- | --- | --- | --- |
| Lifetime | 30, 90, 365 days or none | 1 hour | 30 days, renewed with each refresh |
| Made by | `POST /v1/admin/auth/tokens` (session) | the code or refresh exchange | the same |
| Revoked by | `DELETE /v1/admin/auth/tokens/:id`, an owner, the account's sign-out | revoking the grant | revoking the grant; reuse |

- **Codes** are single-use, live 60 s, are stored hashed in Redis and bound to the client, the
  redirect URI and the PKCE challenge. A code presented twice revokes the grant it bought.
- **Refresh tokens rotate.** Each refresh issues a new pair; presenting a used one more than 10 s
  after its use revokes the whole grant (the thief and the client both hold it, and the server cannot
  tell which is which). Within those 10 s it is a race between two processes of one client and only
  answers `invalid_grant`.
- **Limits:** 25 active PATs per account; a new grant beyond 20 active ones revokes the oldest.
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
| `system_health` | host, Postgres, Redis, data freshness; or the ML half | `SystemHealthService` |
| `queue_status` | Bull counts per queue, recent failures of one | `AdminQueueStatusService` |
| `ml_overview` | forecast components, decisions, comparison summary, drift | `MlOverviewService` |
| `data_quality` | silenced clusters, failing jobs, unreviewed absence retirements, re-issue candidates | `DataQualityMonitorService.report` |
| `duplicates` | duplicate attraction and park pairs with the merge winner | `AttractionMergeService`, `ParkValidatorService` |
| `retirement_candidates` | rides whose source went quiet | `AttractionReviewService` |
| `audit_log` | admin actions, filterable | `AdminAuditService.list` |
| `downtime` | outage events per park and ride over a window | `DowntimeMeasurementService.measure` |

Reads made through the tools do not count as popularity hits (`countHit = false`). The site
analytics service (PAR-1151) is meant to add `analytics_overview`, `analytics_breakdown`,
`analytics_events`, `analytics_event`, `analytics_crosstab`, `search_report` and the `seo_*` reads
here, one file each. A tool that writes would set `minRole` and needs a scope above `admin:read`;
v1 issues none.

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
| GET | `/v1/admin/oauth/requests/:id` | session, any role | what the consent page shows |
| POST | `/v1/admin/oauth/requests/:id/approve` | session, any role | `{ redirectTo }` with the code |
| POST | `/v1/admin/oauth/requests/:id/deny` | session, any role | `{ redirectTo }` with `access_denied` |
| GET | `/v1/admin/auth/tokens[?userId=]` | session; `userId` owner only | active PATs and grants |
| POST | `/v1/admin/auth/tokens` | session | `{ token, item }`, the token shown once |
| DELETE | `/v1/admin/auth/tokens/:id` | session; others' owner only | revokes a PAT or a grant |

The shared pass is refused on every row. The response shapes the frontend reads are in
[endpoints](endpoints.md#api-tokens-and-the-admin-mcp).

Redirect URIs: `https://` on a host in `OAUTH_REDIRECT_HOSTS` (default `claude.ai`, `claude.com`),
or `http://127.0.0.1|localhost|[::1]` with any port. Matched exactly, except that a loopback URI may
name another port than it registered (RFC 8252 §7.3). An unknown client or an unregistered redirect
URI gets an HTML error page from `/v1/oauth/authorize`, never a redirect.

## Security

What was checked when this was built, and where it is enforced:

- **PKCE:** S256 only, `plain` and a missing challenge refused at the authorize step; the verifier
  compared in constant time; the code is consumed (`GETDEL`) before the verifier is checked, so a
  wrong guess burns it.
- **Redirect URIs:** validated at registration and again at authorize (the allowed hosts may have
  narrowed), exact match, no fragment or user info. Errors before the redirect URI is trusted are a
  page here, so `/v1/oauth/authorize` is no open redirect; `redirectTo` is always a registered URI.
- **Replay:** codes single-use and 60 s, a replayed code revokes its grant; refresh tokens rotate
  with reuse detection (conditional `UPDATE`, so of two parallel refreshes exactly one wins).
- **Leakage:** secrets stored only as SHA-256 (pending requests and codes in Redis too); tokens only
  in the `Authorization` header, never in a URL; `code`, `code_verifier`, `access_token`,
  `refresh_token`, `client_secret` and `state` are redacted from logged URLs (`redactUrl`); nothing
  logs headers or OAuth bodies; every OAuth answer is `no-store`, the authorize redirect too, and
  API answers carry `Referrer-Policy: no-referrer`.
- **Consent CSRF:** the approve call reaches the API only through park.fan's admin proxy with the
  session as a bearer header, which no other site can attach; the proxy's cookie is `SameSite=Strict`
  ([the frontend's admin docs](https://github.com/PArns/park.fan/blob/main/docs/features/admin.md)).
  The pending request id is 256 random bits, stored hashed, and answered once.
- **Consent phishing:** anyone may register a client called "Claude", so the consent page shows the
  redirect host, and https redirects are limited to `OAUTH_REDIRECT_HOSTS`. A grant obtained through
  a phished approval lands in the approving admin's own claude.ai session or on their own machine.
- **DCR abuse:** 10 registrations per address and hour (Redis) plus 10 a minute (throttler), names
  stripped of control and bidi characters and cut at 100, at most 10 redirect URIs. Registrations
  never used for a grant are deleted after a week, and beyond 1000 of them the oldest give way;
  refusing new ones instead would let a few dozen addresses close registration for everybody.
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
  with `@Throttle()` per minute: register 10, authorize 30, token 60, revoke 30.

## Environment

| Variable | Meaning |
| --- | --- |
| `API_PUBLIC_URL` | this API's public origin: issuer and resource base; default `https://api.park.fan` |
| `FRONTEND_URL` | where `/admin/oauth/consent` is; default the origin of `REVALIDATE_URL` |
| `OAUTH_REDIRECT_HOSTS` | https redirect hosts, comma-separated, `*` for any; default `claude.ai,claude.com` |

## Known limits

- **SDK line.** This uses `@modelcontextprotocol/sdk` 1.x (spec 2025-11-25). Its README calls 1.x
  the maintenance line, with security fixes until at least 2027-01-27; v2 ships as
  `@modelcontextprotocol/server`. The SDK is used in `mcp-server.factory.ts` and `mcp.controller.ts`
  only.
- **Consent cookie.** The admin cookie is `SameSite=Strict`, so the first page load of a consent link
  that started on claude.ai carries no cookie; the consent page has to read the session from the
  browser, not from that first request.
- A PAT can be created with a session alone, without the password: a stolen session can mint a
  read-only token that outlives it until the account signs out everywhere.
