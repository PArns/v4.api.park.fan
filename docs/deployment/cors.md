# CORS: which browser origins may call the API

Which origins get CORS headers, why a foreign origin still gets a normal answer, and why only uncached
endpoints are safe to call from a browser. Open it before changing `CORS_ORIGINS` or letting the
frontend call a new endpoint directly.

Cloudflare adds no CORS headers to `api.park.fan` (checked 2026-10-23: no
`Access-Control-Allow-Origin` on any answer, `OPTIONS` preflight 404). The
allowed origins are decided in the app, by `buildCorsOptions()` in
`src/common/cors/cors.config.ts`, which `src/main.ts` passes to
`app.enableCors`.

## The list

| `NODE_ENV`    | `CORS_ORIGINS` unset                             | `CORS_ORIGINS` set     |
| ------------- | ------------------------------------------------ | ---------------------- |
| `production`  | `https://park.fan`, `https://www.park.fan`       | exactly the given list |
| anything else | `http(s)://localhost:<port>`, `127.0.0.1:<port>` | exactly the given list |

`CORS_ORIGINS` is comma-separated and **replaces** the default, so a production
value that adds a preview has to name park.fan again:

```bash
CORS_ORIGINS=https://park.fan,https://www.park.fan,https://park-*-arns.vercel.app
```

A `*` stands for one or more letters, digits or hyphens inside a host label.
It never matches a dot, so `https://park-*-arns.vercel.app` covers
`park-7kbbuacsi-arns.vercel.app` and `park-fan-git-<branch>-arns.vercel.app`
and nothing outside `vercel.app`.

A write with a JSON body — park.fan's trip and push calls — is preceded by an
`OPTIONS` preflight. `maxAge: 7200` lets the browser keep that answer for two
hours, which is Chrome's cap, so one visit pays for it once.

## An origin that is not on the list gets a normal answer

A request from a foreign origin, or with no `Origin` header at all, is answered
as usual, just without CORS headers; the browser then refuses to hand the body
to the page. The callback must never raise for it: `cors` passes the error to
Express, which answers 500. That is what the code before this page did as soon
as `CORS_ORIGINS` was set, for every server-side caller — park.fan's own
renders, the prewarm crons, scripts, monitoring — because none of them send an
`Origin`.

## Only uncached endpoints are safe to call cross-origin

The answer to an allowed origin carries `Access-Control-Allow-Origin: <that
origin>` and `Vary: Origin`. Cloudflare ignores `Vary` for every header except
`Accept-Encoding`, so an endpoint it caches keeps whichever copy it stored
first: one rendered for a server-side caller has no CORS header, and a browser
that is served it fails. Endpoints answering `private, no-store`
(`/v1/discovery/nearby`, `/v1/favorites`) bypass the cache and are safe. A
cached endpoint needs an origin-independent answer, set at Cloudflare, before a
browser may call it directly.

## The admin MCP's public surface

`buildCorsDelegate` (what `src/main.ts` passes) answers `/v1/mcp`, `/v1/oauth/register`,
`/v1/oauth/token`, `/v1/oauth/revoke` and `/.well-known/oauth-*` with
`Access-Control-Allow-Origin: *` and no credentials, whatever `CORS_ORIGINS` says, and exposes
`WWW-Authenticate`. A browser-based MCP client (the MCP Inspector) calls them from its own origin,
and none of them reads a cookie: the credential is a bearer token or a PKCE verifier the caller
attaches itself. `*` is also origin-independent, so the cached discovery documents do not run into
the `Vary` problem above. `/v1/oauth/authorize` is a navigation and the consent endpoints go
through park.fan's proxy, so they keep the list. See [admin MCP](../admin/mcp.md).

## Checking it

```bash
curl -sI -H "Origin: https://park.fan" \
  "https://api.park.fan/v1/discovery/nearby?radius=1000" | grep -i access-control
curl -s -o /dev/null -w "%{http_code}\n" "https://api.park.fan/v1/discovery/nearby?radius=1000"
```

The first prints `access-control-allow-origin: https://park.fan`, the second
`200`.
