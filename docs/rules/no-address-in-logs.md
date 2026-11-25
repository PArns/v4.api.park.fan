# No full address in any log line

The privacy policy says the server does not log IP addresses. A log line is where one would end up
without anybody deciding it, so the rule is about the lines, not the intent. Open this page before
writing a `logger` call, a `logToFile` record or a request filter that touches the caller.

## The rule

- **A log line carries no address.** Not `request.ip`, not `clientIp(request)`, not a forwarded
  header, not a socket address, in a message, a template or a file record.
- **One exception, and only truncated.** The request line `LoggingInterceptor` writes for an admin
  route (`/v1/admin/…`: accounts, not visitors) names the caller's network, a /24 for IPv4 and a /48
  for IPv6, from `truncateIp` in `src/common/utils/request.util.ts`
  (`addressForLog` decides where). A route that is not the admin's, a visitor's counter included,
  names nothing. Use `truncateIp` for any other need; it is the one helper, also behind
  `admin_users.last_login_ip` and the session's address.
- **The URL is redacted before it is logged** (`redactUrl`): the query parameter `ip`, which the
  location and favourites routes take to look up another address, is `***` like a secret is.
- **So is where a visitor stands.** `lat`, `lng`, `lon`, `long`, `latitude` and `longitude` are
  `***` in every logged URL, in any casing and as `lat[]=`; no log line names a position, a
  distance from a park or the park a visitor stands in (PAR-1430). See
  [what is kept of a position](#what-is-kept-of-a-position).
- **Bound parameters are cut too.** The slow-query file and the error line of a failed statement
  write the parameters; `withoutAddresses` (`typeorm-slow-query-logger.ts`) turns any address among
  them into its network. A statement that binds a visitor's address does not exist, and the audit
  row's `actor_ip` is a network before it is bound.

## What is kept, and why that is not a log

- **`admin_audit_log.actor_ip`** holds the network of the account that did something, a /24 or a /48
  from `truncateIp`, which is all the audit trail needs ("did this come from somewhere new"). A nightly
  job clears it after 90 days and cuts any full address still stored
  ([authentication](../admin/authentication.md#the-audit-log)). It is a table of accounts' actions, not
  of visitors' requests.
- **Redis counters** key on an address bucket or on an HMAC under the day's key and expire with the
  day ([site statistic](../frontend/site-analytics.md#privacy)); no address is a key's text.
- The host's and Cloudflare's own logs are outside this API; how long the containers keep theirs
  and how to see what the host logs: [container logs](../deployment/coolify.md#container-logs).

## Checks

- `src/common/utils/no-address-in-logs.spec.ts` reads every file in `src/` and fails on a logger,
  console or `logToFile` call whose code names an address (`ip`, `clientIp`, `request.ip`,
  `remoteAddress`, the forwarding headers). Words in a message do not count.
- `logging.interceptor.spec.ts`, `http-exception.filter.spec.ts`, `redact-url.util.spec.ts` and
  `typeorm-slow-query-logger.spec.ts` pin each path with a full address going in.

## What is kept of a position

Two routes take a visitor's coordinates, both in the query string: `GET /v1/discovery/nearby` and
`GET /v1/favorites`. Parky's chat takes them in its body.

| Where | What the API keeps |
| --- | --- |
| request line of a slow (> 1 s) or failed request, the error file (`api-errors`), the error body's `path` | the URL with `lat=***&lng=***` |
| `/v1/discovery/nearby` | nothing: no log line, no table, no Redis entry per caller. The parks it answers with each get one anonymous hit in the popularity counter (a park id and a count) |
| `/v1/favorites` | the answer in Redis for 5 minutes, under a key that holds the ids asked for and the position rounded to three decimals (about 110 m), and the answer's own `userLocation`. No table, no log line |
| `POST /v1/assistant/chat` `location` | rounded to two decimals (about a kilometre) on arrival and used for that answer (`find_parks_nearby`); the coordinates are not written to the chat's rows or a log. What Parky then says ("you are in park X, ride Y is 200 m away") is the answer's text, kept 7 days with the chat |
| the slow-query file | the statement and its bound parameters; no statement binds a visitor's position (both routes measure distance in memory) |

Without `lat`/`lng` both routes look the caller's address up in the local GeoLite2 file and keep
neither the address nor the result.

Outside this API, and not visible from this repository: the reverse proxy's and Cloudflare's own
request logs see the full query string, coordinates included
([container logs](../deployment/coolify.md#container-logs)).
