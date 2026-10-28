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
- The host's and Cloudflare's own logs are outside this API.

## Checks

- `src/common/utils/no-address-in-logs.spec.ts` reads every file in `src/` and fails on a logger,
  console or `logToFile` call whose code names an address (`ip`, `clientIp`, `request.ip`,
  `remoteAddress`, the forwarding headers). Words in a message do not count.
- `logging.interceptor.spec.ts`, `http-exception.filter.spec.ts`, `redact-url.util.spec.ts` and
  `typeorm-slow-query-logger.spec.ts` pin each path with a full address going in.

## Known limit

A request's coordinates (`lat`, `lng` on the location routes) stay in the URL of an error line. They
are not an address, but they say where a reader is; nothing redacts them yet.
