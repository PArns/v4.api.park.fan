# Admin authentication

How `/v1/admin/*` is protected: named accounts, opaque Redis sessions, scrypt passwords, TOTP, roles, the
audit log, the login rate limits and Turnstile, and the deprecated shared pass. Open this before adding an
admin endpoint or touching the login. The UI on the other side: the frontend's `docs/features/admin.md`.

Cloudflare's rule is the outer lock and this is the inner one: without it, anything
reaching the origin directly could merge parks or flush caches, unattributed.

## The guard

`AdminAuthGuard` sits on the controller **class**, so a new endpoint is protected unless it says
otherwise (`@AdminPublic()`, used by the login). `@AdminMinRole('editor')` sets a minimum role;
undecorated endpoints need any valid session.

| Role | May |
| --- | --- |
| `owner` | everything, including accounts and destructive maintenance (merges, retirements, repair, a full cache reset); keep at least two |
| `editor` | curated content (names, seasons, heights, ride profiles), job triggers |
| `author` | blog posts and media metadata, not park data |
| `viewer` | dashboards; writes nothing |

Roles are ranked (`owner > editor > author > viewer`), so "editor or above" keeps meaning that when a
role is added between them.

## Sessions: opaque tokens in Redis

An admin session can merge parks, so "log this device out" and "deactivate this account" must take effect
now — a JWT cannot be revoked. Redis stores the **SHA-256 of the token**, never the token
(`admin-session.store.ts`). Two clocks: a **12 h idle window** that slides on use and a **7-day absolute
ceiling**. A login re-reads `is_active` and `role` after creating its session, so a deactivation that
landed mid-login is not outlived.

## Passwords: Node's scrypt

bcrypt and argon2 are native addons, and the image has no build toolchain. scrypt is memory-hard, in the
standard library, ~100 ms per verification at `N=32768, r=8, p=1` (`password.util.ts`). Parameters live
in the stored hash (`scrypt$N$r$p$salt$key`), so raising the cost later invalidates nothing; `needsRehash`
upgrades a hash at its owner's next login. Policy: **12 characters minimum**, no composition rules
(they push people towards `Password1!`).

## Login rate limits

`CfThrottlerGuard` skips any request carrying a valid `THROTTLE_BYPASS_KEYS` value, and the frontend sends
one on every server-side call — so a `@Throttle()` on the login would skip every real admin and limit only
strangers. `AdminLoginRateLimitService` counts in Redis unconditionally, in fixed 15-minute windows:

- **by address** (25 failures): one place guessing many accounts, which the per-account lockout never
  sees;
- **by account** (10 failures): many places guessing one account, shedding load before scrypt runs.

Every attempt is counted **before** the password check and refunded if it was not a failure (counted
afterwards, a hundred parallel guesses all read the same counter). The durable defence is the per-account
lockout in Postgres (`ADMIN_LOGIN_LOCKOUT_THRESHOLD` / `_MINUTES`, default 8 / 15), incremented in one
statement. The password is verified **before** a lockout is reported, and only to someone who got it
right — answering "locked" first made the endpoint an account-existence oracle. An expired lockout resets
its counter (else one bad guess every 15 min kept an account shut forever). Both Redis buckets fail
**open**: a Redis outage must not lock every administrator out.

The same limiter guards `auth/totp/begin`, `auth/totp/disable` and `auth/change-password`
(`checkAction` / `recordActionFailure` / `recordActionSuccess`, 10 failures per account per action per
15 min). `totp/disable` is the sharp case: with three valid codes at any moment (`WINDOW_STEPS = 1`), a
stolen session plus the password needed ~333k unthrottled requests to strip the second factor.

**The address counted.** `clientIp` prefers the forwarded address over `cf-connecting-ip` only for a
request carrying a valid bypass key: api.park.fan is itself behind Cloudflare, so on the admin UI's
requests `cf-connecting-ip` is the Vercel function, which put every administrator in one bucket. Both
headers are `isIP()`-validated, bounding the Redis key space.

## Turnstile on the login

The frontend verifies a Cloudflare Turnstile token in its own `/api/admin/session` route before
forwarding, but `POST /v1/admin/auth/login` is public, so a bot can skip the frontend. The login therefore
demands a solved token from every caller **except** one presenting a valid `THROTTLE_BYPASS_KEYS` value —
the same test as the throttler, in one function (`carriesThrottleBypassKey`). The frontend is exempt
because a token can be redeemed once and it already did. The check runs before the password is read and
before either limiter counts, so an attacker cannot burn an editor's eight attempts for free.

It is off until **both** `TURNSTILE_SECRET_KEY` (same name and value as the frontend's — one widget) and
`THROTTLE_BYPASS_KEYS` are set; with no bypass keys the admin proxy would look like a stranger and every
login would fail. Once on, nothing is waved through: a missing secret, a rejected token and an unreachable
siteverify all refuse. `ADMIN_TURNSTILE_TIMEOUT_MS` (5000) caps the siteverify call;
`ADMIN_LOGIN_TURNSTILE=false` switches it off while the secret stays set.

## TOTP

Implemented here (HMAC and base32, `totp.util.ts`) rather than as a dependency in the login's trust path.
The consumed step is stored and claimed with a conditional `UPDATE`, so each code works once, also under
parallel replay. Enrolling and disabling both need the password (a stolen session must neither remove a
second factor nor add an attacker's); disabling also needs a current code.

**Recovery:** an owner can clear another account's second factor
(`POST /v1/admin/auth/users/:id/reset-totp`), which also ends that account's sessions. With a single owner,
a lost phone locks out the whole admin.

**`ADMIN_REQUIRE_TOTP`** is enforced at the login: an unenrolled account still gets a session (refusing it
would lock out the people who need to enrol), but it reaches only the enrolment endpoints, like a pending
password change; enrolling clears that on the sessions already open.

## The audit log

`admin_audit_log` exists because the `curated_*` columns are claims about the world and a claim needs an
author (`curated_minimum_height = 0` on Winni Splash means somebody read Phantasialand's conditions).
Mutating endpoints audit themselves through `AdminAuditInterceptor`; the ones that write a richer row are
`@SelfAudited()`. `before` / `after` hold **only the changed fields**, which is what makes undo possible
([curation](curation.md#undo)).

## The bootstrap account

`ADMIN_BOOTSTRAP_EMAIL` + `ADMIN_BOOTSTRAP_PASSWORD` create the first owner **only when `admin_users` is
empty** (re-running on every boot would resurrect deleted accounts or reset rotated passwords). The account
owes a password change, so the bootstrap value works exactly once.

## The legacy shared pass

Deprecated but still accepted, because maintenance scripts and runbooks send `?pass=`. It is compared in
constant time against `ADMIN_LEGACY_PASS`, logged at most once a minute, granted `ADMIN_LEGACY_PASS_ROLE`
(default `owner`), and switched off with `ADMIN_LEGACY_PASS_ENABLED=false`. Audit rows say
`actorEmail: 'legacy-pass'`.

**It cannot manage accounts, whatever its role**: otherwise a secret living in a runbook could mint a real
owner account attributed to nobody. `@AdminLegacyAccess(false)` sits on the whole `AdminAuthController`
and `true` on the two handlers a legacy caller needs (`me`, which draws the frontend's legacy banner, and
`logout`). Unset means "no opinion", which keeps the job and merge endpoints working; a `false` on a class
is inherited by any account endpoint added later.

**The secret travels in the query string, so nothing may log a raw URL.** `LoggingInterceptor` logs only
requests that returned (a `tap()`), `HttpExceptionFilter` the ones that threw; both call `redactUrl()`
(`src/common/utils/redact-url.util.ts`). The failing half matters most: `cache/reset` without
`?confirm=true`, and every `@AdminLegacyAccess(false)` endpoint, refuse a pass after authenticating it.

## Environment

| Variable | Meaning |
| --- | --- |
| `ADMIN_BOOTSTRAP_EMAIL` / `ADMIN_BOOTSTRAP_PASSWORD` | first owner, first boot only |
| `ADMIN_LEGACY_PASS` | the deprecated shared secret; empty ⇒ unavailable |
| `ADMIN_LEGACY_PASS_ENABLED` | `false` closes that path while the secret is still set |
| `ADMIN_LEGACY_PASS_ROLE` | what the shared secret may do; default `owner`, never account management |
| `ADMIN_LOGIN_LOCKOUT_THRESHOLD` / `_MINUTES` | per-account lockout, default 8 / 15 |
| `ADMIN_REQUIRE_TOTP` | `true` makes two-factor mandatory and un-removable |
| `TURNSTILE_SECRET_KEY` | Turnstile secret, shared with the frontend |
| `ADMIN_TURNSTILE_TIMEOUT_MS` | siteverify timeout, default 5000 |
| `ADMIN_LOGIN_TURNSTILE` | `false` switches the check off |
| `THROTTLE_BYPASS_KEYS` | the frontend's key; exempts it from the throttler and Turnstile |

`admin_users` and `admin_audit_log` come from `synchronize`; every column is nullable or defaulted
([the entities are the schema](../rules/the-entities-are-the-schema.md)).
