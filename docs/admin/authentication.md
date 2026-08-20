# Admin authentication

## What it was

Every endpoint under `/v1/admin/*` carried this comment:

> ⚠️ SECURITY NOTICE: These administrative endpoints are protected in production
> via Cloudflare. Access requires `pass=XXX` query parameter with valid API key.
> On development/local environments, endpoints are accessible without
> authentication.

That describes traffic which arrives through Cloudflare, and says nothing about
traffic that does not. **This application never checked the `pass` parameter it
advertised** — there was no guard, in any environment. Anything that could reach
the origin directly could merge parks, retire attractions, or reset every cache,
with no credential at all. And because the secret was shared, nothing that
happened was attributable to anybody.

## What it is

Named accounts, server-side sessions, roles, and an audit trail.
`AdminAuthGuard` sits on the controller **class**, not on its methods, so a new
endpoint is protected unless it explicitly says otherwise. Cloudflare's rule
stays where it is; this is the second lock, on the inside of the same door.

### Sessions are opaque tokens in Redis, not JWTs

An admin session grants the right to merge parks and retire attractions, so "log
this device out now" and "deactivate this account now" have to mean *now* — and
a JWT cannot be revoked. Statelessness buys nothing here either: every admin
request already touches Postgres.

Redis stores the **SHA-256 of the token**, never the token. An attacker who
reads a dump holds hashes they cannot present.

Two clocks bound a session: a **12 h idle window** that slides on use, and a
**7 day absolute ceiling** that does not. A working curation session stays
alive; a forgotten tab eventually dies regardless.

### Passwords use Node's own scrypt

bcrypt and argon2 are native addons that compile per platform, and this API
ships in a slim image with no build toolchain — a login is not worth a node-gyp
dependency. scrypt is memory-hard, in the standard library, and costs ~100 ms
per verification at `N=32768, r=8, p=1`, which prices offline guessing out of
reach while a human logging in once a day never notices.

The parameters live **inside** the stored hash (`scrypt$N$r$p$salt$key`), so
raising the cost later does not invalidate existing hashes; `needsRehash`
upgrades one on its owner's next successful login, the only moment the plaintext
exists.

Policy is length only: **12 characters minimum**. Composition rules measurably
push people towards `Password1!`, which is shorter and worse.

### The login limiter is not the global throttler

`CfThrottlerGuard` skips any request carrying a valid `THROTTLE_BYPASS_KEYS`
value — and our own frontend sends exactly that on every server-side call. Since
the admin UI reaches this API only through that proxy, a `@Throttle()` on the
login handler would be skipped for every real login attempt and enforced only
against callers who are *not* the admin UI. The precise inverse of what is
wanted.

So `AdminLoginRateLimitService` counts in Redis, unconditionally, in two buckets:

- **by address** — one place guessing many accounts (a spray). The per-account
  lockout never sees this, because each account only fails once or twice.
- **by account** — many places guessing one account. Overlaps the lockout on
  purpose: the lockout is durable and visible to an owner, this one is cheap and
  sheds load *before* ~100 ms of scrypt is spent.

Both count failures only, both use a fixed window (a sliding one lets a slow
attacker hold the door shut forever), and both fail **open** if Redis is down —
the durable defence is the per-account lockout in Postgres, and a Redis outage
must not lock every administrator out of their own admin.

### TOTP is implemented here

Thirty lines of HMAC and a base32 codec, and the alternative is a dependency in
the trust path of the login. The consumed step is stored, which makes each code
single-use: without that, a six-digit code observed once — over a shoulder, off
a proxy log — stays valid for the rest of its 30-second window.

Disabling it needs the password **and** a current code. A stolen session must not
be able to remove the second factor; a stolen password must not either.

## Roles

`owner > editor > author > viewer`, ranked rather than enumerated, so an
endpoint that asks for "editor or above" keeps meaning that when a role is added
between them.

| Role | May |
| --- | --- |
| `owner` | Everything, including accounts and the destructive maintenance — merges, retirements, repair, a full cache reset. There is always at least one. |
| `editor` | Curated content: names, seasons, heights, ride profiles. Job triggers. |
| `author` | Blog posts and media metadata, not park data. |
| `viewer` | Dashboards. Writes nothing. |

Declared with `@AdminMinRole('editor')`. Undecorated endpoints require any valid
session.

## The audit log

`admin_audit_log` is the smaller half of this change and the actual reason for
it: the `curated_*` columns are claims about the world, and a claim needs an
author. `curated_minimum_height = 0` on Winni Splash means somebody read
Phantasialand's Nutzungsbedingungen and found no minimum — reasoning that used to
live in a commit message at best.

Mutating endpoints audit themselves through `AdminAuditInterceptor` rather than
by hand, because there are ~35 of them and only a handful are curation writes
worth a hand-written row. The rest trigger jobs and flush caches, which is
exactly what somebody wants to reconstruct after an incident and exactly what
nobody would remember to instrument one by one. Endpoints that write a richer row
mark themselves `@SelfAudited()`.

`before`/`after` hold **only the changed fields**. An audit row is read by a
human comparing two values, and burying those in a 40-key dump is how audit
trails stop being read. It is also what makes the undo path possible.

## The bootstrap account

`ADMIN_BOOTSTRAP_EMAIL` + `ADMIN_BOOTSTRAP_PASSWORD` create the first owner —
**only when the table is empty**. Re-running it on every boot would let a stale
environment variable resurrect a deleted account or reset a rotated password.

The account is created owing a password change, so the bootstrap value works
exactly once: it lives in a deployment config, a shell history and probably a
password-manager note, and it should not survive the first login.

## The legacy shared pass

It stays, deprecated. The maintenance scripts and the curation runbooks in
`todo.md` still send `?pass=`, and turning it off before the frontend had a
session transport would have locked everyone out. It is now genuinely enforced
for the first time (constant-time compared against `ADMIN_LEGACY_PASS`), logged
at most once a minute, granted a configurable role, and switchable off with
`ADMIN_LEGACY_PASS_ENABLED=false`.

Audit rows from that path say `actorEmail: 'legacy-pass'`, which is visibly not a
person. That is the point.

## Environment

| Variable | Meaning |
| --- | --- |
| `ADMIN_BOOTSTRAP_EMAIL` / `ADMIN_BOOTSTRAP_PASSWORD` | First owner, first boot only. |
| `ADMIN_LEGACY_PASS` | The deprecated shared secret. Empty ⇒ unavailable. |
| `ADMIN_LEGACY_PASS_ENABLED` | `false` closes that path while the secret is still set. |
| `ADMIN_LEGACY_PASS_ROLE` | What the shared secret may do. Defaults to `owner` — what it effectively had. |
| `ADMIN_LOGIN_LOCKOUT_THRESHOLD` / `_MINUTES` | Per-account lockout. Defaults 8 / 15. |
| `ADMIN_REQUIRE_TOTP` | `true` makes two-factor mandatory and un-removable. |

Schema changes land through TypeORM `synchronize` on boot — there are no
migrations — so `admin_users` and `admin_audit_log` appear on the next deploy.
Every column on them is nullable or has a default, which is a requirement rather
than a style choice under `synchronize`.

## Related

- `docs/admin/curation.md` — what the curated columns are and how they resolve
- frontend: `docs/features/admin.md` — the UI on the other side of this
