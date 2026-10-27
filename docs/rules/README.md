# Standing rules (backend)

The rules that apply to every change in this repo, one page each. Open the page for the thing you are
about to touch; each one carries the rule, the identifiers to grep for, and why.

- [Park timezone, always](park-timezone.md) — no `new Date()` for business logic; park-local "today" via `src/common/utils/date.util.ts`.
- [The entities are the schema](the-entities-are-the-schema.md) — no migrations; `DB_SYNCHRONIZE=true` in production; nullable columns, no renames, text over enums, `HYPERTABLES`.
- [Unified crowd levels](crowd-levels.md) — typical-day-peak daily vs ratio-vs-P50 live, never mixed; sum minutes then divide; `unknown`, `CROWD_LEVEL_VALUES`.
- [An absent fact never becomes a confident one](absent-facts.md) — `UNKNOWN` over `CLOSED`, `MIN_OBSERVED_DAYS`, curated columns beside synced ones, research never recall.
- [Automated retirement, upstream identity and production one-offs](automated-retirement-and-identity.md) — count a destructive first run, `externalId` not slug, `applyParkMoves`, per-child `try/catch`, `prod-psql.sh`.
- [Publish a write outside-in](publish-outside-in.md) — write, `invalidateParkCaches`, `revalidateTags`, again after `CDN_SETTLE_MS`.
