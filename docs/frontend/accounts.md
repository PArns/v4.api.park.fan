# Visitor accounts (`/v1/account`)

A visitor's own account on park.fan: role `user`, the lowest, in `admin_users` like every other
account. It carries what the site otherwise keeps in one browser, so it follows the visitor to
another device: the favorites (the `favorites` cookie), the rides marked as ridden and the
temperature unit. Trips keep their own id as credential, and push alerts belong to one device's
subscription, so neither moves into the account.

| Route                                     | What it does                                                                                                    |
| ----------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `POST /v1/account/register`               | `{ email, displayName?, password, turnstileToken? }`; creates the account and signs it in, 409 `account-exists` |
| `GET /v1/account`                         | the caller's `account` (email, name, role) and `data`                                                           |
| `PUT /v1/account/data`                    | `{ favorites?, riddenRides?, temperatureUnit? }`; replaces each named field, keeps the others                   |
| `DELETE /v1/account`                      | `{ password }`; deletes a visitor's or developer's own account with everything on it                            |
| `GET /v1/account/storage`                 | the keys of the member's own storage, newest first, with their size                                             |
| `GET/PUT/DELETE /v1/account/storage/:key` | one value: any JSON up to 64 KB under a key of `a-z0-9._-`, at most 100 keys                                    |

- **Storage:** one row per account in `account_data` (`AccountData`): `favorites` jsonb by kind,
  `ridden_rides` jsonb, `temperature_unit` `C`/`F`. Ids are UUIDs, at most `MAX_ACCOUNT_IDS` per
  list, each once.
- **Storage** (`account_storage`, `AccountStorageEntry`): a members-only feature's own notebook
  per account, under a key it chooses. The API keeps the value opaque and bounds it (64 KB per
  value, 100 keys: 413 `value-too-large`, 400 `too-many-keys`, 400 `invalid-key`).
- **Merging is the browser's job.** The API replaces what it is sent; the site unions its local
  state with the account's on sign-in and writes the result.
- **Access:** `AdminAuthGuard` at `user`, `@AdminLegacyAccess(false)`, never cached
  (`NoCdnCacheInterceptor`). Every handler reads and writes only the caller's row. Staff accounts
  reach it too; a staff account cannot delete itself (403), an owner deletes it.
- **Deletion:** `AccountService` registers with `AdminAuthService.onAccountDeletion`, so deleting an
  account, by its owner or by an owner on `/admin/users`, removes its row
  ([authentication](../admin/authentication.md#deleting-an-account)). Audited as
  `account.register` and `account.delete`.
- **Tests:** `test/e2e/account.e2e-spec.ts`; `src/api-keys/developer-role.spec.ts` pins the
  handlers the role reaches.
