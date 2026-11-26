# App start check (`/v1/app/config`)

The app asks this route when it starts and stops showing its content when its own version is
below the minimum the owner set. Code: `src/app-config/`.

## `GET /v1/app/config`

Open: no data key, no session (`@OpenRoute`), so an old app whose key was revoked or never sent can
still read it. `Cache-Control: public, max-age=300`. It never answers 5xx for missing settings or an
unreadable database; both give the defaults, which block nothing.

```json
{
  "ios": {
    "minVersion": "1.0.1",
    "minBuild": 4,
    "updateUrl": "https://apps.apple.com/app/id1",
    "message": { "de": "…", "en": "…" }
  },
  "android": {
    "minVersion": null,
    "minBuild": null,
    "updateUrl": null,
    "message": null
  }
}
```

Both blocks are always present, with the same fields and rules; `PLATFORMS` in
`app-config.service.ts` lists them, so a third platform is one more entry. Android's `minBuild` is
compared against its `versionCode`; no Android app is released yet, the block is prepared.

- `minVersion`: `\d+(\.\d+){0,2}` or `null` (no minimum). Parts compare as numbers, missing parts
  are 0 (`1.10` is above `1.9`, `1.0` equals `1.0.0`).
- `minBuild`: positive integer or `null`. Only looked at when the app's version equals `minVersion`.
- The app is too old when its version is below `minVersion`, or equal with a build below
  `minBuild` (`isBelowMinimum` in `app-version.util.ts`). A version the server cannot read blocks
  nothing.
- `updateUrl`: https or `null`. `message`: per language (`de en nl fr es it`), at most 300
  characters each, or `null`; a missing language means the app's own text.

## Admin (`admin/app-config`)

| Method | Path                         | Role   | Does                                                                                               |
| ------ | ---------------------------- | ------ | -------------------------------------------------------------------------------------------------- |
| GET    | `admin/app-config`           | viewer | `platforms` (as above), `updatedBy`, `updatedAt` and `samples` per platform for `?version=&build=` |
| PUT    | `admin/app-config/:platform` | owner  | `ios` or `android`: replaces its four values (`null` clears); `400` names the bad field; audited   |

`samples.ios.text` reads like `1.0.0 (3) → blocked`; without a query the sample is version `1.0.0`.
`minBuild` without `minVersion` is a `400`; the PUT answers like the GET.

## Storage

One row (`id = 1`) in `app_config`, an entity like `assistant_settings`, with the platforms in one
`platforms` jsonb column. With `DB_SYNCHRONIZE=true`
the table appears on the first deploy; no row means no minimum.
