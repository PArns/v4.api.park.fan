### Added — A minimum app version per platform

`GET /v1/app/config` (open, cached five minutes) tells the iOS and the Android app which version and build (Android: versionCode) they must at least be, where to update and an optional note per language; the owner sets each platform under `admin/app-config/:platform`, and the answer echoes what a sample version would meet. Nothing set blocks nothing. See [app start check](frontend/app-config.md).
