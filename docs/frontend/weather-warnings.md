# Severe-weather warnings — `weather.warnings` (frontend guide)

> How to render official severe-weather warnings on a park page. Shipped
> 2026-06-19. Source: MeteoGate (EUMETNET → DWD/MeteoAlarm).

## Where it lives — two places

The same `WeatherWarningDto[]` is exposed in two spots:

1. **Embedded in the park response** — `weather.warnings`:
   ```
   GET /v1/parks/:continent/:country/:city/:parkSlug   (the park detail)
   → weather.warnings: WeatherWarningDto[]
   ```
   Cached ~15 min with the rest of the park payload — fine for first paint.

2. **On the live nowcast** — `/weather/nowcast`:
   ```
   GET …/:parkSlug/weather/nowcast → warnings: WeatherWarningDto[]
   ```
   This endpoint is **polled live client-side** (re-derived every ~15 min), so
   it carries the **most up-to-date** warnings. Use the nowcast warnings for the
   live banner; the embedded copy just seeds the prerendered HTML.

An **empty array** (`[]`) means no active warnings → render nothing. The backend
only returns warnings that are currently valid (`expires > now`), so you don't
need to filter expired ones — but `onset`/`expires` are there if you want to.

## Shape (`WeatherWarningDto`)

```ts
interface WeatherWarning {
  alertId: string;        // stable id — use as React key / dedup
  event: string;          // event type, German, e.g. "GEWITTER"
  eventEn?: string | null;// English, e.g. "thunderstorms"
  severity?: string | null; // Minor | Moderate | Severe | Extreme (CAP)
  urgency?: string | null;  // CAP urgency (Immediate | Expected | Future …)
  category?: string | null; // CAP category / awareness type
  onset?: string | null;    // valid from (ISO 8601)
  expires?: string | null;  // valid until (ISO 8601)
  headline?: string | null;     headlineEn?: string | null;
  description?: string | null;  descriptionEn?: string | null;
  instruction?: string | null;  instructionEn?: string | null; // safety advice
  area?: string | null;     // affected area, e.g. "Kreis Freyung-Grafenau"
  source: string;           // "meteogate" (attribution)
}
```

## Localization

Every text field ships in **German and English** — pick per the active locale:

```ts
const t = locale.startsWith("de") ? "de" : "en";
const event = t === "en" && w.eventEn ? w.eventEn : w.event;
const headline = t === "en" ? (w.headlineEn ?? w.headline) : w.headline;
// same for description / instruction
```

(The German fields are always present; English is best-effort — fall back to
the German value when an `*En` field is null.)

## Severity → colour

Map `severity` to the MeteoAlarm awareness colours:

| severity   | meaning        | suggested colour |
|------------|----------------|------------------|
| `Minor`    | be aware       | yellow           |
| `Moderate` | be prepared    | orange           |
| `Severe`   | be very alert  | red              |
| `Extreme`  | take action    | dark red / purple|

Show the most severe warning first if you collapse multiple into one badge
(sort by this order, then by `onset`).

## Rendering rules

- Render a **banner/badge** per warning (or one badge + a "N warnings" expander).
  Headline + severity colour is the minimum; `description` and `instruction`
  (the safety advice) go in the expanded view.
- Show the validity as "until {expires}" (and "{onset}–{expires}" if the
  warning hasn't started yet — `onset` can be in the future).
- `area` tells the user *which* region is affected — useful when a park sits
  near a warning boundary.
- Use `alertId` as the stable key.

## Coverage & freshness

- **European parks only** (DWD/MeteoAlarm covers ~40 countries). Parks elsewhere
  (US, …) simply return `warnings: []` until another source is added.
- Synced every **15 minutes** server-side; the nowcast poll surfaces changes
  within that window.
- Attribution: warnings are sourced via MeteoGate/MeteoAlarm from the national
  weather service (DWD for Germany) — credit per their terms if you display a
  source line.
