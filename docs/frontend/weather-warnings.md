# Severe-weather warnings (`weather.warnings`)

The contract for official severe-weather warnings on a park (German parks: DWD via
Bright Sky; the rest of Europe: MeteoAlarm via MeteoGate): where they appear, the
`WeatherWarningDto` shape, German and English text, severity colours and coverage.
Open it when rendering a weather warning banner. How the two sources are synced:
[weather](../architecture/weather.md#severe-weather-warnings).

## Where it lives

The same `WeatherWarningDto[]` in two places:

1. **The park detail** — `weather.warnings`, cached with the park payload. Fine for
   the first paint and the prerendered HTML.
2. **The nowcast** — `GET …/:parkSlug/weather/nowcast` → `warnings`
   ([nowcast](./weather.md#the-nowcast)). Use this one for the live banner.

`[]` means no active warning: render nothing. Only currently valid warnings are
returned (`expires > now`, `src/parks/weather-warnings.service.ts`), so no client
filtering is needed.

## Shape (`src/parks/dto/weather-warning.dto.ts`)

```ts
interface WeatherWarning {
  alertId: string;              // stable — React key, dedupe
  event: string;                // German, e.g. "GEWITTER"
  eventEn?: string | null;      // e.g. "thunderstorms"
  severity?: string | null;     // CAP: Minor | Moderate | Severe | Extreme
  urgency?: string | null;      // CAP urgency
  category?: string | null;     // CAP category / awareness type
  onset?: string | null;        // ISO 8601, may be in the future
  expires?: string | null;      // ISO 8601
  headline?: string | null;     headlineEn?: string | null;
  description?: string | null;  descriptionEn?: string | null;
  instruction?: string | null;  instructionEn?: string | null; // safety advice
  area?: string | null;         // e.g. "Kreis Freyung-Grafenau"
  source: string;               // "brightsky" (Germany) | "meteogate" (elsewhere)
}
```

## Rendering

- **Language**: the unsuffixed fields carry German where the issuer publishes it
  (otherwise English, otherwise its first language) and are always present; the
  `*En` fields are best-effort. For a German locale read the unsuffixed field;
  otherwise the `*En` field with the unsuffixed one as fallback.
- **Severity → MeteoAlarm colour**: `Minor` yellow (be aware), `Moderate` orange (be
  prepared), `Severe` red (be very alert), `Extreme` dark red (take action). When
  several collapse into one badge, show the most severe first, then by `onset`.
- Headline plus severity colour is the minimum; `description` and `instruction` go in
  the expanded view. Show "until {expires}", or "{onset}–{expires}" when the warning
  has not started. `area` says which region is affected.
- Where a source line is shown, credit by `source`: `brightsky` → Deutscher
  Wetterdienst (via Bright Sky); `meteogate` → the national weather service via
  MeteoAlarm.

## Coverage

**European parks only** (the ~40 MeteoAlarm countries); elsewhere `warnings: []`.
Synced every 15 minutes (`weather-warnings-cron`).
