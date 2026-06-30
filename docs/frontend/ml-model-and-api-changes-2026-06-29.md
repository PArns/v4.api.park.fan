# ML-Modelle & API-Änderungen — Frontend-Guide (2026-06-29)

> Was das Frontend von den gestrigen Modell- und API-Änderungen wissen muss.
> Kurzfassung: **Es gibt keine Breaking Changes an den Endpunkten, die das
> Frontend konsumiert.** Wir haben neue Vorhersagemodelle eingebaut, die hinter
> den bestehenden Endpunkten arbeiten — gleiche Response-Shapes, bessere Zahlen.
> Die einzigen *neuen* Endpunkte sind interne Admin-/Ops-Endpunkte.

Hintergrund-Architektur: [serving-and-shadows-status.md](../ml/serving-and-shadows-status.md).

---

## TL;DR für Frontend-Entwickler:innen

1. **Keine Vertrags-Änderung.** `crowdLevel`, `predictedWaitTime`, `avgWaitTime`,
   `confidencePercentage`, `source: "ml"` — alle Felder bleiben identisch. Kein
   Code-Change im Frontend nötig.
2. **Die Zahlen werden genauer.** Intraday-Wartezeiten und Crowd-Level können
   sich gegenüber vorher verschieben, weil ein besseres Modell (PCN) sie liefert.
   Keine neuen Werte/Enums — nur andere (bessere) Zahlen im selben Wertebereich.
3. **Das Frontend kann nicht erkennen, welches Modell** eine Vorhersage erzeugt
   hat. `source` ist weiterhin konstant `"ml"`. Es gibt kein `modelType`-Feld auf
   den öffentlichen Vorhersage-Responses (bewusst — die Modellwahl pro Horizont
   ist serverseitig und kann sich ändern).
4. **Neue Endpunkte sind Admin-only** (`/v1/admin/…`) — relevant nur für ein
   internes ML-/Ops-Dashboard, nicht für die öffentliche App.

---

## Was sich geändert hat: ein Modell pro Vorhersage-Horizont

Wir bedienen jetzt unterschiedliche Modelle je nach **Zeithorizont** der
Vorhersage. Das passiert komplett serverseitig und ist für das Frontend
transparent:

| Horizont | Was das Frontend rendert | Aktiv serviert von |
|---|---|---|
| **Intraday 0–24 h** (15-Min-/Stunden-Slots) | Stündliche `predictedWaitTime` + `crowdLevel` | **PCN** (neu, hinter Flag) → sonst CatBoost |
| **Daily ≤ ~60 Tage** | Tages-`crowdLevel`, `avgWaitTime` | TFT (unverändert) |
| **Far-daily 61–365 Tage** | Jahres-Kalender `crowdLevel` | CatBoost (unverändert) |

Nur der **Intraday-Pfad** hat sich gestern geändert (PCN). Daily/Far-daily
bleiben wie gehabt.

### PCN — der neue Intraday-Nowcaster

- **Was:** Ein neues Modell („Park-Crowd Nowcaster", graphbasiert) ersetzt die
  CatBoost-Stundenvorhersage für den Intraday-Bereich. Es schlägt CatBoost in
  jedem getesteten Segment (z. B. „busy"-Fehler 19.2 statt 25.0 Minuten).
- **Wo es sichtbar wird:** überall, wo intraday-Wartezeiten/Crowd-Level über die
  bestehenden Endpunkte kommen — die Werte werden ersetzt und das `crowdLevel`
  aus der neuen Wartezeit neu berechnet.
- **Feature-Flag:** Der Swap hängt an `SERVE_PCN_INTRADAY` (Server-Env,
  **default OFF**). Solange OFF, sieht das Frontend exakt die alten
  CatBoost-Werte. Wird das Flag eingeschaltet, ändern sich nur die Zahlen — **kein
  Deploy oder Code-Change im Frontend nötig**.
- **Fallback:** Wo PCN (noch) keine Vorhersage hat, bleibt CatBoost die Quelle.
  Das Frontend bekommt also immer einen vollständigen Datensatz, nie Lücken.

### Shape — Tageskurven-Modell (noch nicht serviert)

Ein zweites neues Modell („Shape", Level×Shape-Tageskurve) läuft aktuell **nur im
Shadow-Modus** und liefert noch **nichts ans Frontend**. Hier ist nichts zu tun;
nur als Kontext erwähnt, falls es in Boards/Docs auftaucht.

---

## Betroffene (unveränderte) Frontend-Endpunkte

Diese Endpunkte liefern ML-Vorhersagen und behalten ihre Shape. Nur die *Werte*
können sich (bei aktivem PCN-Flag) im Intraday-Bereich verbessern:

| Endpunkt | ML-Felder |
|---|---|
| `GET /v1/parks/:continent/:country/:city/:parkSlug/calendar` | Stündliche `HourlyPrediction` (`crowdLevel`, `predictedWaitTime`), Tages-`crowdLevel` |
| `GET …/:parkSlug/predictions/yearly` | `ParkDailyPredictionDto[]` (`crowdLevel`, `avgWaitTime`, `confidencePercentage`, `source`, `recommendation`) |
| `GET …/:parkSlug/wait-times` | Live + vorhergesagte Wartezeiten |
| `GET …/:parkSlug` / `…/attractions` | Eingebettetes `crowdLevel` |

**Wertebereiche (unverändert):**

- `crowdLevel`: `very_low` · `low` · `moderate` · `high` · `very_high` ·
  `extreme` · `closed`
- `predictedWaitTime` / `avgWaitTime`: ganze Minuten
- `confidencePercentage`: 0–100
- `source`: konstant `"ml"`

---

## Neue Endpunkte (Admin / nur internes Dashboard)

Diese sind **nicht** für die öffentliche App gedacht — sie liegen unter
`/v1/admin/` und dienen einem internen Modell-Vergleichs-/Ops-Dashboard:

### `GET /v1/admin/ml-comparison`

Modell-Vergleichs-Board mit drei Sektionen + Verdicts:

- `daily` — TFT vs CatBoost (reif)
- `intraday` — PCN vs CatBoost (reif, PCN gewinnt)
- `shape` — Shape vs CatBoost (reift noch)

Nutzbar, wenn ein internes Admin-UI gebaut werden soll, das zeigt, welches Modell
pro Horizont gewinnt. **Nicht** in der Endnutzer-App verwenden.

### `POST /v1/admin/pcn/:action` und `POST /v1/admin/shape/:action`

Manuelle Trigger für die Shadow-Jobs (`train` | `forecast` | `score`). Reine
Ops-Endpunkte, kein Frontend-Bezug.

---

## Was das Frontend tun sollte

- **Nichts ändern müssen** — die bestehende Integration funktioniert weiter.
- **Keine Annahmen über die Modellquelle** in der UI hart kodieren (z. B. nicht
  „CatBoost"/„PCN" anzeigen) — die Modellwahl ist serverseitig und kann wechseln.
- Falls eine UI die intraday-Wartezeiten cached: damit rechnen, dass sich Werte
  beim Aktivieren des PCN-Flags einmalig spürbar verschieben können (kein Bug,
  sondern das bessere Modell).
- Wenn ein **internes Admin-Dashboard** gewünscht ist, das die Modell-Performance
  zeigt → `GET /v1/admin/ml-comparison` ist die Datenquelle.

---

## Referenzen

- Serving-/Shadow-Architektur: [docs/ml/serving-and-shadows-status.md](../ml/serving-and-shadows-status.md)
- Modell-Übersicht (CatBoost): [docs/ml/model-overview.md](../ml/model-overview.md)
- Quantil-Serving (welche Quantile welche Nutzerzahl wird): [docs/ml/quantile-serving-and-calibration.md](../ml/quantile-serving-and-calibration.md)
- PCN-Modell-Design: [docs/ml/custom-intraday-model-design.md](../ml/custom-intraday-model-design.md)
- Shape-Modell-Design: [docs/ml/shape-model-design.md](../ml/shape-model-design.md)
</content>
</invoke>
