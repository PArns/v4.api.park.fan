# Validierung: Schedule-Sync-Änderungen & Multi-Source

**Datum:** 2026-02-11

## Schedule-Datenquellen (Überblick)

| Quelle | Job/Trigger | Typ | fillScheduleGaps |
|--------|-------------|-----|------------------|
| **ThemeParks Wiki** | sync-all-parks (03:00), sync-schedules-only (15:00), sync-park-schedule (on-demand) | OPERATING, CLOSED | Ja (nach saveScheduleData) |
| **Wartezeiten** | fetch-opening-times (06:00) | Nur OPERATING (wenn opened_today) | Nein |
| **Wait-times (Live)** | Beim Wait-Times-Sync | operatingHours: OPERATING, CLOSED, etc. | Nein |

**Reihenfolge/Konflikt:** Wiki überschreibt für Parks mit Wiki-Daten. Wartezeiten/Live werden übersprungen, wenn Wiki-Schedule für heute existiert (`getTodaySchedule`).

## Änderungen im Detail

### 1. saveScheduleData: Bidirektionale Bereinigung

**Verhalten:** Beim Speichern von CLOSED löschen wir OPERATING für dasselbe Datum (und umgekehrt).

**Multi-Source-Validierung:**
- **ThemeParks:** Kann OPERATING und CLOSED liefern. Bereinigung korrekt.
- **Wartezeiten:** Liefert nur OPERATING. Keine CLOSED-Bereinigung nötig. OPERATING überschreibt ggf. altes CLOSED (korrekt).
- **Wait-times Live:** Kann `window.type` = OPERATING/CLOSED übernehmen. Bereinigung greift.

**Kein Konflikt:** Alle Quellen nutzen dieselbe `saveScheduleData`-Logik. Pro Datum bleibt eine klare Aussage (OPERATING oder CLOSED).

### 2. getScheduleExtended: Vorheriger Monat

**Verhalten:** Zusätzlich zum aktuellen Monat wird der Vormonat mit abgefragt (`i = -1`).

**Multi-Source-Validierung:** Betrifft nur **ThemeParks**. Wartezeiten/Live nutzen diesen Client nicht. Kein Einfluss auf andere Quellen.

### 3. fillScheduleGaps: CLOSED → UNKNOWN Demotion

**Verhalten:** Gap-fill-CLOSED (mit `description = "Gap-filled"`) wird zu UNKNOWN, wenn das Datum **nach** dem letzten OPERATING liegt.

**Multi-Source-Validierung:**
- fillScheduleGaps läuft nur nach ThemeParks-Sync (park-metadata.processor).
- min/max OPERATING kommen aus **allen** Einträgen (inkl. Wartezeiten/Live).
- **Fix (2026-02-11):** Nur Einträge mit `description = "Gap-filled"` werden demotet. API-CLOSED (z.B. Winter von ThemeParks) bleibt unverändert, auch wenn `openingTime`/`closingTime` null sind.

### 4. ML Service: Kein Inference für CLOSED-Tage

**Verhalten:** Zeilen mit `status = CLOSED` oder `UNKNOWN` werden vor dem Modell-Inference aus dem DataFrame entfernt.

**Multi-Source-Validierung:** Der ML-Service liest nur aus `schedule_entries`. Die Herkunft (Wiki, Wartezeiten, Live) spielt keine Rolle. Die Entscheidung basiert ausschließlich auf `status` pro Datum.

## Edge Cases (geprüft)

| Szenario | Ergebnis |
|----------|----------|
| Wiki sagt Jan CLOSED, Wartezeiten hatte zuvor OPERATING für Jan | Wiki-CLOSED gewinnt, OPERATING wird gelöscht. |
| Wartezeiten-only Park: Nur OPERATING für heute | Kein fillScheduleGaps. Andere Tage ohne Eintrag → UNKNOWN (buildCalendarDay). |
| Winter: Dec OPERATING, Jan CLOSED (API), Feb OPERATING | maxOp = Feb. Jan bleibt CLOSED (nicht demotet, da API-Eintrag). |
| Winter: Dec OPERATING, Jan Gap-CLOSED, Feb OPERATING → später Feb entfernt | Jan wird zu UNKNOWN demotet (`description = "Gap-filled"`, Jan > maxOp). |
| Live operatingHours mit CLOSED für heute | saveScheduleData speichert CLOSED, löscht OPERATING. |

## Fazit

Alle Änderungen sind mit den mehreren Schedule-Quellen konsistent. Der Fix für die Demotion (nur `description = "Gap-filled"`) verhindert, dass explizite API-CLOSED-Einträge fälschlich zu UNKNOWN umgesetzt werden.
