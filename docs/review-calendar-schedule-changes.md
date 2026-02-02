# Review: Calendar & Schedule Sync Changes

Kurzes Review aller Änderungen aus der Session (Plausibilität & Vollständigkeit).

---

## 1. Schedule-Sync & Öffnungszeiten

| Änderung | Plausibel | Vollständig |
|----------|-----------|-------------|
| **Job-Name** `fetch-all-parks` → `sync-all-parks` in Scheduler + Admin | ✅ Processor lauscht nur auf `sync-all-parks`; Cron und Admin legen jetzt denselben Job. | ✅ Beide Stellen angepasst. |
| **Neuer Job** `sync-schedules-only` (täglich 15:00) | ✅ Nur Schedules, kein Full Discovery; entlastet und bringt neue Monate schneller. | ✅ Handler + Cron registriert. |
| **On-Demand** `sync-park-schedule` (einzelner Park) | ✅ Wird aus Calendar getriggert, wenn Range 14+ Tage über letztem Schedule endet; Rate-Limit 12h. | ✅ Handler + Trigger in CalendarService; UNKNOWN zählt als „Daten bis X“. |
| **Weniger aggressiv**: Gap 14 Tage, Rate-Limit 12h | ✅ Weniger API-Last, trotzdem zeitnahe Aktualisierung. | ✅ Konstanten + Doku angepasst. |

---

## 2. Calendar-Warmup

| Änderung | Plausibel | Vollständig |
|----------|-----------|-------------|
| Calendar **nicht** mehr im 5-Min-Park-Warmup | ✅ Calendar 1× täglich reicht; spart Last. | ✅ Aufruf aus `warmupParkCache` entfernt. |
| **Neuer Job** `warmup-calendar-daily` (täglich 5:00) | ✅ Nach Schedules-Sync, vor typischem Traffic. | ✅ Processor + Cron; `warmupCalendarForAllParks()` nutzt `warmupCalendarForPark()` pro Park. |
| Warmup baut Range „aktueller + nächster Monat“ | ✅ fromStr = 1. des Monats, toStr = letzter Tag nächster Monat (JS `new Date(y, m, 0)` für letzten Tag). | ✅ Zeitraum in Park-Timezone; Kommentar angepasst (nur noch von warmupCalendarForAllParks). |

---

## 3. Per-Monat-Cache (Calendar)

| Änderung | Plausibel | Vollständig |
|----------|-----------|-------------|
| **Nur** Per-Monat-Keys: `calendar:month:{parkId}:YYYY-MM:{includeHourly}` | ✅ Überlappende Ranges (z. B. Feb 1–15 und Feb 10–28) teilen denselben Monat. | ✅ Full-Range-Cache entfernt; Lesen nur noch aus Monats-Cache. |
| **Lesen**: Monate im Range holen, mergen, auf [from, to] slicen | ✅ `getMonthsInRange` timezone-sicher (formatInParkTimezone pro Tag). | ✅ Leerer Range (z. B. from > to) → `monthsInRange = []` → `[].every(...)` = true → leere Response; in Praxis durch Controller abgedeckt. |
| **Schreiben**: Nur **vollständige** Monate cachen | ✅ Prüfung: Länge = letzter Tag, erster Tag = YYYY-MM-01, letzter = YYYY-MM-lastDay. | ✅ TTL: 5 min wenn Monat „heute“ enthält, sonst 1h. |

---

## 4. Query-Optimierungen

| Änderung | Plausibel | Vollständig |
|----------|-----------|-------------|
| **Weather** `getWeatherData(..., timezone?)` | ✅ Calendar hat Park inkl. timezone; ein Park-Lookup pro Build gespart. | ✅ Nur Calendar übergibt timezone; Controller/andere Aufrufer unverändert (3 Argumente). |
| **QueueData** `innerJoin` statt `innerJoinAndSelect` | ✅ Calendar nutzt nur `timestamp` und `waitTime`, keine Attraction-Felder. | ✅ Kein Zugriff auf `qd.attraction` im Calendar-Code (geprüft). |
| **Crowd-Level** Redis-MGET für historische Tage | ✅ Ein MGET statt N GETs; Keys = `analytics:crowdlevel:park:{parkId}:{date}`. | ✅ Prefetch-Map wird an `buildCalendarDay` übergeben; bei Treffer kein `calculateCrowdLevelForDate`. |
| **Hourly ML** einmal pro Build, dann `buildHourlyPredictionsFromList` | ✅ Kein N+1 mehr (z. B. 2 ML-Calls → 1). | ✅ Nur bei `includeHourly !== "none"` geholt. |

---

## 5. Index

| Änderung | Plausibel | Vollständig |
|----------|-----------|-------------|
| **Kein** zusätzlicher Index `(parkId, date)` auf `schedule_entries` | ✅ `(parkId, date, scheduleType)` deckt Range-Abfragen per Leftmost-Prefix ab; doppelter Index entfernt. | ✅ Entity und Doku konsistent. |

---

## 6. Doku & Kommentare

| Stelle | Status |
|--------|--------|
| `docs/architecture/schedule-sync-and-calendar.md` | ✅ Sync, UNKNOWN, Calendar, Optimierungen, Warmup (inkl. Per-Monat-Key). |
| `docs/architecture/caching-strategy.md` | ✅ Warmup-Calendar als Per-Monat-Key beschrieben. |
| `docs/architecture/job-queues.md` | ✅ Park-Metadata + Schedule-Sync erwähnt. |
| `CLAUDE.md` | ✅ Link auf Schedule Sync & Calendar. |
| Cache-Warmup-Kommentar „Called from warmupParkCache“ | ✅ Auf „warmupCalendarForAllParks (daily warmup)“ geändert. |
| Schedule-sync doc „Effect: Warms calendar:…“ | ✅ Auf „per-month keys calendar:month:…“ korrigiert. |

---

## 7. Tests & Build

- **Build**: `npm run build` erfolgreich.
- **Calendar Unit-Test** (`test/unit/calendar.service.spec.ts`): Mockt nur Parks, Weather, ML, Holidays, Attractions, Shows, Redis; **nicht** QueueDataService, StatsService, AnalyticsService, park-metadata Queue. Beim Ausführen der Unit-Tests könnte `buildCalendarResponse` daher fehlschlagen, sobald diese Dependencies aufgerufen werden. Optional: fehlende Mocks ergänzen, falls die Calendar-Specs laufen sollen.

---

## 8. Edge Cases (stichprobenartig)

- **Leerer Monats-Range** (z. B. from > to): `getMonthsInRange` → `[]`, `monthCached.every(...)` = true → Response mit leeren `days`; in Praxis durch Controller-Validierung abgefangen.
- **Park ohne timezone**: Weather-Fallback (einfache Datums-Range) bzw. Calendar übergibt `park.timezone`; wenn undefined, wird in Warmup `tz = park.timezone || "UTC"` verwendet.
- **QueueData nach Join**: Calendar liest nur `timestamp` und `waitTime`; kein Zugriff auf `attraction` → `innerJoin` ohne Select unkritisch.

---

**Fazit**: Änderungen sind plausibel und konsistent umgesetzt; Doku und Kommentare angepasst. Einzige optionale Nachbesserung: Calendar-Unit-Test um fehlende Mocks erweitern, falls die Specs ausgeführt werden.
