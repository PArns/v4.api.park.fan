# Daten-Rekalkulierung und Korrekturjobs

Dieses Dokument beschreibt die Prozesse zur nachträglichen Korrektur von aggregierten Daten (Tagesstatistiken und Baselines).

## 1. Tagesstatistiken (ParkDailyStats)

Die Tabelle `park_daily_stats` speichert tägliche Zusammenfassungen (P50, P90, Max Wartezeit). Diese Werte werden normalerweise stündlich und einmal nachts festgeschrieben. Wenn sich Berechnungslogiken ändern (z. B. Threshold-Anpassungen), müssen diese Werte neu berechnet werden.

### Manueller Backfill-Job

Es wurde ein Bull-Job `backfill-stats` im `stats` Prozessor implementiert.

**Ausführung via CLI:**
```bash
# Backfill für alle Parks für die letzten 30 Tage
pnpm run job:stats-backfill

# Backfill für einen spezifischen Park für das letzte Jahr (365 Tage)
pnpm run job:stats-backfill -- --days=365 --parkId=UUID

# Backfill ab einem bestimmten Startdatum
pnpm run job:stats-backfill -- --days=90 --startDate=2026-01-01
```

**Parameter:**
- `--days`: Anzahl der Tage, die zurückgegangen werden soll (Standard: 30).
- `--parkId`: Optionale UUID eines Parks. Ohne diesen Parameter werden alle Parks verarbeitet.
- `--startDate`: Optionales Startdatum im Format YYYY-MM-DD (Standard: heute).

## 2. P50 Baselines

Baselines bilden die Grundlage für die prozentuale Auslastungsberechnung. Sie basieren auf den Rohdaten (`queue_data`) der letzten 548 Tage.

### Heilungsprozess
Da Baselines jede Nacht um 3 Uhr nachts komplett neu auf Basis der Rohdaten berechnet werden, "heilen" sie sich bei Logikänderungen (im Code) automatisch innerhalb von 24 Stunden.

### Sofortige Neuberechnung
Um Baselines sofort nach einer Änderung zu aktualisieren, kann der Job `calculate-park-baselines` in der `p50-baseline` Queue manuell getriggert werden (z. B. via Bull Board).

## 3. Strategie bei Logikänderungen

Wenn globale Filter (wie `MIN_WAIT_TIME_THRESHOLD`) geändert werden, sollte folgendes Vorgehen gewählt werden:

1. Code anpassen (SQL-Queries und Konstanten).
2. `backfill-stats` Job für den gewünschten Zeitraum ausführen (z. B. 365 Tage), um die Historie im Kalender zu korrigieren.
3. Baselines neu berechnen lassen (automatisch nachts oder manuell triggern), damit Live-Werte und Baselines wieder zusammenpassen.

## 4. Cache-Invalidierung

Nach großen Logik-Anpassungen sollten die bestehenden Caches geleert werden, um die neuen Berechnungen sofort sichtbar zu machen.

### Manueller Cache-Clear
Es wurde ein Script erstellt, das alle relevanten Kalender- und Crowd-Level-Caches in Redis löscht.

**Ausführung:**
```bash
# Löscht alle calendar:month:*, analytics:crowdlevel:* und park:integrated:* Keys
pnpm run job:clear-calendar-cache
```

**Was wird gelöscht?**
- `calendar:month:*`: Die fertig zusammengebauten Monatsansichten.
- `analytics:crowdlevel:*`: Die berechneten Crowd-Level pro Tag.
- `park:integrated:*`: Die vollständigen Park-Antworten (inkl. Live-Daten).
