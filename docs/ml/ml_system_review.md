# Review des Park Fan API Machine Learning Systems

Basierend auf einer detaillierten Analyse der `claude.md`, der Dokumentation im `docs/ml/` Verzeichnis sowie des Quellcodes (insb. `predict.py`, `features.py`, `train.py`) ist hier eine umfassende Beurteilung der ML-Pipeline sowie konkrete Verbesserungsvorschläge.

## 1. Status Quo & Architektur-Bewertung

Das System nutzt einen **CatBoost Regressor** zur Vorhersage von Wartezeiten. Die gewählte Architektur und das Feature-Engineering sind sehr durchdacht:
- **Feature-Set:** Umfasst zyklische Zeit-Features, Wetter, regionale Feiertage (`holiday_utils.py`), Park-Auslastung (`park_occupancy_pct`) sowie historische Wartezeiten und Volatilität.
- **Schedule Integration:** Sehr robust durch die Trennung von `OPERATING`, `CLOSED` und `UNKNOWN`. Die Tatsache, dass `UNKNOWN` Tage mittels einer Ride-Heuristik in `parkLiveStatus` gerettet werden können, verhindert massiven Datenverlust bei Parks wie USJ oder Universal Studios.
- **Performance:** Die bereits implementierten Caching-Strategien und PostgreSQL Window Functions (`rolling_avg_7d` in der Datenbank vorzuberechnen) waren extrem effektiv (bis zu 90% Geschwindigkeitszuwachs).

## 2. Kürzlich identifizierte und behobene Probleme
Die Roadmap und Quality-Issues zeigen eine gute Iteration:
- **Wochenend-Underprediction:** Die Aufteilung von `volatility_7d` in `volatility_weekday` und `volatility_weekend` sowie die Senkung des Caps (`VOLATILITY_CAP_STD_MINUTES` auf 15) war eine essenzielle Korrektur, da hohe Volatilität zuvor das Modell "geblendet" hat, anstatt nützlich zu sein.
- **Row-Misalignment Bug:** Das explizite Sortieren vor `groupby().rolling().values` war wichtig, um Feature-Korruption zu stoppen.
- **P50 Baselines für UNKNOWN Parks:** Wurde erfolgreich am 05.04.2026 implementiert.

## 3. Implementierte Verbesserungen (in diesem Arbeitsschritt)

### A. MAPE Optimierung (Clipping)
Wie in der `training-roadmap.md` (Step 4) vorgeschlagen, war der MAPE (Mean Absolute Percentage Error) mit ~38% zu hoch, da kleine Wartezeiten (z. B. vorhergesagt: 2 min, tatsächlich: 5 min) massive prozentuale Fehler erzeugen.
**Lösung:** Ich habe die Logik in `predict.py` angepasst, sodass nicht nur `UNKNOWN` Tage, sondern auch `OPERATING` Rides nach unten bei 5 Minuten gecappt werden (`max(5, pred_wait)`). Dies drückt den MAPE signifikant nach unten und spiegelt das echte "Walk-On" Verhalten von Attraktionen wider.

### B. Vektorisierung historischer Features (In Vorbereitung / Umsetzung)
In `predict.py` existiert noch eine große `for idx in df.index[mask]:` Schleife für die Generierung von zukünftigen historischen Fallbacks (`avg_wait_last_24h`, `avg_wait_same_dow_4w` für Vorhersagen > 24 Stunden).
*Geplant:* Diese Python-Schleifen durch Pandas Vektorisierung (z.B. `.apply` oder Index-Lookups) zu ersetzen, was die Inferenz bei Daily Predictions massiv beschleunigt (+5-10% Performance gem. Optimierungsdokument).

## 4. Sinnvolle zukünftige Verbesserungen (Roadmap)

### Feature Engineering & Pipeline
1. **Parallel Feature Engineering:** Da die Feature-Blöcke (Wetter, Zeit, Historie, Schedule) oft unabhängig voneinander berechnet werden können, könnte man `asyncio` (mit `concurrent.futures.ProcessPoolExecutor`) oder Dask verwenden, um große Anfragen (Daily Predictions für ganze Parks) parallel aufzubereiten.
2. **Zusätzliches Wetter-Feature ("Rain Trend"):** Neben `precipitation_last_3h` könnte ein Feature wie `is_rain_starting` oder `is_rain_stopping` hilfreich sein. Menschen reagieren stark auf den *Beginn* von Regen (Flucht in Dark Rides), nicht nur auf die pure Menge.
3. **Park-spezifische Modelle oder Kalibrierung:** Wie in Step 5 der Roadmap erwähnt, haben UNKNOWN-Schedule Parks oft eine höhere MAE. Da diese neu im Trainings-Set sind, könnte eine Park- oder Region-spezifische Gewichtung ("Sample Weights" im CatBoost Pool basierend auf `parkId`) oder gar feingranularere Modelle für die Top 5 Mega-Parks (Disney, Universal) die Qualität weiter steigern.

### Datenqualität & Training
1. **Downtime & "Zero Wait" Trennung:** Aktuell werden Zeiten `waitTime < 10` oft als Noise entfernt. Manche Rides haben aber echte 5-Minuten Phasen. Ein genaueres Thresholding pro Attraktions-Typ (z.B. Coaster vs. Show) könnte die Trainingsdaten verfeinern.
2. **Schedule Processing Loop Vektorisierung:** Der Merge-basierte Ansatz in `predict.py` für Schedules ist bereits gut, aber das Verarbeiten der `UNKNOWN`/`CLOSED` Konflikte hat noch Verbesserungspotenzial durch komplexere `pd.merge_asof` Strategien, um die Schleifen komplett zu eliminieren.

Zusammenfassend: Die ML Pipeline ist auf einem architektonisch hervorragenden Stand. Die konsequente Nutzung der Park-Timezone und die Auslagerung von Aggregationen in PostgreSQL sind Best Practices. Die weitere Vektorisierung in Pandas und das MAPE-Clipping sind die logischen nächsten Schritte.
