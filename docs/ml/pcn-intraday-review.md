# PCN-Intraday-Review — Befunde & Verbesserungsvorschläge (2026-07-02)

> Status: **Review + Umsetzung**. Die Punkte 1–5 aus §7 sind im selben PR umgesetzt
> (siehe §8 Umsetzungsstand); §7 Punkt 6–8 sind vorbereitet (Bake-off-Knobs, Kanal-
> Evolutions-Contract), aber bewusst NICHT geflippt — der Bake-off entscheidet.
> Scope: das neue Intraday-Modell (pcn-service + Champion-Swap + Shadow-Boards),
> plus allgemeine ML-System-Empfehlungen. Grundlage: vollständiger Code-Read
> (pcn-service, Serving-Pfad `ml.service.ts`, Scorer, NestJS-Wiring), die Live-Boards
> vom 2026-07-02 (PCN vs CatBoost, Shape vs CatBoost, Accuracy/Drift, TFT-Daily)
> und die Design-Docs ([custom-intraday-model-design.md](./custom-intraday-model-design.md),
> [serving-and-shadows-status.md](./serving-and-shadows-status.md)).

---

## 0. TL;DR

PCN ist ein sauber gebautes System und der Live-Win gegen CatBoost ist plausibel echt
(alle Segmente grün, busy Δ+6.5). Aber:

1. **P0 — zwei Korrektheits-Lücken im Serving:** (a) Der Champion-Swap rechnet das
   **Crowd-Level aus PCNs q0.5** statt q0.8 — ein stiller Bruch der dokumentierten
   Quantil-Semantik (Crowd-Signal wird systematisch zu niedrig, genau auf busy).
   (b) Park-Kurve, Kalender und Deviation-Badges laufen **am PCN-Override vorbei**
   (CatBoost), während die Attraction-Cards PCN zeigen — zwei Modelle im selben View.
2. **P0 — die Board-Evidenz ist verzerrt:** Der Scorer überschreibt gereifte Tage mit
   immer kleineren Rolling-Window-Ausschnitten. Gereifte Daten enthalten am Ende nur
   noch die **letzte ~Stunde jedes Tages**, und Zellen desselben Tages stammen aus
   verschiedenen Läufen — im Board sichtbar: Lead-Buckets summieren sich auf **mehr**
   als die „all"-Zeile (busy: 1 515+38+6=1 559 ≠ 1 453). Gleicher Bug im Shape-Scorer.
   Der +6.5-busy-Verdikt steht damit auf einem kleinen, abend-lastigen Sample.
3. **P1 — Betrieb:** `pcn_forecasts` wächst **unbegrenzt** (~10⁷ Zeilen/Tag, kein
   Retention-Job, Serving liest die Tabelle pro Request); der Forecast-Tick baut alle
   15 Min **548 Tage** Panel pro Park neu, obwohl Inferenz nur ~2 Tage Kontext braucht.
4. **P2 — der größte Qualitäts-Hebel:** Das servierte GraphWaveNet hat mit
   `layers=2, kernel=2` ein **Receptive Field von 4 Slots = 1 Stunde**. Es bekommt
   192 Slots Kontext, kann aber architektonisch nur die letzte Stunde sehen —
   kein Tagesverlauf, kein Gestern. `layers=8` → RF 256 Slots (>L) bei weiterhin
   parallel-in-time Training. Dazu fehlen die im Design-Doc geplanten Kanäle
   (DOW, Holiday, Schedule, Wetter) komplett im Tensor.

Empfohlene Reihenfolge: §2 (Serving-Fixes) → §3 (Scorer-Fix, dann Board 1–2 Wochen
sauber reifen lassen) → §4 (Retention + kurzes Inferenz-Fenster) → §5 (RF/Features
als Bake-off) → §6 (allgemeines ML).

---

## 1. Was gut ist (und so bleiben soll)

- **Masken-Disziplin:** `loss_mask = obs_mask × park_open` (`tensor.py`) verhindert
  exakt den Spurious-Zero-Fehler des 2026-05-23-PoC; im Loss (`torch_model.py`)
  konsequent durchgezogen.
- **Going-Forward-Shadow** mit immutable-per-origin `pcn_forecasts` — die einzig
  saubere Intraday-Vergleichsbasis (CatBoosts Dedup zerstört Vergangenheit), korrekt
  umgesetzt.
- **Ablation-Kontrolle:** `localgru` als No-Graph-Kontrolle, Persistence +
  yesterday-same-slot als Pflicht-Baselines, gematchte Population im Scorer
  (INNER-Join, gleiche n) — methodisch vorbildlich.
- **Betriebs-Härtung:** Flag + CatBoost-Fallback, 3h-Staleness-Guard (25e6aa5),
  Lock-Self-Heal (86dd17f), 36h-Stale-Park-Guard, Ride-Set-Guard vor Inferenz.
- **Train-Harness-Neutralität:** alle Kandidaten teilen `TorchSeqModel` — ein Win ist
  ein Architektur-Win, kein Loop-Artefakt.

---

## 2. P0 — Serving-Korrektheit (Champion-Swap)

### 2a. Crowd-Level aus q0.5 statt q0.8 (Semantik-Regression)

[`quantile-serving-and-calibration.md`](./quantile-serving-and-calibration.md) legt fest:
**q0.5 = angezeigte Wartezeit, q0.8 = Crowd-Signal** (CatBoost schreibt `crowdLevel`
beim Erzeugen aus q0.8, `ml-service/predict.py` `SERVING_CROWD_QUANTILE=0.8`).

Der PCN-Override (`src/ml/ml.service.ts:1385` `applyPcnIntradayOverride`) ersetzt die
Wartezeit durch PCN-q0.5 (korrekt) — **rechnet aber `crowdLevel` aus demselben q0.5
neu**. Damit ist das Crowd-Level auf allen PCN-Slots vom Median statt vom oberen
Quantil abgeleitet → systematisch niedrigere Crowd-Levels, am stärksten dort, wo die
Verteilung am schiefsten ist (busy). Der busy-Bias von PCN-q0.5 ist −10 min
(Board ≤3h) — genau die Lücke, die q0.8 als Crowd-Signal abfangen soll.

**Fix:** `getPcnIntradayWaits` (`ml.service.ts:1346`) zusätzlich `quantile = 0.8`
holen (steht bereits in `pcn_forecasts`, wird heute von **niemandem** gelesen) und
`determineCrowdLevel(q0.8/baseline)` rechnen; q0.5 bleibt Display.

**Dazu:** PCN erzwingt keine Quantil-Monotonie — `masked_pinball` trainiert
q0.5/q0.8/q0.9 unabhängig, `predict_quantiles` (`torch_model.py:189`) gibt sie roh
aus. CatBoost hatte dafür den dokumentierten Non-Crossing-Fix
(`np.maximum.accumulate`). Beim Verdrahten von q0.8 denselben Ein-Zeilen-Fix in
`predict_quantiles` einbauen, sonst kann das Crowd-Signal unter die angezeigte
Wartezeit fallen.

### 2b. Zwei Modelle im selben View (Override-Bypass)

Der Override greift nur in `getStoredPredictions` / `getBatchStoredPredictions` /
`getAttractionPredictionsWithFallback`. Daran vorbei laufen:

| Konsument | Pfad | Modell heute |
|---|---|---|
| Park-Detail „heute"-Kurve | `park-integration.service.ts:198` → `getParkPredictions(…, "hourly")` → ml-service HTTP + Redis-Cache | **CatBoost live** |
| Kalender (heutige Stunden) | `calendar.service.ts:223` → `getParkPredictions` | **CatBoost live** |
| Attraction-Cards im selben Park-Response | `park-integration.service.ts:516` → `getBatchStoredPredictions` | **PCN** |
| Deviation-Badges | `prediction-deviation.service.ts` liest `wait_time_predictions` direkt | **CatBoost stored** |
| Accuracy-Tracking (`prediction_accuracy`, Admin „Live MAE") | `prediction-accuracy.service` | **CatBoost stored** |

Konsequenzen: dieselbe 15-Min-Slot-Zahl kann auf einer Seite zweimal verschieden
erscheinen; Deviation-Badges bewerten eine Zahl, die der Nutzer nie sieht; das
Admin-„Live MAE 8.70" misst nicht mehr das servierte Produkt (siehe §6a).

**Fix-Optionen** (aufsteigender Aufwand): (1) Override auch in `getParkPredictions`
nach dem ml-service-Call anwenden (Achtung: Ergebnis wird per Redis bis Tagesende
gecacht → Override **vor** dem Cache-Write oder TTL kürzen); (2) Deviation-Service
auf die Serving-Sicht (inkl. Override) umstellen; (3) mittelfristig: einen einzigen
„served predictions"-Leseweg definieren, den alle Konsumenten teilen.

---

## 3. P0 — Shadow-Scorer: Rolling-Window zerstört gereifte Tage

`score.py` läuft stündlich mit `lookback_hours=24` und upsertet pro
`(target_date, model, segment, lead_bucket)`. Das Fenster ist **rollierend**:

- Lauf um 00:30 lokal: gestern fast vollständig im Fenster → Zeile ≈ ganzer Tag ✓
- Lauf um 12:30: gestern nur noch 12:30–24:00 → **Overwrite mit halbem Tag**
- Lauf um 21:30: gestern nur noch 21:30–24:00 → Overwrite mit ~1 h
- Zellen mit n=0 werden übersprungen (`aggregate_comparison`) → **stale Zellen aus
  früheren Läufen bleiben stehen**, während „all" weiter schrumpft.

Endzustand pro gereiftem Tag: jede Zelle spiegelt nur den letzten Lauf, in dem sie
n>0 hatte — für busy ≈ die **letzte Stunde des Busy-Fensters**; verschiedene Zellen
desselben Tages stammen aus verschiedenen Fenstern. Genau das zeigt das Live-Board:
Summe der Lead-Buckets > „all" in **jedem** Segment (busy 1 559 vs 1 453, all 27 814
vs 26 232, mid 5 457 vs 5 094, quiet 20 807 vs 19 693). Und ~1.9k gematchte
Slots/Tag über die ganze Flotte sind ein Bruchteil der tatsächlich gematchten
Population — >90 % der Evaluations-Daten gehen beim Überschreiben verloren.

**Folge:** Der Board-Verdikt (busy +6.5) beruht auf einem kleinen, abend-lastigen,
in sich inkonsistenten Sample. Vermutlich bleibt der Win real (der Effekt trifft
beide Modelle gleich, die Population ist weiter gematcht) — aber die Gate-Evidenz
soll das nicht „vermutlich" nötig haben.

**Fix (klein, chirurgisch):** `lookback_hours=48` + pro Park nur `target_date ∈
{gestern, heute}` (park-lokal) schreiben — dann ist jeder Write eine Obermenge des
vorherigen (heute wächst monoton, gestern wird einmal final mit vollem Tag
überschrieben), Zellen bleiben konsistent. Alternativ: nur vollständig vom Fenster
abgedeckte Tage upserten. Danach Board-Historie einmal invalidieren (die alten
Zeilen sind nicht reparierbar) und **1–2 Wochen sauber reifen lassen**, bevor der
Swap-Win als bestätigt gilt.

**Gleicher Bug in `shape-service/score.py`** (96h-Lookback, täglicher Score):
gereifte Tage verlieren dort den Morgen (Fenster-Start ~10:00 UTC). Fix mitziehen —
das Shape-Board (Shape verliert überall, busy −3.9) ist damit ebenfalls teilweise
verzerrt und sollte erst nach dem Fix bewertet werden (§6b).

**Zweiter Eval-Befund — Lead-Buckets sind quasi leer by design:**
`fetch_pcn_forecasts_window` (`db.py:229`) nimmt pro Target-Slot nur den **freshest
origin** → unter gesundem 15-Min-Betrieb ist Lead ≈ 15–30 Min. Die 3-6h/>6h-Buckets
(N=38/6!) füllen sich nur aus Ausfällen/Stale-Phasen — eine verzerrte Population.
Gleichzeitig **serviert** die UI längere Leads (Rest-des-Tages aus dem neuesten
Origin, bis 12 h) — deren Qualität misst das Board also gar nicht. Der gespeicherte
Fan (48 Leads × alle Origins) enthält alles Nötige: pro Target zusätzlich die
Origins bei Lead 1h/3h/6h joinen und PCN@Lead vs Ist (+ Persistence-Baseline)
scoren. Ein CatBoost-Head-to-Head bei langen Leads bräuchte den im Design-Doc
§12.3 spezifizierten, aber **nicht implementierten** CatBoost-Co-Snapshot —
optional nachrüsten, wenn der Lead-Vergleich gegen CatBoost gewünscht ist.

Kleinigkeit: `system-health.service.ts:458` `LIMIT 400` schneidet bei 14 Tagen ×
bis zu 32 Zeilen/Tag (=448) den ältesten Tag an → Verdikt-Gewichtung minimal
verzerrt; Limit auf ≥ `days*32` heben.

---

## 4. P1 — Betrieb & Effizienz

### 4a. `pcn_forecasts` wächst unbegrenzt; Serving liest sie pro Request

Pro Forecast-Lauf: Rides × H(48) × 2 Quantile; bei ~2.5k aktiven Rides und ~56
origin-fortschreitenden Läufen/Tag ⇒ **Größenordnung 10⁷ Zeilen/Tag** (zweistellige
GB/Woche inkl. Index). Es gibt **keinen** Retention-/Prune-Job (andere Tabellen
haben Cleanups; `shape_forecasts` betrifft dasselbe). Gleichzeitig läuft
`getPcnIntradayWaits` bei jedem Prediction-Read gegen diese Tabelle, mit einem
Filter (`target_slot AT TIME ZONE p.timezone >= …`), den der PK-Index nicht
abdecken kann — die Query degradiert linear mit dem Tabellenwachstum.

**Fix-Paket:**
1. **Retention-Job** (nightly): `DELETE WHERE origin_slot < now()-'14 days'` —
   oder Hypertable + `add_retention_policy` (TimescaleDB ist ohnehin da). Vorher
   entscheiden, ob für die Lead-Kurve (§3) ältere Origins ausgedünnt (z. B. nur
   :00-Origins) statt gelöscht werden sollen.
2. **Index fürs Serving:** `(created_at)` oder `(attraction_id, created_at DESC)`
   — der 3h-Staleness-Filter macht die Treffermenge klein, der Index macht sie
   auch *findbar* klein. Alternativ ein kompakter Serving-Pfad: der Producer
   schreibt den neuesten Fan zusätzlich in eine kleine `pcn_serving`-Tabelle
   (ein Fan pro Attraction, replace-per-run) — Reads berühren das Archiv nie.
3. `write_pcn_forecasts` batcht als executemany über `text()`-Upserts — bei
   ~300k Zeilen/Lauf lohnt `execute_values`/COPY (Messung zuerst).

### 4b. Forecast-Tick baut 548 Tage Tensor pro Park — alle 15 Minuten

`forecast_park` → `pipeline.build_park_tensor` → `fetch_cross_ride_panel` mit
`PCN_WINDOW_DAYS=548` (`config.py:33`). Für **jeden** Park, **jeden** Tick: eine
Percentile-Aggregation über die volle Historie + Tensor-Assembly über ~20–50k Slots
(hunderte MB transient). Die Inferenz braucht aber nur `L=192` Slots (2 Tage)
Kontext — der Scale-Faktor kommt aus dem Checkpoint, nicht aus den Daten.

**Fix:** eigenes Inferenz-Fenster (z. B. `PCN_FORECAST_WINDOW_DAYS=4`) für den
Forecast-Pfad; Training behält 548. Reduziert DB-Last und RAM des Ticks um
~2 Größenordnungen. Dazu zwei Mikro-Optimierungen: (a) den Staleness-Skip **vor**
den Panel-Fetch ziehen (billige `MAX(timestamp)`-Query statt Voll-Fetch — heute
wird der 548-Tage-Fetch auch für stale Parks bezahlt, `forecast.py:51-64`);
(b) optional: wenn der Origin-Slot unverändert ist (Park zu, nachts), Re-Inferenz
überspringen und nur `created_at` der bestehenden Origin-Zeilen bumpen (hält den
3h-Guard zufrieden, spart die nächtlichen No-Op-Inferenz-Zyklen).

### 4c. Sichtbarkeit von Silent-Fallbacks

Ride-Set-Änderung (`forecast.py:72`) oder Staleness schalten PCN **park-weise
still** auf CatBoost-Fallback (bis zum 08:30-Retrain am Folgetag). Das ist korrekt
defensiv — aber unsichtbar. `/health` bzw. system-health um „parks forecasted /
skipped (reason)" erweitern, sonst fällt PCN-Coverage-Erosion erst im Board auf.
Mittelfristig: Subset-tolerantes Serving (Tensor auf das trainierte Ride-Set
reindizieren; neue Rides fallen back, statt den ganzen Park zu deaktivieren).

---

## 5. P2 — Modell-Qualität: die zwei strukturellen Hebel

### 5a. Receptive Field: das Modell sieht 1 Stunde, nicht 2 Tage

Das servierte GraphWaveNet (`backbones.py:49`, `layers=2, kernel=2`, Dilations
[1,2]) hat RF = 1+1+2 = **4 Slots = 1 h**; der Head liest nur den letzten Zeitschritt
(`[..., -1]`). Der 192-Slot-Kontext (2 Tage, extra auf Tages-Saisonalität
dimensioniert, `config.py:52-55`) ist zu ~98 % unerreichbar: kein Morgen-Ramp,
kein Mittagspeak-Kontext, kein Gestern. Dass PCN CatBoost (mit 24h/7d-Features)
trotzdem schlägt, zeigt wie stark Live-Zustand + Graph-Kopplung sind — und wieviel
Headroom hier liegen dürfte. Der busy-Bias −10 bei ≤3h passt zur Diagnose
(persistenz-nahe Extrapolation ohne Tagesform-Wissen).

| layers | Dilations | RF | Abdeckung |
|---|---|---|---|
| 2 (heute) | 1,2 | 4 Slots | 1 h |
| 4 | 1..8 | 16 | 4 h |
| 6 | 1..32 | 64 | 16 h |
| **8** | 1..128 | **256** | 64 h > L=192 ✓ |

**Empfehlung:** `layers=8` (Compute wächst ~linear, bleibt parallel-in-time und
damit weit unter gpstgnn-Kosten) als Bake-off-Lauf gegen die heutige Config +
`gpstgnn` (das als RNN den vollen Kontext nutzt — der Speed-Swap 37ae4ef hat den
RF-Kollaps mitgekauft, ohne dass es der Bake-off je isoliert gemessen hat) +
`localgru`. Falls 8 Layer auf kleinen Parks überfitten: 6 Layer (16 h) ist der
sinnvolle Mittelweg — deckt den ganzen Betriebstag ab.

### 5b. Feature-Kanäle: die geplanten Known-Future-Signale fehlen komplett

`tensor.CHANNELS` = wait_ffill, obs_mask, down, slot/hour sin·cos, park_occ.
Nicht drin (alle im Design-Doc §4 Baustein 3 vorgesehen): **Day-of-Week**,
**Holiday/Schulferien**, **Schedule** (Minuten seit Öffnung/bis Schließung),
**Wetter**, statische Ride-Busyness. Konsequenz: Das Modell kann Freitag→Samstag
nur aus dem 2-Tage-Fenster raten, Feiertagsspitzen gar nicht antizipieren — und
mit RF=1h (§5a) nicht einmal das.

Priorisiert nach Aufwand/Nutzen:
1. **`dow_sin/cos` (+ `is_weekend`)** — reine Zeitachsen-Features, 5 Zeilen in
   `_slot_time_features`, kein neuer Fetch. Zuerst.
2. **`is_holiday` / `is_school_break`** — `holiday_utils`-Logik aus ml-service
   spiegeln (per Land/Region des Parks, `python-holidays`); als broadcast-Kanal.
3. **Schedule-relative Zeit** („Minuten seit Open/bis Close" aus
   `schedule_entries`) — erklärt Open-Ramp/Close-Taper besser als Uhrzeit,
   normalisiert über Parks mit verschiedenen Öffnungszeiten.
4. **Wetter** (Ist + Forecast als Future-Kanal) — die Worst-MAE-Liste
   (Cheetah Hunt 68, Wolfpack Raft Slide 58, Manta 51 — Wasser-/Outdoor-Rides)
   ist ein deutlicher Hinweis, dass hier ein eigener Fehler-Cluster liegt.

Jeder Kanal einzeln durch den Bake-off (bestehende Disziplin), Ziel-Metrik busy-MAE/Bias.

### 5c. Kleinere Modell-/Trainings-Punkte

- **Kein Validation-Split / Early-Stopping:** fix 500 Steps für jeden Park
  (10 Rides oder 100). Letzten Tag als Val halten, early-stop, final loss je Park
  loggen — macht degenerierte Park-Modelle sichtbar, bevor sie servieren.
- **Ein Scale pro Park** (`_scale` = Median-|wait|): Headliner (80 min) und
  Walk-on (5 min) teilen eine Skala; Node-Embeddings müssen Level absorbieren.
  Per-Ride-Normalisierung als Ablation (Achtung: ändert die implizite
  Loss-Gewichtung von Minuten auf relative Fehler — bewusst entscheiden).
- **Regime-Kopf (P(busy)) aus Design-Doc §4.4** ist nicht gebaut; Tweedie ist
  scaffolded, aber nie im Board angetreten. Nach §5a/b als dritter Hebel testen —
  Reihenfolge so, weil fehlendes Signal (Features/RF) nicht per Kopf reparierbar ist.
- Testbarkeits-Nit: `pool_scores`-Tests scheitern ohne torch, weil
  `run_bakeoff` → `backbones` torch beim Import zieht — `pool_scores` nach
  `metrics.py` schieben oder torch-Import in die Factories verlagern.
- `fetch_actuals_local` hardcodet `waitTime >= 5` statt `PCN_MIN_WAIT` (db.py:264).

---

## 6. P3 — Allgemeines ML-System

### 6a. KPIs müssen dem servierten Modell folgen

„Accuracy (Training vs Live)" (Live MAE 8.70, R² 0.52) und die Drift-Warnung
(24.58/20) messen **CatBoost-stored** — intraday serviert aber PCN. Das Haupt-KPI
misst also das Fallback. Vorschlag: (a) `prediction_accuracy`-Pipeline auf die
Serving-Sicht (inkl. PCN-Override) umstellen oder ein zweites „served"-Panel;
(b) Drift-Monitor nach Horizont splitten — CatBoost-Drift ist jetzt primär ein
Far-Daily-Problem (61–365 d), dort aber weiterhin relevant, weil CatBoost dort
alleiniger Level-Lieferant bleibt.

### 6b. Shape: Live widerspricht Offline — erst reconcilen, nichts flippen

Offline-Claim: smoothing+additive −7.4 % busy vs crowd-Baseline; Live-Board:
Shape verliert **überall** (busy −3.9, all −1.6, bias busy −20.1). Drei Kandidaten,
in dieser Reihenfolge prüfen: (1) Scorer-Bug §3 (Morgen fehlt in gereiften Tagen)
— erst nach Fix neu lesen; (2) das **Level**, auf das Shape rendert, unterschätzt
busy-Tage (bias −20 riecht nach Level-, nicht Kurvenfehler — Shape kann nur so gut
sein wie sein Level-Input); (3) Offline-Population ≠ Live-Population (matched
INNER-Join vs Backtest-Maske). Bis dahin: kein Producer-Swap auf `learned.py`.

### 6c. TFT-Daily & Roadmap-Hygiene

TFT schlägt CatBoost daily weiterhin deutlich (Board 07-01: busy 19.3 vs 30.4,
hdlnr 13.3 vs 18.6) — der Split (TFT ≤60 d, CatBoost 61–365) bleibt richtig.
Die dokumentierte Kadenz einhalten: Horizont-Ausdehnung 60→90 mit Datenreife
(~Aug re-testen), volle Jahres-Saisonalität ~Dez 2026. Die im Design-Doc §11.5
benannten billigen Experimente stehen noch aus: **Chronos-Bolt zero-shot** als
Foundation-Baseline und **TouringPlans-Pretraining** gegen das Historie-Problem —
beide sind Wochenend-große Experimente mit klarem Erkenntniswert.

### 6d. Drei Scorer, eine Semantik

nf (`score-comparison`), pcn (`score.py`), shape (`score.py`) implementieren
dieselbe Idee dreimal leicht anders — der Rolling-Window-Bug existiert bereits
zweimal (§3). Mindestens: Fix in beide portieren + einen kurzen „Scorer-Kontrakt"
dokumentieren (Fenster-Ausrichtung an lokalen Tagen, upsert nur vollständige Tage,
Segment-/Lead-Definitionen). Optional: die pure Aggregation als geteiltes
Python-Modul zwischen pcn/shape ziehen.

---

## 7. Konkrete Reihenfolge (Vorschlag)

| # | Was | Aufwand | Wirkung |
|---|---|---|---|
| 1 | Crowd-Level auf PCN-q0.8 + Non-Crossing-Guard (§2a) | ~½ Tag | Korrekte Crowd-Semantik auf busy |
| 2 | Scorer-Fix pcn+shape (48h, nur volle Tage) + Board-Reset (§3) | ~1 Tag | Belastbare Gate-Evidenz |
| 3 | Retention + Serving-Index für `pcn_forecasts`/`shape_forecasts` (§4a) | ~1 Tag | Stoppt unbegrenztes Wachstum, hält Reads schnell |
| 4 | Inferenz-Fenster 548→~4 Tage + Stale-Skip vor Fetch (§4b) | ~½ Tag | ~100× weniger DB/RAM pro Tick |
| 5 | Serving-Konsistenz Park-Kurve/Kalender/Deviation (§2b) | 1–2 Tage | Ein Modell pro View |
| 6 | Bake-off: layers=8-RF + DOW-Kanal (§5a/b.1) | 2–3 Tage | Der eigentliche Qualitäts-Hebel |
| 7 | Lead-Kurven-Scoring aus dem gespeicherten Fan (§3) | 1–2 Tage | Misst, was wirklich serviert wird |
| 8 | Holiday/Schedule/Wetter-Kanäle iterativ (§5b) | je 1–2 Tage | Busy-Tail, Wetter-Cluster |
| 9 | KPI auf served-Modell, Shape-Reconcile, Chronos/TouringPlans (§6) | fortlaufend | System-Hygiene |

> Leitplanke unverändert: **nichts flippt ohne Busy/Headliner-Win auf sauberer
> Evidenz** — Punkt 2 ist deshalb bewusst vor Punkt 6: erst das Messgerät
> kalibrieren, dann am Modell drehen.

---

## 8. Umsetzungsstand (2026-07-02, gleicher PR)

| §7 | Stück | Status |
|---|---|---|
| 1 | Crowd-Level aus PCN-q0.8 (`getPcnIntradayWaits` holt q0.5+q0.8; Crowd = max(q0.8, q0.5)) + Non-Crossing (`metrics.enforce_quantile_monotonicity` in `predict_quantiles`) | ✅ |
| 2 | Full-Day-Contract in beiden Scorern (`full_day_window`: Lookback 48h/96h, nur voll abgedeckte lokale Tage, aktueller Teil-Slot ausgeschlossen) + Tests | ✅ (Board-Reset nötig, s.u.) |
| 3 | Retention (`prune_pcn_forecasts` 14d / `prune_shape_forecasts` 30d, im Score-Job) + `created_at`-Index (Serving-Staleness-Filter & DELETE) | ✅ |
| 4 | Inferenz-Fenster `PCN_FORECAST_WINDOW_DAYS=7` (Fallback auf volles Fenster bei dünnem Grid) + billiger `park_has_fresh_data`-Pre-Check VOR dem Panel-Fetch | ✅ |
| 5 | Serving-Konsistenz: `getParkPredictions` (Park-Kurve + Kalender) wendet den Override nach dem Cache-Read an (Cache bleibt CatBoost-pur, 30-min-TTL); Deviation-Service misst gegen den servierten PCN-Wert (`getServedPcnWait`, geteilte Konstante `pcn-serving.constants.ts`); `LIMIT 400` → tagesproportional | ✅ |
| 6 | RF-Bake-off: `PCN_GWN_LAYERS` (env) + `run_bakeoff.py --layers`; **Default bleibt 2** — erst Bake-off-Win, dann flippen | 🟡 vorbereitet |
| 7 | Lead-Kurven-Scoring aus dem gespeicherten Fan | ⬜ offen |
| 8 | Feature-Kanäle: `dow_sin/dow_cos/is_weekend` im Tensor (append-only) + **Kanal-Evolutions-Contract** (Checkpoint speichert Trainings-Kanäle, `_model_features` selektiert per Name → alte Modelle servieren bis zum Retrain weiter); Holiday/Schedule/Wetter | 🟡 DOW drin, Rest offen |

**Einmaliger Board-Reset nach Deploy** (die vor dem Fix geschriebenen, gereiften
Zeilen sind fenster-degradiert und nicht reparierbar; gestern + heute schreibt der
nächste Stunden-Lauf vollständig neu):

```sql
DELETE FROM pcn_intraday_comparisons WHERE target_date < CURRENT_DATE;
DELETE FROM shape_comparisons       WHERE target_date < CURRENT_DATE;
```

Danach **1–2 Wochen sauber reifen lassen**, bevor der PCN-Swap-Win als final bestätigt
gilt (und bevor Shape offline-vs-live neu bewertet wird). Erwartung: die N pro Tag
steigen um ein Vielfaches (volle Tage statt letzter Stunde), Buckets summieren sich
exakt auf „all", und der Bias-Mix verschiebt sich (bisher abend-lastig).

**Betriebs-Notizen:** (a) Beim ersten Score-Lauf nach Deploy löscht die Retention
initial zig Millionen alter `pcn_forecasts`-Zeilen — der DELETE läuft über den neuen
`created_at`-Index, kann aber einige Minuten dauern (einmalig; danach ~Stundentakt-
kleine Batches). Optional vorab manuell in Batches löschen. (b) Nach dem nächtlichen
Retrain (08:30) trainieren die Modelle auf den neuen 11 Kanälen; bis dahin servieren
die alten Checkpoints unverändert über den Kanal-Contract.
