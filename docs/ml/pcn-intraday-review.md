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

> **Update (PAR-854, 2026-10-11, English):** the reader-facing figures now follow
> the served model — the attraction accuracy badge scores the served PCN + blend
> value (`prediction_accuracy.served_*`, captured two hours ahead) and
> `/plan/day` `leadTimeMae` the served TFT/CatBoost daily level. The admin KPIs
> above still score raw CatBoost by design. See
> [Reader-facing accuracy figures](reader-facing-accuracy.md).

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

## 9. §8 row 5 was a no-op on the park-level path (PAR-817, 2026-10-09)

Row 5 above wired the override into `getParkPredictions`, but it never fired there.
`getPcnIntradayWaits` keyed its lookup on `Date.toISOString()` (`…T12:15:00.000Z`) and
`applyPcnIntradayOverride` looked up `p.predictedTime` as a string. The DB read paths
build `predictedTime` with `toISOString()` too, so ride pages matched; the park-level
path hands over the ml-service response, whose `predictedTime` is python
`isoformat()` (`…T12:15:00+00:00`, `ml-service/predict.py`). Same instant, different
string, zero matches. Park page curve, `/plan/day` (today's hours), calendar today and
favorites therefore served pure CatBoost while ride pages served PCN + the §7.7
persistence blend — one slot, two numbers.

Rules since then:

- **The lookup compares instants** (epoch ms on both sides), never ISO strings.
- **Serving and storing are separate reads.** `getRawParkPredictions` is the cached,
  CatBoost-pure ml-service answer and the only park-level read the 15-min/daily
  prediction cron may hand to `storePredictions`; `wait_time_predictions` and
  `prediction_accuracy` are the CatBoost side of the PCN-vs-CatBoost board. The
  served `getParkPredictions` applies the override on copies of the predictions, so
  neither the Redis entry nor the stored rows ever carry a `+pcn` number. (Before the
  fix the cron called the served method — harmless only because the override was a
  no-op; fixing the keys alone would have written PCN into the CatBoost board.)
- The blend anchor (`fetchCurrentWaits`, latest STANDBY reading within 3 h) is read
  from `queue_data` per call, so it works identically on every path. Since PAR-1029
  the reading in force must be OPERATING with a wait (the rule of
  `ForecastArchiveService.liveWaits`); a ride that just went DOWN has no anchor.

### 9.1 The first hour serves the current wait (PAR-1026, PAR-1029)

Measured 2026-10-24: 94.3 % of live anchors are under `BLEND_GATE_MIN` (40 min), so
the gate left almost every ride on raw PCN even 15 minutes ahead, and on the live
forward archive persistence beat the served curve by 1.7 min MAE in the first hour.
The rules of `persistenceBlendServe` since then:

| lead | anchor | served q0.5 |
| --- | --- | --- |
| ≤ 1 h | ≥ 5 min (`BLEND_FIRST_HOUR_MIN_ANCHOR`) | the anchor |
| ≤ 1 h | < 5 min or none | raw PCN |
| 1 – 3 h | ≥ 40 min (`BLEND_GATE_MIN`) | linear anchor → PCN |
| 1 – 3 h | < 40 min or none | raw PCN |
| ≥ 3 h | any | raw PCN |

The crowd level's q0.8 goes through the same blend (same anchor, same α) and is
still clamped to ≥ the shown wait, so the tier and the wait agree in the first
hour. A PCN slot serves `uncertaintyMinutes: null`: `pcn_forecasts` holds only
q0.5 and q0.8 (`PCN_SERVE_QUANTILES`), so there is no PCN band, and CatBoost's
band beside a PCN wait described a different forecast. The same `rules` hold in
`pcn-service/score.py persistence_blend`, so the `pcn_blend` shadow board scores
what is served.

The lead is measured from the actual time (`nowMs`, default `Date.now()`;
`servedIntradayValues` passes its generation time), not from the current slot
start. On the 15-min grid this changes nothing in the first hour — a slot ≤ 1 h
from the slot start is ≤ 1 h from now too — but on the 1–3 h ramp α now follows
the clock inside a slot, so the served value drifts by up to ~1/8 of the
anchor–PCN gap within one slot, and a cached response or the :05 archive capture
can differ slightly from a read later in the same slot. To check it live:
`live_h0_1` persistence vs served in `forecast_archive_paired` (definition 2)
should move toward 0.

## 10. `pcn_forecasts` write volume and retention (PAR-873, 2026-10-12)

The DB audit of 2026-10-12 found `pcn_forecasts` at 28 GB (122 M rows, 14-day
retention) with 1.73 B updates against 1.06 B inserts, 0.2 % of them HOT, and the
lead-curve scorer (`fetch_pcn_leadcurve_window`) reading ~77 M blocks a day because
it walks every row of a ride over the whole retention.

### What still rewrites rows

Those counters are cumulative since May. Most of the rewriting stopped on
2026-10-01 with the in-process `_written` memo in `forecast.py` (skip an origin
already written with the same checkpoint). Measured on 2026-10-12 over the last
24 h, by comparing each row's `created_at` with its origin instant:

| Rows written ≤ 1 h after their origin (first write) | Written later (rewrites) |
|---|---|
| 8.50 M (95.2 %) | 0.43 M (4.8 %) |

That split is a lower bound: a rewrite within an hour of its origin lands in the
first column. The exact figure for one deploy comes from `pg_stat_user_tables`
sampled at 20:36:57 and 21:02:47 UTC, a window with one deploy (20:50) in it:
+131 712 inserts, **+223 872 updates**, 0 of them HOT. The first tick after that
deploy (21:00) wrote 308 448 rows over 128 parks, and 223 872 of them — the same
number — carried an origin more than 30 minutes older than their write, over 94
parks. The ticks without a restart in between (20:30, 20:45) rewrote nothing.

Grouped by the tick that wrote them, the older rewrites are the **first tick after each
deploy** too: 195 k rows over 82 parks at 20:15 (container started 20:04), 80 k at
12:30, 65 k at 20:45 the day before, 42 k at 07:30, 26 k at 16:30, plus a few
thousand around the 09:00 retrain. A restart empties the memo, so every park whose
origin stopped moving (the closed ones) is re-predicted from the same checkpoint
and the same panel and upserted with the same numbers. Each such upsert is a dead
tuple and three index entries, and it moves `created_at` to now — which re-arms the
3-hour staleness guard of `getPcnIntradayWaits` for a park the guard had already
handed back to CatBoost.

### The conditional upsert

```sql
ON CONFLICT (attraction_id, target_slot, origin_slot, quantile)
DO UPDATE SET predicted_wait = EXCLUDED.predicted_wait,
              model_version  = EXCLUDED.model_version,
              created_at     = now()
WHERE abs(pcn_forecasts.predicted_wait - EXCLUDED.predicted_wait) > 0.001  -- :tol
```

- **"Changed" means more than a thousandth of a minute apart**, not bit-unequal:
  GPU inference is not guaranteed to be bit-reproducible across processes, and a
  last-bit difference would turn every restart rewrite back on. Served waits are
  whole minutes, so nothing anyone reads can move by that much.

- **`created_at` keeps one meaning**: when this origin was forecast with these
  values. A value-identical re-send leaves it alone, so the staleness guard reads
  the same thing after a restart as between restarts. No heartbeat column is
  needed: the guard is meant to trip for a park whose origin stopped moving, and
  for an open park the origin moves every tick, which writes new rows (inserts,
  not updates) with a fresh `created_at`.
- **A retrained checkpoint** that changes the values for an origin still rewrites
  and restamps those rows, as before.
- **`model_version` is not compared**: it is the tick's own timestamp and differs
  on every call.
- Tested against Postgres in `pcn-service/test_db_write_volume.py`: a re-send
  (also one jittered by 10⁻⁵) writes zero row versions and leaves the serving read's output identical, a
  changed value rewrites exactly the changed rows, and a closed park's rows stay
  out of the 3-hour window after a re-send. Against the old statement those four
  fail.
- **Expected effect**: zero updates on the first tick after a deploy, i.e.
  ~0.2 M fewer dead tuples and ~0.7 M fewer index entries per deploy, and no
  restart re-freshening a closed park for the guard. Whether the restart
  re-prediction is value-identical (same checkpoint, same panel for a park whose
  feed stopped) was not run on production; check it after the deploy with the
  same `pg_stat_user_tables` sample around the next deploy's first tick:
  `n_tup_upd` should stay flat while `n_tup_ins` grows.

### Retention: 14 → 4 days

Every reader of `pcn_forecasts`, and the oldest row it needs:

| Reader | Where | Reads |
|---|---|---|
| Serving override | `MLService.getPcnIntradayWaits` | `created_at` ≥ now − 3 h |
| Served value for deviation checks | `PredictionDeviationService.getServedPcnWait` | 3 h |
| Served-accuracy recorder | `MLService.servedIntradayValues` → the serving path | 3 h |
| ML overview probe | `MlOverviewService.components` | newest row, last 2 days |
| Main shadow board | `score.py` → `fetch_pcn_forecasts_window` | targets from yesterday 00:00 park-local, origins up to 12 h (`PCN_HORIZON`) earlier: **≤ 60 h** |
| Lead-curve board | `score.py` → `fetch_pcn_leadcurve_window` | same window, leads ≤ 6 h: ≤ 60 h |
| Park merge | `merge-dependencies.ts` | re-points whatever exists |
| `ml-bench` | — | does not read the table |
| Ad-hoc exports (`ml-review-2026-10` E6, a 14-day lead sample, run once on 2026-10-09) | — | whatever is kept |

The scorer is the binding reader at ~60 h; a missed scoring run does not need more,
because the scorer only ever looks back 48 h and the board freezes older days. The
owner's floor is 4 days, which is also the smallest whole-day value with a full day
of margin over 60 h, so `PCN_FORECAST_RETENTION_DAYS` is now **4**. An ad-hoc lead
analysis over a longer window has to export within 4 days of its targets.

`test_db_write_volume.py` pins the invariant (retention ≥ the scorer's oldest row
+ 14 h) so a later change to the lookback or horizon cannot silently outgrow it.

### The prune is batched

Dropping the retention leaves ~87 M expired rows (planner estimate on 2026-10-12)
for the first prune. One `DELETE` of that size would be one transaction holding a
snapshot for its whole run with a WAL burst to match, so `prune_pcn_forecasts`
deletes by `ctid` in batches of 200 k rows, one transaction each, at most 50 per
call (10 M rows). The hourly score job drains the backlog in about nine runs; in
steady state (~0.4 M rows an hour) it is two batches.

### Getting the disk back

Deleted rows are reused by later inserts, so the table stops growing at about the
4-day size (~8 GB with indexes), but the file stays at 28 GB until it is rewritten.
Nothing in this change does that. Options, after the backlog has drained:

1. **`VACUUM FULL pcn_forecasts`** in a quiet window, with `SERVE_PCN_INTRADAY`
   off first. It takes an ACCESS EXCLUSIVE lock for the rewrite of ~36 M rows; the
   serving read would *wait* on that lock rather than fail over to CatBoost, so
   switching the override off is what keeps the park pages answering. Run it
   through `scripts/prod-psql.sh` with a raised `PARKFAN_PSQL_STATEMENT_TIMEOUT`
   and outside 01:00–09:30 UTC (the 04:50 VACUUM and the 06:00 training).
2. **`pg_repack --table pcn_forecasts`** if the extension is installed in the
   TimescaleDB image (it is not today): the lock is held only for the swap at the
   end.
3. Do nothing: the space is reused by the table itself, and only the other
   tables' growth would want it back.

## 11. Chronos-2 takes 45–120 min when switched on (PAR-911, 2026-10-17)

`SERVE_CHRONOS_INTRADAY` (default off) layers a second override after the PCN
override + §7.7 blend: Chronos-2's cached `live` curve replaces the served
value on the slots 45–120 minutes ahead; ≤ 30 min stays the blend (persistence
wins at 15 min) and > 120 min stays PCN, which is also the fallback when the
Chronos curve is older than 90 min or missing. Crowd level from Chronos's q80
against the carried P50, as PCN's q0.8. `pcn_forecasts` and the PCN shadow
board are untouched. Design, cadence, the archive's incumbent surface,
switch-on checklist and rollback: [forward-archive.md §10](forward-archive.md#10-chronos-2-served-on-the-live-surface-par-911).
