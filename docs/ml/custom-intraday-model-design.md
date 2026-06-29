# Eigenes Intraday-Modell — Konzept, Aufwand, Daten & Machbarkeit (RTX 5080)

> Status: **Konzept / Entscheidungsvorlage** (2026-06-29). Kein Produktionscode geändert.
> Ausgangspunkt: CatBoost und TFT wurden ausführlich erprobt (siehe
> [`tft-vs-catboost-clean-comparison.md`](./tft-vs-catboost-clean-comparison.md),
> [`neuralforecast-tft-evaluation.md`](./neuralforecast-tft-evaluation.md)).
> **Beide passen für das Intraday-Problem nicht sauber.** Dieses Dokument leitet aus den
> gemessenen Schwächen ein *eigenes* Modell ab — mit Fokus auf den Intraday-/Nowcast-Layer,
> wo die Fehlerquote heute am höchsten ist.

---

## 0. TL;DR

- **Wo es weh tut:** Der Intraday-Layer (15-Min-Slots, Re-Inferenz alle 15 Min) wird heute
  *nur* von CatBoost sauber bedient — TFT verliert dort sogar gegen naive Baselines auf der
  Busy-Spitze. Aber auch CatBoost ist intraday **nicht gut genug**: live gemessen
  Quiet(<30) Bias **+11.2 min** (≈92 % aller Slots!), Busy(≥60) Bias **−11.3 min**. Das ist
  kein Tuning-Problem, sondern **strukturell**.
- **Zwei strukturelle Lücken**, die *kein* bestehendes Modell schließt:
  1. **Geteilter Park-Crowd-Zustand** als *gelernte latente Größe*. CatBoost hat nur einen
     groben Skalar (`park_occupancy_pct`), TFT ist univariat (blind dafür). Die wahre Ursache
     „dieser Ride ist voll" ist „der *ganze Park* ist gerade voll" — ein ride-übergreifendes
     Signal, das beide Modelle nicht *als Zustand* modellieren.
  2. **Ehrliche Schiefe / Tail.** Beide regredieren auf den Median einer rechtsschiefen
     Verteilung → Busy unterschätzt, Quiet überschätzt. Der q0.8-„Daumen" repariert Busy nur
     auf Kosten von Quiet (+3.4 → live +11.2). **Kein einzelner Punkt-Forecast gewinnt beides**
     (eigene Bake-off-Messung).
- **Empfehlung:** Ein *eigenes*, **park-bewusstes, probabilistisches Multi-Horizon-Nowcast-Modell**
  (Arbeitstitel **„PCN" – Park-Crowd Nowcaster**), das genau diese zwei Lücken adressiert:
  ride-übergreifende Cross-Attention auf den geteilten Crowd-Zustand **+** ein ehrlicher,
  schiefer/Regime-Wahrscheinlichkeits-Kopf statt eines Quantil-Daumens.
- **Machbarkeit RTX 5080 16 GB:** **Hoch.** Das Datenvolumen ist klein (≈2.7k Serien,
  ~6–7 Monate, ~47 M Slot-Zeilen), die Cross-Attention läuft *pro Park* (37–100 Rides) und ist
  billig. Engpass ist **nicht** Rechenleistung sondern VRAM-Disziplin beim Windowing — beim
  bestehenden TFT schon erlebt. Mit Park-Batching passt das Modell locker in 16 GB.
  ⚠️ Blackwell (sm_120) braucht **CUDA 12.8+ / PyTorch ≥ 2.7** — Setup-Stolperstein, sonst
  trivial.
- **Aufwand:** ~**6–10 Wochen** fokussierter Arbeit bis zum schattenlaufenden, messbaren
  Modell (eine Person), plus laufende Datenakkumulation. Risiko über die bestehende
  Backtest-Harness (`backtest_intraday_nowcast.py`) eng kontrollierbar.

---

## 1. Warum CatBoost *und* TFT intraday nicht passen

| | CatBoost (Prod intraday) | TFT (nf-service) |
|---|---|---|
| Live-Zustand (Queue *jetzt*, Velocity) | ✅ Top-Feature (`avg_wait_last_1h` 17.7 %) | nur über Encoder-Fenster, schwächer |
| Park-Crowd-Signal | ⚠️ grober Skalar (#2-Feature), 50 % Dropout | ❌ univariat, blind (hist_exog-Test regressierte) |
| Known-Future (Holidays/Wetter) | ⚠️ wird von Rolling-Avgs verdrängt | ✅ first-class (genau TFTs Daily-Stärke) |
| Busy-Tail intraday | Bias ~−9.6 (besser als TFT) | Bias −18…−20, *verliert* gegen `yesterday-same-slot` |
| Quiet-Kalibrierung intraday | **+11.2 live** (q0.8-Daumen) | gut (Median) |
| Schiefe ehrlich modelliert | ❌ Quantil-Daumen | ❌ Median-Regression |

**Kernbefund (aus euren eigenen Messungen):** Intraday ist der Layer, wo (a) der Live-Zustand
dominiert — TFTs Daily-Vorteil (Known-Future-Kalender) **irrelevant** ist — und (b) die Schiefe
am extremsten ist (Busy-Bias intraday −20 vs. daily −8). CatBoost gewinnt intraday gegen TFT,
weil es Live-Velocity/Occupancy hat. Aber CatBoost selbst ist limitiert durch:

1. **Tabellarische Sicht ohne Sequenz-/Cross-Serien-Struktur.** Es sieht die Queue-Dynamik nur
   als handgebaute Lags, nicht als gemeinsamen Park-Zustand über *alle* Rides hinweg.
2. **Median-Regression + Quantil-Daumen** statt einer ehrlichen schiefen Verteilung.

Genau diese zwei Punkte sind die Design-Hebel des eigenen Modells.

---

## 2. Diagnose: das Problem ist hierarchisch, schief und nowcast-getrieben

Die Intraday-Wartezeit `w(ride, t)` lässt sich als Produkt dreier Effekte denken:

```
w(ride, t) ≈  Profil(ride, dow, slot)          # stabile Tagesform je Ride (Open-Ramp, Mittag, Abend)
            × Crowd(park, day, t)               # GETEILTER latenter Park-Zustand (alle Rides koppeln)
            × Sensitivität(ride)                # wie stark dieser Ride auf Crowd reagiert
            + Live-Residuum(state_jetzt, t)     # Velocity, Downtime-Rebound, Schocks
            + ε(schief, heteroskedastisch)      # rechtsschiefer, varianzgetriebener Rest
```

- **CatBoost** mischt all das in einen GBT — `Crowd` nur als grober Skalar, `Profil`/`Sensitivität`
  implizit über `attractionId`-Embedding + Rolling-Avgs.
- **TFT** modelliert `Profil` + Live-Fenster je Ride sauber, hat aber **kein `Crowd`** (univariat)
  und **kein ehrliches ε** (Median).

Ein eigenes Modell, das `Crowd` als **gelernte, ride-übergreifende latente Zeitreihe** und `ε` als
**schiefe/Regime-Verteilung** explizit macht, schließt beide Lücken auf einmal. Das ist der
defensible Grund, *bespoke* zu bauen statt weiter an Off-the-Shelf zu drehen.

---

## 3. Konzepte (3 Optionen, von empfohlen bis Hedge)

### Konzept A (empfohlen): „PCN" — Park-Crowd Nowcaster

Ein **globales** (ein Modell über alle Parks/Rides), **park-bewusstes**, **probabilistisches**
Multi-Horizon-Modell. Vier Bausteine:

1. **Per-Ride Temporal-Encoder** über das letzte `L`-Slot-Fenster (z. B. 4–8 h = 16–32 Slots).
   Erfasst Live-Zustand & Velocity (CatBoosts Stärke). Leichtgewichtig: **TCN** oder kleines
   **GRU**/Patch-Encoder — *kein* schwergewichtiger Transformer pro Ride nötig.
2. **Park-Cross-Attention je Zeitschritt** (die Neuheit): Attention/Pooling über *alle* Rides
   desselben Parks zum Zeitpunkt `t` → ein **gelernter latenter Crowd-Vektor** statt des groben
   Skalars. Das ist exakt das Signal, das TFT fehlt und das CatBoost nur als Mittelwert hat —
   hier ride-spezifisch gewichtet (ein Ride „lädt" stark auf Crowd, ein Walk-on kaum).
3. **Static Embeddings + Known-Future**: Ride-Embedding (Typ via Namens-Heuristik, historische
   Busyness, Park/Region) + Kalender/Holiday/Schulferien/Wetter-Forecast/Öffnungszeiten als
   Future-Kovariaten (TFTs Daily-Stärke — billig mitgenommen, hilft an Holiday-Spitzen).
4. **Probabilistischer Kopf — der Kern gegen die Schiefe.** Statt Median ODER Quantil-Daumen:
   ein **Zwei-Teil-/Mixture-Kopf**, der *Regime-Wahrscheinlichkeit* und *Magnitude* trennt:
   - `P(busy-Regime | Kontext)` — explizite Wahrscheinlichkeit einer Spitze,
   - bedingte schiefe Magnitude (Log-Normal / Gamma / Tweedie) je Regime.

   Das Serving liest dann **zweckabhängig** die richtige Größe: Median der Verteilung für die
   angezeigte Wartezeit (löst die +11.2-Quiet-Inflation), eine obere Quantile / `P(busy)` für das
   Crowd-Level-Signal. Das ist *kein* Daumen auf dem Loss — es ist die ehrliche Verteilung,
   zweckgerecht serviert (genau die Schlussfolgerung eurer MultiQuantile-Analyse, nur ins
   Modell eingebaut statt nachträglich gewählt).

**Warum A die zwei Lücken schließt:** Baustein 2 = geteilter Crowd-Zustand (Lücke 1),
Baustein 4 = ehrliche Schiefe/Tail (Lücke 2). Bausteine 1+3 sichern, dass es CatBoosts Live-Stärke
*und* TFTs Known-Future-Stärke behält.

### Konzept B (Hedge / inkrementell): Residual-Hybrid

CatBoost (oder ein GBT) bleibt als **Level-Backbone**; ein kleines neuronales Modell liefert nur:
(a) die latente **Park-Crowd-Trajektorie** (ein billiges Modell pro Park-Tag) und (b) eine
**Cross-Ride-Residual-Korrektur**. Vorteil: *inkrementell* schiffbar, CatBoost-Investition bleibt,
jeder Schritt einzeln messbar. Nachteil: zwei Systeme, die Schiefe wird nur halb adressiert.
→ **Als Fallback/Stufe-1**, falls A zu groß startet.

### Konzept C (billige Baselines zum Bake-off): Off-the-Shelf erneut, aber gezielt

Bevor/parallel zu A: **PatchTST**, **TSMixerx**, **DeepAR** (NeuralForecast, gleiche Harness) auf
*sauberen* Daten gegen die Busy-Spitze messen. DeepAR ist nativ probabilistisch/autoregressiv,
TSMixerx billig mit Exog. **Erwartung:** Keines hat die Cross-Ride-Struktur → keines schließt
Lücke 1. Aber als *Kostenhebel* und Sanity-Baseline für A unverzichtbar (Disziplin: nichts
flippt Produktion ohne Backtest-Gewinn).

> **Empfohlene Reihenfolge:** C als Baseline-Sweep (1 Woche) → A bauen → B nur falls A-Risiko zu hoch.

---

## 4. Architektur-Detail (Konzept A, konkret)

```
Eingaben pro (ride, base_time t):
  X_self    : [L, F_dyn]   letzte L Slots dieses Rides (wait, velocity, status, downtime, slot-of-day)
  X_park    : [R, L, F_dyn] dieselben Slots für alle R Rides des Parks   ← Cross-Ride-Tensor
  X_static  : Ride-Embedding (Typ, Park, Region, hist. P50/P90-Busyness)
  X_future  : [H, F_futr]  Kalender/Holiday/Schulferien/Wetter/Öffnungszeit für Horizont H

Encoder:
  h_self  = TCN/GRU(X_self)                         # Live-Zustand des Rides
  c_t     = CrossAttention(query=h_self, keys/vals=Encoder(X_park))   # gelernter Park-Crowd-Vektor
  z       = concat(h_self, c_t, X_static)

Decoder (Multi-Horizon, H Slots voraus):
  für jeden Horizont-Schritt h: g_h = MLP(z, X_future[h], pos_enc(h))

Probabilistischer Kopf je (ride, h):
  p_busy_h          = sigmoid(W_b · g_h)            # Regime-Wahrscheinlichkeit
  (μ_h, σ_h)        = LogNormal/Gamma/Tweedie-Parameter(g_h)
  → Verteilung; Serving zieht Median (Anzeige) bzw. q0.8 / P(busy) (Crowd)
```

- **Horizont H:** intraday 1–24 h in 15-Min-Slots (4–96 Schritte). Re-Inferenz alle 15 Min mit
  aktuellem Fenster (wie heute CatBoost; GPU ist 99 % idle → praktisch gratis).
- **`available_mask`:** geschlossene Slots aus dem Loss nehmen (kein 0-Fill — der bekannte Fehler,
  der den Bias verzerrte). Verifiziert in NeuralForecast; im eigenen Loss 1:1 nachbauen.
- **Globales Modell, per-Park-Batch:** ein Gewichtssatz, Training in Park-Chunks (löst zugleich
  das VRAM-Problem, siehe §6).
- **Loss:** negative Log-Likelihood der schiefen Verteilung **+** BCE auf das Regime; optional
  CRPS für probabilistische Kalibrierung. **Kein** asymmetrisches Forcing.

**Verworfene/risikobehaftete Alternativen (Ehrlichkeit):**
- Reines Quantil-/Loss-Forcing → euer dokumentierter Sackgassen-Pfad (Quiet-Inflation). Nicht.
- Voll-Transformer pro Ride mit langem Kontext → unnötig teuer; Daten zu kurz (~6 Mon).
- Park-Mittelwert-Occupancy als Feature → schon getestet, regressierte (zu grob/kollinear). Die
  *gelernte* Cross-Attention ist genau die Reparatur dafür.

---

## 5. Benötigte Daten

**Vorhanden (Wiederverwendung aus `ml-service`/`nf-service`):**

| Datum | Quelle | Status |
|---|---|---|
| 15-Min-Wait-Panel je Ride | `queue_data` (resample, `available_mask`) | ✅ vorhanden |
| Live-Zustand (Velocity, last-1h, Momentum) | `features.py` | ✅ vorhanden |
| **Cross-Ride-Tensor** (alle Rides je Park auf 15-Min-Grid ausgerichtet) | aus `queue_data` aufzubauen | 🟡 *muss assembliert werden* (Hauptdaten-Task) |
| Park-Occupancy / P50-P90-Baselines | `attraction_p50/p90_baselines` | ✅ vorhanden |
| Kalender/Holiday/Schulferien/Bridge-Days | `holiday_utils.py` | ✅ vorhanden |
| Wetter (Ist + sinusoidales Tagesprofil) | `features.py` | ✅ (Forecast bei Inferenz prüfen) |
| Öffnungszeiten / Schedule | `schedule_entries` + `schedule_filter.py` | ✅ vorhanden |
| Downtime-Rekonstruktion (Pent-up Demand) | `features.py` | ✅ vorhanden |
| Static Ride-Meta (Typ-Heuristik, Park, Region) | `attraction_features.py` | ✅ vorhanden |

**Der einzige echte neue Daten-Task:** den **Cross-Ride-Tensor** bauen — alle Rides eines Parks auf
demselben 15-Min-Grid, mit `available_mask` für geschlossene Slots. Das ist reine
Pipeline-Arbeit (Pivot/merge_asof), keine neue Datenquelle.

**Datenreife — die reale Abhängigkeit (mehrfach in euren Docs bestätigt):** aktuell nur
**~6–7 Monate** Historie (Dez 2025 →). Sequenzmodelle müssen ein Regime *gesehen* haben; Busy-Episoden
je Ride sind intraday noch dünn. Erwartung: Busy-Kalibrierung verbessert sich monoton mit Historie
(daily-Beleg: TFT schlägt CatBoost auf Busy-Headlinern, sobald genug Tage da sind). → **Mit
≥ 12 Monaten (voller Saisonzyklus) materiell besser.** Das eigene Modell deshalb so bauen, dass es
mit wachsenden Daten skaliert (periodischer Re-Backtest, gleiche Kadenz wie heute).

**Wünschenswert, falls beschaffbar (würde Tail spürbar heben):**
- Ride-**Kapazität/Durchsatz** (Throughput) — erklärt, warum gleiche Crowd unterschiedliche Queues macht.
- **Attendance-/Ticketing-Proxy** oder Event-Kalender (Konzerte, Paraden, Sonderevents).
- Fastpass/Single-Rider-Verfügbarkeit. (Alle extern — als „nice to have" markiert, kein Blocker.)

---

## 6. Machbarkeit auf RTX 5080 (16 GB)

**Verdikt: hoch machbar.** Begründung mit konkreten Zahlen:

**Datenvolumen (klein für ein NN):**
- ~2.7k Serien × ~96 Slots/Tag × ~200 Tage ≈ **~47 M Slot-Zeilen** gesamt (Panel). Pro Park
  37–100 Rides → Cross-Ride-Tensor je Park-Tag ist winzig (z. B. 100 × 96 × ~8 Features).
- Cross-Attention ist **pro Park** (R ≤ ~100) → O(R²·L) ist vernachlässigbar. Kein globaler
  All-Pairs-Attention-Blowup.

**VRAM (der eigentliche Engpass — aus eurer TFT-Erfahrung):**
- Beim bestehenden TFT OOM bei `hidden=96/wb=128` → 16 GB ist die echte Grenze, *nicht* Rechenzeit.
- Lösung ist dieselbe wie schon produktiv: **Training in Park-Chunks** (`NF_PARK_CHUNK_SIZE`-Muster),
  kleine `windows_batch_size`, BF16-Mixed-Precision. Damit bleibt jeder Fit bei ~1–3 GB.
- Das PCN-Modell selbst ist klein (hidden 64–128, TCN/GRU + ein Cross-Attention-Layer) →
  Gewichte + Aktivierungen weit unter 16 GB.

**Rechenzeit:**
- Training: Minuten bis ~1–2 h/Nacht (Park-Chunks, ~500–1500 Steps). GPU heute ~6 Min/Nacht
  ausgelastet → reichlich Headroom.
- Inferenz: alle 15 Min ein Forward-Pass über die aktiven Parks — Millisekunden bis Sekunden,
  „praktisch gratis" (GPU 99 % idle).

**⚠️ Blackwell-Stolperstein (wichtig, sonst kein Start):** RTX 5080 = Blackwell, Compute
Capability **sm_120**. Braucht **CUDA 12.8+** und **PyTorch ≥ 2.7** (frühere Wheels haben kein
sm_120-Kernel → „no kernel image available"). Lightning/NeuralForecast entsprechend pinnen. Das ist
der einzige nicht-triviale Setup-Punkt; danach ist 16 GB komfortabel.

**Fazit:** Die Hardware ist **nicht** der limitierende Faktor. Limitierend sind (1) Datenreife und
(2) VRAM-Disziplin beim Windowing — beides bekannt und beherrschbar.

---

## 7. Aufwandsabschätzung

Eine Person, fokussiert. Wiederverwendung der bestehenden DB-Loader & Backtest-Harness senkt den
Aufwand deutlich.

| Phase | Inhalt | Aufwand |
|---|---|---|
| **0. Daten-Pipeline** | Cross-Ride-Tensor + einheitlicher 15-Min-Loader (`available_mask`), aus `nf-service/db.py` + `ml-service/features.py` ableiten | **1–2 Wochen** |
| **1. Baseline-Sweep (Konzept C)** | PatchTST/TSMixerx/DeepAR über bestehende Harness, Busy-Gate messen | **~1 Woche** |
| **2. PCN v1** | Per-Ride-Encoder + Park-Cross-Attention + Multi-Quantile-Kopf; offline Backtest | **2–3 Wochen** |
| **3. Probabilistischer/Regime-Kopf** | Zwei-Teil/Mixture + schiefe Likelihood, Tuning, Kalibrierung (CRPS) | **~2 Wochen** |
| **4. Shadow-Serving** | „Going-forward shadow" neben CatBoost speichern, scoren, Split-Entscheidung | **1–2 Wochen** + Akkumulation |
| | **Summe bis messbares Shadow-Modell** | **~6–10 Wochen** |

Danach: laufende Datenakkumulation + periodischer Re-Backtest (gleiche Kadenz wie heute, alle
~3 Wochen). **Produktions-Flip nur bei Backtest-Gewinn auf Busy/Headliner** — kein Override eurer
bestehenden Disziplin.

**Risiko-Reduktion:** Phasen 0–1 liefern sofort Wert (saubere Pipeline + Baseline-Zahlen),
unabhängig davon, ob PCN am Ende gewinnt. B (Residual-Hybrid) als Fallback, falls A-Risiko zu hoch.

---

## 8. Risiken & Messlatte

| Risiko | Gegenmaßnahme |
|---|---|
| Datenreife (~6–7 Mon) zu dünn für Busy-Tail | Mit Daten skalierende Architektur; Re-Backtest-Kadenz; ehrlich kommunizieren, dass ≥12 Mon hilft |
| Cross-Attention bringt < erwarteter Lift | Baseline-Sweep (C) zuerst; A nur weiterführen, wenn es C+CatBoost auf Busy schlägt |
| VRAM-OOM (wie bei TFT) | Park-Chunking + kleine `windows_batch_size` + BF16 (bewährtes Muster) |
| Über-Engineering | Inkrementell (B als Stufe 1 möglich); jeder Baustein einzeln gegen das Gate gemessen |
| Blackwell/CUDA-Toolchain | CUDA 12.8+ / PyTorch ≥ 2.7 früh verifizieren (Smoke-Test vor Phase 2) |

**Erfolgsmetrik (unverändert eure Disziplin):** **Busy-/Headliner-MAE + -Bias auf der gematchten
Population**, gegen naive Baselines (persistence, yesterday-same-slot) **und** gegen CatBoost-intraday.
Niemals Overall-MAE (Quiet wäscht das Signal aus). Guardrail: ein Hebel muss Busy verbessern, **ohne**
Quiet zu inflationieren.

---

## 9. Empfehlung

1. **Sofort:** Daten-Pipeline (Phase 0) + Baseline-Sweep (Phase 1) — liefert Wert unabhängig vom
   Ausgang und etabliert die Zahlen, gegen die PCN antreten muss.
2. **Dann:** PCN (Konzept A) bauen — der einzige Ansatz, der *beide* strukturellen Lücken
   (geteilter Crowd-Zustand **+** ehrliche Schiefe) gleichzeitig schließt, was weder CatBoost noch
   TFT tun.
3. **Fallback:** Residual-Hybrid (Konzept B), falls A zu groß startet — inkrementell, CatBoost-Investition bleibt.
4. **Unverändert:** Champion bleibt CatBoost intraday, bis ein Backtest-Gewinn auf Busy/Headliner
   vorliegt. Nichts flippt Produktion ohne diesen Nachweis.

> **Leitprinzip (konsistent mit eurem „feed, don't force"):** Den Busy-Tail *nicht* per
> Loss-Daumen erzwingen, sondern dem Modell das fehlende **Signal** (gelernter Park-Crowd-Zustand)
> und die richtige **Verteilung** (ehrliche Schiefe, zweckgerecht serviert) geben.
