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
- **Long-Term gehört in ein *zweites* Modell, nicht in dasselbe.** Das dominante Signal wechselt
  mit dem Horizont: intraday entscheidet der **Live-Zustand**, long-term entscheiden **Known-Future-
  Kovariaten** (Kalender/Holiday/Saison). Ein Modell für beides ist der Kompromiss, der heute schon
  scheitert. → **Zwei Modellfamilien**, horizont-stratifiziert (siehe §10). Das eigene Bau-ROI liegt
  bei **PCN (intraday)**; Long-Term ist überwiegend ein **Daten-** (≥12 Mon) plus
  **Level×Shape-Dekompositions-**Problem, kein neues Architektur-Problem.
- **Tiefenrecherche (§11) bestätigt die Diagnose und liefert „Ansatz 3":** Der Domänen-Stand der
  Technik ist dünn und **niemand modelliert den geteilten Park-Crowd-Zustand** — das ist die offene
  Front. Die *bewährte* Maschinerie dafür kommt aus der **Verkehrsvorhersage**: adaptiv-graph-lernende
  STGNNs (AGCRN/Graph WaveNet/ST-LGSL) + Global-Local-Faktorisierung (DeepGLO) + multivariat-
  probabilistischer Kopf (UQGNN). Damit wird PCN von hand-gerollter Cross-Attention zu einer
  **forschungsgestützten Architektur** aufgewertet, in der der latente Crowd-Faktor ein *first-class-
  Objekt* ist (§11.2). Für die Schiefe: SPADE/Tweedie/EP-Loss/GEV (§11.3). Level×Shape ist durch
  curve-to-curve-Lastprognose belegt (§11.4). **Ehrliche Lücke:** „unified vs. split" und Foundation-
  Models bei wenig Historie sind literaturseitig *nicht* entschieden (§11.5).

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

---

## 10. Long-Term-Vorhersagen — die Zwei-Modell-Architektur

**Grundprinzip: ein Modell pro Horizont-Regime, weil das dominante Signal mit dem Horizont
wechselt.** Genau hier scheitern Ein-Modell-Ansätze (CatBoost *und* TFT) heute: dieselben
autoregressiven Features, die intraday gold sind, degenerieren long-term zu „typischer letzter
Wochentag" — und dieselben Kalender-Features, die long-term entscheidend sind, werden intraday vom
Live-Zustand verdrängt. **Deshalb: zwei Modellfamilien, nicht eine.**

| Horizont | dominantes Signal | Modell | Granularität |
|---|---|---|---|
| **0–24 h** (Nowcast) | **Live-Zustand** (Queue jetzt, Velocity, Park-Crowd heute) | **PCN** (§3–4) | 15-Min-Slots, Re-Inferenz alle 15 Min |
| **~1–3 Tage** (Übergang) | gemischt | **Blend** PCN ⨯ Long-Term | täglich + Shape |
| **1–365 Tage** (Calendar / Long-Term) | **Known-Future** (Kalender, Holiday, Saison, Events, Wetter-Klimatologie) | **LCM + Shape** (§10.1) | Tages-Level + gerenderte Tageskurve |

Der Live-Zustand ist long-term **wertlos** (er ist nicht bekannt), die Known-Future-Kovariaten sind
intraday **fast wertlos** (der heutige Holiday-Flag erklärt nicht, warum *diese Queue jetzt* 45 Min
ist). Zwei Modelle ist also nicht Bequemlichkeit, sondern die korrekte Faktorisierung des Problems.

### 10.1 Das Long-Term-Modell: Level × Shape (Dekomposition)

15-Min-Slots 365 Tage vorauszusagen ist aussichtslos (und sinnlos — niemand will die Queue um
14:15 in 7 Monaten). Die ehrliche Faktorisierung ist **Level × Shape**:

1. **LCM — Long-term Crowd/Level-Modell:** „Wie voll wird Datum *D* für Ride *r*?" → **ein Wert pro
   Ride-Tag** (Tages-Peak P90 / Crowd-Level), getrieben **rein von Known-Future**: Kalender,
   Holiday/Schulferien je Region, Bridge-Days, Saison (Jahres-Seasonality), Event-Kalender,
   Wetter-Klimatologie, geplante Öffnungszeiten. **Das ist genau die Stärke des bestehenden
   TFT-Daily** (`nf-service`, heute bis 60 d geshippt, schlägt CatBoost auf Busy/Headliner). →
   **Kein neues Architektur-Problem.** Der Hebel ist (a) **Horizont ausdehnen** (60 → 90 → 365),
   limitiert durch Datenreife, und (b) Static-/Event-Kovariaten anreichern.
2. **Shape-Modell — die Tageskurve:** gegeben ein Tages-Level, render die **typische 15-Min-/Stunden-
   Kurve** für *diesen* Ride bei *diesem* Crowd-Level / DOW / Saison (Open-Ramp → Mittagspeak →
   Abend). Ein kleines, gelerntes Profil-Modell (oder normalisierte historische Profile je
   Ride×Crowd-Bucket). Billig, datensparsam, und es **bedient den gesamten Stack**: es expandiert
   *jede* Tages-Level-Vorhersage (long-term ODER der Daily-Crowd-Forecast) in eine servierbare
   Stundenkurve — und füllt damit genau die Lücke, die heute CatBoost für Tag 61–365 + intraday-Shape
   notdürftig stopft.

> **Warum das elegant ist:** Das LCM macht die *robuste, kalendergetriebene* Vorhersage (ein Wert,
> wenig Rauschen, viel Signal). Das Shape-Modell macht die *datensparsame* Expansion auf die
> Servier-Granularität. Beides zusammen ist weit dateneffizienter als ein Monster-Modell, das
> 15-Min × 365 d direkt forecastet.

### 10.2 Ehrliche Einordnung: Long-Term ist überwiegend ein Daten-Problem

- Jahres-Seasonality kann ein Modell erst lernen, wenn es **einen vollen Zyklus gesehen** hat.
  Aktuell ~6–7 Mon → **bei ~Dez 2026 ein volles Jahr.** Bis dahin ist der far-future-Horizont
  fundamental durch Daten gedeckelt, **nicht** durch Architektur. Das deckt sich exakt mit eurer
  Doku (h=60 deferred „wants ~8 months of history", Re-Test ~Aug).
- Folgerung: **Nicht** jetzt einen schweren neuen Long-Term-Net bauen. Stattdessen: TFT-Daily als
  LCM weiterführen, Horizont mit der Datenreife ausdehnen, das **Shape-Modell** als das eine neue
  (kleine) Stück ergänzen, und den Blend (§10.3) sauber definieren. Der Bau-Aufwand fließt nach
  **PCN (intraday)**, wo Architektur *tatsächlich* der Hebel ist.

### 10.3 Übergangszone & geteilte Bausteine

- **Blend (~1–3 Tage):** Wo beide Modelle gelten, gewichtet überblenden (PCN-Gewicht fällt mit dem
  Lead, LCM-Gewicht steigt) — analog zu eurem heutigen Merge „TFT near-term über CatBoost long tail".
- **Geteilte Konzepte:** Der **Park-Crowd-Zustand** ist in beiden Welten zentral — intraday als
  *gelernte latente Größe* (PCN), long-term als *vorhergesagtes Tages-Crowd-Level* (LCM). Das
  **Shape-Modell ist geteilte Infrastruktur** (expandiert jeden Level-Forecast). So bleiben die zwei
  Modelle konzeptionell kohärent statt zwei Silos.

### 10.4 Aufwand & Machbarkeit Long-Term-Track

| Stück | Inhalt | Aufwand | RTX 5080 |
|---|---|---|---|
| LCM | TFT-Daily weiterführen; Horizont 60→90→365 mit Datenreife; Event-/Static-Kovariaten | **~1–2 Wo** (+ Warten auf Daten) | trivial (läuft heute ~6 Min/Nacht, oft sogar CPU-fähig) |
| Shape-Modell | normalisierte Profile je Ride×Crowd×DOW×Saison, gelernt | **~1–2 Wo** | trivial (klein) |
| Blend/Handoff | Lead-gewichtetes Überblenden PCN⨯LCM, Serving-Merge | **~1 Wo** | n/a |

→ Der Long-Term-Track ist **deutlich billiger** als PCN und hardware-unkritisch. Das Nadelöhr ist
Datenreife (Zeit), nicht Compute.

**Gesamtbild der Roadmap:**

1. **PCN (intraday)** — der eigentliche eigene Bau, höchstes ROI (§3–9).
2. **LCM = TFT-Daily weiterführen** — Horizont mit Daten ausdehnen, kein neues Net.
3. **Shape-Modell** — das eine neue kleine Stück, bedient beide Tracks.
4. **Blend** — sauberer Lead-gewichteter Übergang.

So bekommt ihr **zwei Modelle, zwei Ansätze** — korrekt nach dominantem Signal getrennt — ohne den
Long-Term-Teil zu über-engineeren, wo ohnehin die Zeit (Datenreife) der bindende Faktor ist.

---

## 11. Tiefenrecherche-Synthese & iteriertes Konzept („Ansatz 3")

> Quelle: strukturierte Tiefenrecherche (2026-06-29), 24 Quellen, 111 Claims extrahiert,
> 25 adversarial verifiziert (24 bestätigt, 1 widerlegt). Alle Übertragungen aus Fremddomänen
> (Verkehr/Energie/Retail) sind **methodische Inferenz, nicht empirisch an Wartezeiten validiert** —
> das ist der Grund, warum die Backtest-Disziplin (§8) unverändert bindend bleibt.

### 11.1 Was die Recherche über den Stand der Technik sagt

- **Der Domänen-Stand der Technik ist erstaunlich dünn.** Belegt sind nur (a) eine grobe
  Saison+Attraktion-**Regression** (touringplans-Daten, ~80 % der *Tages*-Varianz, ~15-Min-Fehler,
  kein Time-of-Day) und (b) ein per-Ride **PINN/Kolmogorov**-Stochastikmodell (2026, kontinuierlicher
  Markov-Prozess je Ride, probabilistisch). **Beide sind per-Ride und modellieren den geteilten
  Park-Crowd-Zustand NICHT.** → Unser Cross-Serien-Crowd-Ansatz ist die **offene Front** des Feldes,
  nicht ein gelöstes Problem — das stärkste Differenzierungsargument.
  [UTSA-Regression](https://rrpress.utsa.edu/server/api/core/bitstreams/7a5a5ad0-eee3-4dbc-9aca-ae8ff36cceae/content),
  [PINN/ERA 2026](https://aimspress.com/article/doi/10.3934/era.2026186)
- **Neuer Daten-Hebel gegen unser Historie-Problem:** TouringPlans veröffentlicht einen **freien
  WDW-Datensatz 2012–heute** mit *posted* UND *actual* Wartezeiten (vier Parks, monatlich
  aktualisiert, explizit für ML). Tauglich als **Transfer-Learning-/Pretraining-Seed** und zum
  Modellieren der posted-vs-actual-Lücke. (Caveat: andere Parks/Granularität als unser Korpus —
  nur zum Vortrainieren, nicht zum direkten Servieren.)
  [TouringPlans Dataset](https://touringplans.com/blog/disney-world-wait-times-available-for-data-science-and-machine-learning/)

### 11.2 Der geteilte Crowd-Zustand — *so* baut man ihn (bewährte Bausteine)

Die Recherche bestätigt: das fehlende Signal ist real, und die **Verkehrsvorhersage** hat die
Maschinerie dafür schon gelöst — Zeitreihen ohne vorgegebenen Graphen, deren Kopplung *gelernt*
werden muss. Das ist exakt unser Ride×Ride-Problem (kein physischer Graph zwischen Rides).

| Baustein | Was es liefert | Übernahme für PCN |
|---|---|---|
| **AGCRN** — Data-Adaptive Graph Generation + **Node-Adaptive Parameter Learning** | lernt den Inter-Serien-Graphen *ohne* Vorgabe; pro-Ride-Parameter statt globalem Satz | **der gelernte Graph = der Park-Crowd-Zustand**; NAPL = unsere `Sensitivität(ride)` |
| **Graph WaveNet** — self-adaptive adjacency via Node-Embeddings | versteckte Kopplung end-to-end gelernt | Alternative/Ergänzung zum Adjazenz-Lernen |
| **ST-LGSL** — MLP+kNN Latent-Graph-Learner | latente Topologie aus Raum+Zeit-Dynamik | Variante für das Graph-Lernen |
| **DeepGLO** — globale TCN-regularisierte **Matrix-Faktorisierung** + lokales Per-Serien-Netz | jede Serie = Linearkombination von k≪N Basis-Serien + lokale Dynamik | **die globale Low-Rank-Komponente IST der latente Park-Crowd-Faktor** — eleganter als Cross-Attention |
| **UQGNN** — multivariat-probabilistisch (Mittelwert + Kovarianz) | Off-Diagonale = Cross-Ride-Co-Movement, Diagonale = per-Ride-Unsicherheit | **gemeinsamer** probabilistischer Kopf statt unabhängiger Punkt-Forecasts |
| **STG4Traffic** — 16-Modell-Benchmark-Codebase (STGCN, DCRNN, GraphWaveNet, AGCRN, MTGNN, GMAN …) | fertige Vergleichsbasis | **direkt für unseren Bake-off** (statt selbst implementieren) |

Quellen: [AGCRN](https://arxiv.org/pdf/2007.02842),
[Graph WaveNet](https://arxiv.org/abs/1906.00121),
[ST-LGSL](https://arxiv.org/pdf/2202.12586),
[DeepGLO](https://arxiv.org/abs/1905.03806),
[UQGNN (SIGSPATIAL'25)](https://arxiv.org/pdf/2508.08551),
[STG4Traffic](https://github.com/trainingl/STG4Traffic).

> **Schlüsselerkenntnis (DeepGLO):** Standard-Deep-Forecaster erzeugen *selbst trainiert auf allen
> Serien* pro-Dimension-Vorhersagen, die hauptsächlich von der eigenen Vergangenheit der Serie
> abhängen — exakt unser Problem mit TFT. Die **globale Faktorisierung** zwingt einen *geteilten*
> Low-Rank-Zustand heraus. Das ist der Park-Crowd-Faktor als first-class-Objekt, ohne ihn von Hand
> als Feature zu bauen.

### 11.3 Die Schiefe — die Literatur verwirft Median/MSE

Bestätigt, dass „kein Punkt-Forecast gewinnt beides" kein lokales Artefakt ist, sondern Konsens.
Vier übernehmbare Hebel (statt nur Quantil-Daumen):

- **SPADE (Amazon, NeurIPS 2024):** zerlegt in **zwei Aufgaben** — Peak-Events vs. Baseline —
  mit maskierten Convolutions + Peak-Attention. Das *Gegenteil* einer unifizierten Regression.
  [SPADE](https://arxiv.org/abs/2411.05852)
- **Tweedie / TweedieGP (Compound-Poisson-Gamma):** rechtsschiefe positive Dichte; TweedieGP ist
  **bestes Modell auf den 0.90–0.99-Quantilen** (genau die entscheidungsrelevanten Busy-Quantile).
  ⚠️ Tweedies Null-Punktmasse passt eher zu intermittierender Nachfrage als zu stets-offenen Rides —
  **die transferierbare Eigenschaft ist die Rechtsschiefe**, nicht die Null-Masse.
  [TweedieGP](https://arxiv.org/html/2502.19086)
- **Enhanced-Peak (EP) Loss:** adaptive, richtungsbewusste asymmetrische Strafe oberhalb einer
  Fehlerschwelle (getrennte Unter-/Über-Schätzungsfaktoren). (Caveat: Einzelstudie, vom Erfinder
  evaluiert, 2-1-Vote — als *eine* Option, nicht als gesetzt.)
  [EP-Loss](https://www.mdpi.com/2571-9394/7/4/75)
- **Nichtstationäre GEV-Ensembles (Extremwerttheorie):** rekursive Partitionierung des
  Kovariatenraums, lokales GEV je Partition → Verteilungsparameter variieren mit Kovariaten.
  Für den *Tail* der Busy-Spitzen, falls SPADE/Tweedie nicht reichen.
  [GEV-Ensemble](https://arxiv.org/pdf/2506.01358)

> Folgerung für PCN §4: der probabilistische Kopf wird konkret **SPADE-artige Peak/Baseline-
> Trennung** ODER **Tweedie-Likelihood** (beides messbar im Bake-off), nicht eine vage „schiefe
> Verteilung". Zweckgerechtes Quantil-Serving bleibt der Servier-Mechanismus.

### 11.4 Level×Shape (Long-Term) ist belegt — aber als Analogie

Die curve-to-curve-Lastprognose (Xu, Chen, Goude & Yao 2020) prognostiziert die **ganze Tageskurve
funktional** mit gemeinsamen probabilistischen Bändern und stellt fest: **„das bloße Zusammenfügen
einzeln prognostizierter Intervalle verliert sofort die Wahrscheinlichkeitsinterpretation"** und
ignoriert die Inter-Slot-Abhängigkeit. → Das validiert unser **Level×Shape** (§10.1) als
*prinzipiellen* Weg zu kohärenten Mehr-Slot-Tagesprognosen.
[Curve-to-Curve (2020)](https://arxiv.org/pdf/2009.01595)
**Caveat:** stützt den **Shape/Kurven-Mechanismus**, *nicht* die breitere Behauptung „zwei Modelle
schlagen ein unifiziertes" — dafür gibt es keinen head-to-head (§11.5).

### 11.5 Ehrliche Lücken (was die Recherche NICHT entschieden hat)

1. **Unified vs. Split — kein direkter Beleg.** Keine überlebende Quelle hat *ein* unifiziertes
   Multi-Horizon-Modell gegen *zwei* getrennte (Nowcast/Calendar) verglichen, noch
   Mixture-of-Experts über die Lead-Zeit. → Unsere Zwei-Modell-Empfehlung ruht auf dem
   **Signal-Argument** (live-state vs. known-future) + der Energy-Analogie, **nicht** auf einem
   Benchmark. Das ist eine begründete Design-Entscheidung, kein bewiesener Fakt — und damit selbst
   ein **Backtest-Experiment** (siehe Brücke unten).
2. **Foundation-Models bei wenig Historie — offen.** Kein verifizierter Claim zu TimesFM/Chronos/
   Moirai/TimeGPT/Lag-Llama auf ~2700 Serien mit nur 6–7 Mon. → Empfehlung: **Chronos-Bolt
   zero-shot** als billige Baseline testen (kein Training, sofortige Vergleichszahl) und TouringPlans
   als Pretraining-Seed prüfen. [Chronos-Bolt/AutoGluon](https://aws.amazon.com/blogs/machine-learning/fast-and-accurate-zero-shot-forecasting-with-chronos-bolt-and-autogluon/)
3. **Single-GPU/RTX-5080-Machbarkeit — von der Literatur nicht beziffert.** Eigene Einordnung
   (§6) bleibt gültig: AGCRN/Graph WaveNet trainieren Verkehrs-Benchmarks (PeMS, 300–880 Knoten)
   problemlos auf *einer* GPU; unsere Parks haben ≤100 Rides/Knoten → deutlich kleiner. Per-Park-
   Batching hält den VRAM klein. Blackwell-Stolperstein (CUDA 12.8+/PyTorch ≥2.7) unverändert.
4. **Cross-Domain-Transfer unbewiesen.** Ob ein gelernter Latent-Graph wirklich einen sinnvollen
   Park-Crowd-Faktor rekonstruiert, ist *Hypothese* → genau das misst der PoC.

### 11.6 „Ansatz 3" — das iterierte, forschungsgestützte Intraday-Modell

Die Recherche verschmilzt unsere drei PCN-Bausteine zu **einer** kohärenten, publizierten
Architektur — nenne sie **GP-STGNN** (Graph-Probabilistic Spatio-Temporal Net). Statt
hand-gerollter Cross-Attention:

```
1. Adaptive Graph Learning  (AGCRN-DAGG / GraphWaveNet self-adaptive)
   → lernt den Ride×Ride-Kopplungsgraphen OHNE Vorgabe  = der Park-Crowd-Zustand
2. Global-Local-Faktorisierung  (DeepGLO)
   → globale Low-Rank-Basis  = latenter Crowd-Faktor (first-class);  lokal = Ride-Dynamik + Live-State
3. Node-Adaptive Parameters  (AGCRN-NAPL)
   → pro-Ride-Verhalten  = Sensitivität(ride)
4. Known-Future-Kovariaten  (Kalender/Holiday/Wetter)  → als Exog eingespeist (TFT-Stärke mitgenommen)
5. Multivariat-probabilistischer + peak-aware Kopf  (UQGNN-Kovarianz × SPADE/Tweedie)
   → gemeinsame Unsicherheit + ehrliche Schiefe;  Serving zieht Quantil je Zweck
```

**Warum das besser ist als mein ursprünglicher PCN-Entwurf:** Der latente Crowd-Faktor ist kein
angebauter Attention-Layer mehr, sondern ein **strukturell erzwungenes** Objekt (gelernter Graph /
globale Faktorisierung) — genau das, was DeepGLO zeigt, dass Standardmodelle es sonst *nicht* lernen.
Und alle Bausteine sind publiziert + es gibt eine Benchmark-Codebase (STG4Traffic), d. h. weniger
Eigenbau-Risiko.

**Bake-off-Plan (ersetzt Konzept C aus §3):** Über **STG4Traffic** AGCRN / Graph WaveNet / MTGNN /
DeepGLO auf unserem Cross-Ride-Tensor gegen CatBoost-intraday + naive Baselines messen, mit
SPADE/Tweedie-Kopf. Gewinner-Architektur → GP-STGNN v1.

**Die Brücke (offene Frage 1 als Experiment):** Ein adaptive-graph-STGNN *kann* Known-Future-
Kovariaten aufnehmen → theoretisch könnte **ein** Modell beide Horizonte bedienen (Mixture-of-
Experts über die Lead-Zeit). Da die Literatur split-vs-unified nicht entscheidet, behandeln wir es
als **explizites Backtest-Experiment**: Start mit dem Split (Signal-Argument), aber GP-STGNN so
bauen, dass es Known-Future kann — dann den unifizierten Multi-Horizon-Lauf gegen den Split messen.

### 11.7 Aktualisierte Roadmap (ersetzt §9 Punkt 1–2, Long-Term unverändert)

1. **Daten-Pipeline (Phase 0):** Cross-Ride-Tensor (unverändert nötig — jetzt als STGNN-Knoten-Input).
   **✅ implementiert** in [`pcn-service/`](../../pcn-service/) (`tensor.py` baut die volle
   `[Rides × Slots]`-Matrix + `available_mask`/`park_open`/`park_occ`; `db.py` holt das
   park-lokale 15-Min-Panel via `date_bin`; CLI `build_cross_ride_tensor.py`; 11 Unit-Tests
   grün ohne DB). Verallgemeinert das skalare `add_occupancy` aus dem nf-Backtest zur vollen
   Matrix — der Punkt, damit das STGNN den Graphen *lernt* statt einen Mittelwert zu bekommen.
2. **Baseline-Sweep über STG4Traffic** (statt Eigenbau): AGCRN/GraphWaveNet/MTGNN/DeepGLO +
   **Chronos-Bolt zero-shot** als Foundation-Baseline. Gegen CatBoost-intraday + naive Baselines.
3. **GP-STGNN v1:** Gewinner-Backbone + SPADE/Tweedie-Kopf + Known-Future-Exog. TouringPlans-
   Pretraining gegen das Historie-Problem prüfen.
4. **Split-vs-Unified-Experiment:** den Horizont-Split gegen einen unifizierten GP-STGNN-Lauf messen.
5. **Shadow-Serving + Gate (Phase 4, §12):** Going-Forward-Shadow neben CatBoost
   (`pcn_forecasts` + `score-intraday-comparison` → `pcn_intraday_comparisons` → system-health),
   gespiegelt an der bestehenden daily-Vergleichs-Mechanik. Nichts flippt Produktion ohne
   Busy/Headliner-Gewinn (Gate §8).

**Aufwand-Update:** Der STG4Traffic-Bake-off *senkt* den Eigenbau-Aufwand der frühen Phasen
(fertige Implementierungen) — die ~6–10-Wochen-Schätzung (§7) bleibt, verschiebt sich aber von
„selbst implementieren" zu „adaptieren + messen", was risikoärmer ist.

---

## 12. Shadow-Betrieb & Vergleich (Phase 4) — wie wir es heute schon machen

> Anforderung: PCN/GP-STGNN soll **als Shadow** neben CatBoost laufen und sauber
> vergleichbar sein. Gute Nachricht: dafür gibt es im System **bereits ein bewährtes
> Muster** — wir spiegeln es 1:1, statt etwas Neues zu erfinden.

### 12.1 Wie der Shadow-Vergleich heute funktioniert (Bestandsaufnahme)

**Daily (TFT vs. CatBoost) — das saubere Vorbild:**

| Baustein | Datei / Tabelle | Funktion |
|---|---|---|
| Durable Forward-Snapshots | `tft_forecasts`, `catboost_daily_forecasts` | **eine immutable Zeile pro (attraction, target_date, forecast_date)**; ein erneuter Lauf am selben Tag überschreibt nur die heutige Zeile, vergangene Forecast-Daten bleiben → der *echte* Forward-Record (vor dem Target erstellt) bleibt erhalten |
| Scoring-Job | `nf-forecast.processor.ts` `score-comparison` (~08:00 UTC) | **INNER-Join** beider Modelle auf die gematchte `(attraction, target_date)`-Schnittmenge bei vergleichbarem Lead, gegen realisierten Tages-P90 |
| Ergebnis-Tabelle | `model_comparisons` (Entity vorhanden) | PK `(target_date, model, segment)`; Segmente **all / busy (P90≥40) / headliner**; Felder `n, mae, bias, meanActual, meanPred, avgLeadDays` |
| Sichtbarkeit | `/v1/admin/system-health` | liest die Segmente → Admin-Board |

**Intraday (CatBoost live):** `wait_time_predictions` (`predictionType='hourly'`, 15-Min-Slots)
→ `PredictionAccuracyService.compareWithActuals()` matcht gegen Ist-Werte → `prediction_accuracy`
(+ aggregierte Stats/Badges).

**Die entscheidende Lektion (aus den Docs):** Ein historischer Intraday-Backtest aus
*gespeicherten* CatBoost-Preds ist **unmöglich** — der 15-Min-Dedup zerstört alle vergangenen
Forward-Records außer dem letzten. → Der einzige saubere Intraday-Vergleich ist der
**Going-Forward-Shadow**: beide Modelle speichern ihre Forward-Forecasts **durable** zum
Inferenz-Zeitpunkt, gescort wird später gegen Ist.

### 12.2 Zwei Instrumente — beide schon gebaut bzw. spezifiziert

1. **Offline-Backtest (rückwärts):** `pcn-service/backtest.py` — Rolling-Origin, leakage-frei,
   Kandidat vs. persistence/yesterday, segmentiert nach Busy + Lead. **✅ implementiert.** Das ist
   das schnelle Iterations-Instrument (kein Warten auf Reifung).
2. **Going-Forward-Shadow (vorwärts, produktionsnah):** das hier spezifizierte Phase-4-Stück —
   das *belastbare* Urteil gegen die live CatBoost-Realität.

### 12.3 PCN-Shadow — die konkrete Spezifikation (spiegelt §12.1)

| Baustein | neu für PCN | mirror von |
|---|---|---|
| **Durable Snapshot** `pcn_forecasts` | eine immutable Zeile pro `(attraction, target_slot, origin_ts, quantile)` — geschrieben bei jeder PCN-Inferenz (alle 15 Min), `predicted_wait` je Quantil (q0.5/q0.8) | `tft_forecasts` (immutable per origin) |
| **Co-Snapshot CatBoost** | CatBoosts live `wait_time_predictions` zum selben `origin_ts` durabel mitschreiben (oder direkt referenzieren), damit der Past-Dedup nicht zuschlägt | die going-forward-Shadow-Logik der Docs |
| **Scoring-Job** `score-intraday-comparison` | INNER-Join PCN vs. CatBoost auf gematchte `(attraction, 15-min slot)` bei gleichem `origin`, gegen realisierten 15-Min-Median | `score-comparison` |
| **Ergebnis-Tabelle** `pcn_intraday_comparisons` | PK `(target_date, model, segment, lead_bucket)`; Segmente **quiet<30 / mid / busy≥60**, Lead-Buckets **≤3h / 3–6h / >6h**; Felder wie `model_comparisons` | `model_comparisons` (+ Lead-Achse, weil intraday lead-sensitiv ist) |
| **Sichtbarkeit** | `/v1/admin/system-health` um den Intraday-Block erweitern | bestehende system-health-Anbindung |

**Serving-Pfad (Shadow = kein Nutzer-Impact):** PCN läuft im **Schatten** — es schreibt nur
`pcn_forecasts`, CatBoost bleibt der servierte Champion. Erst wenn `pcn_intraday_comparisons`
einen **Busy/Headliner-Gewinn ohne Quiet-Inflation** über ein ausreichendes Fenster zeigt
(dieselbe Gate-Disziplin wie §8), wird ein Flip erwogen. Wiederverwendung der bestehenden
`MLService`-Merge-Logik (heute „TFT near-term über CatBoost long-tail") für einen späteren
graduellen Intraday-Flip.

**CUDA / Betrieb:** PCN-Inferenz läuft auf der GPU (RTX 5080, cu128) und ist billig — die GPU
ist ~99 % des Tages idle, also kann alle 15 Min mit dem aktuellen Zustand neu inferiert werden
(genau das Re-Inferenz-Muster, das CatBoost intraday auch fährt), ohne CatBoost (CPU) zu berühren.

### 12.4 Aufwand Phase 4

| Stück | Aufwand |
|---|---|
| `pcn_forecasts` Snapshot + PCN-Inferenz-Hook (alle 15 Min, durable Write) | ~3–5 Tage |
| `score-intraday-comparison`-Job + `pcn_intraday_comparisons` (mirror von `score-comparison`) | ~3–5 Tage |
| system-health-Intraday-Block + Admin-Sichtbarkeit | ~2–3 Tage |
| **Summe Phase 4** (Shadow lauffähig + vergleichbar) | **~2 Wochen** + Reifungsfenster |

→ Reiht sich als **Phase 4** in die Roadmap (§11.7 Punkt 5) ein: *erst* offline-Bake-off-Gewinn
(`backtest.py`), *dann* Going-Forward-Shadow zur Bestätigung gegen die Live-Realität, *dann* —
nur bei Gate-Erfüllung — Flip-Entscheidung.

### 12.5 Implementierungsstand & NestJS-Wiring (der eine reviewte Schritt)

**✅ In `pcn-service/` gebaut & getestet (34 Tests grün, CUDA-first):**

| Stück | Datei | Status |
|---|---|---|
| Durable `pcn_forecasts` (immutable per origin) + Writer/DDL | `db.py` | ✅ |
| Shadow-Producer (re-inferenz → `pcn_forecasts`) | `forecast.py` | ✅ |
| `pcn_intraday_comparisons` + Scorer (pcn ⋈ actual ⋈ CatBoost, segmentiert) | `db.py`, `score.py` | ✅ |
| Per-Park-Training + Persistenz | `train.py`, `train_runner.py` | ✅ |
| FastAPI-Service (`/health /gpu /train /forecast /score /status`) | `main.py` | ✅ |
| Bake-off + GP-STGNN/LocalGRU-Ablation | `run_bakeoff.py`, `gp_stgnn.py`, `backbones.py` | ✅ |
| Deploy: `pcn-service` in docker-compose (Port 8002, `pcn-models`-Volume, cu128) | `docker-compose.yml`, `Dockerfile` | ✅ |

**Verbleibend — NestJS-Wiring (live-berührend → bewusst als Spec, nicht blind verdrahtet,
da hier nicht baubar/testbar):**

1. **Cron-Trigger** (`src/queues/services/queue-scheduler.service.ts`): drei repeatable
   BullMQ-Jobs analog zu nf — `POST {PCN_SERVICE_URL}/forecast` alle 15 Min, `…/train`
   nächtlich (~05:30, nach den P50-Baselines), `…/score` stündlich. `PCN_SERVICE_URL` ist
   bereits in der `api`-Env der compose-Datei gesetzt.
2. **Processor** (neu, analog `nf-forecast.processor.ts`): dünner HTTP-Aufruf der drei
   Endpoints + Logging; keine eigene Logik (die lebt im Python-Service).
3. **Admin-Sichtbarkeit** (`src/admin/system-health.service.ts`): `pcn_intraday_comparisons`
   lesen (z. B. letzte `target_date`, Segmente busy/quiet × Lead) und neben dem
   bestehenden `model_comparisons`-Block ausgeben — gleiche Darstellung wie der daily-Board.

> Bewusste Grenze: Der Python-Service ist vollständig, isoliert (neue Tabellen via
> `CREATE TABLE IF NOT EXISTS`, eigener Container) und getestet. Das NestJS-Wiring ändert
> die *produktive* Job-Steuerung und ist hier nicht baubar/lauffähig — daher als präzise,
> direkt anwendbare Spec übergeben statt ungetestet in den Live-Code geschrieben.
