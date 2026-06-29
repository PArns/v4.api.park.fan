# Shape-Modell — Tageskurven-Expansion (Level × Shape)

> **Status:** Phase 0 (Daten-Fundament) — 2026-06-29.
> **Kontext:** Das zweite Stück der Zwei-Modell-Architektur aus
> [custom-intraday-model-design.md §10](custom-intraday-model-design.md). PCN (intraday) ist
> gebaut/deployt; das Shape-Modell ist die **geteilte Infrastruktur**, die *jeden* Tages-Level-Forecast
> (Long-Term-LCM **oder** Daily-Crowd) in eine servierbare Stundenkurve expandiert.

---

## 1. Wozu — der `Profil`-Term, explizit gemacht

Der PCN-Doc faktorisiert die Intraday-Wartezeit (§2) als:

```
w(ride, t) ≈  Profil(ride, dow, slot)   × Crowd(park, day, t) × Sensitivität(ride) + Live-Residuum + ε
              └──────────┬───────────┘     └──────────────┬─────────────────────┘
                   SHAPE (dies)                     LEVEL (LCM / Daily-Forecast)
```

- **Level** = „wie voll wird Tag *D* für Ride *r*?" → **ein Wert pro Ride-Tag** (Tages-Peak). Liefert
  bereits der **TFT-Daily** (LCM, `nf-service`) bzw. der Daily-Crowd-Forecast. **Kein neues Netz.**
- **Shape** = die **stabile Tagesform** dieses Rides (Open-Ramp → Mittag/Nachmittag-Peak → Abend-Taper),
  **normalisiert** (level-frei). Variiert v.a. nach **Slot-of-Day**, **DOW** (Wochenende ≠ Werktag) und
  **Crowd-Bucket** (volle Tage haben ein flacheres Plateau). **Das baut dieses Modell.**

Servieren = `wait(ride, day, slot) ≈ Level(ride, day) × Shape(ride, slot | dow, crowd)`.

**Warum eigenständig & jetzt buildable:** Anders als der Long-Term-Level (jahres-saisonal → datengelimitet
bis ~Dez 2026) braucht die *Form* keinen vollen Jahreszyklus — nur genug operierende Tage je
Ride×Bedingung. Wir haben ~6–7 Mon × ~150 Parks → reichlich. Das Modell ist **klein, datensparsam,
hardware-unkritisch** (§10.4) und **bedient den gesamten Stack**: es ersetzt das, was CatBoost heute für
**Tag 61–365** (far-daily Shape) **und** die intraday-Shape notdürftig stopft.

## 2. Die Dekomposition (sauber)

Für jeden **operierenden Tag** eines Rides:

1. **Tages-Level** `L(ride, day)` = robuste Tages-Spitze (Default `peak` = max über die Tagesslots;
   konfigurierbar auf ein Quantil, um mit dem zu mappen, was das LCM vorhersagt — siehe §6).
2. **Normalisierte Kurve** `s(slot) = y(slot) / L` ∈ (0, 1] — **level-frei**, die reine Form.
3. **Bedingungen** je Tag: `dow_bucket` (Wochenende/Werktag) und `crowd_bucket` (Tertil der Tages-Peaks
   *dieses* Rides → quiet/mid/busy — selbst-kalibrierend aus den Daten, keine externe Crowd-Quelle nötig).

Das **Shape-Profil** = der **Mittelwert der normalisierten Kurven** je `(ride, crowd_bucket, dow_bucket, slot)`
über alle qualifizierenden Tage, plus `n_days` je Zelle.

**Rendern** (Serving):
```
shape(ride, crowd, dow)[slot]  = profile_cell  (mit Fallback-Hierarchie, §3)
wait_curve[slot]               = predicted_level × shape[slot]
```
`crowd` am Serve-Punkt wird aus dem **vorhergesagten Level** über dieselben per-Ride-Tertil-Schwellen
abgeleitet → build/serve konsistent.

## 3. Robustheit: Fallback-Hierarchie

Dünne Zellen (`n_days < SHAPE_MIN_OBS_PER_CELL`) degradieren **graceful** statt Rauschen zu servieren:

```
(ride, crowd, dow, slot)  →  (ride, crowd, slot)  →  (ride, dow, slot)  →  (ride, slot)  →  (park, slot)
```

Jede Stufe ist ein über mehr Tage gemittelter normalisierter Wert; die erste mit genug `n_days` gewinnt.
So bekommt jeder Ride×Slot eine vertrauenswürdige Form, auch saisonale/seltene Bedingungen.

## 4. Datenquelle

`queue_data`, **park-lokal** auf den 15-Min-Slot-Grid gebinnt — **identische Konventionen wie PCN/nf**
(`STANDBY`, `status='OPERATING'`, `waitTime >= SHAPE_MIN_WAIT`, `date_bin(... AT TIME ZONE tz ...)`,
JOIN `attractions`, da `queue_data` kein `parkId` hat). Pro `(ride, park-lokaler Tag, slot-of-day)` der
**Median** der Real-Waits. Slot-Auflösung konfigurierbar (15-Min default; Stunde möglich).

> Alternative leichtere Quelle: `attraction_hourly_history` (vor-aggregiert, **stündlich**) — vom
> Rope-Drop-Feature genutzt. Für 15-Min-Serving brauchen wir `queue_data`; die hourly-Tabelle bleibt eine
> Option für einen reinen Stunden-Shape.

## 5. Kandidaten (Bake-off, spätere Phase)

| Kandidat | Was | Rolle |
|---|---|---|
| **NP-Profile** (Phase 0) | nicht-parametrische normalisierte Profile + Fallback (oben) | **starke Baseline**, robust, datensparsam — oft schwer zu schlagen |
| **Learned Shape** (Phase 2) | kleines Netz/GBT: `(ride-emb, slot, dow, crowd, [season]) → norm` | glättet/teilt Stärke über Rides, Saison-Interpolation |
| **CatBoost-Shape** (Referenz) | die heutige intraday/far-daily Kurve aus CatBoost | der Status-quo, den wir schlagen müssen |

Phase 0 liefert die **NP-Profile** — sie sind zugleich Baseline *und* sofort servierbar.

## 6. Serving-Integration (Phase 1)

- **Input:** ein Tages-Level je Ride-Tag — aus LCM (TFT-Daily, far-daily) **oder** dem Daily-Crowd-Forecast.
- **Output:** 15-Min-Kurve = `level × shape`. Ersetzt CatBoosts far-daily- (Tag 61–365) und
  intraday-Shape-Lückenfüller; expandiert auch jeden Long-Term-Level.
- **Level-Statistik-Alignment:** Normalisiert wird mit `SHAPE_LEVEL_STAT` (Phase 0: `peak`). Wenn das LCM
  ein P90 (nicht das Maximum) liefert, gleicht Phase-1-Serving mit einem konstanten `peak/P90`-Faktor je
  Ride×Crowd ab — ein Kalibrier-Detail, **kein** Modellproblem.
- **Blend (§10.3):** in der 1–3-Tage-Zone PCN-Nowcast ⨯ (Level×Shape) lead-gewichtet überblenden.

## 7. Evaluation

Zwei getrennte Fehler, damit Shape isoliert vom Level beurteilt wird:

1. **Shape-Fehler** (kern): gegeben den **wahren** Tages-Level, MAE der gerenderten Kurve vs. Ist-Slots —
   misst nur die Form. Vs. CatBoost-Shape + „flache Kurve"-Baseline, **nach Busy-Segment** (volle Tage sind
   die harte Form).
2. **End-to-End:** mit dem **vorhergesagten** Level (LCM) — der servierte Fehler.

## 8. Phasen

- **Phase 0 (dies):** Daten-Fundament — `shape-service/` mit `db.py` (park-lokaler Panel-Fetch),
  `profiles.py` (PURE: Normalisierung, Buckets, Fallback-Profile, Render — DB-frei, unit-getestet),
  `pipeline.py` (DB→Profile), `build_profiles.py` (CLI), Tests. Gegen die Live-DB verifiziert.
- **Phase 1:** Persistenz (`shape_profiles`-Tabelle) + Render-API + Serving-Merge + Shape-Backtest vs CatBoost.
- **Phase 2:** Learned-Shape-Kandidat + Bake-off.
- **Phase 3:** Blend/Handoff PCN ⨯ (Level×Shape) sauber definieren (§10.3).

## 8a. Phase-0-Befunde (Backtest 2026-06-29, 7 datenreiche Parks, 14d Holdout)

Shape-MAE **bei wahrem Tages-Level** (isoliert die Form), gepoolt; und Zell-Besetzung über
~6 Mon Historie:

| Konditionierung | all | busy≥60 | mid | quiet | median Tage/Zelle |
|---|---|---|---|---|---|
| park (gröbste) | 7.47 | 19.10 | 9.93 | 3.03 | — |
| ride (global form) | 6.67 | 17.45 | 8.54 | 2.76 | 50 |
| **ride×crowd** | **6.58** | 17.29 | 8.69 | **2.58** | 18 |
| ride×crowd×dow (feinste NP) | 6.77 | 17.96 | 8.90 | 2.62 | **9** (46% <10 Tage) |
| **ride×daytype** (wend/feiertag/ferien/brücke/saison → 5 Archetypen) | 6.61 | **17.11** | **8.46** | 2.79 | — |
| ride×crowd×daytype (multiplikativ) | 6.74 | 17.66 | 8.98 | 2.61 | — |

**Drei harte Lehren:**
1. **Ride-Form zählt** (park ≫ schlechter), **crowd ist der NP-Sweet-Spot** (knapp vor ride-global).
2. **Die User-Faktoren (Wochenende/Feiertag/Ferien/Brücke/Saison) tragen ECHTES Form-Signal** —
   als 5-Wege-`daytype` schlagen sie crowd **auf busy (17.11 vs 17.29) und mid (8.46 vs 8.69)**,
   den Segmenten, die das Gate entscheiden.
3. **NICHT multiplikativ kombinieren:** `crowd×dow` und `crowd×daytype` sind beide schlechter —
   die feinste NP-Zelle hat median **9 Tage** (46% <10), Wetter/Holiday obendrauf → ~4–5 Tage →
   unter der Vertrauensschwelle. Das ist die **quantifizierte Datenwand für non-parametrisches
   Zell-Splitting**.

### 8b. Bake-off (2026-06-29): wie man die Faktoren *gemeinsam* nutzt

Non-parametrische Zellen können crowd + daytype nicht multiplikativ kombinieren (Sparsity, oben).
Getestet wurden drei Wege, beide Faktoren zu vereinen — gemessen am Shape-MAE bei wahrem Level,
faire Common-Rows:

| Kandidat | busy-MAE | Urteil |
|---|---|---|
| gelerntes MLP (ride-emb + slot-emb + Faktoren als Features) | 17.5 | **verliert** — kann ride-spezifische Busy-Form nicht memorieren |
| gelerntes Residual (NP-crowd-Basis + gelernte Korrektur) | 18.6 | **verliert** — Tag-Faktor-Korrektur ist ride-spezifisch, globales Net **überfittet** |
| **additive Shrinkage** `base + α·(crowd−base) + β·(daytype−base)` | **16.4** | **GEWINNER** |
| Wetter (wet/dry) als 3. additiver Term | — | **abgelehnt** (optimales γ=0, jedes Gewicht verschlechtert) |

**Gewähltes Phase-1-Modell:** die **additive Shrinkage** mit grid-gesuchten Gewichten **α=0.5 (crowd),
β=0.6 (daytype)** → busy **16.4–16.7** vs crowd 17.0–17.3 (**−3.5%**) und daytype-allein 16.8 (−2.3%).
Beide Faktoren tragen additiv bei; Shrinken (<1) regularisiert die verrauschten Per-Zell-Abweichungen.
Implementiert als `ShapeProfiles.render_additive` (profiles.py); daytype aus `daytypes.py` (Holidays).

**Warum kein gelerntes Modell:** Der Tag-Faktor-Form-Effekt ist **ride-spezifisch**; NP memoriert das pro
Ride besser als ein globales Net es generalisiert, und das Net überfittet die Korrektur. Das ist *nicht*
„Modell zu schwach", sondern die korrekte Erkenntnis: bei dieser Datenmenge schlägt die robuste,
interpretierbare NP-Komposition das gelernte Modell. Ein gelerntes Modell wird erst mit deutlich mehr
Daten (mehr Tage je Ride×Bedingung) konkurrenzfähig.

**Datengrenze erreicht:** Wetter trägt keine Rest-Form (γ=0), feiner-NP ist data-walled, das gelernte
Modell überfittet. Weitere Gewinne (Wetter-Form, feinere daytypes, echte Saison) brauchen **mehr
Tage/Jahreszyklus** (~Dez 2026). Das LCM (TFT-Daily) nutzt dieselben Faktoren bereits fürs **Level**.

### 8c. Glättung — der Gratis-Gewinn (2026-06-29, zweite Runde)

Nach „Datenwand" weiter gesucht (zu Recht): die Per-Slot-Mean-Form ist **verrauscht** (jede Zelle
mittelt ~18 Tage), aber benachbarte 15-Min-Slots sind glatt. Eine **±2-Slot (±30 min) gleitende
Mittelung** der servierten Form senkt die MAE deutlich — **kostenlos**, ohne neue Daten:

| | busy≥60 | all |
|---|---|---|
| additive, ungeglättet | 16.4 | 6.28 |
| **additive + smooth ±2** | **16.0** | **6.19** |
| crowd-Baseline | 17.3 | 6.58 |

→ Das Finalmodell ist **smooth(ride_base + 0.5·crowd_dev + 0.6·daytype_dev)**, busy **16.0 vs 17.3
crowd = −7.4%** (vorher −3.5%); gewinnt jetzt auf **allen** Segmenten inkl. quiet. `SHAPE_SMOOTH_SLOTS=2`.

**Was getestet wurde (und nicht half):** EB-/hierarchische Shrinkage der crowd×daytype-Interaktion
(bestes K minimal schlechter auf busy → die Interaktion trägt auch geschrumpft kein Signal). **Offen
als nächster großer Hebel:** ein gelerntes Modell über ALLE Parks (nicht 7) mit Early-Stopping + NP-Form
als Anker-Feature — der eigentliche ML-Test (der erste MLP war auf 7 Parks unterpowert).

## 9. Ehrliche Einordnung

Klein, billig, **nicht** datengelimitet (Form ≠ Jahres-Saison), **geteilte Infrastruktur** für beide Tracks.
Genau das Stück, das §10 als „das eine neue kleine Stück" benennt. ROI: hoch pro Aufwand, weil es eine reale
Serving-Lücke (CatBoost far-daily + intraday Shape) mit einem datensparsamen, transparenten Baustein schließt.
