# Serving & Shadow-Modelle — Status-Snapshot (2026-06-29)

> Handoff-Stand der Multi-Modell-Architektur: drei Challenger (TFT, PCN, Shape) gegen CatBoost,
> pro Horizont. Quelle: diese Session. Details in den verlinkten Docs.

## Architektur-Leitsatz: ein Modell pro Horizont

| Horizont | Serviertes Modell (Champion) | Challenger / Status |
|---|---|---|
| **Intraday 0–24 h** (15-Min) | **PCN** (Flag `SERVE_PCN_INTRADAY=true`, **LIVE**) | **PCN gewinnt** alle Segmente bei jedem Lead (gereift 29.06: busy 24.9 vs 26.2, all 8.3 vs 8.9). CatBoost-Fallback bei stale/fehlend (3h-Guard). |
| **Daily ≤45–60 d** | **TFT** (gemerged) | live; TFT schlägt CatBoost daily (−39…46%). |
| **Far-daily 61–365 d** | **CatBoost (Level)** | bleibt — TFT datengelimitet bis ~Dez 2026; Shape liefert kein Level. **Long-term weiter nötig.** |
| **Intraday/far-daily Kurve** | CatBoost-Shape | **Shape-Modell** (Level×Shape) als Shadow — Board reift noch. |

## CatBoost wird weiter gebraucht (long-term)

`getServingDailyPredictions` merged **TFT-nah + CatBoost-fern**. CatBoost ist der einzige Level-Lieferant
für Tag 61–365, bis TFTs Horizont mit Datenreife wächst (~Dez 2026). Das **Shape-Modell ersetzt CatBoost
dort nicht** — es rendert dessen Tages-Level in eine bessere 15-Min-Kurve (späterer Schritt).

## PCN — intraday Nowcaster (SHADOW, Swap-bereit)

- **Deployed & live**: pcn-service, arch=**graphwavenet** (6× schneller als gpstgnn, GPU-saturierend).
  Cron: train 08:30 / forecast `*/15` / score stündlich. Board: `pcn_intraday_comparisons`.
- **Board (gereift, heute)**: PCN schlägt CatBoost auf **jedem** Segment (busy 19.2 vs 25.0, all 6.9 vs 8.8).
- **Champion-Swap LIVE** (`ml.service.ts`, `SERVE_PCN_INTRADAY=true` in prod): PCNs q0.5 ersetzt die
  CatBoost-`hourly`-Waits in beiden Read-Pfaden (`getStoredPredictions` + `getBatchStoredPredictions`),
  crowdLevel aus dem neuen Wert neu gerechnet, **CatBoost-Fallback** wo PCN nichts hat **oder zu alt ist**
  (Staleness-Guard `created_at < 3h`, commit 25e6aa5 — schützt vor eingefrorenem Forecast-Producer).
- **Forecast-Lock-Bug gefixt (2026-06-30, commit 86dd17f)**: per-kind forecast/score-Locks froren bei
  Restart-mitten-im-Job ein → Forecasts 24h still → pcn-service Startup heilt die Locks jetzt selbst.
- Doc: [project_pcn_intraday_shadow](.) / `docs/ml/tft-vs-catboost-clean-comparison.md`.

## Shape — Level×Shape Tageskurve (SHADOW, frisch deployt)

- **Modell fertig** (`shape-service`, CPU-only): `smooth(ride_base + 0.5·(crowd−base) + 0.6·(daytype−base))`.
  daytype = Wochenende/Feiertag/Ferien/Brücke/Saison → 5 Archetypen. busy 16.0 vs crowd 17.3 (−7.4%).
  **Iterativ ausgereizt**: additiv > daytype/crowd allein; Glättung ±2 = gratis −4%; gelernte Korrektur
  (`learned.py`, 18 Parks, Early-Stop, NP-Anker) = weitere −2.8% busy → **das ist die echte Datengrenze**
  (reichere Features überfitten). Lehre: „Datenwand" erst nach *richtigem* ML-Test rufen.
- **Shadow deployt heute**: build/forecast/score-Crons (09:00/09:30/10:00 UTC), `shape_forecasts` (2.8M
  forward-Zeilen geschrieben), `shape_comparisons`-Board. Admin: `POST /v1/admin/shape/:action`,
  Board auf `/v1/admin/ml-comparison`.
- **Board reift erst** (~2–3 Tage, Daily-Horizont → Forecasts müssen vergehen, dann Score gegen Ist+CatBoost).
- **Offen**: gelerntes Modell als Producer einwechseln (statt NP additive+smooth) ist dokumentierter Upgrade-Pfad.
- Doc: `docs/ml/shape-model-design.md` (§8a–§8d).

## Board-Surface

`GET /v1/admin/ml-comparison` liefert drei Sektionen + Verdicts:
- `daily` — TFT vs CatBoost (reif).
- `intraday` — PCN vs CatBoost (reif, PCN gewinnt).
- `shape` — Shape vs CatBoost (reift noch, „no shadow board yet").

## Nächste Schritte

1. **PCN-Swap ist scharf** (`SERVE_PCN_INTRADAY=true`, live): Board über ein paar post-fix saubere Tage
   beobachten (der 30.06 ist durch den Lock-Bug kontaminiert), bei Bedarf sofort zurück (Flag).
2. **Shape-Board** in ~2–3 Tagen prüfen; wenn Shape CatBoost-Kurve schlägt → far-daily/intraday-Shape auf
   Shape umstellen (CatBoost-Level bleibt).
3. **Shape learned-Modell** als Producer einwechseln (+2.8% busy), wenn der NP-Shadow validiert ist.
4. **Datenreife ~Dez 2026**: TFT-Horizont 45→90→365 ausdehnen; Shape-Saison + Wetter-Konditionierung
   re-testen (heute datengelimitet).
