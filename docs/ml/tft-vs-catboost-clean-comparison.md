# TFT vs CatBoost — Clean Comparison & TFT Optimization

> Status: **in progress** (started 2026-05-30). Living doc. Goal: compare TFT vs
> CatBoost **cleanly** wherever CatBoost runs (daily calendar AND the live intraday
> surface), and optimize TFT the **honest** way — by giving it the right *signal*,
> not by tilting the loss. Companion to
> [TFT vs CatBoost daily split](./neuralforecast-tft-evaluation.md) (the production
> daily decision) and [busy-day challenger](./busy-day-prediction-challenger.md)
> (why CatBoost forcing is a dead end).

---

## 0. TL;DR

- The **raw forward scoreboard overstated TFT** on the daily surface: it scored the
  two models on **different populations at different lead times** (CatBoost ~400–750
  attraction-days at lead 4–6d vs TFT ~2350 at lead 1d). Root cause: `deduplicatePredictions`
  **deletes CatBoost's fresh forward records**, so only stale ones survive, while TFT
  snapshots durably. → Fix: a **symmetric durable CatBoost snapshot** + score on the
  **matched intersection at comparable lead**. On the matched population TFT's edge
  shrinks from "2× better" to **~tie** (because it's *all* attractions — quiet rides
  wash out TFT's real edge, which lives on **busy headliners**).
- **CatBoost serves 15-MIN slots** (verified), refreshed **every 15 min** with current
  features (`generate-hourly` cron `*/15`). A fair intraday TFT must therefore predict
  15-min slots and **re-infer ~every 15 min with the current state** (NOT nightly
  precompute — that is only correct for the far-future daily calendar).
- A historical intraday backtest of CatBoost from stored preds is **impossible** (the
  15-min dedup destroyed all forward records but the latest). → the only clean intraday
  comparison is the **going-forward shadow** (durably store both models, score later).
- **Optimize TFT with signal, not force.** Loss-weighting / quantile to chase the busy
  tail is the same dead end CatBoost hit (challenger doc: 3 forcing iterations failed;
  even the "successful" q0.8w is a thumb on the scale that inflates quiet days). The
  honest lever for intraday busy accuracy is the one signal a univariate TFT is **blind
  to**: **park-wide occupancy** (cross-ride busyness — CatBoost's #2 feature, ~17–20%
  importance), fed as `hist_exog`.

---

## 1. The clean daily scoreboard (implemented)

`src/queues/processors/nf-forecast.processor.ts` `score-comparison`:

- **Symmetric durable snapshot** `catboost_daily_forecasts` (mirror of `tft_forecasts`):
  one immutable row per `(attraction, target_date, forecast_date)`. Snapshotted daily
  from the freshest daily-peak predictions created in the last 26h (so `forecast_date=today`
  is honest), horizon capped at +45d (mirrors TFT's 30-day surface). Idempotent bootstrap
  backfill from surviving `wait_time_predictions` forward records so the matched board is
  not empty for the first ~2 days.
- **Matched scoring**: both models' freshest genuine-forward forecast per `(attraction,
  target_date)`, **INNER-joined** → identical population, joined to the realised daily
  P90. `n` and `meanActual` are identical across the two emitted rows; leads align
  because both snapshots are now durable.

**Why the raw board was unfair (verified on the live DB, 2026-05-30):**

| target | CatBoost n / lead | TFT n / lead | intersection |
|---|---|---|---|
| 2026-05-29 | 587 / 5.4d | 2576 / 1.1d | 341 |
| 2026-05-28 | 747 / 3.5d | 2572 / 1.0d | 501 |

Dry-run of the matched query (same population, leads ~1–2.6 vs ~1):

| day | CatBoost MAE/bias | TFT MAE/bias | n |
|---|---|---|---|
| 05-29 | 13.9 / −8.7 | 9.7 / −2.9 | 285 |
| 05-27 | 13.6 / −10.1 | **15.2 / −13.7** | 242 |
| 05-25 | 13.7 / −8.1 | 13.2 / +1.1 | 213 |

→ On equal footing TFT is **tie-to-modest-win on all attractions**; one day CatBoost
wins. TFT's real edge is on **busy headliners**.

**Segmented board (implemented):** `model_comparisons` now carries a `segment` column
(`all` / `busy` = realised P90≥40 / `headliner` = `headliner_attractions` join); scoring
emits all three via a lateral segment list. This stops the overall MAE washing out TFT's
tail edge. Validated on live data (14-day matched window):

| segment | CatBoost MAE/bias | TFT MAE/bias | n |
|---|---|---|---|
| all | 13.4 / −8.1 | 12.0 / −5.5 | 1326 |
| **busy (P90≥40)** | 27.0 / −25.8 | **20.8 / −17.5** | 419 |
| **headliner** | 19.1 / −14.9 | **16.3 / −9.0** | 668 |

→ TFT **clearly beats CatBoost on busy + headliner** (where the crowd signal lives) — the
quantitative justification for the production split. The `system-health` endpoint reads
the segment so the admin board shows it.

---

## 2. The intraday surface — architecture

| surface | CatBoost | TFT (fair) |
|---|---|---|
| far-future daily calendar | 1×/day (1am) | nightly precompute ✅ (no live signal needed) |
| **intraday / nowcast** | **15-min slots, every 15 min, current features** | **must mirror: 15-min slots, re-infer ~every 15 min with current inputs** ❌ no nightly precompute |

**Why not precompute intraday:** the value of a nowcast is the **live signal** ("the
queue is 45 min right now"). A univariate TFT sees the current level via its **encoder
input window** (the last `input_size` slots of `y`); to make it a nowcast we update that
window to *now* each cycle and re-run `nf.predict` (the model weights are fixed; only the
context changes). Inference is cheap and the **GPU is idle ~99%/day** (TFT only uses it
~6 min at the nightly train+forecast; serving is a DB read), so frequent re-inference is
effectively free and does not touch CatBoost (which stays on CPU by design).

**Going-forward shadow (the clean instrument):** when CatBoost infers, ask TFT in
parallel, store both forward forecasts durably, score later vs actuals — across **all
parks**, no sample-park selection, mirroring the daily scoreboard. (A historical backtest
cannot use CatBoost's stored intraday preds: the 15-min dedup deleted all but the latest
forward record.)

### `available_mask`, not 0-fill
The 2026-05-23 hourly PoC 0-filled closed hours, which dragged the StudentT mean down
(~−6.6 bias) and trained the loss on spurious zeros — that artifact invalidated its
verdict. NeuralForecast's **`available_mask`** (verified present in 3.1.8 core+tsdataset,
and used as a **float multiplicative weight** in the loss — `BasePointLoss._compute_weights`
returns `weights * mask`) keeps a regular grid but excludes closed slots from the loss.
Backtest (Shanghai Disneyland, rolling-origin, GPU): hourly bias **−6.6 → −3.2**.

---

## 3. Optimizing TFT — signal, not force

**Principle (hard-won):** do not tilt the loss to make the model "see things differently
than they are". The [busy-day challenger](./busy-day-prediction-challenger.md) proved this
with CatBoost — 3 forcing iterations (NaN-dropout, ×3.5 holiday sample-weighting) all
failed (the model substituted level proxies, MAE regressed); even the shippable q0.8w
"works" only by predicting the upper quantile, which **inflates quiet days** (+2.7 bias).
The literature agrees: quantile/loss methods struggle with rare extreme tails (quantile
crossover, LPHC events); richer features + letting the model learn the distribution is
preferred (see Sources).

**`available_mask` as a weight — what's legitimate vs forcing:**

| lever | what it is | verdict |
|---|---|---|
| up-weight busy slots | forcing — TFT "sees" busy ≠ data | ❌ rejected |
| down-weight flat/shows | hygiene — mis-ingested non-rides aren't rides | handle at **data level**, not in the loss |
| real quiet rides | real data | leave alone — per-series RobustScaler already neutralises them (≈0 variance ⇒ ≈0 gradient) |

**The honest lever = the missing signal.** A univariate TFT's encoder already captures
the lagged target (`avg_wait_last_1h`, velocity, `lag_24h`/`lag_1w`) from each ride's own
`y`-window — so those are redundant as `hist_exog`. The one observed-only signal it is
**structurally blind to** is the **cross-ride, park-wide occupancy**: how busy the *whole*
park is right now. That is CatBoost's #2 feature (`park_occupancy_pct`, ~17–20% importance)
and it is exactly what explains *why* a given ride is busy. Feeding realised park
occupancy as `hist_exog` lets TFT learn busy because it **understands the regime** — the
architectural fix (same shape as how TFT only learned holidays once they were first-class
`futr_exog`).

### Experiment: intraday 15-min nowcast (GPU, rolling-origin, available_mask)
`nf-service/backtest_intraday_nowcast.py [park] [n_bases] [plain|occ] [studentt|q0.8]` —
train once < earliest base, predict forward per base with the panel up to base; target =
15-min median; naive baselines (persistence = last real wait held flat; yesterday-same-slot).

**Shanghai Disneyland (37 rides) — TFT vs naive baselines (MAE / bias):**

| segment | TFT | persistence | yesterday |
|---|---|---|---|
| ALL | 10.1 / −5.0 | 13.1 / −4.3 | 12.1 / −5.6 |
| quiet <30 | 4.8 / +2.8 | 7.0 / +3.0 | 5.4 / +1.1 |
| **busy ≥60** | 21.9 / **−20.4** | 22.5 / −17.1 | 23.9 / −16.7 |

**Disneyland Paris (41 rides):**

| segment | TFT | persistence | yesterday |
|---|---|---|---|
| ALL | 7.6 / +0.4 | 11.5 / +1.0 | 8.3 / 0.0 |
| quiet <30 | 5.2 / +2.9 | 8.2 / +2.9 | 4.8 / +1.9 |
| **busy ≥60** | 19.2 / **−18.0** | 25.0 / −12.2 | **17.2 / −12.1** |

**Findings (robust across both parks):**
1. **TFT clearly beats naive baselines on the normal/quiet regime** (ALL + quiet MAE) —
   it is a genuinely good intraday model for typical conditions.
2. **No edge on the busy tail (≥60):** TFT ties persistence and *loses* to
   yesterday-same-slot (Paris 17.2 < 19.2), and under-predicts busy *more* than either
   naive baseline (bias −18 to −20 vs −12 to −17). The busy miss is mean/median-regression
   on skewed per-ride waits — the *same* compression CatBoost has.
3. **The honest signal (park occupancy as `hist_exog`) did NOT fix it** — it regressed:
   ALL 9.1→10.1, busy −16.8→−18.4 (one improvement: near-term lead≤3h bias −4.5→−2.0).
   Naive park-mean occupancy is too crude/collinear and washes out *ride-specific* surges.
4. **Quantile loss (forcing, q0.8) — reference only (Shanghai):** busy≥60 **21.9/−20.4 →
   15.5/−2.5** (huge fix), ALL **10.1/−5.0 → 9.5/+2.9** (even overall MAE improves), at the
   cost of inflating quiet (4.8/+2.8 → 6.7/**+5.8**). → The ONLY lever that moves the busy
   tail is the thumb on the scale. Note intraday is where quantile is *most* justified
   (busy bias −20 is far worse than daily's −8, so the correction is net-positive overall —
   unlike the daily surface where the doc rejected it). Still a distortion (quiet
   over-reads), so not shipped without the user's call — but a real option if intraday TFT
   is ever pursued.

> CatBoost intraday numbers cannot come from stored preds (15-min dedup destroyed past
> forward records); a true head-to-head needs the going-forward shadow OR a CatBoost
> replay. CatBoost's live busy bias was ~−9.6 (challenger doc) — *better* than TFT's −18/−20
> here, because it has real-time velocity/occupancy features TFT lacks. → keep CatBoost on
> intraday; the existing production split (TFT=daily calendar, CatBoost=intraday) holds.

### Loss bake-off (intraday, Shanghai, point forecast vs naive baselines)

| loss (point) | ALL MAE/bias | quiet<30 | busy≥60 | character |
|---|---|---|---|---|
| **StudentT** (prod) | 10.1 / −5.0 | 4.8 / +2.8 | 21.9 / −20.4 | median — good quiet, worst busy |
| MQLoss (median) | 10.8 / −7.6 | **4.1 / +0.5** | 23.5 / −21.7 | best quiet calibration, worst busy |
| HuberMQLoss | 10.7 / −6.5 | 4.4 / +1.5 | 22.3 / −19.9 | robust median, ~same |
| **Tweedie** | 11.0 / −3.2 | 6.4 / +4.2 | 20.0 / −15.1 | honest skew — nudges busy up |
| **Quantile q0.8** | 9.5 / +2.9 | 6.7 / +5.8 | **15.5 / −2.5** | thumb — fixes busy, inflates quiet |

**Conclusion — it's a fundamental skew trade-off, not a model defect.** Median-based
losses (StudentT/MQ/HuberMQ) are best on quiet and worst on busy (the median of a
right-skewed wait distribution sits below the busy reality). Tweedie — the *honest*
distribution for spiky positive data — nudges busy up (−20.4→−15.1) at a moderate quiet
cost (research: Tweedie gives the best high-quantile estimates). Only the upper quantile
(q0.8) substantially fixes busy, at a quiet-inflation cost. **No single point forecast
wins both.** → The principled, non-forcing answer is **probabilistic serving**: keep a
distributional/multi-quantile loss and let the *serving layer* choose the quantile per
purpose (median for a quiet wait display; a higher quantile for the busy/crowd-level
signal). This is research-backed (global models forecast the upper quantiles better) and
is *not* a thumb on the loss — it's serving the right quantile for the right question.

### Implementation audit — `nf-service` (forecast.py/config.py) vs TFT docs

| param | ours | doc default | assessment |
|---|---|---|---|
| `hidden_size` | 64 | 128 | half capacity — stage-2 tune |
| `max_steps` | 500 | 1000 | **likely under-trained** (no early stop either) — testing steps=1500/hidden=128 |
| `early_stop_patience_steps` | unset (−1, off) | −1 | **no validation guard** — needs a val split + early stopping (config.py flags this deferred) |
| `valid_loss` | unset | None | no validation monitoring |
| `scaler_type` | robust | robust | ✓ |
| `windows_batch_size` | 128 | 1024 | reduced for the OOM fix; GPU has headroom now |
| `batch_size` | 16 (daily) | 32 | reduced for memory |
| `start_padding_enabled` | True | False | ✓ correct for our short/gappy series |
| `hist_exog_list` | **none** | None | gap — purely univariate + futr calendar; observed-only signals absent (naive occ tested, didn't help) |
| `n_head`/`dropout`/`attn_dropout` | unset (4 / 0.1 / 0.0) | 4 / 0.1 / 0.0 | ✓ |

**Audit takeaways + MEASURED corrections:**
- **Under-training hypothesis → REJECTED by test.** `max_steps=1500` (vs 500) made it
  *worse*: ALL 10.1/−5.0 → 11.6/−8.2, busy 21.9/−20.4 → 24.1/−21.0 (quiet bias sharpened
  to ~0, but busy + overall degraded). More steps overfits the quiet majority and sharpens
  the median → *worse* on the busy tail. **500 steps is adequate; the model is NOT
  under-trained.** Confirms the busy miss is the fundamental skew/median-regression, not a
  training deficit. (Honest science: the audit guessed under-training; the data said no.)
- **`hidden_size` 64 vs 128 → no meaningful gain.** h128/wb48: ALL 10.1/−5.0 → 10.8/−5.8,
  busy 21.9/−20.4 → 22.4/−18.7 (busy bias marginally better, MAE ~same, overall slightly
  worse — within noise). h96/wb128 OOM'd (16 GB GPU). Capacity is not the lever either.
- `windows_batch_size`/`batch_size` are memory-era settings; idle GPU has headroom (speed,
  not necessarily accuracy).
- Apply to the **production daily model** with care — more training/capacity *sharpens the
  median* and could hurt the busy headliners that are TFT's whole reason for the daily split.

### Data maturity caveat (likely a real contributor)
We have only **~5 months** of history (Dec 2025→now). TFT is a sequence model — it must
have *observed* a regime to forecast it, and per-ride busy episodes are still sparse. This
is consistent with the daily backtest, where TFT **did** beat CatBoost on busy *headliners*
(busy≥70: MAE 22 vs 45) — daily aggregates are less noisy than 15-min slots, so the busy
signal is learnable there but not yet intraday. **Expectation:** intraday busy calibration
should improve as history accumulates → **re-run this backtest periodically** (the same
cadence as the daily re-eval, ~every few weeks). The fix path is *more data + more signal*,
not *less data*.

---

> **Philosophy (user, 2026-05-30): feed, don't remove.** Don't delete training data or
> down-weight to force a view; accumulate history and add *signal*. So hygiene below is
> **fix-the-source + serving-side skip**, NOT training-data deletion: shows should not be
> *created* as attractions (ingestion fix) and should not be *served* a wait prediction
> (serving filter) — but their rows stay; the per-series RobustScaler already neutralises
> flat series, so they don't poison training.

## 4. Data hygiene (prerequisite for everything)

- **Rides only — shows live in a separate table.** `shows`(1943)+`show_live_data`,
  `restaurants`(2909) are separate from `attractions`(5678)=rides; `queue_data` FKs to
  `attractions` → predictions are ~rides-only by design. **But ~contamination exists:**
  shows like *The Hall of Presidents*, *Mickey's PhilharMagic* are mis-ingested into
  `attractions` (from queue-times / wartezeiten-app, which list shows as attractions with
  a "wait"). They predict a flat ~5 min → trivial 0.0 MAE → pollute the accuracy board.
  Name-anti-join to `shows` only catches 53 (most have no `shows` row). `attractionType`
  is empty (useless). **The clean, data-driven discriminator (user's insight):** a series
  whose wait is flat even at p75/p90 (≈ the 5-min floor) is dead/show → it is not worth
  predicting. Scope (30d, ≥20 samples, 2456 attractions): **296 have p90≤5, 580 have
  p90≤10, 447 have p75≤5**. Handle at the data level (drop/quarantine), and fix ingestion
  so SHOW entities are not created as attractions.
- **Schedule-aware operating filter.** Only train/predict slots where the park is open;
  clean handling when no schedule exists. Mirror `ml-service/db.py` (LEFT JOIN
  `schedule_entries` + an operating-day heuristic `odh` when schedule is NULL/UNKNOWN).
- **Sentinel cap.** Mirror `MAX_PLAUSIBLE_WAIT_TIME=400`. Currently a non-issue for
  STANDBY (max 349 in 7d) but keep as a guard so garbage never enters training.

---

## 5. Stage 2 — settings / algo bake-off (DEFERRED, not now)

Once stage-1 (clean comparison instruments + hygiene + the occupancy signal) is landed
and measured, **play with settings** on the idle GPU:

- **Hyperparameters:** `input_size`, `horizon`, `hidden_size`, `max_steps`, learning rate,
  `windows_batch_size`. Current values are PoC defaults (`forecast.py`), explicitly
  "tune up later".
- **Algorithm bake-off** on **clean** data: TFT vs **NHITS** (cheap long-horizon) vs
  **TSMixerx** vs **DeepAR** — "find what fits us best". Use the same backtest harness;
  success = busy MAE on the matched population, not overall MAE.
- **Loss variants** (only if signal alone is insufficient): asymmetric/quantile — but
  measured against the quiet-inflation guardrail, and only on the surface where it is
  justified (intraday busy bias is far worse than daily, where quantile was rejected).

---

## 6. Evaluation methodology — what we evaluated & how

Two instruments, both **forward / leakage-free**, both judged on **busy/headliner MAE+bias,
never overall MAE** (overall is dominated by quiet rides and hides the signal):

1. **Daily forward scoreboard** (production, live): `model_comparisons`, written daily by
   `nf-forecast.processor.ts` `score-comparison`. Both models snapshot their forward
   forecast durably (`tft_forecasts` / `catboost_daily_forecasts`), scored once matured
   against realised daily P90 on the **matched (attraction,target_date) intersection** at
   comparable lead, **segmented all / busy(P90≥40) / headliner**. Surfaced on
   `/v1/admin/system-health`. (Companion offline: `nf-service/backtest_headliners.py`.)
2. **Intraday nowcast backtest** (offline harness): `nf-service/backtest_intraday_nowcast.py
   [park] [n_bases] [plain|occ] [studentt|q0.8|tweedie|mqloss|hubermqloss]`, env
   `BT_STEPS/BT_HIDDEN/BT_WB/BT_WINDOW_DAYS`. Rolling-origin ("train once < earliest base,
   predict forward per base with the panel up to base" = train-nightly/predict-live),
   15-min slots, **`available_mask`** (no 0-fill), target = 15-min median, vs **naive
   baselines** (persistence, yesterday-same-slot), segmented by lead + busy. Run on ≥2
   parks (Shanghai, Paris) to avoid sample-park bias.

**Discipline:** measure-don't-assume — every lever was *measured*, several **rejected**
(occupancy hist_exog, more steps, bigger hidden, busy loss-weighting). Guardrail: a lever
must improve busy/headliner **without** inflating quiet, and beat the naive baseline.

## 7. What to test next — and when

| # | test | when / trigger | success metric |
|---|------|----------------|----------------|
| 1 | **Deploy** the clean segmented daily scoreboard | on approval (ready now) | board shows matched n + busy/headliner segments populating |
| 2 | **Probabilistic serving** PoC: serve a higher quantile (MQLoss/StudentT band) for the busy/crowd-level read, median for the quiet wait display | after #1 + ~1 week of scoreboard data | busy calendar reads lift without quiet over-reading; no loss-forcing |
| 3 | **Re-run daily + intraday backtests** (history has grown) | **~2026-06-14**, then every ~3 weeks | does busy MAE close as history accumulates? recalibrate the ≤30d horizon |
| 4 | **Data hygiene** (feed-not-remove): ingestion fix so SHOW entities aren't created as attractions + serving-side skip for flat (p75≤5) series | next sprint (independent of ML) | shows drop off the accuracy board; no training-data deleted |
| 5 | **Stage-2 algo bake-off** on clean data: TFT vs NHITS vs TSMixerx vs DeepAR (same harness, GPU) | **~2026-06-14** with #3, or when ≥8 mo history | busy/headliner MAE beats TFT-studentt baseline |
| 6 | **Re-evaluate the whole split** (TFT≤30d / CatBoost intraday+far) | when history **≥1 year (~2026-12)** | can TFT extend its horizon / take intraday as data matures? |

**Standing rule:** nothing flips production without a backtest win on busy/headliner.
Champion stays CatBoost; TFT serves only the near-term daily calendar (≤30d). Re-eval
cadence is shared with the [daily split doc](./neuralforecast-tft-evaluation.md) (next
~2026-06-14).

---

## Sources (research, verified 2026-05-30)
- [Temporal Fusion Transformers (Lim et al. 2019)](https://arxiv.org/abs/1912.09363) — static / observed-only (`hist_exog`) / known-future input separation; attention over fixed lags.
- [Picnic Engineering — TFT demand forecasting deep dive](https://blog.picnic.nl/under-the-hood-of-picnics-demand-forecasting-model-a-deep-dive-into-the-temporal-fusion-e281604d65a5) — practitioner use of hist vs future covariates.
- [NeuralForecast TFT docs](https://nixtlaverse.nixtla.io/neuralforecast/models.tft.html) — `hist_exog_list` / `futr_exog_list` / `stat_exog_list`; lagged-target-as-future-exog pattern.
- [Any-Quantile Probabilistic Forecasting (2024)](https://arxiv.org/abs/2404.17451) & [SPADE — Split Peak Attention (2024)](https://arxiv.org/pdf/2411.05852) — quantile methods struggle with rare extreme tails; peak-aware structure / richer features preferred over loss-forcing.
- [Intermittent TS forecasting with GP + Tweedie likelihood (2025)](https://arxiv.org/html/2502.19086v4) — **Tweedie gives the best high-quantile estimates** (matches our bake-off); median-demand scaling eases optimisation. Validates Tweedie as the honest skew lever.
- [Intermittent TS: local vs global models (2026)](https://arxiv.org/html/2601.14031v1) — **global models forecast the upper quantiles (0.8/0.9/0.95) better** → supports a global TFT + upper-quantile serving for the busy tail.
- [Short-term load forecasting — post-training specialization (2024)](https://www.sciencedirect.com/science/article/abs/pii/S1040619024000848) — **"data sparsity at peaks can degrade DL below much simpler models"** — exactly our finding (TFT loses to naive baselines on busy); supports the data-maturity hypothesis + specialised peak models.
- [Unilateral boundary forecasting / UMSE asymmetric loss (2024)](https://www.frontiersin.org/journals/big-data/articles/10.3389/fdata.2024.1376023/full) — asymmetric loss penalising underestimation (a principled but still-asymmetric "forcing" lever, for reference).
- [NeuralForecast losses](https://nixtlaverse.nixtla.io/neuralforecast/losses.pytorch.html) & [TFT model params](https://nixtlaverse.nixtla.io/neuralforecast/models.tft.html) — full loss menu (Tweedie/MQLoss/HuberMQLoss/ISQF) and TFT defaults used in the implementation audit (max_steps=1000, hidden=128, early_stop_patience_steps, etc.).
