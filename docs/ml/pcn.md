# PCN — the intraday nowcaster (pcn-service)

PCN (Park-Crowd Nowcaster) forecasts every ride's next 12 hours in 15-minute slots from the
current state of the whole park. This page covers the model, its targets, the nightly training,
the 15-minute forecast tick, the `pcn_forecasts` table and the shadow boards. How the API serves
PCN's numbers is on [Intraday serving path](intraday-serving.md).

## Why a third model

Intraday is where the live state decides the next hours and where the wait distribution is most
skewed. Neither existing model saw the park as a whole: CatBoost gets the park's crowd as one
scalar (`park_occupancy_pct`), the TFT is univariate per ride and lost to persistence on busy
slots ([TFT](tft.md#rejected-measured)). Under the old single-quantile loss CatBoost's live intraday
bias was +11.2 on quiet slots and −11.3 on busy ones (2026-06-10). PCN models the rides of one park
as a graph whose coupling it learns, so "this ride is busy because the whole park is" becomes
structure rather than a hand-built average.

Nothing in the literature models that shared park state for theme parks (literature review,
2026-06-29); the machinery comes from traffic forecasting, where series couple without a given graph — AGCRN's
adaptive adjacency and node-adaptive parameters, Graph WaveNet's self-adaptive adjacency.

## Model

| | |
|---|---|
| Architecture | `PCN_ARCH=graphwavenet`: gated, dilated, causal TCN over time + a graph layer whose adjacency `softmax(ReLU(E·Eᵀ))` is learned from node embeddings (`backbones.GraphWaveNetModel`) |
| Depth | `PCN_GWN_LAYERS` = 8, kernel 2 → receptive field 1 + Σ2ⁱ = 256 slots, more than the context |
| Context / horizon | `PCN_INPUT_SIZE` 192 slots (2 days), `PCN_HORIZON` 48 slots (12 h) |
| Size / steps | hidden 64, 500 steps, at most 6000 training windows per park |
| Loss | `PCN_LOSS=quantile`: masked pinball at q0.5 / q0.8 / q0.9, outputs made monotonic (`metrics.enforce_quantile_monotonicity`); `tweedie` exists but never ran on the board |
| Scale | one per park: the median absolute observed wait of the training slots, stored in the checkpoint |
| Granularity | one model per park, `/app/models/pcn_<park>.pt`; the node order is the park's ride list at training time |
| Persisted | q0.5 (shown wait) and q0.8 (crowd signal), `PCN_SERVE_QUANTILES` |

Alternatives in `backbones.build_registry`: `gpstgnn` (AGCRN-style recurrent net with a
probabilistic head, `gp_stgnn.py`) and `localgru` (the no-graph ablation). All share
`TorchSeqModel` (`torch_model.py`), so a difference between them is the architecture, not the
training loop. A checkpoint loads with its own trained `arch_kwargs`, `target_mode` and
`residual`, so changing a default never changes what an old checkpoint serves.

### Tensor channels (`tensor.CHANNELS`)

| Channel | Meaning |
|---|---|
| `wait_ffill` | the ride's wait, forward-filled (context input) |
| `obs_mask` | 1 where a real operating wait was observed |
| `down` | DOWN readings in the slot (reopening rebound) |
| `slot_sin/cos`, `hour_sin/cos` | time of day |
| `park_occ` | mean wait over the park's observed rides — the scalar crowd baseline the graph refines |
| `dow_sin/cos`, `is_weekend` | day of week (added 2026-07-02) |

`park_open` = at least `PCN_MIN_RIDES_OPEN` (3) rides report a reading or a DOWN in the slot. The
loss mask is `obs_mask × park_open`: closed slots and sensor gaps never enter the loss (the
0-filled hourly TFT PoC of 2026-05-23 was biased by exactly those zeros). **Channel contract:**
the checkpoint stores the channel names it was trained on and `_model_features` selects by name,
so a channel appended later never breaks an older checkpoint — it serves until its retrain.

### Targets: `PCN_TARGET_MODE` (PAR-1034)

`queue_data` is a change log: a row is written when the wait or status changes and otherwise
about once an hour. The default target, `median`, is the median of the rows written inside each
15-minute slot, so a ride whose wait did not move appears in about one slot of four, and slots
where the wait just changed weigh ~4× in training and in PCN's own board — the regime where the
last observation says least. That is how "PCN beats persistence" offline and the 40-minute
anchor gate of the blend came about.

`in_force` builds targets, the `obs_mask` / `wait_ffill` / `park_occ` channels derived from them,
`park_open` and the board's actuals from the **BENCH-SPEC truth** — the rule of `buildTruthSlots`
(`src/ml/utils/forecast-archive-scoring.util.ts`) and `nf-service/chronos_shadow.truth_slots`: the
reading in force at the slot midpoint (newest row at or before it, ≤ 3 h old), counted only when
it is OPERATING with a wait ≥ `PCN_MIN_WAIT` and the midpoint lies inside the park's published
OPERATING window. `db.fetch_in_force_readings` returns the last row of (midpoint − 15 min,
midpoint] per ride and slot in any status; `tensor.in_force_truth` forward-fills it and drops it
past 3 h, so a CLOSED or DOWN row supersedes the OPERATING one before it.

Deliberate differences from the canonical rule:

- Dates before the park's first published window (all dates for a park that publishes none) fall
  back to the old slot heuristic (≥ `PCN_MIN_RIDES_OPEN` rides with a reading or a DOWN).
- Ages are measured on the park-local wall clock — off by the DST hour on the two switch nights,
  when parks are closed.
- `fetch_operating_windows` merges a day into one window, min(open)–max(close), so a midday
  closure between two published windows counts as open.
- "First published window" is relative to the fetched range (548 days in training, 7 on the
  forecast tick, the board's lookback).
- A reading bins into the slot whose midpoint follows it, so the newest reading can land one slot
  past the grid's end and the last context slot reads the previous one — up to ~15 min staler
  than under `median`.

**The checkpoint records its mode** (`target_mode`; missing = `median`). `forecast.py` builds a
park's inputs under the loaded checkpoint's own mode and logs an error when it differs from the
env, so a flip before the retrain does not feed an old model inputs it was not trained on. Flipping
the mode also changes the board's actuals: `pcn_intraday_comparisons` and
`pcn_lead_curve_comparisons` rows before and after a flip are not comparable (no definition
column).

**Before flipping:** with `in_force` training runs a second 548-day scan per park
(`fetch_in_force_readings`, a `DISTINCT ON` over every STANDBY row of compressed `queue_data`) —
expect the nightly PCN DB time to roughly double. Time it on one large park through
`scripts/prod-psql.sh` outside 05:50–07:30 UTC first. **A/B:** offline
`PCN_TARGET_MODE=in_force python3 run_bakeoff.py` (each mode scores against its own truth there, so
only the in-force run is comparable to the forward archive); live, train with the flag on and
compare the `pcn` surface against served-before and persistence on `forecast_archive_paired`
definition-2 rows for the same parks and dates. There is no second PCN instance for a
same-night shadow.

### `PCN_RESIDUAL` (default off)

With `PCN_RESIDUAL=true` the prediction is the last context `wait_ffill` plus the net's output, so
the net learns the change from the last observation instead of the level. Stored in the
checkpoint; older checkpoints read as absolute. `run_bakeoff.py --residual` compares offline.

### Inputs that are never read

Every PCN `queue_data` read — the panel, the freshness pre-check, the board's actuals, the
in-force readings — drops heartbeat rows (`NOT COALESCE(qd.is_heartbeat, false)`, the forward
archive's `truthRowsSql`). A heartbeat copies the last status and wait forward for up to 24 h for a
ride that vanished from every feed; in the median panel it invented slots with a frozen wait, and
under `in_force` it would reset the 3 h staleness clock. A live feed's hourly "unchanged"
re-write is not a heartbeat and stays — it is what keeps an unchanged ride in force.

## Nightly training

`pcn-shadow/train-pcn` at 08:30 UTC — after TFT (03:00) and CatBoost (06:00), so the GPU and
memory spikes never overlap — calls `POST /train`, which takes the `is_training` lock before it
starts `train_runner.py` as a subprocess (a second `/train` in that gap once launched a second
run, PAR-943) and returns 409 while one runs. The processor polls up to 90 min. Per park
(`train.train_park`): the 548-day panel (`PCN_WINDOW_DAYS`), every valid base, `fit`, atomic save.
One failing park does not stop the run, and its previous checkpoint keeps serving. On boot the
service resets a stale training lock and the per-kind forecast/score locks a restart left behind
(a stuck forecast lock froze forecasts for 24 h on 2026-06-30). Manual
trigger: `POST /v1/admin/pcn/train` (also `forecast`, `score`). When a run fails, the previous
models keep forecasting; `/admin/ml-overview` reads `GET /train/status` for it (PAR-1053).

## The forecast tick

`pcn-shadow/forecast-pcn` every 15 minutes → `forecast.forecast_park` per park:

1. **Fresh-data pre-check** (`db.park_has_fresh_data`): no STANDBY row in the last
   `PCN_MAX_ORIGIN_AGE_HOURS` (36) → skip, before any panel fetch. A seasonally closed park would
   otherwise write forecasts whose targets no window ever scores.
2. **Short window:** the panel covers `PCN_FORECAST_WINDOW_DAYS` (7) — inference needs the 2-day
   context plus warm-up; the 548-day fetch every tick cost ~100× the DB time and RAM. A grid
   shorter than the context falls back to the full window once.
3. Inputs under the checkpoint's own target mode; the grid's newest slot must also be < 36 h old.
4. **Ride axis:** `tensor.align_ride_axis` reindexes to the trained node order. A trained ride
   missing from the window becomes a synthetic quiet node whose outputs are not written; a new
   ride is dropped and falls back to CatBoost until the retrain. (A strict-equality check used to
   disable PCN for the whole park over one roster change.)
5. **Unchanged origin:** the in-process `_written` memo skips an origin already written with the
   same checkpoint. A closed park's origin stops moving; without the memo every tick re-predicted
   it (2026-10-01: 84 of 125 parks, 145 k of 226 k rows per tick).
6. Writes 48 slots × q0.5 / q0.8 per ride from the newest slot (`origin_slot`) into
   `pcn_forecasts`.

## `pcn_forecasts`

```
attraction_id uuid, target_slot timestamp (park-local), origin_slot timestamp (park-local),
quantile real (0.5 | 0.8), predicted_wait double precision (raw), model_version text,
created_at timestamptz   PRIMARY KEY (attraction_id, target_slot, origin_slot, quantile)
```

A plain table (not a hypertable), created by pcn-service (`CREATE TABLE IF NOT EXISTS`, once per
process — run on every write it took a SHARE lock that queued behind VACUUM, PAR-868). One row per
origin, so the forward record survives for scoring. Indexes: `idx_pcn_forecasts_serving`
(`attraction_id, created_at DESC`) for the API's 3-hour read — without it that read was the
slowest query (median ~600 ms) — and `idx_pcn_forecasts_created_at` for the prune. Values stay
raw; only serving and the boards round.

### Conditional upsert (PAR-873)

```sql
ON CONFLICT (attraction_id, target_slot, origin_slot, quantile)
DO UPDATE SET predicted_wait = EXCLUDED.predicted_wait,
              model_version  = EXCLUDED.model_version,
              created_at     = now()
WHERE abs(pcn_forecasts.predicted_wait - EXCLUDED.predicted_wait) > 0.001  -- REWRITE_TOLERANCE_MIN
```

On 2026-10-12 the table held 28 GB (122 M rows at 14-day retention) with 1.73 B updates against
1.06 B inserts, 0.2 % HOT. Most rewriting had stopped with the `_written` memo; what remained was
the **first tick after each deploy**, which re-predicts every closed park from the same checkpoint
and panel (the memo is empty after a restart) — 26–195 k rows of 12–82 parks per deploy, 4.8 % of
a day's writes, each a dead tuple and three index entries, and each moved `created_at` to now,
re-arming the API's 3-hour guard for a park it had already handed back to CatBoost.

- "Changed" is more than a thousandth of a minute: GPU inference is not bit-reproducible across
  processes, and served waits are whole minutes.
- `created_at` keeps one meaning — when this origin was forecast with these values. An open park
  writes new origins every tick (inserts with a fresh `created_at`), so no heartbeat column is
  needed.
- A retrained checkpoint that changes an origin's values still rewrites and restamps those rows.
  `model_version` is not compared (it is the tick's timestamp).
- `test_db_write_volume.py` (against Postgres): a re-send, also one jittered by 10⁻⁵, writes zero
  row versions and leaves the serving read identical; a changed value rewrites exactly the changed
  rows; a closed park stays out of the 3-hour window after a re-send.

### Retention: 4 days

| Reader | Where | Oldest row it reads |
|---|---|---|
| Serving override | `MLService.getPcnIntradayWaits` | `created_at` ≥ now − 3 h |
| Deviation badges | `PredictionDeviationService.getServedPcnWait` | 3 h |
| Served-accuracy recorder | `MLService.servedIntradayValues` | 3 h |
| ML overview probe | `MlOverviewService.components` | newest row, last 2 days |
| Main board | `score.py` → `fetch_pcn_forecasts_window` | targets from yesterday 00:00 park-local, origins up to 12 h earlier: ≤ 60 h |
| Lead-curve board | `score.py` → `fetch_pcn_leadcurve_window` | same, leads ≤ 6 h: ≤ 60 h |
| Park merge | `merge-dependencies.ts` | re-points whatever exists |
| ml-bench | — | does not read it |

The scorer binds at ~60 h; 4 days (`PCN_FORECAST_RETENTION_DAYS`, the owner's floor) is the
smallest whole-day value with a full day of margin. `test_db_write_volume.py` pins retention ≥ the
scorer's oldest row + 14 h. **An ad-hoc lead analysis has to export within 4 days of its targets.**

`prune_pcn_forecasts` runs in the hourly score job: by `ctid` in batches of 200 k rows, one
transaction each, at most 50 batches per call — one huge `DELETE` would hold a snapshot for its
whole run. The ctid trick is correct only while the table is plain; a hypertable would need the
statement rewritten (and a retention policy would replace it). Deleted space is reused by the
table (~8 GB with indexes at 4 days); to shrink the file, either `VACUUM FULL pcn_forecasts` in a
quiet window **with `SERVE_PCN_INTRADAY` off first** (the serving read would wait on the ACCESS
EXCLUSIVE lock rather than fall back), through `scripts/prod-psql.sh` with a raised
`PARKFAN_PSQL_STATEMENT_TIMEOUT`, outside 01:00–09:30 UTC; or `pg_repack` if the image ever has
it. The weekly `REINDEX INDEX CONCURRENTLY pcn_forecasts_pkey` (Sunday 19:10 UTC) is described in
[Nightly DB maintenance](../troubleshooting/nightly-db-maintenance.md); do not deploy pcn-service
on Sunday 19:10–20:05 UTC.

## Shadow boards

`pcn-shadow/score-pcn` hourly → `score.score_all`, read by `GET /v1/admin/ml-comparison` (with a
PCN − CatBoost verdict per segment) and the `pcnComparison` / `pcnLeadCurve` blocks of
`/v1/admin/system-health`.

| Board | Compares | Buckets |
|---|---|---|
| `pcn_intraday_comparisons` | PCN q0.5 (served rounding) vs CatBoost's freshest stored prediction vs the realised 15-min median, inner-joined on (ride, slot) so both have the same n | segment `all` / `quiet` < 30 / `mid` / `busy` ≥ 60 × lead `all` / `<=3h` / `3-6h` / `>6h` |
| `pcn_lead_curve_comparisons` | PCN made 1 / 3 / 6 h before each target vs persistence (the actual L hours before) vs `pcn_blend` (the served blend) | segment × lead `1h` / `3h` / `6h` |

- **Full-day contract** (`full_day_window`, `PCN_SCORE_LOOKBACK_HOURS` = 48): a target date is
  only (re)written from a window that covers it completely — yesterday from 00:00, today up to the
  last finished slot. A 24-hour rolling lookback overwrote matured days with ever smaller windows
  until each held its last hour (lead buckets summed to more than `all`, > 90 % of the matched
  slots lost). Rows written before that fix (2026-07-02) are not usable.
- **Pooled, no park dimension:** all parks are aggregated and written in one delete-and-replace
  per date. A date is frozen once it leaves the easternmost timezone's window (UTC+14,
  `_freeze_old_days`), so a late rewrite by the western parks alone cannot shrink it.
- The main board scores the **freshest** origin per target, so its lead is ~15–30 min under
  healthy operation; `3-6h` / `>6h` fill only from stale-producer phases. The longer leads the UI
  serves are the lead-curve board's job.
- `fetch_actuals_local` still filters `waitTime >= 5` literally rather than `PCN_MIN_WAIT` (equal
  today).

The independent check is the forward archive, which scores the served curve against BENCH truth
(`forecast_archive_paired`, definition 2) whatever PCN's own board says —
[forward archive](forward-archive/README.md).

## Measured

- **Backbone and depth** (`run_bakeoff.py`, 8 busy parks, input 192, quantile loss, 2026-07-02):
  GraphWaveNet at 8 layers beat 2 layers on every segment and lead (busy MAE 18.8 vs 20.1), beat
  the recurrent `gpstgnn` on busy (18.8 vs 19.4) at ~6× less compute (8.6 s vs 54 s per park,
  ~97 % vs ~35 % GPU), and turned the ≤ 3 h loss against persistence into a win (9.8 vs 10.4). At
  2 layers the receptive field had been 4 slots — one hour of a 192-slot context.
- **Champion swap** (2026-06-29): PCN beat CatBoost on every segment of the matched board and
  replaced its hourly slots behind `SERVE_PCN_INTRADAY`. The board's figures from that period were
  distorted by the rolling-window bug above.
- **`is_holiday` channel**: a clean A/B came out flat; not in the channel set — do not re-add it
  without new evidence.

## Rejected before it was built

From the design of 2026-06-29: hand-rolled per-ride cross-attention (replaced by the learned
adjacency, which the literature had already worked out); a full transformer per ride (needs more
history than ~6 months); the park-mean occupancy as the only crowd input (measured on the TFT: it
regressed); a residual hybrid on top of CatBoost (two systems, the skew only half addressed).

## Open

- **PAR-453** — no validation split or early stopping: a fixed 500 steps for a 10-ride and a
  100-ride park alike; hold out the last day, early-stop, log the per-park final loss. A per-ride
  scale (headliner and walk-on share one today) is an ablation for the same ticket.
- **PAR-455 / PAR-456 / PAR-457** — weather, schedule-relative time (minutes since open / to close,
  a `park_open` channel) and school-break channels, each through the bake-off on busy MAE/bias.
  The worst-MAE rides (Cheetah Hunt, Wolfpack Raft Slide, Manta) are water and outdoor rides.
  Measure PAR-456 on top of `in_force`: `wait_ffill` carries the last value through closures,
  which the in-force target no longer does.
- The `in_force` flip and `PCN_RESIDUAL`, as described above.
- A regime head (P(busy)) or the Tweedie loss on the board — after the channels, because a
  missing signal is not repaired by a head.
- Skipped parks and their reason are visible only in the log.
- TouringPlans' public Walt Disney World dataset (posted and actual waits since 2012) as a
  pretraining seed against the short history — untested.
- `pcn-service/README.md` still describes "Phase 0" and lists the NestJS wiring as next; the code
  is past both.
