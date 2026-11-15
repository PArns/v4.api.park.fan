# ML review measurements

Three open model questions from the review of 2026-11-14 that need production data before
anything is built. Each measurement is read-only. Run them the next time database access is at
hand, in this order; the Linear issue of each holds the background.

| # | Question | Run | Issue |
|---|---|---|---|
| 1 | What would a hurdle buy on rides that rarely have a queue? | `scripts/prod-psql.sh < scripts/ml-review/hurdle-replay.sql` | PAR-1335 |
| 2 | Is there enough curated `park_seasons` history to feed a model, and how much busier are event days? | `scripts/prod-psql.sh < scripts/ml-review/park-season-coverage.sql` | PAR-1116 |
| 3 | Is `TFT-median` a better point than the served sample mean? | `docker exec <nf> env NF_BACKTEST_BASE=<date> NF_BACKTEST_END=<date+14> python3 /app/backtest_headliners.py` | PAR-1334 |

All SQL goes through `scripts/prod-psql.sh` (5-minute statement limit); never `docker exec … psql`
by hand. Both files ran against the schema in the e2e Timescale container on 2026-11-14.

## 1. Hurdle replay

Two result sets. The first compares the stored forecast with the hurdle (0 where a ride had a
queue on fewer than half of its scored slots in the two weeks before) per segment:

- `rarely_queues = t`, `quiet`: the gain. `mid` and `busy` in the same group: the price, the rare
  queue hours the hurdle misses.
- `rarely_queues = f`: must show identical served and hurdle figures.

The second shows how many rides and slots sit in each tenth of `p_queue`; the first five rows are
what the rule touches.

**Build it when** the `rarely_queues = t` rows together lose fewer minutes on `mid` and `busy`
than they win on `quiet` (weight each row's MAE difference by its `n`), and say so per segment in
the issue. Then: a nightly `p_queue` per ride, the rule behind a flag, and a shadow in the forward
archive that scores real zeros.

## 2. Park-season coverage and lift

Four result sets. The first two are the inventory of `park_seasons`: per kind, status and shape
(a list of single days in `dates`, or a plain range), with separate-ticket and event-hour
seasons and bound rides counted apart; then the same per park. A model may read listed single
days of a published season as fact, a plain range with care (a weekends-only event recorded as a
range claims its weekdays too), and an `expected` season not as fact. The third counts seasons,
parks and past event days per kind. The fourth compares the headliner day value on event days
with the same park's other days of the same month and day type (weekday or weekend).

A season that covers every day of a month (Phantasialand's Wintertraum) has no comparison days
in that month and drops out of the second result; read its level against the months around it.

**Build a covariate when** a kind has at least ten parks and a few hundred past event days, and
its `lift` is clearly away from 1. First as its own TFT shadow, not inside the drivers shadow
that is judged on 2026-12-24.

## 3. TFT mean against median

`backtest_headliners.py` scores `TFT` (the mean of 1,000 draws, what production serves) and
`TFT-median` from the same fit on the same rows and prints both per segment. Pick a holdout of
14 days that ended at least a day ago. It trains on the GPU: run it outside the nightly windows
(TFT 03:00, CatBoost 06:00, PCN 08:30 UTC) and under the ml-bench GPU lock.

**Switch the served column when** the median wins on `busy>=40` and `busy>=70` and does not lose
on `quiet<40`; the change is one line in `nf-service/main.py` `_tft_column`.
