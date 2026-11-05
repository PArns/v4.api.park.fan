# Forward archive of served curves (PAR-831)

The forward archive answers one question nothing else in the database can: **how good was the
curve a visitor was actually served, one hour to ninety days before their visit?** Every forecast
table keeps only the freshest answer per target (`wait_time_predictions` rewrites future rows on
each run, `prediction_accuracy` keeps the last prediction per slot), so by the time a day can be
scored, the 3-day-ahead curve of it is gone. The archive captures the served curves at fixed
origins, scores them daily against BENCH-SPEC truth, and is the live half of every model switch
decision. Open this folder before touching the capture, the scoring, the admin model comparison
or the Chronos-2 shadow and serving path.

For the daily number (`/plan/day` `leadTimeMae`) the equivalent forward record is
`prediction_lead_snapshots` ([long-range-forecasting.md](../long-range-forecasting.md#6-the-lead-time-snapshot-archive)).

| Page | What it covers |
| --- | --- |
| [capture.md](capture.md) | What is captured at which origin, every surface and column, size and DB impact, retention |
| [scoring.md](scoring.md) | Truth, the daily scores per use case × lead × source × segment, the admin board, open follow-ups |
| [chronos-shadow.md](chronos-shadow.md) | The Chronos-2 shadow (PAR-852): variants, covariates, information cut, paired board, safety switches |
| [model-comparison.md](model-comparison.md) | The admin model comparison (PAR-855): paired scoring of every model against what is served, questions, cell states, endpoints |
| [chronos-serving.md](chronos-serving.md) | Chronos-2 served on the 45–120 min slots (PAR-911): lead routing, serving cache, switch-on checklist and rollback |

## Code and jobs

| Piece | Where |
| --- | --- |
| Capture, retention | `src/parks/services/forecast-archive.service.ts` |
| Truth, scoring, board | `src/parks/services/forecast-archive-scoring.service.ts`, pure metrics in `src/ml/utils/forecast-archive-scoring.util.ts` |
| Model comparison | `src/parks/services/model-comparison.service.ts`, `src/ml/utils/model-comparison.util.ts`, `src/ml/utils/weekday-average.util.ts`, DTO `src/ml/dto/model-comparison.dto.ts`, offline verdicts `src/ml/data/offline-model-verdicts.json` |
| Chronos-2 | `src/parks/services/chronos-shadow.service.ts` (shadow call), `src/ml/chronos-serving.ts` (serving), nf-service `chronos_shadow.py` / `chronos_data.py` |
| Queues | `src/queues/processors/forecast-archive.processor.ts`: queue `forecast-archive` (`capture` hourly at :05 UTC; `driver-level` daily 01:30 UTC) and queue `forecast-archive-score` (`score` daily 12:00 UTC — its own queue so a long scoring run never delays a capture) |
| Tables | `forecast_archive_curves`, `forecast_archive_park_days`, `forecast_archive_scores`, `forecast_archive_paired` — plain tables, no FK, app-side retention |

Downstream readers: the admin ML overview ([ml-overview.md](../ml-overview.md)) derives its open
decisions from the model comparison; ml-bench's [served arm](../ml-bench/served-arm.md) replays
`forecast_archive_curves`.

## When the first scores appear

The archive started with the first capture after deploy (`ARCHIVE_START` = 2026-10-09). Day *D*
is scored at 12:00 UTC on *D + 1*: UC1, UC2, D1 and every `d0` score the day after deploy; lead
*N* needs a capture *N* days before its target, so it first reports on deploy + *N* + 1 days (d7
after 8 days, d30 after 31, **d90 after 91**). A lead has a stable pooled number about two weeks
after its first report (~4,000 ride-days per lead per day).
