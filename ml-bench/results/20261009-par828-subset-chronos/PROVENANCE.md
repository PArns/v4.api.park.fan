# Provenance of `20261009-par828-subset-chronos` — and why `git_sha` says "unknown"

`run-0of7.json` records `"git_sha": "unknown"`. This file establishes what code
actually ran, by other means, and records what that code was missing.

## Why the SHA was lost

Three independent mechanisms had to fail, and all three did:

1. The image was built **without `--build-arg GIT_SHA`**. The Dockerfile's default
   is `ARG GIT_SHA=unknown`, so the image carries `MLBENCH_GIT_SHA=unknown`
   (`docker image inspect -f '{{json .Config.Env}}' ml-bench:par-828` still shows
   it). The build log of 2026-10-09 22:17 has no `GIT_SHA` build arg.
2. `git_sha()`'s fallback shells out to `git rev-parse HEAD` with
   `cwd=Path(__file__).parent`. Inside the container that is `/app/mlbench`, and
   the image has **no git binary** (`python:3.11-slim` ships none) and **no
   repository** — the code arrives via `COPY . .`. The compute checkout on the host
   is itself a `git archive` extract with no `.git`, so even a bind mount would not
   have helped.
3. `scripts-par828/run.sh` passed `-e MLBENCH_IMAGE_ID=…` but **not**
   `-e MLBENCH_GIT_SHA=…`.

`run --reference` refuses to start without a known SHA, and would have caught this.
The subset run did not use it (`"reference": false`), which is why the gate never
fired. Fixed in this branch: the Dockerfile also writes `/app/GIT_SHA`, `git_sha()`
reads that file before falling back to git, and `run.sh` now refuses to start unless
the worktree is clean, its SHA contains the harness fix the scores depend on, and the
image was built from exactly that SHA — and re-reads every `run-*.json` afterwards,
declaring the run VOID if the recorded SHA is wrong.

## What code actually ran

`run-0of7.json` records `code_sha256: d82b5a17a2b7f8153538c0ae6bf8696a2515b1dd6459b4139a3fa5947f584ccf`,
a sha256 over the `mlbench` sources (relative path + bytes of every `*.py`, sorted).
Recomputing it over the compute checkout `~/claude-worktrees/par-828/ml-bench/mlbench`
reproduces that hash **exactly**, so that directory is byte-for-byte the code that ran.
Recomputing it over every candidate commit on this branch and on the PAR-827 branch
matches **none** of them: the run was launched from an uncommitted working tree.

| | |
|---|---|
| `code_sha256` | `d82b5a17a2b7f815…` (reproduced from the compute checkout) |
| image id | `sha256:8df8c8d09abc7a069a6aa59f5238f6d2ecc3d1caf91a23df38c0960fc02658d5` — still the `ml-bench:par-828` tag |
| export | `/data/exports/20261009`, finished 2026-10-09T17:35:33Z (manifest committed as `export-manifest.json`) |
| harness base | PAR-827 `43f73437`, **not** `ea9a8438` — the branch was rebased after the run started |

## What that code was missing — and what it invalidates

Diffing the code that ran against this branch's `mlbench/` shows it predates three
PAR-827 fixes:

- **`689a7b84`** — the `UPDATE tg SET <models> = NULL WHERE ko < 0 OR kc < 0 …` that
  stops a baseline forecasting outside the opening window known at the origin. The
  nulled list is `SLOT_MODELS + ORACLES + ["wt_q80", "wt_q95"]`, i.e. **built-in
  baseline columns only**; plug-in columns are never in it. So the baseline arm was
  credited with slots the plug-in arm never covered.
- **`336b2b62`** — projected windows and the plug-in horizon grid rounded to the
  quarter hour.
- **`a1d20bf8`** — `floor(x/15)` instead of DuckDB's `//` for `ko`/`kc`, which
  truncates toward zero, so a slot just outside the window read as `ko = 0`, the
  first slot after opening. This is the fix that bears directly on the first-hour
  (D5 / D2) segmentation.

Consequences, applied throughout the reports in this directory:

| Comparison | Status |
|---|---|
| any Chronos-2 variant vs `wt_med`, `h5`, `clim`, `snaive7`, `lvlh5_*`, `prod_served` | **VOID** — 925 of 942 `chronos2*` cells |
| Chronos-2 vs `persistence` (UC1 slot MAE, D2 live window) | **valid** — `persistence` is built in `runner.ih_{h}` from the raw `slots` table, appears in no `SLOT_MODELS` list and never passes through `UPDATE tg`; 17 cells |
| variant vs variant (`chronos2` / `_grid` / `_nocov` / `_owx`) | **valid** — all four are plug-ins on the same grid; `n__<variant>` is identical to the unit in all 18 UC2/UC3 MAE cells |

Nothing in this directory may be compared with
`ml-bench/results/20261009-baselines-v2/`, which was scored on the fixed harness.

## Park list

`run-0of7.json` lists 32 `parks` entries, two of which — `"10 NA"` and
`"8 Asia (large and small parks)"` — are fragments of the subset file's comment
header, split on commas before the `#` was cut. They match no park and were dropped
silently (the refusal and the per-line comment cut landed later, in
`PAR-828: cut an @file's comment per line, and refuse a park id that is in no
export`). 30 real parks took part, as intended; see `variant-pairs.md` for the
count read back out of `parts/slot`.
