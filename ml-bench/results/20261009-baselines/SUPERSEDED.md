# Superseded by `20261009-baselines-v2`

This run was scored before the PAR-827 review fixes and must not be quoted. Its
ride-day decision metrics were paired only per park-day, its opening-aligned
forecasts read schedule rows published after the origin, `prod_served` was not
the production profile, and the horizon curve was measured on the full period,
where lead is confounded with season.

Use `ml-bench/results/20261009-baselines-v2/` instead — same export
(`/data/parkfan/ml-bench/exports/20261009`), same 231 origins, code `ea9a8438`:

- `summary_tables_2026-08-15_2026-10-07.md` + `tables_2026-08-15_2026-10-07/` —
  the headline, a common target window so every lead is scored on the same days.
- `summary.md` + `tables/` — the full period, secondary.

The numbers of this run are in the first revision of PR #445 and in git history.
