"""Turn a run's per-origin aggregates into the BENCH-SPEC outputs.

Writes into the run directory:

* ``tables/*.csv`` — one table per use case / decision metric, all with CIs;
* ``tables/horizon_curve.csv`` — metric vs lead per model and use case, with the
  per-lead reference and the paired difference CI;
* ``tables/usable_horizon.csv`` and ``tables/handover.csv`` — the headline;
* ``summary.md`` — every table above in readable form, one section per use case
  and per decision metric (D1–D9).

Reference per lead (BENCH-SPEC "Horizon"): the best naive baseline at that lead
among persistence → seasonal-naive → weekday-median → climatology, chosen on the
overall MAE (or on the decision metric itself for decision metrics), and only
among candidates that cover at least half of the scored rows.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import numpy as np
import pandas as pd

from . import baselines as B
from .config import UC4_LEADS, BenchConfig
from .stats import paired_diff_ci, ratio_ci, significant

UC_LEADS = {
    "UC2": [0, 1],
    "UC3": [1, 2, 3, 4, 5, 6, 7, 10, 14, 21, 30, 45, 60, 90],
}
UC1_KEYS = ["m015", "m030", "m045", "m060", "m075", "m090", "m105", "m120"]
UC2_INTRADAY_KEYS = ["h2-4", "h4-8", "h8+"]
NON_COMPETING = set(B.ORACLES)


# --------------------------------------------------------------------------- io

_T0 = [None]


def _log(msg: str) -> None:
    import sys
    import time

    if _T0[0] is None:
        _T0[0] = time.monotonic()
    print(f"[report {time.monotonic() - _T0[0]:7.1f}s] {msg}", file=sys.stderr, flush=True)

class Data:
    def __init__(self, run: Path, export: Path | None, target_from: str | None, target_to: str | None):
        import duckdb

        self.run = run
        self.con = duckdb.connect()
        self.con.execute("SET threads=4")
        self.con.execute("SET memory_limit='3GB'")
        tmp = run / "work" / f"report-tmp-{target_from or 'all'}"
        tmp.mkdir(parents=True, exist_ok=True)
        self.con.execute(f"SET temp_directory='{tmp}'")
        meta = sorted(run.glob("run-*.json"))
        self.meta = [json.loads(p.read_text()) for p in meta]
        self.export = Path(export or self.meta[0]["export"])
        pq = self.export / "parquet"
        self.con.execute(f"CREATE VIEW parks AS SELECT * FROM read_parquet('{pq}/parks.parquet')")
        self.con.execute(f"""CREATE VIEW rides AS SELECT a.id aid, a.park_id, coalesce(a.is_headliner, false) hl
                              FROM read_parquet('{pq}/attractions.parquet') a""")
        self.con.execute(f"CREATE VIEW ride_day AS SELECT * FROM read_parquet('{pq}/ride_day.parquet')")
        self.con.execute("""CREATE TABLE region AS SELECT id park_id, CASE split_part(timezone, '/', 1)
              WHEN 'Europe' THEN 'EU' WHEN 'America' THEN 'NA' WHEN 'Asia' THEN 'Asia'
              WHEN 'Australia' THEN 'Asia' WHEN 'Pacific' THEN 'Asia' ELSE 'other' END region FROM parks""")
        flt = []
        if target_from:
            flt.append(f"date >= DATE '{target_from}'")
        if target_to:
            flt.append(f"date <= DATE '{target_to}'")
        self.where = (" WHERE " + " AND ".join(flt)) if flt else ""
        self.tables = {}
        for t in ("slot", "rideday", "pairs", "optim", "intraday", "nextbest", "levels", "openness"):
            files = list((run / "parts" / t).glob("*.parquet"))
            if files:
                self.con.execute(f"""CREATE TABLE {t} AS SELECT x.*, r.region FROM read_parquet(
                    '{run}/parts/{t}/*.parquet', union_by_name=true) x
                    LEFT JOIN region r USING (park_id) {self.where}""")
                self.tables[t] = True

    def df(self, sql: str) -> pd.DataFrame:
        return self.con.execute(sql).df()


def _cols(con, table: str, prefix: str) -> list[str]:
    names = [r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()]
    return [n[len(prefix):] for n in names if n.startswith(prefix) and "__" not in n[len(prefix):]]


# --------------------------------------------------------------------------- slot MAE / horizon

def mae_tables(d: Data, table: str, key: str, keys: list, refs: list[str], cfg: BenchConfig,
               uc_of: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per lead (and segment, region): MAE/bias/coverage per model with CIs, the reference,
    and the paired difference vs the reference."""
    models = _cols(d.con, table, "n__")
    rows, curve = [], []
    for k in keys:
        for seg, cond in (("all", "TRUE"), ("busy", "busy")):
            for region in ("all", "EU", "NA", "Asia"):
                rc = "TRUE" if region == "all" else f"region = '{region}'"
                agg = ", ".join(f"sum(n__{m}) n__{m}, sum(sae__{m}) sae__{m}, sum(se__{m}) se__{m}"
                                for m in models)
                pair_cols = [c for c in d.con.execute(f"DESCRIBE {table}").df()["column_name"]
                             if c.startswith(("pn__", "psm__", "psr__"))]
                agg += ", " + ", ".join(f"sum({c}) {c}" for c in pair_cols)
                pd_ = d.df(f"""SELECT park_id, date, sum(n_truth) n_truth, {agg} FROM {table}
                               WHERE {key} = {k!r} AND {cond} AND {rc} GROUP BY park_id, date""")
                if pd_.empty or pd_["n_truth"].sum() == 0:
                    continue
                n_truth = pd_["n_truth"].sum()
                n_origin_days = pd_["date"].nunique()
                # reference: best covered naive candidate on this cell
                cand = []
                for r in refs:
                    if f"n__{r}" in pd_ and pd_[f"n__{r}"].sum() >= 0.5 * n_truth:
                        cand.append((pd_[f"sae__{r}"].sum() / pd_[f"n__{r}"].sum(), r))
                ref = min(cand)[1] if cand else None
                for m in models:
                    n = pd_[f"n__{m}"].sum()
                    if n == 0:
                        continue
                    mae, lo, hi = ratio_ci(pd_[f"sae__{m}"], pd_[f"n__{m}"], cfg.bootstrap_reps, cfg.bootstrap_seed) \
                        if region == "all" else (pd_[f"sae__{m}"].sum() / n, np.nan, np.nan)
                    row = {"uc": uc_of(k), "lead": k, "segment": seg, "region": region, "model": m,
                           "mae": mae, "mae_lo": lo, "mae_hi": hi, "bias": pd_[f"se__{m}"].sum() / n,
                           "n_slots": int(n), "coverage": n / n_truth,
                           "n_park_days": int((pd_[f"n__{m}"] > 0).sum()), "n_origin_days": n_origin_days,
                           "ref": ref}
                    if ref and m != ref and f"pn__{m}__{ref}" in pd_:
                        nn = pd_[f"pn__{m}__{ref}"]
                        if nn.sum() > 0:
                            diff, dlo, dhi = paired_diff_ci(pd_[f"psm__{m}__{ref}"], nn, pd_[f"psr__{m}__{ref}"], nn,
                                                            cfg.bootstrap_reps, cfg.bootstrap_seed) \
                                if region == "all" else (
                                    (pd_[f"psm__{m}__{ref}"].sum() - pd_[f"psr__{m}__{ref}"].sum()) / nn.sum(),
                                    np.nan, np.nan)
                            row.update(diff_vs_ref=diff, diff_lo=dlo, diff_hi=dhi, n_paired=int(nn.sum()))
                    rows.append(row)
                    if region == "all":
                        curve.append(row)
    return pd.DataFrame(rows), pd.DataFrame(curve)


def season_table(d: Data, leads: list[int]) -> pd.DataFrame:
    models = _cols(d.con, "slot", "n__")
    agg = ", ".join(f"sum(sae__{m}) / nullif(sum(n__{m}), 0) AS \"{m}\"" for m in models)
    return d.df(f"""SELECT L, CASE WHEN month(date) IN (12,1,2) THEN 'winter' WHEN month(date) IN (3,4,5)
                   THEN 'spring' WHEN month(date) IN (6,7,8) THEN 'summer' ELSE 'autumn' END season,
                   sum(n_truth) n, {agg} FROM slot WHERE L IN ({','.join(map(str, leads))})
                   GROUP BY ALL ORDER BY L, season""")


def usable_horizon(curve: pd.DataFrame, metric: str, lower_is_better: bool = True) -> pd.DataFrame:
    """Largest lead up to which the model beats the reference (CI excludes 0), contiguous
    from the shortest lead; plus the largest significant lead at all."""
    out = []
    if curve.empty:
        return pd.DataFrame()
    for (uc, seg, model), g in curve.groupby(["uc", "segment", "model"]):
        if model in NON_COMPETING:
            continue
        g = g.sort_values("lead_order")
        contiguous, any_sig, tested = None, None, 0
        broken = False
        for _, r in g.iterrows():
            if r.get("ref") == model or not np.isfinite(r.get("diff_lo", np.nan)):
                # being the reference (or untested) at a lead is not a win: the run breaks
                broken = True
                continue
            tested += 1
            sig = significant(r["diff_lo"], r["diff_hi"], lower_is_better)
            if sig:
                any_sig = r["lead"]
                if not broken:
                    contiguous = r["lead"]
            else:
                broken = True
        out.append({"uc": uc, "segment": seg, "metric": metric, "model": model, "leads_tested": tested,
                    "usable_horizon": "" if contiguous is None else str(contiguous),
                    "max_significant_lead": "" if any_sig is None else str(any_sig)})
    return pd.DataFrame(out)


def handover(curve: pd.DataFrame, metric: str, value_col: str, lower_is_better: bool = True) -> pd.DataFrame:
    out = []
    for (uc, seg, lead), g in curve.groupby(["uc", "segment", "lead"], sort=False):
        g = g[~g["model"].isin(NON_COMPETING)]
        ref = g["ref"].dropna().iloc[0] if g["ref"].notna().any() else None
        nan = pd.Series(np.nan, index=g.index)
        sig = g[[significant(lo, hi, lower_is_better) for lo, hi in zip(g.get("diff_lo", nan),
                                                                       g.get("diff_hi", nan))]]
        if len(sig):
            best = sig.sort_values(value_col, ascending=lower_is_better).iloc[0]
            winner, val, margin = best["model"], best[value_col], best["diff_vs_ref"]
        else:
            winner = ref
            val = g.loc[g["model"] == ref, value_col].iloc[0] if ref in set(g["model"]) else np.nan
            margin = 0.0
        out.append({"uc": uc, "segment": seg, "lead": lead, "metric": metric, "reference": ref,
                    "winner": winner, "winner_value": val, "margin_vs_ref": margin,
                    "n_origin_days": int(g["n_origin_days"].max()) if "n_origin_days" in g else None})
    return pd.DataFrame(out)


# --------------------------------------------------------------------------- decision metrics

def per_parkday_metric(d: Data, sql: str, num: str, den: str, group: list[str], models_col: str,
                       refs: list[str], cfg: BenchConfig, lower_is_better: bool, metric: str,
                       uc: str, lead_col: str) -> pd.DataFrame:
    """Generic: Σnum/Σden per model per group (lead...), CI, and paired diff vs the best naive."""
    df = d.df(sql)
    if df.empty:
        return pd.DataFrame()
    rows = []
    for gkey, g in df.groupby(group, sort=True):
        gkey = gkey if isinstance(gkey, tuple) else (gkey,)
        piv_n = g.pivot_table(index=["park_id", "date"], columns=models_col, values=num, aggfunc="sum").fillna(0)
        piv_d = g.pivot_table(index=["park_id", "date"], columns=models_col, values=den, aggfunc="sum").fillna(0)
        vals = {m: piv_n[m].sum() / piv_d[m].sum() for m in piv_d if piv_d[m].sum() > 0}
        cands = [(vals[r] if lower_is_better else -vals[r], r) for r in refs if r in vals]
        ref = min(cands)[1] if cands else None
        for m in piv_d.columns:
            if piv_d[m].sum() <= 0:
                continue
            v, lo, hi = ratio_ci(piv_n[m], piv_d[m], cfg.bootstrap_reps, cfg.bootstrap_seed)
            row = dict(zip(group, gkey))
            row.update(uc=uc, metric=metric, model=m, value=v, lo=lo, hi=hi, n=float(piv_d[m].sum()),
                       n_park_days=int((piv_d[m] > 0).sum()), n_origin_days=g["date"].nunique(), ref=ref)
            if ref and m != ref:
                both = (piv_d[m] > 0) & (piv_d[ref] > 0)
                if both.any():
                    diff, dlo, dhi = paired_diff_ci(piv_n[m][both], piv_d[m][both], piv_n[ref][both],
                                                    piv_d[ref][both], cfg.bootstrap_reps, cfg.bootstrap_seed)
                    row.update(diff_vs_ref=diff, diff_lo=dlo, diff_hi=dhi)
            rows.append(row)
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.rename(columns={lead_col: "lead"})
    return out


def crowd_levels(d: Data, cfg: BenchConfig) -> pd.DataFrame | None:
    """Park-day levels per origin and lead (headliner mean, matched headliners), the
    typical-day peak as of the origin's month, and bucket/rank per source."""
    if "levels" not in d.tables:
        return None
    srcs = [c for c in d.df("DESCRIBE levels")["column_name"] if c.startswith("lvl_")]
    # typical-day peak as of each origin month (median over days before m0 of the headliner-mean P90)
    d.con.execute("""CREATE OR REPLACE TABLE hl_day AS SELECT r.park_id, r.date, avg(r.p90) lvl
        FROM ride_day r JOIN rides a USING (aid) WHERE a.hl GROUP BY ALL""")
    months = d.df("SELECT DISTINCT date_trunc('month', origin)::DATE m0 FROM levels")["m0"].tolist()
    parts = []
    for m0 in months:
        parts.append(f"""SELECT park_id, DATE '{pd.Timestamp(m0).date()}' m0, median(lvl) typ, count(*) nd
                         FROM hl_day WHERE date < DATE '{pd.Timestamp(m0).date()}' GROUP BY park_id
                         HAVING count(*) >= {cfg.typical_peak_min_days}""")
    if not parts:
        return None
    d.con.execute(f"CREATE OR REPLACE TABLE typ AS {' UNION ALL '.join(parts)}")
    sel = ", ".join(f"avg({s}) FILTER (WHERE true_p90 IS NOT NULL) AS {s}, "
                    f"count({s}) FILTER (WHERE true_p90 IS NOT NULL) AS n_{s}" for s in srcs)
    d.con.execute(f"""CREATE OR REPLACE TABLE pdl AS
        SELECT l.origin, l.park_id, any_value(l.region) region, l.date, l.L, avg(l.true_p90) true_lvl,
               count(l.true_p90) n_hl, {sel}
        FROM levels l GROUP BY l.origin, l.park_id, l.date, l.L HAVING count(l.true_p90) > 0""")
    d.con.execute("""CREATE OR REPLACE TABLE pdl2 AS SELECT p.*, t.typ FROM pdl p
        JOIN typ t ON t.park_id = p.park_id AND t.m0 = date_trunc('month', p.origin)::DATE""")
    return pd.DataFrame({"sources": srcs})


def bucket_np(pct: np.ndarray) -> np.ndarray:
    out = np.full(pct.shape, np.nan)
    ok = np.isfinite(pct)
    p = pct[ok]
    out[ok] = np.select([p <= 60, p <= 89, p <= 110, p <= 150, p <= 200], [0, 1, 2, 3, 4], 5)
    return out


def d6_d7(d: Data, cfg: BenchConfig, srcs: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = d.df("SELECT * FROM pdl2")
    if df.empty:
        return (pd.DataFrame(),) * 4
    df["true_pct"] = df["true_lvl"] / df["typ"] * 100
    df["true_b"] = bucket_np(df["true_pct"].to_numpy())
    df["true_rank"] = df["true_b"] + np.minimum(0.99, np.maximum(0, df["true_lvl"]) / 120)
    for s in srcs:
        pct = df[s] / df["typ"] * 100
        df[f"b_{s}"] = bucket_np(pct.to_numpy())
        df[f"pct_{s}"] = pct
        df[f"rank_{s}"] = df[f"b_{s}"] + np.minimum(0.99, np.maximum(0, df[s]) / 120)
    # D6: bucket accuracy per park-day, per lead
    rows = []
    for s in srcs:
        ok = df[f"b_{s}"].notna() & df["true_b"].notna()
        g = df[ok].assign(
            exact=lambda x: (x[f"b_{s}"] == x["true_b"]).astype(float),
            pm1=lambda x: ((x[f"b_{s}"] - x["true_b"]).abs() <= 1).astype(float),
            busy_t=lambda x: (x["true_b"] >= 3).astype(float),
            busy_p=lambda x: (x[f"b_{s}"] >= 3).astype(float),
            one=1.0)
        g["tp"] = g["busy_t"] * g["busy_p"]
        g["model"] = s
        rows.append(g[["origin", "park_id", "date", "L", "region", "model", "exact", "pm1", "busy_t",
                       "busy_p", "tp", "one", f"pct_{s}", "true_pct"]].rename(columns={f"pct_{s}": "pct"}))
    long = pd.concat(rows) if rows else pd.DataFrame()
    # cross-park pairwise ordering on the same (origin, date): true gap >= 10 points
    cp = []
    for (o, dte, L), g in df.groupby(["origin", "date", "L"]):
        if len(g) < 2:
            continue
        t = g["true_pct"].to_numpy()
        i, j = np.triu_indices(len(g), 1)
        gap = t[i] - t[j]
        keep = np.abs(gap) >= 10
        if not keep.any():
            continue
        for s in srcs:
            p = g[f"pct_{s}"].to_numpy()
            pp = p[i] - p[j]
            ok = keep & np.isfinite(pp)
            cp.append({"origin": o, "date": dte, "L": L, "model": s, "pairs": int(ok.sum()),
                       "correct": int((np.sign(pp[ok]) == np.sign(gap[ok])).sum())})
    cross = pd.DataFrame(cp)
    # D7: day comparison pairs within (origin, park), both days with true + predicted ranks
    dc = []
    star = []
    for (o, park), g in df.groupby(["origin", "park_id"]):
        g = g.sort_values("date")
        tr = g["true_rank"].to_numpy()
        Ls = g["L"].to_numpy()
        i, j = np.triu_indices(len(g), 1)
        tgap = tr[i] - tr[j]
        clear = np.abs(tgap) >= cfg.day_compare_clear
        maxL = np.maximum(Ls[i], Ls[j])
        for s in srcs:
            r = g[f"rank_{s}"].to_numpy()
            pg = r[i] - r[j]
            ok = clear & np.isfinite(pg)
            if ok.any():
                for lb, lo_, hi_ in (("d1-7", 1, 7), ("d8-30", 8, 30), ("d31-90", 31, 90)):
                    sel = ok & (maxL >= lo_) & (maxL <= hi_)
                    if sel.any():
                        tie = np.abs(pg[sel]) < cfg.day_compare_tie
                        win = (~tie) & (np.sign(pg[sel]) == np.sign(tgap[sel]))
                        dc.append({"origin": o, "park_id": park, "date": o, "bucket": lb, "model": s,
                                   "pairs": int(sel.sum()), "correct": int(win.sum()), "ties": int(tie.sum())})
        # star: candidate days of the month 30 days ahead (the calendar's "next month" view)
        if pd.Timestamp(o).day not in (1, 15):
            continue
        tm = (pd.Timestamp(o) + pd.Timedelta(days=30)).to_period("M")
        mm = g[pd.to_datetime(g["date"]).dt.to_period("M") == tm]
        for s in srcs:
            c = mm[mm[f"rank_{s}"].notna() & mm["true_rank"].notna()]
            if len(c) < cfg.star_min_candidates:
                continue
            ps = c[f"rank_{s}"] <= c[f"rank_{s}"].median() - cfg.star_margin
            ts = c["true_rank"] <= c["true_rank"].median() - cfg.star_margin
            star.append({"origin": o, "park_id": park, "date": o, "model": s, "pred_star": int(ps.sum()),
                         "true_star": int(ts.sum()), "both": int((ps & ts).sum()), "days": len(c)})
    return long, cross, pd.DataFrame(dc), pd.DataFrame(star)


# --------------------------------------------------------------------------- coverage horizon

def coverage_horizon(d: Data) -> pd.DataFrame:
    pq = d.export / "parquet"
    rows = []
    for name in ("tft", "cbd"):
        r = d.df(f"""SELECT max(target_date - forecast_date) max_lead,
                      quantile_cont(target_date - forecast_date, 0.5) med_lead, max(forecast_date) last_run
                      FROM read_parquet('{pq}/{name}.parquet')
                      WHERE forecast_date >= (SELECT max(forecast_date) - 7 FROM read_parquet('{pq}/{name}.parquet'))""")
        rows.append({"input": {"tft": "TFT daily level", "cbd": "CatBoost daily level"}[name],
                     "horizon_days": int(r["max_lead"].iloc[0]) if pd.notna(r["max_lead"].iloc[0]) else None,
                     "note": f"last run {r['last_run'].iloc[0]}; beyond: no forecast -> composed level falls back"})
    w = d.df(f"""SELECT max(date) mx, min(date) mn FROM read_parquet('{pq}/weather.parquet')
                 WHERE data_type = 'forecast'""")
    rows.append({"input": "weather forecast (Open-Meteo)", "horizon_days": 16,
                 "note": f"forecast rows {w['mn'].iloc[0]}..{w['mx'].iloc[0]}; NO archive -> backtest uses actuals (ORACLE)"})
    s = d.df(f"""WITH m AS (SELECT park_id, max(date) mx FROM read_parquet('{pq}/windows.parquet') GROUP BY park_id)
                 SELECT quantile_cont(mx - (SELECT max(date) FROM read_parquet('{pq}/truth.parquet')), 0.1) p10,
                        quantile_cont(mx - (SELECT max(date) FROM read_parquet('{pq}/truth.parquet')), 0.5) p50,
                        quantile_cont(mx - (SELECT max(date) FROM read_parquet('{pq}/truth.parquet')), 0.9) p90
                 FROM m""")
    rows.append({"input": "published operator schedule (per park)", "horizon_days": int(s["p50"].iloc[0]),
                 "note": f"p10 {s['p10'].iloc[0]:.0f} d, median {s['p50'].iloc[0]:.0f} d, p90 {s['p90'].iloc[0]:.0f} d "
                         "past the last truth day; latest version only (no history) -> flagged published_final"})
    return pd.DataFrame(rows)


def lead_availability(d: Data, cfg: BenchConfig, first_truth: dt.date, last_truth: dt.date) -> pd.DataFrame:
    rows = []
    first_origin = first_truth + dt.timedelta(days=cfg.window_days)
    for L in sorted(set(cfg.slot_leads) | set(UC4_LEADS)):
        n = (last_truth - first_origin).days - L + 1
        n = max(n, 0)
        status = "ok" if n >= cfg.low_n_origin_days else ("LOW-N" if n > 0 else "not measurable yet")
        when = first_origin + dt.timedelta(days=L + cfg.low_n_origin_days - 1)
        rows.append({"lead": L, "origin_days": n, "status": status,
                     "measurable_from": when.isoformat() if n < cfg.low_n_origin_days else ""})
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- markdown

def md(df: pd.DataFrame, cols: list[str] | None = None, floatfmt: str = "{:.2f}", max_rows: int = 400) -> str:
    if df is None or df.empty:
        return "_no rows_\n"
    df = df[cols] if cols else df
    df = df.head(max_rows)

    def f(v):
        if isinstance(v, float):
            return "" if not np.isfinite(v) else floatfmt.format(v)
        return str(v)

    head = "| " + " | ".join(map(str, df.columns)) + " |"
    sep = "|" + "---|" * len(df.columns)
    body = ["| " + " | ".join(f(v) for v in r) + " |" for r in df.itertuples(index=False)]
    return "\n".join([head, sep, *body]) + "\n"


def ci(v, lo, hi) -> str:
    if not np.isfinite(v):
        return ""
    if not (np.isfinite(lo) and np.isfinite(hi)):
        return f"{v:.2f}"
    return f"{v:.2f} [{lo:.2f}, {hi:.2f}]"


def wide(df: pd.DataFrame, value: str = "mae", lo: str = "mae_lo", hi: str = "mae_hi",
         index: str = "lead", models: list[str] | None = None) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["cell"] = [ci(a, b, c) for a, b, c in zip(df[value], df.get(lo, np.nan), df.get(hi, np.nan))]
    w = df.pivot_table(index=index, columns="model", values="cell", aggfunc="first")
    if models:
        w = w[[m for m in models if m in w.columns]]
    return w.reset_index()


# --------------------------------------------------------------------------- main

def build_report(run: Path, export: Path | None = None, target_from: str | None = None,
                 target_to: str | None = None, reps: int | None = None) -> Path:
    cfg = BenchConfig()
    if reps:
        cfg.bootstrap_reps = reps
    d = Data(run, export, target_from, target_to)
    tdir = run / ("tables" if not (target_from or target_to) else f"tables_{target_from}_{target_to}")
    tdir.mkdir(parents=True, exist_ok=True)
    out: list[str] = []
    w = out.append
    stats_p = d.export / "parquet" / "build_stats.json"
    build_stats = json.loads(stats_p.read_text()) if stats_p.exists() else {}
    manifest_p = d.export / "manifest.json"
    manifest = json.loads(manifest_p.read_text()) if manifest_p.exists() else {}
    first_truth = dt.date.fromisoformat(build_stats.get("truth_dates", ["2025-12-24"])[0][:10])
    last_truth = dt.date.fromisoformat(build_stats.get("truth_dates", ["", "2026-10-08"])[1][:10])
    # same complete-day rule as the runner: the export's UTC days end one park-local day early
    if manifest.get("queue_to"):
        last_truth = min(last_truth, dt.date.fromisoformat(manifest["queue_to"]) - dt.timedelta(days=1))
    if manifest.get("queue_from"):
        first_truth = max(first_truth, dt.date.fromisoformat(manifest["queue_from"]) + dt.timedelta(days=1))

    order_daily = {L: i for i, L in enumerate(sorted(set(cfg.slot_leads)))}
    lead_av = lead_availability(d, cfg, first_truth, last_truth)
    lead_av.to_csv(tdir / "lead_availability.csv", index=False)
    cov = coverage_horizon(d)
    cov.to_csv(tdir / "coverage_horizon.csv", index=False)

    _log("UC2/UC3 slot MAE")
    # ---------------- UC2/UC3 slot MAE (daily origin) and UC1/UC2 intraday
    def uc_daily(L):
        return "UC2" if L == 0 else "UC3"
    slot_rows, slot_curve = mae_tables(d, "slot", "L", sorted(set(cfg.slot_leads)), B.REF_CANDIDATES, cfg,
                                       uc_daily)
    # d1 belongs to UC2 (tomorrow) and UC3 (day 1): duplicate it into UC2
    if not slot_curve.empty:
        d1 = slot_curve[slot_curve["lead"] == 1].assign(uc="UC2")
        slot_curve = pd.concat([slot_curve, d1])
        slot_curve["lead_order"] = slot_curve["lead"].map(order_daily)
    intr_rows, intr_curve = (pd.DataFrame(), pd.DataFrame())
    if "intraday" in d.tables:
        intr_rows, intr_curve = mae_tables(d, "intraday", "lead_key", UC1_KEYS + UC2_INTRADAY_KEYS,
                                           B.INTRADAY_REFS, cfg,
                                           lambda k: "UC1" if k.startswith("m") else "UC2-intraday")
        if not intr_curve.empty:
            intr_curve["lead_order"] = intr_curve["lead"].map(
                {k: i for i, k in enumerate(UC1_KEYS + UC2_INTRADAY_KEYS)})
    pd.concat([slot_rows, intr_rows]).to_csv(tdir / "mae_by_lead_segment_region.csv", index=False)
    curve = pd.concat([slot_curve, intr_curve])
    curve.to_csv(tdir / "horizon_curve_mae.csv", index=False)
    uh = usable_horizon(curve, "MAE")
    ho = handover(curve, "MAE", "mae")

    _log("decision metrics from ride-days")
    # ---------------- decision metrics from ride-days
    dec_curves = []
    if "rideday" in d.tables:
        rd_models = B.SLOT_MODELS + [m for m in d.df("SELECT DISTINCT model FROM rideday")["model"]
                                     if m not in B.SLOT_MODELS and m != "prod_ropedrop_hist"]
        base = "SELECT * FROM rideday"
        specs = [
            ("D3", "best-time hit rate (top-2)", "hit2", "bt_n", False),
            ("D3", "best-time hit rate (±30 min)", "hit30", "bt_n", False),
            ("D3", "best-time regret (min)", "regret", "bt_n", True),
            ("UC2/UC3", "Spearman of slots within ride-day", "sp_sum", "sp_n", False),
            ("UC3", "dayPeak abs error (min)", "dp_sae", "n_rd", True),
            ("D5", "first-hour MAE (opening-aligned)", "fh_sae", "fh_n", True),
        ]
        for uc, metric, num, den, lib in specs:
            t = per_parkday_metric(d, f"""SELECT L, park_id, date, model, sum({num}) num, sum({den}) den
                                          FROM ({base}) WHERE model IN ({','.join(repr(m) for m in rd_models)})
                                          GROUP BY ALL""",
                                   "num", "den", ["L"], "model", B.REF_CANDIDATES, cfg, lib, metric, uc, "L")
            if not t.empty:
                t["lead_order"] = t["lead"].map(order_daily)
                dec_curves.append(t)
        # D5 worth agreement incl. the production rope-drop rule
        t = per_parkday_metric(d, """SELECT L, park_id, date, model, sum(w_tp + w_tn) num,
                                       sum(w_tp + w_fp + w_fn + w_tn) den FROM rideday GROUP BY ALL""",
                               "num", "den", ["L"], "model", B.REF_CANDIDATES + ["prod_ropedrop_hist"], cfg,
                               False, "rope-drop worth agreement", "D5", "L")
        if not t.empty:
            t["lead_order"] = t["lead"].map(order_daily)
            dec_curves.append(t)
        worth_pr = d.df("""SELECT L, model, sum(w_tp) tp, sum(w_fp) fp, sum(w_fn) fn, sum(w_tn) tn,
                           sum(w_tp) / nullif(sum(w_tp + w_fp), 0) AS "precision",
                           sum(w_tp) / nullif(sum(w_tp + w_fn), 0) AS "recall"
                           FROM rideday GROUP BY ALL ORDER BY L, model""")
        worth_pr.to_csv(tdir / "d5_worth_confusion.csv", index=False)
    if "pairs" in d.tables:
        t = per_parkday_metric(d, """SELECT L, park_id, date, model, sum(pw_ok) num, sum(pw_n) den
                                     FROM pairs GROUP BY ALL""", "num", "den", ["L"], "model",
                               B.REF_CANDIDATES, cfg, False, "dayPeak pairwise ordering (headliners)", "D4", "L")
        if not t.empty:
            t["lead_order"] = t["lead"].map(order_daily)
            dec_curves.append(t)
        t = per_parkday_metric(d, """SELECT L, park_id, date, model, sum(dsp) num, count(dsp) den
                                     FROM pairs WHERE dsp IS NOT NULL GROUP BY ALL""", "num", "den", ["L"],
                               "model", B.REF_CANDIDATES, cfg, False, "dayPeak Spearman across rides", "UC3", "L")
        if not t.empty:
            t["lead_order"] = t["lead"].map(order_daily)
            dec_curves.append(t)
    if "optim" in d.tables:
        t = per_parkday_metric(d, """SELECT L, park_id, date, model, sum(plan_cost - opt_cost) num,
                                     count(*) den FROM optim WHERE plan_cost IS NOT NULL
                                     AND isfinite(plan_cost) GROUP BY ALL""", "num", "den", ["L"], "model",
                               B.REF_CANDIDATES, cfg, True, "optimiser regret (min per park-day)", "D4", "L")
        if not t.empty:
            t["lead_order"] = t["lead"].map(order_daily)
            dec_curves.append(t)
    if "nextbest" in d.tables:
        for metric, num, den in (("next-best precision", "n_ok", "n_sug"),
                                 ("next-best precision (top-3)", "n_ok3", "n_sug3"),
                                 ("next-best recall", "n_ok", "n_opp")):
            t = per_parkday_metric(d, f"""SELECT bucket, park_id, date, model, sum({num}) num, sum({den}) den
                                          FROM nextbest GROUP BY ALL""", "num", "den", ["bucket"], "model",
                                   ["snaive7", "wt_med", "clim"], cfg, False, metric, "D1", "bucket")
            if not t.empty:
                t["lead_order"] = t["lead"].map({"0-60": 0, "60-120": 1})
                dec_curves.append(t)
    if "intraday" in d.tables:
        # D2: plan-block live correction window (0-45 min), headliners; persistence = what production does
        models = _cols(d.con, "intraday", "n__")
        for metric, col in (("live-window MAE (headliners)", "sae"), ("live-window bias (headliners)", "se")):
            parts = " UNION ALL ".join(
                f"SELECT 'w0-45' k, park_id, date, '{m}' model, sum({col}__{m}) num, sum(n__{m}) den "
                f"FROM intraday WHERE hl AND lead_key IN ('m015','m030','m045') GROUP BY ALL" for m in models)
            t = per_parkday_metric(d, parts, "num", "den", ["k"], "model", B.INTRADAY_REFS, cfg,
                                   col == "sae", metric, "D2", "k")
            if not t.empty:
                t["lead_order"] = 0
                dec_curves.append(t)
    dec = pd.concat(dec_curves) if dec_curves else pd.DataFrame()
    if not dec.empty:
        dec["segment"] = "all"
        dec.to_csv(tdir / "decision_metrics.csv", index=False)
        for metric, g in dec.groupby("metric"):
            if "bias" in metric:
                continue
            lib = any(k in metric for k in ("regret", "error", "MAE")) and "bias" not in metric
            gg = g.rename(columns={"value": "val"})
            uh = pd.concat([uh, usable_horizon(gg, metric, lib)])
            ho = pd.concat([ho, handover(gg, metric, "val", lib)])

    _log("UC4 / D6 / D7 / D9 daily levels")
    # ---------------- UC4 / D6 / D7 / D9 daily levels
    uc4 = d6 = d6x = d7 = d7s = cov_lvl = pd.DataFrame()
    srcinfo = crowd_levels(d, cfg)
    if srcinfo is not None:
        srcs = srcinfo["sources"].tolist()
        lv = d.df("SELECT * FROM pdl2")
        # UC4: Spearman of daily park level within park-month, busy-day hit rate, spread
        rows = []
        for L in UC4_LEADS:
            g = lv[lv["L"] == L]
            if g.empty:
                continue
            for s in srcs:
                recs = []
                for (park, mon), h in g.assign(mon=pd.to_datetime(g["date"]).dt.to_period("M")).groupby(["park_id", "mon"]):
                    h = h[h[s].notna()]
                    if len(h) < 8:
                        continue
                    sp = h["true_lvl"].rank().corr(h[s].rank())
                    q = h["true_lvl"].quantile(0.75)
                    top_t = h["true_lvl"] >= q
                    top_p = h[s] >= h[s].quantile(0.75)
                    recs.append({"park_id": park, "date": h["date"].min(), "sp": sp if np.isfinite(sp) else np.nan,
                                 "hit": (top_t & top_p).sum(), "ntop": top_t.sum(),
                                 "iqr": h[s].quantile(0.75) - h[s].quantile(0.25),
                                 "iqr_true": h["true_lvl"].quantile(0.75) - h["true_lvl"].quantile(0.25)})
                if not recs:
                    continue
                r = pd.DataFrame(recs)
                spv = r["sp"].dropna()
                sp_ci = ratio_ci(spv.to_numpy(), np.ones(len(spv)), cfg.bootstrap_reps, cfg.bootstrap_seed)
                hit_ci = ratio_ci(r["hit"].to_numpy(), r["ntop"].to_numpy(), cfg.bootstrap_reps, cfg.bootstrap_seed)
                rows.append({"lead": L, "model": s, "park_months": len(r),
                             "spearman_within_month": ci(*sp_ci), "busy_day_hit_rate": ci(*hit_ci),
                             "spread_iqr_pred": r["iqr"].median(), "spread_iqr_true": r["iqr_true"].median(),
                             "sp": sp_ci[0], "sp_lo": sp_ci[1], "sp_hi": sp_ci[2]})
        uc4 = pd.DataFrame(rows)
        # paired: Spearman of model and reference on the SAME days of each park-month
        naive = ["lvl_naive4", "lvl_wt56", "lvl_snaive7", "lvl_clim"]
        u_curve = []
        for L in UC4_LEADS:
            g = lv[lv["L"] == L].assign(mon=lambda x: pd.to_datetime(x["date"]).dt.to_period("M"))
            sub = uc4[uc4["lead"] == L] if not uc4.empty else uc4
            cands = sub[sub["model"].isin(naive)].sort_values("sp", ascending=False)
            if g.empty or cands.empty:
                continue
            ref = cands["model"].iloc[0]
            for s_ in srcs:
                if s_ == ref:
                    continue
                a, b = [], []
                for _, h in g.groupby(["park_id", "mon"]):
                    h = h[h[s_].notna() & h[ref].notna()]
                    if len(h) < 8:
                        continue
                    sa = h["true_lvl"].rank().corr(h[s_].rank())
                    sb = h["true_lvl"].rank().corr(h[ref].rank())
                    if np.isfinite(sa) and np.isfinite(sb):
                        a.append(sa)
                        b.append(sb)
                if len(a) < 5:
                    continue
                one = np.ones(len(a))
                diff, dlo, dhi = paired_diff_ci(np.array(a), one, np.array(b), one, cfg.bootstrap_reps,
                                                cfg.bootstrap_seed)
                row = sub[sub["model"] == s_]
                u_curve.append({"uc": "UC4", "segment": "all", "lead": L, "model": s_, "ref": ref,
                                "val": float(row["sp"].iloc[0]) if len(row) else np.nan,
                                "diff_vs_ref": diff, "diff_lo": dlo, "diff_hi": dhi, "n_park_months": len(a),
                                "n_origin_days": int(g["origin"].nunique())})
            rr = sub[sub["model"] == ref]
            u_curve.append({"uc": "UC4", "segment": "all", "lead": L, "model": ref, "ref": ref,
                            "val": float(rr["sp"].iloc[0]), "n_origin_days": int(g["origin"].nunique())})
        if u_curve:
            uc4c = pd.DataFrame(u_curve)
            uc4c["lead_order"] = uc4c["lead"].map({L: i for i, L in enumerate(UC4_LEADS)})
            uc4c.to_csv(tdir / "uc4_paired_spearman.csv", index=False)
            uh = pd.concat([uh, usable_horizon(uc4c, "UC4 Spearman within park-month", False)])
            ho = pd.concat([ho, handover(uc4c, "UC4 Spearman within park-month", "val", False)])
        uc4.to_csv(tdir / "uc4_calendar.csv", index=False)
        _log("D6/D7")
        long, cross, dcp, star = d6_d7(d, cfg, srcs)
        if not long.empty:
            r6 = []
            for (L, m), g in long[long["L"].isin(UC4_LEADS)].groupby(["L", "model"]):
                pdg = g.groupby(["park_id", "date"]).sum(numeric_only=True)
                r6.append({"lead": L, "model": m, "park_days": len(pdg),
                           "bucket_acc": ci(*ratio_ci(pdg["exact"], pdg["one"], cfg.bootstrap_reps)),
                           "pm1_acc": ci(*ratio_ci(pdg["pm1"], pdg["one"], cfg.bootstrap_reps)),
                           "busy_recall": ci(*ratio_ci(pdg["tp"], pdg["busy_t"], cfg.bootstrap_reps)),
                           "busy_precision": ci(*ratio_ci(pdg["tp"], pdg["busy_p"], cfg.bootstrap_reps))})
            d6 = pd.DataFrame(r6)
            d6.to_csv(tdir / "d6_crowd_bucket.csv", index=False)
        if not cross.empty:
            d6x = (cross[cross["L"].isin(UC4_LEADS)].groupby(["L", "model"])
                   .agg(pairs=("pairs", "sum"), correct=("correct", "sum")).reset_index())
            d6x["accuracy"] = d6x["correct"] / d6x["pairs"]
            d6x.to_csv(tdir / "d6_cross_park_ordering.csv", index=False)
        if not dcp.empty:
            d7 = dcp.groupby(["bucket", "model"]).agg(pairs=("pairs", "sum"), correct=("correct", "sum"),
                                                      ties=("ties", "sum")).reset_index()
            d7["winner_accuracy"] = d7["correct"] / d7["pairs"]
            d7["tie_rate"] = d7["ties"] / d7["pairs"]
            d7.to_csv(tdir / "d7_day_comparison.csv", index=False)
        if not star.empty:
            d7s = star.groupby("model").agg(pred_star=("pred_star", "sum"), true_star=("true_star", "sum"),
                                            both=("both", "sum"), park_months=("days", "size")).reset_index()
            d7s["star_precision"] = d7s["both"] / d7s["pred_star"]
            d7s["star_recall"] = d7s["both"] / d7s["true_star"]
            d7s.to_csv(tdir / "d7_star.csv", index=False)
        cov_lvl = d.df(f"""SELECT L, {', '.join(f'count({s}) FILTER (WHERE true_p90 IS NOT NULL) / '
                                                   f'nullif(count(true_p90), 0) AS {s}' for s in srcs)},
                              count(true_p90) n_ride_days FROM levels
                           WHERE L IN ({','.join(map(str, UC4_LEADS))}) GROUP BY L ORDER BY L""")
        cov_lvl.to_csv(tdir / "d9_level_coverage.csv", index=False)

    _log("D8 calibration")
    # ---------------- D8 calibration
    d8 = pd.DataFrame()
    qm = [c[len("qn__"):] for c in (d.df("DESCRIBE slot")["column_name"] if "slot" in d.tables else [])
          if c.startswith("qn__")]
    if qm:
        d8 = d.df(f"""SELECT L, {', '.join(f'sum(q80__{m}) / nullif(sum(qn__{m}), 0) AS "{m} cov_q80", '
                                         f'sum(q95__{m}) / nullif(sum(qn__{m}), 0) AS "{m} cov_q95", '
                                         f'sum(qn__{m}) / nullif(sum(n_truth), 0) AS "{m} field_coverage"' for m in qm)}
                      FROM slot GROUP BY L ORDER BY L""")
    # stated typical error = trailing 28-day MAE at the same lead (what /plan/day's expectedError is)
    calib = pd.DataFrame()
    if "slot" in d.tables:
        models = [m for m in _cols(d.con, "slot", "n__") if m not in NON_COMPETING]
        frames = []
        for m in models:
            frames.append(d.df(f"""
              WITH pdm AS (SELECT L, park_id, date, sum(sae__{m}) s, sum(n__{m}) n FROM slot GROUP BY ALL),
              st AS (SELECT a.L, a.park_id, a.date, a.s, a.n, sum(b.s) / nullif(sum(b.n), 0) stated
                     FROM pdm a JOIN pdm b ON b.L = a.L AND b.park_id = a.park_id
                       AND b.date < a.date - a.L AND b.date >= a.date - a.L - 28
                     WHERE a.n > 0 GROUP BY a.L, a.park_id, a.date, a.s, a.n)
              SELECT '{m}' model, L, sum(s) / sum(n) realised, sum(stated * n) / sum(n) stated_weighted
              FROM st WHERE stated IS NOT NULL GROUP BY L ORDER BY L"""))
        calib = pd.concat(frames) if frames else pd.DataFrame(columns=["model", "L", "realised",
                                                                      "stated_weighted"])
        calib["ratio_realised_to_stated"] = calib["realised"] / calib["stated_weighted"]
        calib.to_csv(tdir / "d8_typical_error_calibration.csv", index=False)

    _log("D9 openness")
    # ---------------- D9 openness
    opn = pd.DataFrame()
    if "openness" in d.tables:
        models = _cols(d.con, "openness", "op_f__")
        opn = d.df(f"""SELECT L, {', '.join(
            f'sum(op_f__{m}) / nullif(sum(op_f__{m} + op_nf__{m}), 0) AS "{m} P(fc|operated)", '
            f'sum(cl_f__{m}) / nullif(sum(cl_f__{m} + cl_nf__{m}), 0) AS "{m} P(fc|not operated)"' for m in models)},
            sum(op_f__wt_med + op_nf__wt_med) operated_ride_days,
            sum(cl_f__wt_med + cl_nf__wt_med) not_operated_ride_days
            FROM openness GROUP BY L ORDER BY L""")
        opn.to_csv(tdir / "d9_openness.csv", index=False)

    uh.to_csv(tdir / "usable_horizon.csv", index=False)
    ho.to_csv(tdir / "handover.csv", index=False)
    seasons = season_table(d, [0, 1, 3, 7, 14, 30]) if "slot" in d.tables else pd.DataFrame()
    seasons.to_csv(tdir / "mae_by_season.csv", index=False)

    _log("summary.md")
    # ================= summary.md
    w("# ml-bench results\n")
    w(f"Run: `{run.name}` · generated {dt.datetime.now(dt.timezone.utc).isoformat(timespec='minutes')}"
      + (f" · target window {target_from}..{target_to}" if (target_from or target_to) else "") + "\n")
    w(f"- code: `{', '.join(sorted({m.get('git_sha', '?') for m in d.meta}))}`; models: "
      f"{', '.join(B.SLOT_MODELS)} + plug-ins {sorted({x for m in d.meta for x in m.get('models', [])}) or 'none'}")
    w(f"- export: `{d.export}` (git {manifest.get('git_sha', '?')}, finished {manifest.get('export_finished_utc', '?')}); "
      f"truth {build_stats.get('truth_dates')}, {build_stats.get('truth_rides')} rides, "
      f"{build_stats.get('truth_parks')} parks, {build_stats.get('truth')} truth slots")
    secs = [m.get("seconds") for m in d.meta if m.get("seconds")]
    w(f"- runtime: {sum(secs) / 60:.1f} CPU-shard-minutes over {len(d.meta)} shard(s)" if secs else "- runtime: n/a")
    w("- MAE in minutes on 15-min slots; CIs = 95 % park-day cluster bootstrap "
      f"({cfg.bootstrap_reps} reps); `diff` = model − reference on paired slots (negative = better).")
    w("- Weather in the known-future covariates is ORACLE (actuals; no forecast archive exists). "
      "Schedules are the latest published version (no history), flagged `published_final`.\n")
    w("## Horizon — the headline\n")
    w("### Lead availability (origin days per lead)\n")
    w(md(lead_av))
    w("### Coverage horizon of the inputs\n")
    w(md(cov))
    w("### Usable horizon (contiguous leads where the paired CI vs the per-lead reference excludes 0)\n")
    if not uh.empty:
        w(md(uh.sort_values(["metric", "uc", "segment", "model"]),
             ["metric", "uc", "segment", "model", "leads_tested", "usable_horizon", "max_significant_lead"]))
    w("### Hand-over table (input for the serving router)\n")
    if not ho.empty:
        w(md(ho[ho["metric"] == "MAE"], ["uc", "segment", "lead", "reference", "winner", "winner_value",
                                         "margin_vs_ref", "n_origin_days"]))
        w("Decision metrics:\n")
        w(md(ho[ho["metric"] != "MAE"], ["metric", "uc", "lead", "reference", "winner", "winner_value",
                                         "margin_vs_ref"]))
    w("### Level vs shape by lead (MAE, all rides)\n")
    if not slot_curve.empty:
        dec_ = slot_curve[(slot_curve["segment"] == "all") & (slot_curve["uc"] != "UC2")]
        w(md(wide(dec_, models=["lvlh5_naive", "lvlh5_tft", "oracle_level", "oracle_shape", "h5", "wt_med"])))
        w("`oracle_level` = H5 scaled with the TRUE daily P90 (perfect level); `oracle_shape` = the TRUE "
          "shape scaled to the naive level (perfect shape). Where the gap lvlh5 → oracle_level stays large, "
          "the level is what is not forecastable.\n")
    for uc, title in (("UC1", "UC1 — live / next 2 h (intraday origins)"),
                      ("UC2-intraday", "UC2 — rest of today from intraday origins"),
                      ("UC2", "UC2 — today and tomorrow from the 06:00 origin"),
                      ("UC3", "UC3 — planner day 1…90")):
        w(f"## {title}\n")
        for seg in ("all", "busy"):
            c = curve[(curve["uc"] == uc) & (curve["segment"] == seg)]
            if c.empty:
                continue
            w(f"**MAE, {seg}** (busy = ex-ante ride q90 over the 56 days before the origin ≥ 45 min)\n")
            w(md(wide(c)))
            w(f"**Paired difference vs the per-lead reference, {seg}**\n")
            cc = c.dropna(subset=["diff_vs_ref"]) if "diff_vs_ref" in c else pd.DataFrame()
            if not cc.empty:
                w(md(wide(cc, "diff_vs_ref", "diff_lo", "diff_hi").merge(
                    c.groupby("lead")["ref"].first().reset_index(), on="lead")))
            b = c.pivot_table(index="lead", columns="model", values="bias").reset_index()
            w(f"**Bias, {seg}**\n")
            w(md(b))
        if uc in ("UC3", "UC2"):
            reg = slot_rows[(slot_rows["uc"] == uc) & (slot_rows["segment"] == "all")] if not slot_rows.empty else slot_rows
            if not reg.empty:
                w("**MAE by region (all rides)**\n")
                w(md(reg[reg["lead"].isin([0, 1, 3, 7, 14, 30])].pivot_table(
                    index=["lead", "region"], columns="model", values="mae").reset_index()))
    w("## UC3 — by season (MAE, all rides)\n")
    w(md(seasons))
    w("## UC4 — crowd calendar (daily park level = mean headliner P90)\n")
    w(md(uc4, [c for c in ["lead", "model", "park_months", "spearman_within_month", "busy_day_hit_rate",
                           "spread_iqr_pred", "spread_iqr_true"] if c in uc4.columns]))

    def dec_section(title: str, metric_names: list[str], note: str = "") -> None:
        w(f"## {title}\n")
        if note:
            w(note + "\n")
        if dec.empty:
            w("_no rows_\n")
            return
        for mname in metric_names:
            g = dec[dec["metric"] == mname]
            if g.empty:
                continue
            w(f"**{mname}**\n")
            w(md(wide(g, "value", "lo", "hi")))
            gg = g.dropna(subset=["diff_vs_ref"]) if "diff_vs_ref" in g else pd.DataFrame()
            if not gg.empty:
                w("paired difference vs reference:\n")
                w(md(wide(gg, "diff_vs_ref", "diff_lo", "diff_hi").merge(
                    g.groupby("lead")["ref"].first().reset_index(), on="lead")))

    dec_section("D1 — next-best ride", ["next-best precision", "next-best precision (top-3)", "next-best recall"],
                "Suggest a ride when the forecast maximum in the next 120 min is ≥ 10 min above the live wait "
                "(`lib/planner/next-best-ride.ts`, walking time 0). Correct when the TRUE maximum in that window is "
                "also ≥ 10 min above the live wait. `lead` = minutes until the predicted peak. "
                "False-suggestion rate = 1 − precision.")
    dec_section("D2 — plan-block live correction (0–45 min, headliners)",
                ["live-window MAE (headliners)", "live-window bias (headliners)"],
                "Production replaces the forecast by the live wait within ±45 min (`LIVE_WINDOW_MIN`), i.e. "
                "`persistence` is what the frontend serves in this window.")
    dec_section("D3 — best time of the day", ["best-time hit rate (top-2)", "best-time hit rate (±30 min)",
                                               "best-time regret (min)"],
                "Ride-days with ≥ 8 truth slots and a non-flat truth. Hit = a true minimum slot is among the "
                "predicted 2 lowest slots / within ±30 min of the predicted best; regret = true wait at the "
                "predicted best slot − true minimum.")
    dec_section("D4 — planner optimiser", ["optimiser regret (min per park-day)",
                                           "dayPeak pairwise ordering (headliners)"],
                f"Fixed set of the park's top {cfg.optimiser_rides} rides (headliners first, by ex-ante level). "
                "Objective Σwait + 0.5·Σidle (`optimize.ts`), exact optimum over orders and 0–120 min delays; "
                "regret = true cost of the forecast's plan − true cost of the truth-optimal plan. Leads "
                f"{cfg.optimiser_leads}.")
    dec_section("D5 — rope drop", ["first-hour MAE (opening-aligned)", "rope-drop worth agreement"],
                "worth = day peak ≥ 60 ∧ peak − opening wait ≥ 45 (`rope-drop.util.ts`), per ride-day. "
                "`prod_ropedrop_hist` = the production rule on the window medians (one verdict per ride).")
    if "d5_worth_confusion.csv" in [p.name for p in tdir.iterdir()]:
        wc = pd.read_csv(tdir / "d5_worth_confusion.csv")
        w(md(wc[wc["L"].isin([0, 1, 3, 7])]))
    w("## D6 — crowd bucket per park-day (Prognose heute / trip assistant)\n")
    w("Park level = mean headliner P90 ÷ typical-day peak (median over the park's days before the origin's "
      "month, ≥ 30 days) → the `determineCrowdLevel` ladder. Busy = high or above.\n")
    w(md(d6))
    w("Cross-park ordering on the same date (true gap ≥ 10 points):\n")
    w(md(d6x))
    w("## D7 — day comparison and calendar star\n")
    w("rank = bucket + min(0.99, avg headliner level / 120) (`rankOf`); pairs of days from the same origin and "
      "park with a true rank gap ≥ 0.5; a predicted gap < 0.1 is a tie (counted wrong). Star = rank ≤ month "
      "median − 0.5 among ≥ 4 candidate days of the month ~30 days ahead (origins on the 1st and 15th).\n")
    w(md(d7))
    w(md(d7s))
    w("## D8 — uncertainty\n")
    w("Coverage of the empirical q80/q95 (should read 0.80/0.95) and the share of truth slots that HAVE an "
      "interval (`field_coverage`; production composed days have none today):\n")
    w(md(d8))
    w("Stated typical error = the trailing 28-day MAE at the same lead and park (what `expectedError` "
      "reports); ratio > 1 = the stated error is optimistic:\n")
    w(md(calib[calib["L"].isin([0, 1, 3, 7, 14, 30, 60, 90])] if not calib.empty else calib))
    w("## D9 — coverage and openness\n")
    w("Share of operating truth slots with a forecast, per lead:\n")
    if not curve.empty:
        cv = curve[(curve["segment"] == "all") & curve["uc"].isin(["UC2", "UC3"])]
        w(md(cv.pivot_table(index="lead", columns="model", values="coverage").reset_index()))
    w("Daily-level coverage (headliner ride-days with a level, by lead, incl. 90–365 d):\n")
    w(md(cov_lvl))
    w("Forecast present vs ride operated (ride-days with readings; a ride-day without any reading is UNKNOWN "
      "and not counted):\n")
    w(md(opn))
    (run / ("summary.md" if tdir.name == "tables" else f"summary_{tdir.name}.md")).write_text("\n".join(out))
    return tdir


def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--run", required=True, help="results/<run-id>")
    p.add_argument("--export", default=None, help="override the export dir recorded in the run")
    p.add_argument("--target-from", default=None)
    p.add_argument("--target-to", default=None)
    p.add_argument("--reps", type=int, default=None, help="bootstrap reps (default 1000)")


def main(args: argparse.Namespace) -> int:
    t = build_report(Path(args.run), Path(args.export) if args.export else None, args.target_from,
                     args.target_to, args.reps)
    print(f"tables in {t}")
    return 0
