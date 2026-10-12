"""``python -m mlbench drivers`` — the level-driver analysis (PAR-830).

How much of a ride-day's (and park-day's) deviation from the naive level is
explainable by drivers known in advance, and how much is noise?

* target ``y = log(true daily P90 / naive reference)`` per ride-day and lead
  (``drivers.driver_rows_sql``; reference = baseline 5a, information cut at the
  06:00 origin ``date - L``);
* walk-forward by ORIGIN month: the model for the origins of month M is trained
  on ride-days whose target date lies before the first of M — every training
  truth is known before every test origin;
* one model per fold and feature set, pooled over leads (``L`` is a feature),
  scored per lead — so long leads, whose first training rows only exist months
  into the data, still get a model;
* (a) univariate driver effects with park-cluster bootstrap CIs, (b) a GAM-like
  additive model (cubic splines per numeric driver + one-hot categoricals,
  ridge), (c) gradient boosting (sklearn ``HistGradientBoostingRegressor``, CPU)
  as the upper bound;
* scored as out-of-sample R² against the naive reference
  (``1 − Σ(y − ŷ)² / Σy²``: the share of the squared log deviation from the naive
  level the drivers explain), level MAE in minutes vs naive with paired park-day
  bootstrap CIs, at ride and park level (headliner mean), and the UC4 ranking
  (Spearman of the park level within park-month, as the harness scores it).

Weather is ORACLE (daily actuals — no forecast archive exists) and is only ever
reported as its own feature set. Schedules are the latest published version
(``published_final``), so the ``sched`` group is an upper bound beyond the
operator's publishing horizon (median 39 days in the export).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

from . import drivers as D
from .config import BenchConfig
from .stats import paired_diff_ci, ratio_ci

LEADS = [1, 3, 7, 14, 30, 60, 90]
FEATURE_SETS: dict[str, list[str]] = {
    "cal": ["cal"],
    "cal+hol": ["cal", "hol"],
    "cal+sched": ["cal", "sched"],
    "cal+hol+sched": ["cal", "hol", "sched"],
    "cal+hol+sched_final(UPPER)": ["cal", "hol", "sched_final"],
    "cal+hol+sched+wx(ORACLE)": ["cal", "hol", "sched", "wx"],
}
# the additive (GAM-like) model is fitted on the main set only
GAM_SETS = ["cal+hol+sched"]
# the realistic production set: everything known in advance, no oracle weather
MAIN_SET = "cal+hol+sched"
BINARY_EFFECTS = {
    "public holiday (own region)": "hol = 1",
    "school holiday (own region)": "school = 1",
    "any school holiday (own or neighbour)": "school_any = 1",
    "neighbour public holiday": "hol_nb = 1",
    "bridge day": "bridge = 1",
    "day before a public holiday": "hol_next = 1 AND hol = 0",
    "day after a public holiday": "hol_prev = 1 AND hol = 0",
    "school break starts vs reference (d_school > 0)": "d_school > 0",
    "school break ended vs reference (d_school < 0)": "d_school < 0",
    "reference contains a public holiday": "ref_hol > 0 AND hol = 0",
    "ticketed event (TICKETED_EVENT, Sep+ only)": "ticketed = 1",
    "extra hours (EXTRA_HOURS, Sep+ only)": "extra = 1",
    "opening hours >= 1 h longer than reference": "d_hours >= 1",
    "opening hours >= 1 h shorter than reference": "d_hours <= -1",
    "closes >= 2 h later than reference (evening event)": "d_close >= 2",
    "first 7 days of the season": "since_season_start < 7",
    "last 7 days of the season": "to_season_end < 7",
    "first day after a closure of >= 2 days": "since_prev_open >= 3",
    "schedule NOT yet published at the origin": "sched_known = 0",
    "rain >= 10 mm (ORACLE)": "precip >= 10",
    "max temp >= 32 C (ORACLE)": "temp_max >= 32",
    "max temp < 12 C (ORACLE)": "temp_max < 12",
    "temp >= 5 C below reference days (ORACLE)": "d_temp <= -5",
}


# --------------------------------------------------------------------------- data

def load(export: Path, con=None, leads: list[int] | None = None, window_days: int = 56):
    """DuckDB connection with the driver table ``d`` for every ride-day and lead."""
    import duckdb

    con = con or duckdb.connect()
    x = con.execute
    x("SET TimeZone='UTC'")
    pq = export / "parquet"
    for t in ("parks", "attractions", "windows", "park_day_cov", "ride_day"):
        x(f"CREATE OR REPLACE TABLE {t} AS SELECT * FROM read_parquet('{pq}/{t}.parquet')")
    x("""CREATE OR REPLACE TABLE rides AS SELECT a.id AS aid, a.park_id,
         coalesce(a.is_headliner, false) AS is_headliner FROM attractions a""")
    sched = export / "raw" / "schedule.csv.gz"
    x(f"CREATE OR REPLACE TABLE ev AS {D.event_sql(str(sched) if sched.exists() else None)}")
    x("CREATE OR REPLACE VIEW rdh AS SELECT aid, park_id, date, p90 FROM ride_day")
    D.park_day_features(con)
    d0 = x("SELECT min(date) FROM ride_day").fetchone()[0]
    lead_list = ",".join(str(v) for v in (leads or LEADS))
    x(f"""CREATE OR REPLACE TABLE q AS SELECT aid, park_id, date - L AS origin, date, L
          FROM ride_day, (SELECT unnest([{lead_list}]) AS L)
          WHERE date - L >= DATE '{d0}' + {window_days}""")
    x(f"CREATE OR REPLACE TABLE d AS {D.driver_rows_sql(window_days)}")
    return con


def frame(con) -> pd.DataFrame:
    return D.add_asof(con.execute("SELECT * FROM d").df())


# --------------------------------------------------------------------------- models

def design(df: pd.DataFrame, feats: list[str]) -> pd.DataFrame:
    X = df[feats].copy()
    for c in X.columns:
        if c in D.CATEGORICAL:
            X[c] = X[c].astype("category")
        else:
            X[c] = pd.to_numeric(X[c], errors="coerce").astype("float64")
    return X


def make_model(kind: str, feats: list[str], seed: int = 830):
    if kind == "hgb":
        from sklearn.ensemble import HistGradientBoostingRegressor

        return HistGradientBoostingRegressor(
            loss="absolute_error", max_iter=300, learning_rate=0.08, max_leaf_nodes=31,
            min_samples_leaf=200, l2_regularization=1.0, categorical_features="from_dtype",
            random_state=seed)
    if kind == "gam":
        from sklearn.compose import ColumnTransformer
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import Ridge
        from sklearn.pipeline import make_pipeline
        from sklearn.preprocessing import OneHotEncoder, SplineTransformer, StandardScaler

        cats = [f for f in feats if f in D.CATEGORICAL]
        nums = [f for f in feats if f not in D.CATEGORICAL]
        tr = []
        if nums:
            tr.append(("num", make_pipeline(SimpleImputer(strategy="median", add_indicator=True),
                                            StandardScaler(),
                                            SplineTransformer(n_knots=5, degree=3,
                                                              extrapolation="constant")), nums))
        if cats:
            tr.append(("cat", OneHotEncoder(handle_unknown="ignore"), cats))
        return make_pipeline(ColumnTransformer(tr), Ridge(alpha=10.0))
    raise ValueError(kind)


class Fitted:
    """A fitted driver model: predicts the log ratio ŷ for a driver frame."""

    def __init__(self, kind: str, feats: list[str], model, offset: float):
        self.kind, self.feats, self.model, self.offset = kind, feats, model, offset

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        if self.model is None:
            return np.full(len(df), self.offset)
        return self.model.predict(_x(self.kind, df, self.feats)) + self.offset


def _x(kind: str, df: pd.DataFrame, feats: list[str]) -> pd.DataFrame:
    X = design(df, feats)
    if kind == "gam":
        for c in D.CATEGORICAL:
            if c in feats:
                X[c] = X[c].astype(str)
    return X


def fit_model(kind: str, feats: list[str], tr: pd.DataFrame, seed: int = 830) -> Fitted:
    # a driver that never varies in the training rows (e.g. an as-of schedule column
    # that is all NULL in an early fold) carries nothing and breaks binning / splines
    feats = [f for f in feats if tr[f].nunique(dropna=True) > 1]
    y = tr["y"].to_numpy()
    if not feats:
        return Fitted(kind, [], None, float(np.median(y)))
    m = make_model(kind, feats, seed)
    m.fit(_x(kind, tr, feats), y)
    offset = 0.0
    if kind == "gam":
        # ridge fits the MEAN of the log ratio; the level MAE wants the median —
        # shift by the median training residual (heavy tails)
        offset = float(np.median(y - m.predict(_x(kind, tr, feats))))
    return Fitted(kind, feats, m, offset)


def fit_predict(kind: str, feats: list[str], tr: pd.DataFrame, te: pd.DataFrame,
                max_train: int, seed: int = 830) -> np.ndarray:
    if len(tr) > max_train:
        tr = tr.sample(max_train, random_state=seed)
    return fit_model(kind, feats, tr, seed).predict(te)


def walk_forward(d: pd.DataFrame, kinds: list[str], sets: dict[str, list[str]],
                 min_train: int = 20000, max_train: int = 400_000, log=print) -> pd.DataFrame:
    """Out-of-sample predictions ``yhat__<kind>__<set>`` for every row whose origin
    month has a model (training rows: target date before the first of the month)."""
    d = d.copy()
    d["om"] = pd.to_datetime(d["origin"]).dt.to_period("M")
    out = []
    for om in sorted(d["om"].unique()):
        start = om.to_timestamp().date()
        tr = d[pd.to_datetime(d["date"]).dt.date < start]
        te = d[d["om"] == om].copy()
        if len(tr) < min_train:
            log(f"  fold {om}: {len(tr)} training rows < {min_train}, skipped")
            continue
        t0 = time.monotonic()
        for kind in kinds:
            for name, groups in sets.items():
                if kind == "gam" and name not in GAM_SETS:
                    continue
                te[f"yhat__{kind}__{name}"] = fit_predict(kind, D.features(groups), tr, te, max_train)
        te["yhat__none__bias"] = float(np.median(tr["y"]))
        out.append(te)
        log(f"  fold {om}: train {len(tr)} test {len(te)} {time.monotonic() - t0:.0f}s")
    return pd.concat(out, ignore_index=True) if out else d.iloc[:0]


# --------------------------------------------------------------------------- scoring

def _ci(p, lo, hi, nd=3):
    return f"{p:.{nd}f} [{lo:.{nd}f}, {hi:.{nd}f}]"


def score(p: pd.DataFrame, cols: list[str], cfg: BenchConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Ride-level and park-level (headliner mean) R² vs naive and level MAE vs naive."""
    reps, seed = cfg.bootstrap_reps, cfg.bootstrap_seed
    p = p.copy()
    p["unit"] = p["park_id"] + "|" + p["date"].astype(str)
    rows, prow = [], []
    for L, g in p.groupby("L"):
        e0 = np.abs(g["ref"] - g["true_p90"])
        agg0 = g.assign(y2=g["y"] ** 2, e0=e0).groupby("unit")[["y2", "e0"]].sum()
        n = g.groupby("unit").size().reindex(agg0.index)
        h = g[g["is_headliner"] == 1]
        pk = h.groupby(["park_id", "date"]).agg(true_lvl=("true_p90", "mean"), naive=("ref", "mean"))
        pk["y"] = np.log(pk["true_lvl"] / pk["naive"])
        for c in cols:
            lv = g["ref"] * np.exp(g[c])
            r = g["y"] - g[c]
            a = pd.DataFrame({"unit": g["unit"], "r2": r ** 2, "e": np.abs(lv - g["true_p90"])})
            a = a.groupby("unit").sum().reindex(agg0.index)
            r2 = ratio_ci(a["r2"].to_numpy(), agg0["y2"].to_numpy(), reps, seed)
            md = paired_diff_ci(a["e"].to_numpy(), n.to_numpy(), agg0["e0"].to_numpy(), n.to_numpy(), reps, seed)
            rows.append({"lead": L, "model": c, "rows": len(g), "park_days": len(agg0),
                         "r2_vs_naive": 1 - r2[0], "r2_lo": 1 - r2[2], "r2_hi": 1 - r2[1],
                         "mae_naive": e0.mean(), "mae_model": a["e"].sum() / n.sum(),
                         "mae_diff": md[0], "mae_diff_lo": md[1], "mae_diff_hi": md[2]})
            # park level
            hp = h.assign(lv=h["ref"] * np.exp(h[c])).groupby(["park_id", "date"])["lv"].mean()
            k = pk.join(hp.rename("pred"))
            k["yp"] = np.log(k["pred"] / k["naive"])
            e0p, ep = np.abs(k["naive"] - k["true_lvl"]), np.abs(k["pred"] - k["true_lvl"])
            one = np.ones(len(k))
            r2p = ratio_ci(((k["y"] - k["yp"]) ** 2).to_numpy(), (k["y"] ** 2).to_numpy(), reps, seed)
            mdp = paired_diff_ci(ep.to_numpy(), one, e0p.to_numpy(), one, reps, seed)
            prow.append({"lead": L, "model": c, "park_days": len(k),
                         "r2_vs_naive": 1 - r2p[0], "r2_lo": 1 - r2p[2], "r2_hi": 1 - r2p[1],
                         "mae_naive": e0p.mean(), "mae_model": ep.mean(),
                         "mae_diff": mdp[0], "mae_diff_lo": mdp[1], "mae_diff_hi": mdp[2]})
    return pd.DataFrame(rows), pd.DataFrame(prow)


def uc4_spearman(p: pd.DataFrame, cols: list[str], cfg: BenchConfig, min_days: int = 8) -> pd.DataFrame:
    """Within park-month Spearman of the headliner-mean level, paired vs naive (as the harness)."""
    h = p[p["is_headliner"] == 1]
    rows = []
    for L, g in h.groupby("L"):
        agg = {"true_lvl": ("true_p90", "mean"), "naive": ("ref", "mean")}
        gg = g.assign(**{f"lv__{c}": g["ref"] * np.exp(g[c]) for c in cols})
        for c in cols:
            agg[c] = (f"lv__{c}", "mean")
        k = gg.groupby(["park_id", "date"]).agg(**agg).reset_index()
        k["mon"] = pd.to_datetime(k["date"]).dt.to_period("M")
        res = {c: [] for c in cols}
        base = []
        for _, m in k.groupby(["park_id", "mon"]):
            if len(m) < min_days:
                continue
            sb = m["true_lvl"].rank().corr(m["naive"].rank())
            vals = {c: m["true_lvl"].rank().corr(m[c].rank()) for c in cols}
            if not np.isfinite(sb) or not all(np.isfinite(v) for v in vals.values()):
                continue
            base.append(sb)
            for c in cols:
                res[c].append(vals[c])
        if len(base) < 5:
            continue
        b = np.array(base)
        one = np.ones(len(b))
        rows.append({"lead": L, "model": "naive", "park_months": len(b), "spearman": b.mean()})
        for c in cols:
            a = np.array(res[c])
            dd = paired_diff_ci(a, one, b, one, cfg.bootstrap_reps, cfg.bootstrap_seed)
            rows.append({"lead": L, "model": c, "park_months": len(a), "spearman": a.mean(),
                         "diff_vs_naive": dd[0], "diff_lo": dd[1], "diff_hi": dd[2]})
    return pd.DataFrame(rows)


def univariate(d: pd.DataFrame, L: int, cfg: BenchConfig, reps: int = 500) -> pd.DataFrame:
    """Mean log deviation from naive with the driver vs without, park-cluster bootstrap."""
    import duckdb

    g = d[d["L"] == L]
    con = duckdb.connect()
    con.register("g", g)
    parks = g["park_id"].unique()
    rng = np.random.default_rng(cfg.bootstrap_seed)
    W = rng.poisson(1.0, size=(reps, len(parks))).astype(np.float64)
    rows = []
    for label, cond in BINARY_EFFECTS.items():
        s = con.execute(f"""SELECT park_id, coalesce(({cond}), false) AS f, count(*) n, sum(y) sy
                            FROM g GROUP BY ALL""").df()
        if s.empty or s["f"].sum() == 0:
            continue
        on = s[s["f"]].set_index("park_id").reindex(parks).fillna(0)
        off = s[~s["f"]].set_index("park_id").reindex(parks).fillna(0)
        n1, s1, n0, s0 = (on["n"].to_numpy(), on["sy"].to_numpy(), off["n"].to_numpy(), off["sy"].to_numpy())
        if n1.sum() < 30:
            continue
        eff = s1.sum() / n1.sum() - s0.sum() / n0.sum()
        with np.errstate(invalid="ignore", divide="ignore"):
            bs = (W @ s1) / (W @ n1) - (W @ s0) / (W @ n0)
        bs = bs[np.isfinite(bs)]
        lo, hi = np.quantile(bs, [0.025, 0.975]) if bs.size else (np.nan, np.nan)
        rows.append({"driver": label, "lead": L, "rows_with": int(n1.sum()),
                     "parks_with": int((n1 > 0).sum()), "share": n1.sum() / (n1.sum() + n0.sum()),
                     "effect_log": eff, "lo": lo, "hi": hi, "effect_pct": np.expm1(eff) * 100,
                     "pct_lo": np.expm1(lo) * 100, "pct_hi": np.expm1(hi) * 100,
                     "significant": bool(lo > 0 or hi < 0)})
    return pd.DataFrame(rows).sort_values("effect_log", key=np.abs, ascending=False)


def permutation_ranking(d: pd.DataFrame, feats: list[str], test_month: str, L_set: list[int],
                        n_eval: int = 60000, seed: int = 830) -> pd.DataFrame:
    """Permutation importance (MAE of y) of the HGB main-set model on one late fold."""
    from sklearn.inspection import permutation_importance

    d = d.copy()
    d["om"] = pd.to_datetime(d["origin"]).dt.to_period("M").astype(str)
    start = pd.Period(test_month).to_timestamp().date()
    tr = d[pd.to_datetime(d["date"]).dt.date < start]
    te = d[(d["om"] == test_month) & d["L"].isin(L_set)]
    if len(tr) > 400_000:
        tr = tr.sample(400_000, random_state=seed)
    if len(te) > n_eval:
        te = te.sample(n_eval, random_state=seed)
    f = fit_model("hgb", feats, tr, seed)
    feats = f.feats
    r = permutation_importance(f.model, design(te, feats), te["y"].to_numpy(), n_repeats=5,
                               random_state=seed, scoring="neg_mean_absolute_error")
    grp = {f: g for g, fs in D.GROUPS.items() for f in fs}
    return (pd.DataFrame({"feature": feats, "group": [grp[f] for f in feats],
                          "mae_increase": r.importances_mean, "sd": r.importances_std})
            .sort_values("mae_increase", ascending=False))


# --------------------------------------------------------------------------- report

def md(df: pd.DataFrame, cols: list[str] | None = None, nd: int = 3) -> str:
    if df is None or df.empty:
        return "_(no rows)_\n"
    df = df[cols] if cols else df
    out = ["| " + " | ".join(df.columns) + " |", "|" + "---|" * len(df.columns)]
    for _, r in df.iterrows():
        cells = []
        for v in r:
            cells.append(f"{v:.{nd}f}" if isinstance(v, (float, np.floating)) else str(v))
        out.append("| " + " | ".join(cells) + " |")
    return "\n".join(out) + "\n"


def add_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--export", required=True, help="export dir (parquet/ and raw/schedule.csv.gz)")
    p.add_argument("--out", required=True, help="output dir for tables + drivers.md")
    p.add_argument("--max-train", type=int, default=400_000)
    p.add_argument("--kinds", default="gam,hgb")


def main(args: argparse.Namespace) -> int:
    cfg = BenchConfig()
    out = Path(args.out)
    (out / "tables").mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    log = lambda s: print(f"[{time.monotonic() - t0:6.0f}s] {s}", flush=True)  # noqa: E731
    con = load(Path(args.export))
    d = frame(con)
    log(f"driver table: {len(d)} rows, {d['aid'].nunique()} rides, {d['park_id'].nunique()} parks")
    noise = d.groupby("L").agg(rows=("y", "size"), mean_y=("y", "mean"), sd_y=("y", "std"),
                               mean_abs_y=("y", lambda s: s.abs().mean()),
                               schedule_known_at_origin=("sched_known", "mean")).reset_index()
    noise = noise.rename(columns={"L": "lead"})
    noise.to_csv(out / "tables/target_by_lead.csv", index=False)

    kinds = args.kinds.split(",")
    sets = dict(FEATURE_SETS)
    # group ablations: cal+hol (drop sched) and cal+sched (drop hol) against cal+hol+sched
    p = walk_forward(d, kinds, sets, max_train=args.max_train, log=log)
    cols = ["yhat__none__bias"] + [c for c in p.columns if c.startswith("yhat__") and c != "yhat__none__bias"]
    # common target window: every lead scored on the same target days (the first
    # scored origin + the longest lead), so the horizon curve compares like with like
    first = pd.to_datetime(p["origin"]).min()
    common_start = (first + pd.Timedelta(days=max(LEADS))).date()
    p_common = p[pd.to_datetime(p["date"]).dt.date >= common_start]
    for tag, pp in (("common", p_common), ("all", p)):
        ride, park = score(pp, cols, cfg)
        uc4 = uc4_spearman(pp, cols, cfg)
        for t in (ride, park, uc4):
            t["model"] = t["model"].str.replace("yhat__", "")
            t.insert(0, "window", tag)
        ride.to_csv(out / f"tables/r2_mae_ride_{tag}.csv", index=False)
        park.to_csv(out / f"tables/r2_mae_park_{tag}.csv", index=False)
        uc4.to_csv(out / f"tables/uc4_spearman_{tag}.csv", index=False)
        log(f"scored {tag}")
    uni = pd.concat([univariate(d, L, cfg) for L in (1, 7, 30)])
    uni.to_csv(out / "tables/univariate_effects.csv", index=False)
    log("univariate")
    last = sorted(pd.to_datetime(p["origin"]).dt.to_period("M").astype(str).unique())
    feats = D.features(["cal", "hol", "sched", "wx"])  # wx included to rank it (ORACLE)
    perm = permutation_ranking(d, feats, last[-2] if len(last) > 1 else last[-1], [1, 7, 30])
    perm.to_csv(out / "tables/permutation_importance.csv", index=False)
    log("permutation")
    # per-season / per-region breakdown of the main model at L=7
    main_col = f"yhat__hgb__{MAIN_SET}" if "hgb" in kinds else f"yhat__{kinds[0]}__{MAIN_SET}"
    br = []
    for key, f in (("region", lambda x: x["region"]),
                   ("season", lambda x: pd.to_datetime(x["date"]).dt.month.map(
                       lambda m: "winter" if m in (12, 1, 2) else "spring" if m in (3, 4, 5)
                       else "summer" if m in (6, 7, 8) else "autumn"))):
        for k, g in p.assign(k=f(p)).groupby("k"):
            r, _ = score(g, [main_col], cfg)
            r.insert(0, key, k)
            br.append(r)
    pd.concat(br).to_csv(out / "tables/breakdown_region_season.csv", index=False)
    meta = {"export": str(args.export), "leads": LEADS, "common_target_start": str(common_start), "feature_sets": sets, "groups": D.GROUPS,
            "rows": len(d), "scored_rows": len(p), "folds": sorted(p["om"].astype(str).unique().tolist()),
            "seconds": round(time.monotonic() - t0, 1),
            "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}
    (out / "drivers-run.json").write_text(json.dumps(meta, indent=2, default=str))
    log("done")
    return 0
