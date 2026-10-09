"""Runner: strict information cut, plug-in path == built-in path, report end to end."""

import datetime as dt

import duckdb
import numpy as np
import pandas as pd

from mlbench import baselines as B
from mlbench.config import BenchConfig
from mlbench.models import LevelH5Example
from mlbench.report import build_report
from mlbench.runner import Runner

PRED_COLS = ["snaive7", "wt_med", "wt_q80", "wt_q95", "clim", "h5", "lvlh5_naive", "lvlh5_tft",
             "lvlh5_cbd", "prod_served", "busy"]


def _targets(r: Runner, c: dt.date) -> pd.DataFrame:
    B.snapshot_tables(r.con, c.replace(day=1).isoformat(), r.cfg)
    B.window_tables(r.con, c.isoformat(), r.cfg)
    B.target_tables(r.con, c.isoformat(), r.slot_leads(c), r.cfg)
    return r.con.execute(f"SELECT aid, slot_utc, L, {', '.join(PRED_COLS)} FROM tg ORDER BY aid, slot_utc").df()


def test_information_cut(synth_export, tmp_path):
    """Poisoning everything at/after the origin must not move any forecast made at it."""
    cfg = BenchConfig(memory_limit="1GB", threads=2)
    r = Runner(synth_export, tmp_path / "run", cfg, [], materialize=True)
    c = dt.date(2026, 4, 20)
    before = _targets(r, c)
    origin_cut = "(SELECT o.origin_utc FROM o WHERE o.park_id = {t}.park_id)"
    r.con.execute(f"UPDATE truth SET y = y * 10 + 7 WHERE slot_utc + INTERVAL 15 MINUTE > {origin_cut.format(t='truth')}")
    r.con.execute(f"UPDATE ride_day SET p90 = p90 * 10 WHERE date >= DATE '{c}'")
    r.con.execute("UPDATE tft SET peak = peak * 10 WHERE created_utc >= (SELECT min(origin_utc) FROM o)")
    r.con.execute("UPDATE cbd SET peak = peak * 10 WHERE created_utc >= (SELECT min(origin_utc) FROM o)")
    after = _targets(r, c)
    assert len(before) == len(after) > 0
    for col in PRED_COLS:
        a, b = before[col].to_numpy(dtype=float), after[col].to_numpy(dtype=float)
        assert np.allclose(a, b, equal_nan=True), col


def test_plugin_example_equals_builtin(synth_export, tmp_path):
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3, 7])
    out = tmp_path / "run"
    r = Runner(synth_export, out, cfg, [LevelH5Example()], origins=("2026-04-10", "2026-04-12"))
    r.run()
    s = duckdb.connect().execute(f"""SELECT sum(n__lvlh5_naive) a, sum(n__example_level_h5) b,
        sum(sae__lvlh5_naive) c, sum(sae__example_level_h5) d FROM '{out}/parts/slot/*.parquet'""").fetchone()
    assert s[0] == s[1] and s[0] > 0
    assert abs(s[2] - s[3]) < 1e-6


def test_end_to_end_report(synth_export, tmp_path):
    cfg = BenchConfig(memory_limit="1GB", threads=2, slot_leads=[0, 1, 3, 7, 14])
    out = tmp_path / "run"
    import json
    out.mkdir()
    (out / "run-0of1.json").write_text(json.dumps({"export": str(synth_export), "git_sha": "test"}))
    Runner(synth_export, out, cfg, [], origins=("2026-04-01", "2026-04-20")).run()
    tdir = build_report(out, reps=50)
    summary = (out / "summary.md").read_text()
    for section in ("## Horizon", "Hand-over", "## UC1", "## UC3", "## UC4", "## D1", "## D2", "## D3",
                    "## D4", "## D5", "## D6", "## D7", "## D8", "## D9"):
        assert section in summary, section
    curve = pd.read_csv(tdir / "horizon_curve_mae.csv")
    assert {"UC1", "UC2", "UC3"} <= set(curve["uc"])
    # MAE of a model is never pooled across leads: one row per (uc, lead, segment, model)
    assert not curve.duplicated(["uc", "lead", "segment", "model"]).any()
