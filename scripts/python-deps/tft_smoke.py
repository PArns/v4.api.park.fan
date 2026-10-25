"""nf-service's TFT call pattern (DistributionLoss StudentT, futr/hist/stat exog, robust
scaler, start padding, fit+predict with futr_df) on a tiny synthetic panel."""
import os
os.environ.setdefault("PYTORCH_ENABLE_MPS_FALLBACK", "1")
import numpy as np, pandas as pd, torch, neuralforecast
from neuralforecast import NeuralForecast
from neuralforecast.models import TFT
from neuralforecast.losses.pytorch import DistributionLoss
torch.set_num_threads(2)
rng = np.random.default_rng(0)
H, L = 7, 28
rows = []
for u in ["r1", "r2", "r3"]:
    n = 120 if u != "r3" else 20  # r3 shorter than input_size -> start padding
    ds = pd.date_range("2026-03-01", periods=n + H, freq="D")
    for i, d in enumerate(ds):
        rows.append({"unique_id": u, "ds": d, "y": 30 + 10 * (d.dayofweek >= 5) + rng.normal(0, 3),
                     "is_holiday": float(d.dayofweek == 0), "occ": rng.random()})
df = pd.DataFrame(rows)
last = df.groupby("unique_id").ds.transform("max") - pd.Timedelta(days=H)
hist, fut = df[df.ds <= last], df[df.ds > last].drop(columns=["y", "occ"])
static = pd.DataFrame({"unique_id": ["r1", "r2", "r3"], "is_headliner": [1.0, 0.0, 1.0]})
m = TFT(h=H, input_size=L, futr_exog_list=["is_holiday"], hist_exog_list=["occ"], stat_exog_list=["is_headliner"],
        scaler_type="robust", loss=DistributionLoss(distribution="StudentT", level=[80, 90]), max_steps=20,
        batch_size=8, windows_batch_size=16, inference_windows_batch_size=16, start_padding_enabled=True,
        hidden_size=16, learning_rate=1e-3, random_seed=1, enable_progress_bar=False, enable_model_summary=False,
        accelerator="cpu", logger=False)
nf = NeuralForecast(models=[m], freq="D")
nf.fit(df=hist, static_df=static)
yh = nf.predict(df=hist, static_df=static, futr_df=fut)
yh = yh.reset_index() if yh.index.name else yh
print("neuralforecast", neuralforecast.__version__, "torch", torch.__version__)
print("columns", list(yh.columns), "rows", len(yh), "finite", bool(np.isfinite(yh.filter(like="TFT").to_numpy()).all()))
print("TFT mean", round(float(yh["TFT"].mean()), 3) if "TFT" in yh else yh.filter(like="TFT").iloc[:, 0].mean())
