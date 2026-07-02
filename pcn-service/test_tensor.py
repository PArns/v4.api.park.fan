"""Unit tests for the cross-ride tensor assembly (no DB).

Covers the logic where bugs hide: common-grid alignment, observed/closed masking,
the park-open heuristic, the scalar crowd baseline (mean over OBSERVED rides only),
and forward-fill of the context level.

    cd pcn-service && python3 -m pytest test_tensor.py -q
"""

import numpy as np
import pandas as pd
import pytest

import tensor
from tensor import CHANNELS


def _row(uid, ts, y, n_obs, down=0):
    return {"unique_id": uid, "ds": pd.Timestamp(ts), "y": y,
            "n_obs": n_obs, "down_count": down}


def _scenario():
    """3 rides over four 15-min slots 10:00..10:45.
    a: gap at 10:30;  b: full;  c: DOWN-only at 10:00, observed only at 10:30."""
    return pd.DataFrame([
        _row("a", "2026-06-01 10:00", 10.0, 5),
        _row("a", "2026-06-01 10:15", 20.0, 5),
        _row("a", "2026-06-01 10:45", 40.0, 5),
        _row("b", "2026-06-01 10:00", 30.0, 4),
        _row("b", "2026-06-01 10:15", 30.0, 4),
        _row("b", "2026-06-01 10:30", 30.0, 4),
        _row("b", "2026-06-01 10:45", 30.0, 4),
        _row("c", "2026-06-01 10:00", np.nan, 0, down=2),  # ride down, park open
        _row("c", "2026-06-01 10:30", 100.0, 6),
    ])


def test_regularize_builds_common_grid():
    reg = tensor.regularize(_scenario(), freq="15min")
    # 3 rides × 4 slots = 12 rows, every (ride, slot) present.
    assert len(reg) == 12
    assert sorted(reg["unique_id"].unique()) == ["a", "b", "c"]
    assert reg["ds"].nunique() == 4
    # a@10:30 is a gap → y NaN, obs_mask 0
    a1030 = reg[(reg.unique_id == "a") & (reg.ds == pd.Timestamp("2026-06-01 10:30"))]
    assert np.isnan(a1030["y"].iloc[0])
    assert a1030["obs_mask"].iloc[0] == 0.0


def test_shapes_and_channels():
    crt = tensor.build(_scenario(), park_id="P", min_rides_open=2)
    assert crt.ride_ids == ["a", "b", "c"]
    assert len(crt.slots) == 4
    assert crt.wait_raw.shape == (3, 4)
    assert crt.features.shape == (3, 4, len(CHANNELS))
    assert crt.channel_names == CHANNELS


def test_obs_mask_matches_observations():
    crt = tensor.build(_scenario(), min_rides_open=2)
    # rows sorted a,b,c ; slots 10:00,10:15,10:30,10:45
    np.testing.assert_array_equal(crt.obs_mask[0], [1, 1, 0, 1])  # a
    np.testing.assert_array_equal(crt.obs_mask[1], [1, 1, 1, 1])  # b
    np.testing.assert_array_equal(crt.obs_mask[2], [0, 0, 1, 0])  # c (down-only @10:00)


def test_park_occ_is_mean_over_observed_rides_only():
    crt = tensor.build(_scenario(), min_rides_open=2)
    # 10:00 → mean(a=10, b=30) = 20 (c not observed)
    # 10:15 → mean(20, 30) = 25
    # 10:30 → mean(b=30, c=100) = 65 (a is a gap)
    # 10:45 → mean(40, 30) = 35
    np.testing.assert_allclose(crt.park_occ, [20.0, 25.0, 65.0, 35.0])


def test_park_open_heuristic():
    # reporting per slot (obs OR down): 10:00=3 (a,b,c-down), others=2
    open2 = tensor.build(_scenario(), min_rides_open=2).park_open
    np.testing.assert_array_equal(open2, [1, 1, 1, 1])
    open3 = tensor.build(_scenario(), min_rides_open=3).park_open
    np.testing.assert_array_equal(open3, [1, 0, 0, 0])


def test_wait_ffill_forward_fills_then_zero():
    crt = tensor.build(_scenario(), min_rides_open=2)
    # a: 10,20,(ffill)20,40 ; c: leading gap → 0,0,100,(ffill)100
    np.testing.assert_allclose(crt.wait_ffill[0], [10, 20, 20, 40])
    np.testing.assert_allclose(crt.wait_ffill[2], [0, 0, 100, 100])
    # wait_ffill must be channel 0
    assert crt.channel_names[0] == "wait_ffill"
    np.testing.assert_allclose(crt.features[..., 0], crt.wait_ffill)


def test_down_channel():
    crt = tensor.build(_scenario(), min_rides_open=2)
    di = crt.channel_names.index("down")
    np.testing.assert_array_equal(crt.features[2, :, di], [2, 0, 0, 0])  # c down @10:00


def test_dow_channels_broadcast_and_weekend_flag():
    # _scenario() is Mon 2026-06-01 → is_weekend 0; a Saturday panel → 1.
    crt = tensor.build(_scenario(), min_rides_open=2)
    wi = crt.channel_names.index("is_weekend")
    np.testing.assert_array_equal(crt.features[..., wi], np.zeros((3, 4)))
    dsi = crt.channel_names.index("dow_sin")
    np.testing.assert_allclose(crt.features[0, :, dsi], np.sin(0.0))  # Monday = dow 0

    sat = pd.DataFrame([_row("a", "2026-06-06 10:00", 10.0, 5),
                        _row("b", "2026-06-06 10:00", 30.0, 4)])
    crt_sat = tensor.build(sat, min_rides_open=1)
    wsat = crt_sat.channel_names.index("is_weekend")
    np.testing.assert_array_equal(crt_sat.features[..., wsat], np.ones((2, 1)))
    # dow channels must sit AFTER the original 8 (append-only evolution contract —
    # older checkpoints select their channels by prefix-compatible names).
    assert CHANNELS.index("dow_sin") > CHANNELS.index("park_occ")


def test_loss_mask_excludes_closed_and_gaps():
    crt = tensor.build(_scenario(), min_rides_open=2)
    # park open everywhere → loss_mask == obs_mask ; total observed = 3+4+1 = 8
    np.testing.assert_array_equal(crt.loss_mask, crt.obs_mask)
    assert crt.loss_mask.sum() == 8
    # raise the bar so only slot 0 is open → loss only on observed rides in slot 0 (a,b)
    crt3 = tensor.build(_scenario(), min_rides_open=3)
    assert crt3.loss_mask.sum() == 2


def test_all_unobserved_interior_slot():
    """A slot with neither obs nor down → park_occ 0, park_open 0, no NaN leak."""
    panel = pd.DataFrame([
        _row("a", "2026-06-01 10:00", 10.0, 5),
        _row("a", "2026-06-01 10:45", 40.0, 5),
    ])
    crt = tensor.build(panel, min_rides_open=1)
    assert len(crt.slots) == 4  # grid spans 10:00..10:45
    np.testing.assert_array_equal(crt.park_open, [1, 0, 0, 1])
    np.testing.assert_allclose(crt.park_occ, [10.0, 0.0, 0.0, 40.0])
    assert not np.isnan(crt.features).any()  # no NaN anywhere in the tensor


def test_empty_panel():
    reg = tensor.regularize(pd.DataFrame(columns=["unique_id", "ds", "y", "n_obs", "down_count"]))
    assert reg.empty


def test_align_ride_axis_reorders_fills_and_drops():
    """Serving alignment: tensor ride axis → trained node order. Present rides keep
    their rows (reordered), rides absent from the window become synthetic quiet nodes
    (ride channels 0, shared time/park channels kept, flagged in the mask), and rides
    unknown to the model are dropped (CatBoost fallback until retrain)."""
    crt = tensor.build(_scenario(), park_id="P", min_rides_open=2)  # rides a, b, c
    trained = ["b", "zombie", "a"]  # reordered, one long-dead ride, c dropped
    aligned, present = tensor.align_ride_axis(crt, trained)

    assert aligned.ride_ids == trained
    np.testing.assert_array_equal(present, [True, False, True])
    # present rides carry their original rows in the NEW order
    bi, ai = crt.ride_ids.index("b"), crt.ride_ids.index("a")
    np.testing.assert_allclose(aligned.features[0], crt.features[bi])
    np.testing.assert_allclose(aligned.features[2], crt.features[ai])
    # absent ride: ride-specific channels zeroed, shared channels preserved
    for name in ("wait_ffill", "obs_mask", "down"):
        ci = aligned.channel_names.index(name)
        np.testing.assert_array_equal(aligned.features[1, :, ci], np.zeros(4))
    for name in ("slot_sin", "hour_cos", "park_occ", "dow_cos", "is_weekend"):
        ci = aligned.channel_names.index(name)
        np.testing.assert_allclose(aligned.features[1, :, ci], crt.features[0, :, ci])
    # wait_raw NaN for the absent ride; park-level arrays untouched
    assert np.isnan(aligned.wait_raw[1]).all()
    np.testing.assert_array_equal(aligned.park_open, crt.park_open)
    assert not np.isnan(aligned.features).any()


def test_align_ride_axis_identity_when_matching():
    crt = tensor.build(_scenario(), min_rides_open=2)
    aligned, present = tensor.align_ride_axis(crt, list(crt.ride_ids))
    assert present.all()
    np.testing.assert_allclose(aligned.features, crt.features)


def test_npz_roundtrip(tmp_path):
    crt = tensor.build(_scenario(), park_id="P", min_rides_open=2)
    p = tmp_path / "crt.npz"
    crt.to_npz(str(p))
    z = np.load(p, allow_pickle=True)
    np.testing.assert_allclose(z["park_occ"], crt.park_occ)
    np.testing.assert_allclose(z["features"], crt.features)
    assert list(z["channel_names"]) == CHANNELS
    assert z["features"].shape == (3, 4, len(CHANNELS))


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
