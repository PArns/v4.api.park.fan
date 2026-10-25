"""Checkpoints are written atomically (PAR-1054).

    cd pcn-service && python3 -m pytest test_atomic_save.py -q
"""

import os

import pytest
import torch

import torch_model


def _model():
    m = torch_model.TorchSeqModel()
    m.net = torch.nn.Linear(2, 1)
    m._dims = (1, 2, 1)
    return m


def test_save_replaces_the_file_and_leaves_no_temp(tmp_path):
    path = tmp_path / "park.pt"
    _model().save(str(path), ride_ids=["a"])
    assert torch.load(str(path), weights_only=False)["ride_ids"] == ["a"]
    assert os.listdir(tmp_path) == ["park.pt"]


def test_a_crash_mid_write_keeps_the_old_checkpoint(tmp_path, monkeypatch):
    path = tmp_path / "park.pt"
    _model().save(str(path), ride_ids=["old"])

    def broken_save(obj, f, *a, **k):
        f.write(b"half a checkpoint")
        raise OSError("killed mid-write")

    monkeypatch.setattr(torch_model.torch, "save", broken_save)
    with pytest.raises(OSError):
        _model().save(str(path), ride_ids=["new"])

    assert torch.load(str(path), weights_only=False)["ride_ids"] == ["old"]
    assert os.listdir(tmp_path) == ["park.pt"]
