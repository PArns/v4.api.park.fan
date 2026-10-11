import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))


@pytest.fixture(scope="session")
def synth_export(tmp_path_factory):
    import synth
    from mlbench.build import build, connect

    root = tmp_path_factory.mktemp("export")
    synth.write(root / "raw")
    con = connect("1GB", 2)
    build(root / "raw", root / "parquet", con)
    return root


@pytest.fixture(autouse=True)
def _foundation_stub(monkeypatch):
    """The foundation-model plug-ins (PAR-828) need GPU weights; the suite runs their
    series construction against a deterministic stub backend instead."""
    import os

    if not os.environ.get("MLBENCH_FM_REAL"):
        monkeypatch.setenv("MLBENCH_FM_BACKEND", "stub")
