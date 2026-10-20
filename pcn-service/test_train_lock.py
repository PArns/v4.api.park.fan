"""/train takes the training lock before the runner exists (PAR-943).

    cd pcn-service && python3 -m pytest test_train_lock.py -q
"""

import threading

import pytest
from fastapi import HTTPException

import main


class _Runner:
    """A train_runner that has not written its own status yet."""

    def __init__(self, *args, **kwargs):
        self.release = threading.Event()
        _Runner.last = self

    def wait(self):
        self.release.wait(5)
        return 0

    returncode = 0


@pytest.fixture
def status_file(monkeypatch, tmp_path):
    path = str(tmp_path / "status.json")
    monkeypatch.setattr(main, "_TRAIN_STATUS", path)
    monkeypatch.setattr(main.settings, "MODEL_DIR", str(tmp_path))
    return path


def test_a_second_train_is_refused_before_the_runner_writes(monkeypatch, status_file):
    monkeypatch.setattr("subprocess.Popen", _Runner)

    main.train(main.TrainRequest(version="pcn-a"))
    try:
        with pytest.raises(HTTPException) as err:
            main.train(main.TrainRequest(version="pcn-b"))
        assert err.value.status_code == 409
    finally:
        _Runner.last.release.set()


def test_a_runner_that_cannot_start_releases_the_lock(monkeypatch, status_file):
    def boom(*args, **kwargs):
        raise OSError("no python")

    monkeypatch.setattr("subprocess.Popen", boom)

    main.train(main.TrainRequest(version="pcn-a"))
    for t in threading.enumerate():
        if t is not threading.current_thread() and t.daemon:
            t.join(2)

    st = main._read(status_file, {})
    assert st["is_training"] is False
    assert st["status"] == "failed"
