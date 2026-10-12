"""The pcn_forecasts DDL runs once per process, not on every write (PAR-868).

`CREATE INDEX IF NOT EXISTS` takes a SHARE lock on pcn_forecasts before it sees
the index exists, which collides with VACUUM. After the first committed write
the producer must issue only its DML.

    cd pcn-service && python3 -m pytest test_db_ddl_once.py -q
"""

import contextlib

import pytest

import db


class _Conn:
    def __init__(self, log):
        self.log = log

    def execute(self, stmt, params=None):
        self.log.append(str(stmt).strip().split()[0:3])

        class _Res:
            rowcount = 0

        return _Res()


class _Engine:
    def __init__(self, log):
        self.log = log

    @contextlib.contextmanager
    def begin(self):
        yield _Conn(self.log)


@pytest.fixture
def statements(monkeypatch):
    log: list[list[str]] = []
    monkeypatch.setattr(db, "engine", lambda: _Engine(log))
    monkeypatch.setattr(db, "_PCN_FORECASTS_SCHEMA_READY", False)
    return log


def _ddl(log):
    return [s for s in log if s[0] == "CREATE"]


def test_first_write_runs_the_ddl_later_writes_do_not(statements):
    row = {"aid": "a", "ts": 0, "os": 0, "q": 0.5, "pw": 1.0}
    db.write_pcn_forecasts([dict(row)], "v1")
    assert len(_ddl(statements)) == 3

    statements.clear()
    db.write_pcn_forecasts([dict(row)], "v1")
    db.prune_pcn_forecasts(3)
    assert _ddl(statements) == []
    assert [s[0] for s in statements] == ["INSERT", "DELETE"]


def test_a_failed_transaction_does_not_mark_the_schema_ready(monkeypatch, statements):
    class _Boom(_Engine):
        @contextlib.contextmanager
        def begin(self):
            yield _Conn(self.log)
            raise RuntimeError("rolled back")

    monkeypatch.setattr(db, "engine", lambda: _Boom(statements))
    with pytest.raises(RuntimeError):
        db.prune_pcn_forecasts(3)
    assert db._PCN_FORECASTS_SCHEMA_READY is False
