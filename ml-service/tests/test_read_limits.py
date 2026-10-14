"""The training read sets its own deadline (PAR-837)."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import db  # noqa: E402


class RecordingSession:
    def __init__(self):
        self.statements = []

    def execute(self, clause, *args, **kwargs):
        self.statements.append(str(clause))


def test_apply_read_limits_issues_local_settings_only():
    session = RecordingSession()
    db.apply_read_limits(session, 300_000, 10_000)
    assert session.statements == [
        "SET LOCAL statement_timeout = 300000",
        "SET LOCAL lock_timeout = 10000",
    ]


def test_training_read_deadline_is_a_ceiling_not_a_tight_fit():
    # 42.6 s measured on 2026-10-10; anything under 3x would fail on growth.
    assert db.TRAINING_READ_STATEMENT_TIMEOUT_MS >= 3 * 42_600
