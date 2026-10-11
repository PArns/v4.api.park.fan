"""Who produced an artefact: the git SHA, the hash of the sources, the image.

Two kinds of identity live here and they are NOT equally trustworthy:

* ``code_sha256`` is **measured** — a sha256 over the ``mlbench`` sources as they
  exist inside the process that is writing the artefact. It cannot be stale.
* ``git_sha`` / ``image_id`` are **asserted** by whoever launched the container
  (``--build-arg GIT_SHA`` / ``-e MLBENCH_IMAGE_ID``). A launcher that reuses a
  shell variable from an earlier commit records a SHA the code never had. That
  happened: ``results/20261010-sens-hb/run-0of1.json`` claims ``git_sha
  5c216ffa`` while its ``code_sha256`` is the one of ``925f9ac6``, and both
  sensitivity arms claim the same ``image_id`` although their code differs. So
  every record says which source its SHA came from, and the report cross-checks.

The report writes its own ``report-<tables-dir>.json`` as well, because the
tables are produced by the REPORT code, not by the code that produced
``parts/``: on this run the `parts/` are from ``ea9a8438`` and the published
tables from a commit three fixes later. Without a per-report record the only
evidence of which code wrote a table was its mtime.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path


def git_sha() -> str:
    env = os.environ.get("MLBENCH_GIT_SHA")
    if env and env != "unknown":
        return env
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True,
                                       cwd=Path(__file__).parent, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return "unknown"


def git_sha_source() -> str:
    """``env`` = asserted by the launcher (can be stale), ``git`` = read from the
    working tree, ``unknown`` = neither was available."""
    env = os.environ.get("MLBENCH_GIT_SHA")
    if env and env != "unknown":
        return "env"
    return "git" if git_sha() != "unknown" else "unknown"


def code_hash() -> str:
    """sha256 over the mlbench sources (path + bytes), so an artefact can be tied to
    code even when the SHA is a working tree or a stale environment variable.

    Reproducible from a git tree without running anything:

        for f in $(git ls-tree -r --name-only <sha> ml-bench/mlbench/ | grep '\\.py$')
    """
    h = hashlib.sha256()
    root = Path(__file__).resolve().parent
    for p in sorted(root.rglob("*.py")):
        h.update(str(p.relative_to(root)).encode())
        h.update(p.read_bytes())
    return h.hexdigest()


def image_id() -> str:
    return os.environ.get("MLBENCH_IMAGE_ID", "unknown")


def stamp() -> dict:
    """The provenance block every artefact writer embeds."""
    return {"git_sha": git_sha(), "git_sha_source": git_sha_source(),
            "code_sha256": code_hash(), "image_id": image_id()}
