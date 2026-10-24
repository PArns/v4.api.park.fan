"""PAR-1067: the one list of benchmark-only models, and the block every verdict writer calls.

A benchmark-only model may be scored, never proposed for serving: ``refuse_benchmark_only``
raises before an offline verdict, decision or matrix entry can name it."""

import json
from pathlib import Path

BENCHMARK_ONLY: dict[str, str] = json.loads(
    (Path(__file__).with_name("benchmark_only_models.json")).read_text())["prefixes"]


def is_benchmark_only(name: str) -> bool:
    return name.lower().startswith(tuple(BENCHMARK_ONLY))


def refuse_benchmark_only(names) -> None:
    bad = sorted({n for n in names if is_benchmark_only(n)})
    if bad:
        raise ValueError(f"benchmark-only models cannot get a serving verdict: {bad} "
                         f"(licences: mlbench/benchmark_only_models.json)")
