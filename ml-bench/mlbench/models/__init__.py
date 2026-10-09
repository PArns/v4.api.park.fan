"""Plug-in models. Add yours to REGISTRY (or pass ``--model pkg.module:Class``)."""

from .base import HistoryView, Model, Origin
from .example_level_h5 import LevelH5Example
from .foundation import VARIANTS as FOUNDATION_VARIANTS

REGISTRY: dict[str, type[Model]] = {
    LevelH5Example.name: LevelH5Example,
    # PAR-828 foundation models, keyed by the name they are scored under
    # (heavy imports happen on first predict, not here)
    **{m.scored_name(): m for m in FOUNDATION_VARIANTS},
}

__all__ = ["HistoryView", "Model", "Origin", "REGISTRY", "LevelH5Example"]
