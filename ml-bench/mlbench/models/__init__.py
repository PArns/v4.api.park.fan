"""Plug-in models. Add yours to REGISTRY (or pass ``--model pkg.module:Class``)."""

from .base import HistoryView, Model, Origin
from .driver_level import DriverLevel
from .example_level_h5 import LevelH5Example
from .foundation import VARIANTS as FOUNDATION_VARIANTS
from .served import Served, ServedIncumbent

#: replay archived production curves instead of forecasting from truth (PAR-1043); they
#: read no truth, so the runner's poison test does not apply — tests/test_served.py does
SERVED_ARMS: dict[str, type[Model]] = {Served.name: Served, ServedIncumbent.name: ServedIncumbent}

REGISTRY: dict[str, type[Model]] = {
    LevelH5Example.name: LevelH5Example,
    DriverLevel.name: DriverLevel,
    # PAR-828 foundation models, keyed by the name they are scored under
    # (heavy imports happen on first predict, not here)
    **{m.scored_name(): m for m in FOUNDATION_VARIANTS},
    **SERVED_ARMS,
}

__all__ = ["HistoryView", "Model", "Origin", "REGISTRY", "SERVED_ARMS", "LevelH5Example",
           "DriverLevel", "Served", "ServedIncumbent"]
