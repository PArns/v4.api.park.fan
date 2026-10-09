"""Plug-in models. Add yours to REGISTRY (or pass ``--model pkg.module:Class``)."""

from .base import HistoryView, Model, Origin
from .driver_level import DriverLevel
from .example_level_h5 import LevelH5Example

REGISTRY: dict[str, type[Model]] = {
    LevelH5Example.name: LevelH5Example,
    DriverLevel.name: DriverLevel,
}

__all__ = ["HistoryView", "Model", "Origin", "REGISTRY", "LevelH5Example", "DriverLevel"]
