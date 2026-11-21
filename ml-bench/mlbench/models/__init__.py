"""Plug-in models. Add yours to REGISTRY (or pass ``--model pkg.module:Class``)."""

from .base import HistoryView, Model, Origin
from .driver_level import DriverLevel
from .example_level_h5 import LevelH5Example
from .foundation import VARIANTS as FOUNDATION_VARIANTS
from .served import Served, ServedIncumbent
from .tabular_fm import VARIANTS as TABULAR_FM_VARIANTS

#: replay archived production curves instead of forecasting from truth (PAR-1043); they
#: read no truth, so the runner's poison test does not apply — tests/test_served.py does
SERVED_ARMS: dict[str, type[Model]] = {Served.name: Served, ServedIncumbent.name: ServedIncumbent}

REGISTRY: dict[str, type[Model]] = {
    LevelH5Example.name: LevelH5Example,
    DriverLevel.name: DriverLevel,
    # PAR-828 foundation models, keyed by the name they are scored under
    # (heavy imports happen on first predict, not here)
    **{m.scored_name(): m for m in FOUNDATION_VARIANTS},
    # PAR-1405 tabular foundation models as the daily-level learner (heavy imports on first fit)
    **{m.scored_name(): m for m in TABULAR_FM_VARIANTS},
    **SERVED_ARMS,
}

_TIMESFM3 = ("ruled out before scoring: no q95 head, and the weights' licence excludes production "
             "(docs/ml/foundation-models/timesfm.md)")
_TOTO2 = ("PAR-853: behind Chronos-2 at every lead it was scored on; TiRex-2 is the fallback kept "
          "(docs/ml/foundation-models/tirex-toto.md)")
_NF = ("PAR-829 / PAR-869: beat the incumbents on live leads but lose every head to head against "
       "Chronos-2 (docs/ml/ml-bench/results-neuralforecast.md)")

#: PAR-1405: arms whose question is answered. The code and its tests stay, so nobody builds
#: them twice, but ``mlbench run`` refuses them unless ``--allow-retired`` is passed (a retired
#: arm scored that way is a reference, never a candidate). Keyed by the name an arm is scored
#: under; a ``package.module:Class`` spec is matched by its class's name too.
RETIRED: dict[str, str] = {
    "timesfm3": _TIMESFM3,
    "timesfm3_owx": _TIMESFM3,
    "toto2": _TOTO2,
    "toto2_nocov": _TOTO2,
    "toto2_grid": _TOTO2,
    "tabpfn_ts": ("PAR-1405: the TabPFN-3.5 weights are non-commercial and non-production, so no "
                  "result could ever be served, and on TabArena two servable tabular models rank "
                  "above the TabPFN-3.5 it runs on (docs/ml/foundation-models/tabular.md)"),
    "nf_tide": _NF,
    "nf_tide_wx": _NF,
    "nf_nhits": _NF,
    "nf_tsmixerx": _NF,
}

__all__ = ["HistoryView", "Model", "Origin", "REGISTRY", "RETIRED", "SERVED_ARMS", "LevelH5Example",
           "DriverLevel", "Served", "ServedIncumbent"]
