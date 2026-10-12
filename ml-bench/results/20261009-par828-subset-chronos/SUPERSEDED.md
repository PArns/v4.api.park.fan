# SUPERSEDED by `20261010-par828-subset-chronos-v2`

This run is **VOID as a measurement** and is kept for exactly one purpose: it is the
other half of the void-vs-fixed adjudication in `docs/ml/foundation-models.md`.

It was scored by a pre-rebase working tree that matched **no commit** (measured
`code_sha256` `d82b5a17…`), and therefore without the three coverage-parity fixes
`689a7b84` / `336b2b62` / `a1d20bf8`. Without them a plug-in and a built-in baseline
are scored on different slot sets: over the six origins that could be compared, the
built-in baselines covered **6 767** slots at d0–d7 that the plug-in did not, against
**0** in the fixed run.

What the adjudication established, and why this directory still has to exist for it to
be checkable:

* the **plug-in arm is bit-identical** between the two runs (`n__chronos2` 121 110 →
  121 110, `sae__chronos2` 1 049 849.8 → 1 049 849.8, delta exactly 0), and so are the
  **paired sums** `pn`/`psm`/`psr`;
* what moved is every **built-in baseline's own unpaired coverage and MAE** — `h5`
  −2 473 slots and 8.5635 → 8.5828, `wt_med` −2 378 and 8.6258 → 8.6462, uniformly
  upward by +0.005…+0.082 min because the slots the fix removes were easier than
  average;
* so **reference-based claims were void** (reference selection is an unpaired argmin
  over exactly those shifted MAEs) while **per-baseline paired margins survived**.

Nothing in this directory may be quoted as a result. Use
`../20261010-par828-subset-chronos-v2/`.
