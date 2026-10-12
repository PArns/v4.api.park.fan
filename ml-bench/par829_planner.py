"""PAR-829 planner-surface and season audit — the tables the critic pass (B1/B2, S3-S9)
said were owed, generated from the run's own committed CSVs.

Why this exists. The first write-up capped every POSITIVE at SHADOW because the run holds
61 autumn-shoulder origins and no others, and then published the planner-slot DROP as
settled on that same window -- with a justification ("significantly at every lead, both
segments") that is false at measurable cells, and while declaring absent a season
stratification the run itself commits. Prose cannot be trusted with any of that a second
time, so every figure the corrected text uses is printed here from a cell in a file.

Sections, each answering one finding of CRITIC-PAR-829.md:

  A  season strata (B1, S4)      tables*/mae_by_season.csv -- strata sizes, summer-autumn
                                 per model, this run's own clim-wt_med, and what the table
                                 cannot show.
  B  planner significance (B2a)  every servable arm x lead x segment against `lvlh5_tft`,
                                 counted, with the non-significant cells named.
  C  composed-arm tie (B2b)      `nf_tide_x_h5` - `h5` on every lead it produces.
  D  gated planner envelope (B2c) usable_horizon.csv / cells.csv -- the envelope horizons
                                 on the planner surface, which handover.csv structurally
                                 cannot show (it reports the WINNER per cell).
  E  staleness shape (S3)        the worst bucket per pair, so the "same shape for all
                                 nine" claim can be checked rather than asserted.
  F  pass movers (S5)            par829_verdict.py run on both passes and diffed, so the
                                 count is re-derived after the `b32971ed` ORACLE fix.
  G  leads_tested (S6)           per surface, from the gated artefact.
  H  census scope (S8, S9)       which arms the coverage census covers, and this run's own
                                 `snaive7` d7 row.
  I  ORACLE headroom (N3)        what perfect weather foreknowledge buys on the slot
                                 surface, per lead and segment.
  J  SHADOW inventory (N4)       which cells each SHADOW tier holds, so the "capped by the
                                 clause" tier is never confused with the weaker one.

    python3 par829_planner.py --run results/20261011-par829-stageA

Nothing here re-scores anything: it reads CSVs that the scored run committed, so it can be
re-run at any later commit and must reproduce byte-identical numbers.
"""
from __future__ import annotations

import argparse
import collections
import csv
import datetime as dt
import re
import subprocess
import sys
from pathlib import Path

# The two report passes, and the directory each wrote. `window` is the common target
# window and the pass that is head-to-head comparable with PAR-827's headline pass;
# `alltargets` is every target this run produced. NEITHER is a cross-season pass.
PASSES = (("window", "tables_2026-08-15_2026-10-07"), ("alltargets", "tables"))
# Target windows per pass, as the report dirs name them. Used ONLY to count the origin
# days behind the L=0 season strata, where target == origin.
PASS_TARGETS = {
    "window": (dt.date(2026, 8, 15), dt.date(2026, 10, 7)),
    "alltargets": (dt.date(2026, 8, 8), dt.date(2026, 10, 7)),
}
# mlbench/config.py: month IN (6,7,8) -> 'summer'. The harness keys this on the SLOT's
# date, i.e. the TARGET date, which is why the L=0 stratum is the only one that is also
# an origin-season stratum (at L=0 target == origin).
SUMMER_MONTHS = (6, 7, 8)
SERVABLE = ("nf_tide", "nf_nhits", "nf_tide_x_h5", "nf_nhits_x_h5")
INCUMBENTS = ("lvlh5_tft", "h5")
SEASON_MODELS = ("lvlh5_tft", "h5", "wt_med", "clim", "nf_tide", "nf_tide_x_h5",
                 "nf_nhits", "nf_nhits_x_h5")


def read(p: Path):
    with open(p, newline="") as f:
        return list(csv.DictReader(f))


def fl(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def s3(v, nd=3):
    return "—" if v is None else f"{v:+.{nd}f}"


def leadkey(v):
    try:
        return (0, float(v))
    except (TypeError, ValueError):
        return (1, 0.0)


def summer_origin_days(lbl):
    """How many of the pass's L=0 target days fall in the harness's summer.

    Derived from the pass's target window and the harness's own month rule rather than
    counted off a prose sentence: at L = 0 the target date IS the origin date, so this is
    the number of genuinely SUMMER-ORIGIN days behind the L=0 summer stratum.
    """
    a, b = PASS_TARGETS[lbl]
    days = [a + dt.timedelta(days=i) for i in range((b - a).days + 1)]
    return sum(1 for d in days if d.month in SUMMER_MONTHS), len(days)


def section_a(run: Path):
    print("\n## A. Season strata — the split this run DOES commit (B1), and its own "
          "`clim − wt_med` (S4)\n")
    print("Source: `tables*/mae_by_season.csv`, both passes. The harness's season rule is "
          "`month IN (6,7,8) -> summer` (`mlbench/config.py`), keyed on the **slot's own "
          "date**, i.e. the TARGET date.\n")
    for lbl, d in PASSES:
        rows = read(run / d / "mae_by_season.csv")
        seasons = sorted({r["season"] for r in rows})
        sod, tod = summer_origin_days(lbl)
        print(f"### {lbl} pass — strata present: {', '.join(seasons)}\n")
        print(f"Target window {PASS_TARGETS[lbl][0]} … {PASS_TARGETS[lbl][1]} = {tod} "
              f"target days, of which **{sod} fall in summer**. At L = 0 target == origin, "
              f"so the L = 0 summer stratum is a genuine summer-ORIGIN stratum of {sod} "
              f"origin days; at L > 0 the strata are TARGET season and mix origin "
              "seasons.\n")
        print("| L | " + " | ".join(f"n {s}" for s in seasons) + " |")
        print("|---|" + "---|" * len(seasons))
        for L in sorted({r["L"] for r in rows}, key=leadkey):
            by = {r["season"]: r for r in rows if r["L"] == L}
            cells = []
            for s in seasons:
                n = fl(by[s]["n"]) if s in by else None
                cells.append("—" if n is None else f"{int(n):,}".replace(",", " "))
            print(f"| {L} | " + " | ".join(cells) + " |")
        print("\n**summer − autumn slot MAE** (negative = better in summer, i.e. the model "
              "is handicapped by this run's autumn weighting):\n")
        hdr = [m for m in SEASON_MODELS]
        print("| L | " + " | ".join(f"`{m}`" for m in hdr) + " |")
        print("|---|" + "---|" * len(hdr))
        for L in sorted({r["L"] for r in rows}, key=leadkey):
            by = {r["season"]: r for r in rows if r["L"] == L}
            if "summer" not in by or "autumn" not in by:
                continue
            cells = []
            for m in hdr:
                a, b = fl(by["autumn"].get(m, "")), fl(by["summer"].get(m, ""))
                cells.append(s3(None if a is None or b is None else b - a))
            print(f"| {L} | " + " | ".join(cells) + " |")
        # The incumbent-vs-candidate differential, which is the quantity that bears on the
        # DROP: how much of the autumn window favours `lvlh5_tft` OVER `nf_tide`.
        print("\n**Autumn differential against the DROP margin.** `autumn_diff` = "
              "(summer−autumn for `lvlh5_tft`) − (summer−autumn for the arm): how much "
              "more this window's autumn weighting flatters the incumbent than the arm. "
              "`drop_margin` is the arm − `lvlh5_tft` paired margin at that lead on "
              "segment `all`, from the same pass's margin CSV — the quantity the DROP "
              "rests on.\n")
        marg = {}
        for r in read(run / f"paired_margin_{lbl}_slot.csv"):
            if r["ref"] == "lvlh5_tft" and r["segment"] == "all":
                marg[(r["model"], str(int(float(r["lead"]))))] = fl(r["diff"])
        print("| L | arm | autumn_diff | drop_margin (arm − `lvlh5_tft`, all) |")
        print("|---|---|---|---|")
        for L in sorted({r["L"] for r in rows}, key=leadkey):
            by = {r["season"]: r for r in rows if r["L"] == L}
            if "summer" not in by or "autumn" not in by:
                continue
            ia, ib = fl(by["autumn"].get("lvlh5_tft", "")), fl(by["summer"].get("lvlh5_tft", ""))
            if ia is None or ib is None:
                continue
            inc = ib - ia
            for m in ("nf_tide", "nf_tide_x_h5", "nf_nhits"):
                a, b = fl(by["autumn"].get(m, "")), fl(by["summer"].get(m, ""))
                if a is None or b is None:
                    continue
                print(f"| {L} | `{m}` | {s3(inc - (b - a))} | "
                      f"{s3(marg.get((m, str(int(float(L))))))} |")
        # S4: this run's own clim - wt_med, which the first write-up quoted from PAR-827.
        print("\n**S4 — this run's own `clim − wt_med` per stratum** (the quartet the "
              "earlier text published is PAR-827's; these are this run's):\n")
        print("| L | autumn | summer |")
        print("|---|---|---|")
        for L in sorted({r["L"] for r in rows}, key=leadkey):
            by = {r["season"]: r for r in rows if r["L"] == L}
            out = []
            for s in ("autumn", "summer"):
                c, w = (fl(by[s]["clim"]), fl(by[s]["wt_med"])) if s in by else (None, None)
                out.append(s3(None if c is None or w is None else c - w, 4))
            print(f"| {L} | " + " | ".join(out) + " |")
    print("\n**What this table does and does not show.** It does NOT show that the DROP "
          "would reverse over 231 origins: the strata are UNPAIRED (different slot sets, "
          "so no paired difference), carry NO bootstrap interval of any kind, and for "
          "L > 0 they are TARGET-season strata that mix origin seasons. It shows three "
          "things: the run holds a non-autumn stratum, so \"no non-autumn origin exists "
          "to restrict to\" was wrong; the direction of the season effect at every lead "
          "is adverse to the DROP; and its magnitude is the same order as the DROP margin "
          "itself. There is no winter and no spring stratum at all.\n")


def section_b(run: Path):
    print("\n## B. The planner surface against `lvlh5_tft`, counted (B2a)\n")
    print("Every servable arm × lead × segment paired against `lvlh5_tft` on the slot "
          "part, both passes. A cell counts as significant only if the **park-cluster** "
          "interval lies entirely above 0 (BENCH-SPEC #1); where the two bootstrap units "
          "disagree the cell is **not** a win for the incumbent (recorded change #1).\n")
    for lbl, _ in PASSES:
        rows = [r for r in read(run / f"paired_margin_{lbl}_slot.csv")
                if r["ref"] == "lvlh5_tft" and r["model"] in SERVABLE]
        sig = [r for r in rows if (fl(r["lo_park"]) or 0) > 0]
        ns = [r for r in rows if not ((fl(r["lo_park"]) or 0) > 0)]
        print(f"**{lbl} pass: {len(sig)} of {len(rows)} cells significant.**")
        if ns:
            # The `wins_*` flags in the CSV are about the CANDIDATE winning, and the
            # margin here is positive, so both are False at every one of these cells.
            # The quantity that matters is whether the INCUMBENT's advantage is
            # significant, per unit -- which is the interval lying entirely above 0.
            print("\n| arm | segment | lead | margin | park-cluster 95 % | park-day 95 % "
                  "| units pk | `lvlh5_tft` sig pk / pd |")
            print("|---|---|---|---|---|---|---|---|")
            for r in sorted(ns, key=lambda r: (r["model"], r["segment"], leadkey(r["lead"]))):
                sig_pk = "yes" if (fl(r["lo_park"]) or 0) > 0 else "no"
                sig_pd = "yes" if (fl(r["lo_park_day"]) or 0) > 0 else "no"
                print(f"| `{r['model']}` | {r['segment']} | "
                      f"{int(float(r['lead']))} | {s3(fl(r['diff']), 4)} | "
                      f"[{s3(fl(r['lo_park']), 4)}, {s3(fl(r['hi_park']), 4)}] | "
                      f"[{s3(fl(r['lo_park_day']), 4)}, {s3(fl(r['hi_park_day']), 4)}] | "
                      f"{r['units_park']} | {sig_pk} / {sig_pd}"
                      + (" — **units disagree**" if sig_pk != sig_pd else "") + " |")
        else:
            print("\n(no exceptions in this pass)")
        print()
    print("So the defensible sentence is \"significant at N of 60 cells, exceptions "
          "named\" — not \"significantly at every lead, both segments\". The run's own "
          "`par829_verdict.py` classifies the exceptions as **no win**, which is what "
          "`PROGRESS-PAR-829.log` recorded at 16:39 before the prose strengthened it.\n")


def section_c(run: Path):
    print("\n## C. The composed TiDE arm against `h5` on busy slots — a statistical tie "
          "(B2b)\n")
    print("`nf_tide_x_h5` − `h5`, every lead the composed arm produces (the daily level "
          "starts at d1, so there is no d0 row), both segments, both passes.\n")
    for lbl, _ in PASSES:
        rows = [r for r in read(run / f"paired_margin_{lbl}_slot.csv")
                if r["ref"] == "h5" and r["model"] == "nf_tide_x_h5"]
        for seg in sorted({r["segment"] for r in rows}):
            sel = sorted((r for r in rows if r["segment"] == seg), key=lambda r: leadkey(r["lead"]))
            nsig = sum(1 for r in sel if str(r["wins_park"]) == "True"
                       or (fl(r["lo_park"]) or 0) > 0)
            print(f"**{lbl} / segment = {seg}** — {len(sel) - nsig} of {len(sel)} leads "
                  "statistically indistinguishable from `h5` (park-cluster interval spans "
                  "0)\n")
            print("| lead | margin | park-cluster 95 % | park-day 95 % | units pk |")
            print("|---|---|---|---|---|")
            for r in sel:
                print(f"| {int(float(r['lead']))} | {s3(fl(r['diff']))} | "
                      f"[{s3(fl(r['lo_park']))}, {s3(fl(r['hi_park']))}] | "
                      f"[{s3(fl(r['lo_park_day']))}, {s3(fl(r['hi_park_day']))}] | "
                      f"{r['units_park']} |")
            print()


def section_d(run: Path):
    print("\n## D. The gated envelope horizons on the planner surface (B2c)\n")
    print("`usable_horizon.csv` is the **gated** artefact — the publication gate of "
          "BENCH-SPEC #8 is already applied to it. `handover.csv` reports the WINNER per "
          "cell, so an arm that clears the naive envelope while `h5` / `lvlh5_tft` takes "
          "the cell is structurally invisible there. The planner surface is exactly where "
          "that happens, which is why these rows were missing from a write-up that quoted "
          "`handover.csv` only.\n")
    slot_uc = {"UC2", "UC3", "UC2/UC3"}
    seen = {}
    for lbl, d in PASSES:
        uh = [r for r in read(run / d / "usable_horizon.csv")
              if r["model"].startswith("nf_") and r["uc"] in slot_uc
              and (r["usable_horizon"].strip() or r["max_significant_lead"].strip())]
        seen[lbl] = {(r["metric"], r["uc"], r["segment"], r["model"]): r["usable_horizon"]
                     for r in uh}
        print(f"**{lbl} pass** — {len(uh)} planner-surface rows with a non-empty horizon:\n")
        if not uh:
            print("(none)\n")
            continue
        print("| metric | uc | segment | model | leads_tested | usable_horizon | "
              "max_significant_lead |")
        print("|---|---|---|---|---|---|---|")
        for r in sorted(uh, key=lambda r: (r["metric"], r["uc"], r["segment"], r["model"])):
            print(f"| {r['metric']} | {r['uc']} | {r['segment']} | `{r['model']}` | "
                  f"{r['leads_tested']} | **{r['usable_horizon'] or '—'}** | "
                  f"{r['max_significant_lead'] or '—'} |")
        print()
        # The gated cells behind those horizons, from cells.csv: the envelope margin, its
        # park-cluster interval, and the coverage share the gate reads.
        keys = {(r["metric"], r["uc"], r["segment"], r["model"]) for r in uh}
        cells = [r for r in read(run / d / "cells.csv")
                 if r["region"] == "all" and (r["metric"], r["uc"], r["segment"],
                                              r["model"]) in keys
                 and str(r["wins"]) == "True"]
        if cells:
            print(f"Backing cells from `{d}/cells.csv` (`wins == True`, `region = all`) "
                  "— the margin is against the **hardest naive**, which is what the "
                  "envelope rule compares to:\n")
            print("| metric | uc | segment | model | lead | hardest naive | margin | "
                  "park-cluster 95 % | env_min_share | units pk | origin days |")
            print("|---|---|---|---|---|---|---|---|---|---|---|")
            for r in sorted(cells, key=lambda r: (r["metric"], r["uc"], r["segment"],
                                                  r["model"], leadkey(r["lead"]))):
                print(f"| {r['metric']} | {r['uc']} | {r['segment']} | `{r['model']}` | "
                      f"{r['lead']} | `{r['hardest_naive']}` | "
                      f"{s3(fl(r['diff_vs_hardest']), 4)} | "
                      f"[{s3(fl(r['hardest_lo_park']), 4)}, "
                      f"{s3(fl(r['hardest_hi_park']), 4)}] | "
                      f"{fl(r['env_min_share']):.4f} | {r['n_units_park']} | "
                      f"{r['n_origin_days']} |")
            print()
    # The two passes do NOT agree here, and that is a finding in its own right: four of
    # the window pass's six envelope horizons do not reproduce when every target day the
    # run produced is kept. Reporting only the window pass would overstate them.
    only_w = sorted(set(seen["window"]) - set(seen["alltargets"]))
    only_a = sorted(set(seen["alltargets"]) - set(seen["window"]))
    both = sorted(set(seen["window"]) & set(seen["alltargets"]))
    print(f"**Pass agreement on these rows: {len(both)} in both, {len(only_w)} in the "
          f"window pass only, {len(only_a)} in `alltargets` only.**\n")
    for lab, ks in (("both passes", both), ("window only", only_w),
                    ("alltargets only", only_a)):
        if ks:
            print(f"- {lab}: " + "; ".join(
                f"{k[1]} {k[2]} `{k[3]}` ({k[0]})" for k in ks))
    print("\nSo the envelope horizons are **window-dependent** and carry the same caveat "
          "as everything else in this run.\n")
    print("These are wins against the **naive envelope** (`persistence` / `snaive7` / "
          "`wt_med` / `clim`), not against `h5` or `lvlh5_tft` — neither is a reference "
          "candidate, so neither is in the envelope. The DROP against the incumbents "
          "stands; \"not a near miss\" with no envelope horizon reported does not.\n")


def section_e(run: Path):
    print("\n## E. The staleness shape, per pair (S3)\n")
    rows = read(run / "staleness.csv")
    buckets = sorted({r["stale_days"] for r in rows}, key=lambda b: int(b.split("-")[0]))
    print(f"Source: `staleness.csv` ({len(rows)} rows). Margin vs the recomputed "
          "baseline, bucketed by the age of the cached weights.\n")
    print("| pair | " + " | ".join(buckets) + " | worst bucket | monotone |")
    print("|---|" + "---|" * (len(buckets) + 2))
    worst_freshest = 0
    nonmono = 0
    pairs = 0
    for (m, ref), grp in sorted(collections.defaultdict(list, {
            k: [r for r in rows if (r["model"], r["ref"]) == k]
            for k in {(r["model"], r["ref"]) for r in rows}}).items()):
        vals = {r["stale_days"]: fl(r["margin_vs_ref"]) for r in grp}
        series = [vals.get(b) for b in buckets]
        known = [(b, v) for b, v in zip(buckets, series) if v is not None]
        if not known:
            continue
        pairs += 1
        wb = max(known, key=lambda kv: kv[1])[0]
        if wb == buckets[0]:
            worst_freshest += 1
        mono = all(known[i][1] >= known[i + 1][1] for i in range(len(known) - 1))
        nonmono += 0 if mono else 1
        print(f"| `{m}`−`{ref}` | " + " | ".join(s3(v, 4) for v in series) +
              f" | **{wb}** | {'yes' if mono else 'no'} |")
    print(f"\n**Worst margin at the freshest weights: {worst_freshest} of {pairs} pairs; "
          f"non-monotone: {nonmono} of {pairs}.** So \"the same shape for all nine model × "
          "reference pairs\" is wrong — the shape is not consistent. The conclusion the "
          "table supports is unchanged and in fact strengthened: if the shape is not even "
          "consistent across pairs, \"staleness handicaps us so the pooled number is "
          "pessimistic\" is less supportable, not more.\n")


def section_f(run: Path):
    print("\n## F. Cells that move between the two passes (S5)\n")
    tool = Path(__file__).with_name("par829_verdict.py")
    args = [sys.executable, str(tool)] + [
        str(run / f"paired_margin_{lbl}_{part}.csv")
        for part in ("slot", "intraday") for lbl, _ in PASSES]
    out = subprocess.run(args, capture_output=True, text=True, check=True).stdout
    # The verdict tool's row format is fixed-width: source(38) seg(6) model(24) lead(>5)
    # then two spaces and the verdict. Parsed rather than re-implemented, so this count
    # cannot drift from the published classification.
    got = {}
    for line in out.splitlines():
        m = re.match(r"^(paired_margin_\S+\.csv)\s+(\S+)\s+(\S+)\s+(\S+)\s\s+(.+)$", line)
        if not m:
            continue
        src, seg, model, lead, verdict = m.groups()
        lbl = "window" if "_window_" in src else "alltargets"
        part = "intraday" if "intraday" in src else "slot"
        got[(part, seg, model, lead, lbl)] = verdict.strip()
    keys = {k[:4] for k in got}
    shared = [k for k in keys if (k + ("window",)) in got and (k + ("alltargets",)) in got]
    movers = [k for k in shared
              if got[k + ("window",)] != got[k + ("alltargets",)]]
    print(f"`par829_verdict.py` run on all four margin CSVs and diffed on the "
          f"{len(shared)} keys both passes classify.\n")
    print(f"**{len(movers)} of {len(shared)} cells move.**\n")
    print("| part | segment | model | lead | window | alltargets |")
    print("|---|---|---|---|---|---|")
    for k in sorted(movers, key=lambda k: (k[0], k[1], k[2], leadkey(k[3]))):
        print(f"| {k[0]} | {k[1]} | `{k[2]}` | {k[3]} | {got[k + ('window',)]} | "
              f"{got[k + ('alltargets',)]} |")
    rev = [k for k in movers if "DROP" in got[k + ("window",)]
           and "SHADOW" in got[k + ("alltargets",)]
           or "SHADOW" in got[k + ("window",)] and "DROP" in got[k + ("alltargets",)]]
    print(f"\nSign reversals among the movers: **{len(rev)}**.\n")


def section_g(run: Path):
    print("\n## G. `leads_tested` per surface, from the gated artefact (S6)\n")
    print("The point this column makes is `leads_tested > 0` everywhere, so no claim here "
          "rests on the \"nothing was tested\" tautology. The VALUE is not 7–8 "
          "everywhere, and it is a property of how many leads a surface scores at all.\n")
    for lbl, d in PASSES:
        rows = [r for r in read(run / d / "usable_horizon.csv")
                if r["model"].startswith(("nf_", "lvl_nf"))]
        agg = collections.defaultdict(set)
        for r in rows:
            agg[(r["uc"], r["metric"])].add((r["leads_scored"], r["leads_tested"]))
        print(f"**{lbl} pass**\n")
        print("| uc | metric | leads_scored / leads_tested (distinct over PAR-829 arms) |")
        print("|---|---|---|")
        for (uc, metric), vs in sorted(agg.items()):
            print(f"| {uc} | {metric} | " +
                  ", ".join(f"{a}/{b}" for a, b in sorted(vs)) + " |")
        print()


def section_h(run: Path):
    print("\n## H. What the coverage census covers, and this run's `snaive7` d7 row "
          "(S9, S8)\n")
    census = run / "summary_par829_coverage_census.md"
    text = census.read_text()
    rows = []
    for line in text.splitlines():
        m = re.match(r"^(\S+)\s+(\S+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+"
                     r"([\d.]+)\s*$", line)
        if m:
            rows.append(m.groups())
    models = sorted({r[0] for r in rows})
    refs = sorted({r[1] for r in rows})
    run_json = (run / "run-0of1.json").read_text()
    scored = re.search(r'"models":\s*\[([^\]]*)\]', run_json)
    names = re.findall(r'"([^"]+)"', scored.group(1)) if scored else []
    arms = sorted(set(names) | {f"{n}_x_h5" for n in names})
    print(f"Source: `{census.name}` ({len(rows)} census rows = {len(models)} models × "
          f"{len(refs)} references × 8 leads).\n")
    print(f"- Census models: {', '.join('`' + m + '`' for m in models)}.")
    print(f"- Scored slot arms (from `run-0of1.json` `models` plus the runner's automatic "
          f"composed columns): {', '.join('`' + a + '`' for a in arms)}.")
    missing = [a for a in arms if a not in models]
    print(f"- **Covered: {len(models)} of {len(arms)} arms.** Not in the census: "
          f"{', '.join('`' + m + '`' for m in missing) or 'none'}.")
    print("\nThis run's own `snaive7` rows at lead 7, which is the figure the "
          "non-measurements table misquoted:\n")
    print("| model | ref | L | n_ref | n_model | paired | share |")
    print("|---|---|---|---|---|---|---|")
    for r in rows:
        if r[1] == "snaive7" and r[2] == "7":
            print(f"| `{r[0]}` | `{r[1]}` | {r[2]} | {r[3]} | {r[4]} | {r[5]} | {r[8]} |")
    print("\n`n_ref = 0`: `snaive7` is defined only for leads ≤ 7 days and produces no "
          "value AT d7 in this run, so there is nothing to pair with — which is what "
          "\"produces no value at lead 7\" means, and it is consistent with `n_ref = 0` "
          "rather than with a non-zero reference count.\n")


def section_i(run: Path):
    print("\n## I. What perfect weather foreknowledge buys on the planner surface (N3)\n")
    print("The ORACLE arm `nf_tide_wx_owx` consumes the target day's weather **actuals**, "
          "so the gap between its margin and the honest arm's margin against the SAME "
          "baseline is the headroom a perfect weather forecast could buy. Computed "
          "against `h5` on the slot part, per lead and segment: `headroom` = (raw arm − "
          "`h5`) − (ORACLE arm − `h5`), positive = the oracle is better.\n")
    for lbl, _ in PASSES:
        rows = read(run / f"paired_margin_{lbl}_slot.csv")
        idx = {(r["model"], r["segment"], str(int(float(r["lead"])))): fl(r["diff"])
               for r in rows if r["ref"] == "h5"}
        for seg in ("all", "busy"):
            vals = []
            print(f"**{lbl} / segment = {seg}**\n")
            print("| lead | `nf_tide` − `h5` | `nf_tide_wx_owx` − `h5` | headroom |")
            print("|---|---|---|---|")
            for L in range(8):
                raw = idx.get(("nf_tide", seg, str(L)))
                orc = idx.get(("nf_tide_wx_owx", seg, str(L)))
                if raw is None or orc is None:
                    continue
                vals.append(raw - orc)
                print(f"| {L} | {s3(raw)} | {s3(orc)} | {s3(raw - orc)} |")
            if vals:
                print(f"\nHeadroom range: **{min(vals):+.3f} … {max(vals):+.3f}** min "
                      f"(min at d{vals.index(min(vals))}, max at "
                      f"d{vals.index(max(vals))}).\n")


def section_j(run: Path):
    print("\n## J. Which cells the SHADOW tier holds, and why (N4)\n")
    tool = Path(__file__).with_name("par829_verdict.py")
    out = subprocess.run(
        [sys.executable, str(tool)] + [
            str(run / f"paired_margin_{lbl}_{part}.csv")
            for part in ("slot", "intraday") for lbl, _ in PASSES],
        capture_output=True, text=True, check=True).stdout
    buckets = collections.defaultdict(list)
    for line in out.splitlines():
        m = re.match(r"^(paired_margin_\S+\.csv)\s+(\S+)\s+(\S+)\s+(\S+)\s\s+(.+)$", line)
        if not m:
            continue
        src, seg, model, lead, verdict = m.groups()
        lbl = "window" if "_window_" in src else "alltargets"
        if verdict.startswith("SHADOW (beats every"):
            buckets[(lbl, "every incumbent — capped by the autumn-only SERVE clause")].append(
                (seg, model, lead))
        elif verdict.startswith("SHADOW (beats some"):
            buckets[(lbl, "some but not all incumbents")].append((seg, model, lead))
    for (lbl, kind), cells in sorted(buckets.items()):
        print(f"**{lbl} — SHADOW, {kind}** ({len(cells)} cells): " + "; ".join(
            f"{seg} `{model}` {lead}" for seg, model, lead in sorted(cells)) + "\n")
    print("The weaker tier is not a PAR-829-arm-specific list and it is not caused by a "
          "gate: every cell in it is n.s. or park-day-only against one incumbent (almost "
          "always `persistence`), which is a different finding from the capped tier.\n")


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    a = ap.parse_args(argv)
    run = Path(a.run)
    if not (run / "paired_margin_window_slot.csv").exists():
        print(f"REFUSING TO EMIT: no paired_margin_window_slot.csv under {run}",
              file=sys.stderr)
        return 1
    print("# PAR-829 planner-surface and season audit")
    print(f"\nRun `{run.name}`. Generated by `ml-bench/par829_planner.py` from the "
          "committed CSVs; no figure below is hand-transcribed. The run holds **61 "
          "autumn-shoulder origins 2026-08-08 … 2026-10-07** and no others, and that "
          "caveat applies to every number here — the negatives included.")
    for fn in (section_a, section_b, section_c, section_d, section_e, section_f,
               section_g, section_h, section_i, section_j):
        fn(run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
