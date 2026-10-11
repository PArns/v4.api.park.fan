"""PAR-829: turn a report pass's ``handover.csv`` into the published markdown tables.

``handover.csv`` -- not ``cells.csv`` -- is the source for every verdict-carrying
hand-over line (BENCH-SPEC recorded change #8: ``wins`` in ``cells.csv`` is only the
statistical verdict, while the publication gate ``report.tested()`` is applied when
``handover.csv`` and ``usable_horizon.csv`` are built, so one run emits two artefacts
that can contradict each other). This tool therefore reads the gated artefact and
prints the gate columns in the same row as the value.

WHAT A "winner" HERE MEANS, because it is narrower than it looks: the hand-over winner
beats every candidate in the NAIVE ENVELOPE (persistence / snaive7 / wt_med / clim) at
that cell. It does NOT mean the arm beat ``h5`` or ``lvlh5_tft`` -- those are not
reference candidates and are not in the envelope. The paired margins against the real
incumbents are a separate artefact (``par829_paired_margin.py``), and PAR-829's
pre-registered rule needs BOTH.

    python3 par829_handover.py <tables-dir> [...]
"""
import collections
import csv
import sys
from pathlib import Path

ORIGIN_LABEL = "autumn-shoulder origins 2026-08-08..2026-10-07"


def read(p):
    with open(p, newline="") as f:
        return list(csv.DictReader(f))


def n(v, nd=4):
    try:
        return f"{float(v):+.{nd}f}"
    except (TypeError, ValueError):
        return "—"


def main(argv):
    if not argv:
        print(__doc__)
        return 2
    rc = 0
    for d in argv:
        tdir = Path(d)
        f = tdir / "handover.csv"
        if not f.exists():
            print(f"REFUSING TO EMIT: no handover.csv under {tdir}", file=sys.stderr)
            rc = 1
            continue
        rows = read(f)
        mine = [r for r in rows if r["winner"].startswith(("nf_", "lvl_nf_"))]
        owx = [r for r in rows if "owx" in r["winner"]]
        tally = collections.Counter(r["winner"] for r in rows)

        print(f"\n### Hand-over winners — `{tdir.name}` ({ORIGIN_LABEL})\n")
        print(f"Source: `{tdir.name}/handover.csv`, **{len(rows)} rows**. A PAR-829 arm "
              f"wins **{len(mine)} of {len(rows)}**. ORACLE (`_owx`) rows winning: "
              f"**{len(owx)}** (the harness excludes `_owx` from this table by a suffix "
              "test, so 0 is the correct and expected value).\n")
        print("| winner | hand-over rows |")
        print("|---|---|")
        for k, v in tally.most_common():
            star = " **(PAR-829)**" if k.startswith(("nf_", "lvl_nf_")) else ""
            print(f"| `{k or '(none)'}`{star} | {v} |")

        if mine:
            print(f"\n**The {len(mine)} cells a PAR-829 arm wins** — margin is against the "
                  "hardest naive in the envelope, park-cluster CI; `pd` = park-day CI. "
                  "`share` is the winner's paired share; `nod` the origin days behind the "
                  "cell; `unit` the unit the LOW-N gate was applied on.\n")
            print("| uc | metric | seg | lead | winner | margin vs hardest | park-cluster CI "
                  "| park-day CI | hardest | share | nod | unit | units agree |")
            print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
            for r in sorted(mine, key=lambda r: (r["uc"], r["metric"], r["segment"],
                                                 str(r["lead"]).zfill(5))):
                print(f"| {r['uc']} | {r['metric']} | {r['segment']} | {r['lead']} "
                      f"| `{r['winner']}` | **{n(r['margin_vs_hardest'])}** "
                      f"| [{n(r['hardest_lo_park'])}, {n(r['hardest_hi_park'])}] "
                      f"| [{n(r['margin_lo_park_day'])}, {n(r['margin_hi_park_day'])}] "
                      f"| `{r['hardest_naive']}` "
                      f"| {n(r['winner_paired_share']).lstrip('+')} | {r['n_origin_days']} "
                      f"| {r['unit']} | {r['park_day_unit_agrees']} |")

        # The surfaces a PAR-829 arm does NOT win are as much of the result as the wins,
        # so they are printed rather than left as an absence for the reader to notice.
        slot = [r for r in rows if r["metric"] == "MAE"
                and r["uc"] in ("UC2", "UC3") and str(r["lead"]) in list("01234567")]
        if slot:
            print(f"\n**The planner slot surface (UC2 / UC3 MAE, d0–d7) — who wins it** "
                  f"({ORIGIN_LABEL}):\n")
            print("| uc | seg | lead | winner | margin vs hardest | hardest | n_tested | nod |")
            print("|---|---|---|---|---|---|---|---|")
            for r in sorted(slot, key=lambda r: (r["segment"], str(r["lead"]).zfill(3), r["uc"])):
                print(f"| {r['uc']} | {r['segment']} | {r['lead']} | `{r['winner']}` "
                      f"| {n(r['margin_vs_hardest'])} | `{r['hardest_naive'] or '—'}` "
                      f"| {r['n_tested']} | {r['n_origin_days']} |")

        lvl = [r for r in rows if r["uc"] in ("UC4", "D6", "D7")]
        if lvl:
            print(f"\n**The daily-level surfaces (UC4 / D6 / D7) — who wins them** "
                  f"({ORIGIN_LABEL}). No PAR-829 level source wins any of these cells; the "
                  "paired regressions are in the diagnostics table below.\n")
            print("| uc | metric | lead | winner | unit | n_tested | nod |")
            print("|---|---|---|---|---|---|---|")
            for r in sorted(lvl, key=lambda r: (r["uc"], str(r["lead"]).zfill(5))):
                print(f"| {r['uc']} | {r['metric']} | {r['lead']} | `{r['winner']}` "
                      f"| {r['unit']} | {r['n_tested']} | {r['n_origin_days']} |")

        uh = tdir / "usable_horizon.csv"
        if uh.exists():
            urows = [r for r in read(uh) if r["model"].startswith(("nf_", "lvl_nf_"))]
            if urows:
                print(f"\n**Usable horizon / `leads_tested`, PAR-829 arms** "
                      f"(`{tdir.name}/usable_horizon.csv`). `leads_tested > 0` everywhere "
                      "below, so none of these is the 'nothing was tested' tautology; an "
                      "empty `usable_horizon` with a non-empty `max_significant_lead` means "
                      "the wins form a **band**, not a prefix from the first lead.\n")
                print("| metric | uc | seg | model | unit | leads_scored | leads_tested "
                      "| usable_horizon | max_significant_lead | not_tested_because |")
                print("|---|---|---|---|---|---|---|---|---|---|")
                for r in sorted(urows, key=lambda r: (r["uc"], r["metric"], r["segment"],
                                                      r["model"])):
                    print(f"| {r['metric']} | {r['uc']} | {r['segment']} | `{r['model']}` "
                          f"| {r['unit']} | {r['leads_scored']} | **{r['leads_tested']}** "
                          f"| {r['usable_horizon'] or '—'} "
                          f"| {r['max_significant_lead'] or '—'} "
                          f"| {r['not_tested_because'] or '—'} |")
    return rc


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
