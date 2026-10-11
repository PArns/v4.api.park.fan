"""PAR-829: turn the run's committed CSVs into the markdown tables published in the PR
and in RESULTS, so every number traces to a cell in a file rather than to a
transcription. Review finding B6 was two numbers in a PR body that traced to no cell at
all, and the B3 provenance field was hand-transcribed wrong twice in two days.

PASS LABELS. ``window`` is the common target window 2026-08-15..2026-10-07 and is the
pass that is head-to-head comparable with PAR-827's headline pass. ``alltargets`` is
every target this run produced. ``alltargets`` is NOT a "full period" pass: to PAR-827
and PAR-830 that phrase means 231 cross-season origins, and findings REVERSED between
the two passes for both of them.

Every table carries the autumn-shoulder origin label in its caption, by coordinator
ruling -- it is a per-table label and a subtitle, not a caveats-section line.

    python3 par829_tables.py <run-dir>
"""
import csv
import sys
from pathlib import Path

ORIGIN_LABEL = "autumn-shoulder origins 2026-08-08..2026-10-07"
PASS_LABEL = {
    "window": "common target window 2026-08-15..2026-10-07 "
              "(comparable with PAR-827's headline pass)",
    "alltargets": "every target this run produced "
                  "(NOT a full-period pass -- no non-autumn origin exists in this run)",
}


def read(p):
    with open(p, newline="") as f:
        return list(csv.DictReader(f))


def fnum(v, nd=3):
    try:
        return f"{float(v):+.{nd}f}"
    except (TypeError, ValueError):
        return "—"


def win(r):
    pk, pd_ = r.get("wins_park", ""), r.get("wins_park_day", "")
    t = {"True": "Y", "False": "n", "": "?"}
    s = f"{t.get(pk, '?')}/{t.get(pd_, '?')}"
    return s + (" **disagree**" if pk != pd_ else "")


def main(argv):
    if len(argv) != 1:
        print(__doc__)
        return 2
    run = Path(argv[0])
    found, absent = 0, []
    for part in ("slot", "intraday"):
        for lbl in ("window", "alltargets"):
            f = run / f"paired_margin_{lbl}_{part}.csv"
            if not f.exists():
                absent.append(f.name)
                continue
            rows = read(f)
            if not rows:
                absent.append(f.name + " (empty)")
                continue
            found += 1
            print(f"\n#### {part} / {lbl} — {ORIGIN_LABEL}\n")
            print(f"{PASS_LABEL[lbl]}. Source: `{f.name}`. "
                  "`margin < 0` means the candidate is better; a win requires the "
                  "**park-cluster** interval to exclude 0.\n")
            for seg in sorted({r["segment"] for r in rows}):
                print(f"\n**segment = {seg}** ({ORIGIN_LABEL})\n")
                print("| lead | model | baseline | model MAE | baseline MAE | margin "
                      "| park-cluster CI | park-day CI | win pk/pd | units pk "
                      "| paired_share | n_origin_days |")
                print("|---|---|---|---|---|---|---|---|---|---|---|---|")
                sel = [r for r in rows if r["segment"] == seg]

                def key(r):
                    try:
                        return (0, float(r["lead"]), r["model"], r["ref"])
                    except ValueError:
                        return (1, 0.0, str(r["lead"]) + r["model"], r["ref"])

                for r in sorted(sel, key=key):
                    print(f"| {r['lead']} | `{r['model']}` | `{r['ref']}` | "
                          f"{fnum(r['model_mae_paired']).lstrip('+')} | "
                          f"{fnum(r['ref_mae_paired']).lstrip('+')} | "
                          f"{fnum(r['diff'])} | "
                          f"[{fnum(r['lo_park'])}, {fnum(r['hi_park'])}] | "
                          f"[{fnum(r['lo_park_day'])}, {fnum(r['hi_park_day'])}] | "
                          f"{win(r)} | {r.get('units_park', '')} | "
                          f"{fnum(r.get('paired_share_of_ref', ''), 4).lstrip('+')} | "
                          f"{r.get('n_origin_days', '')} |")

    st = run / "staleness.csv"
    if st.exists():
        rows = read(st)
        if rows:
            found += 1
            print(f"\n#### Staleness slope — {ORIGIN_LABEL}\n")
            print(f"Source: `{st.name}`.\n")
            ks = [k for k in rows[0] if k]
            print("| " + " | ".join(ks) + " |")
            print("|" + "---|" * len(ks))
            for r in rows:
                print("| " + " | ".join(str(r[k]) for k in ks) + " |")
    else:
        absent.append("staleness.csv")

    # Loud, not silent. The previous version of this tool looked for the pre-rename
    # `headline`/`full` file names and would have printed NOTHING for a perfectly good
    # run -- a generator that fails quietly is how an empty tables section ships.
    if absent:
        print("\n<!-- inputs not found: " + ", ".join(absent) + " -->")
    if not found:
        print("REFUSING TO EMIT: no margin or staleness CSV was found under "
              f"{run} -- expected paired_margin_{{window,alltargets}}_{{slot,intraday}}.csv",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
