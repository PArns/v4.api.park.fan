#!/usr/bin/env python3
"""Splice the generated tables into RESULTS-TIDE-VS-CHRONOS.md (PAR-869).

The results document is prose plus two marker pairs, `<!-- KURZFASSUNG -->` /
`<!-- /KURZFASSUNG -->` and `<!-- ZAHLEN -->` / `<!-- /ZAHLEN -->`. Everything numeric
comes from `tide_vs_chronos.py`; this script only moves whole `## ` sections of that
output between the markers, so no figure is ever retyped, the hand-written sections
outside the markers are never touched, and re-running after a new report pass
refreshes the document in place instead of stacking copies.

Usage:
    python3 splice_results.py --doc ~/ml-review-2026-10/RESULTS-TIDE-VS-CHRONOS.md \
        --generated results/<run>/summary_tide_vs_chronos.md
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Which generated sections go to which marker, by the start of their `## ` heading.
PLACEMENT = {
    "KURZFASSUNG": ["5. Kurzfassung", "4. Antwort-Tabelle"],
    "ZAHLEN": ["Provenance", "Sample", "1. Head to head", "2. Each model against",
               "3. Who should serve"],
}


def sections(text: str) -> list[tuple[str, str]]:
    """[(heading, block)] for every `## ` section, in order. The leading `# ` title and
    anything before the first `## ` is dropped: the results document has its own title."""
    out = []
    for m in re.finditer(r"^## (.+?)$", text, re.M):
        start = m.start()
        nxt = text.find("\n## ", m.end())
        out.append((m.group(1).strip(), text[start:(nxt if nxt != -1 else len(text))].rstrip()))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--doc", required=True)
    ap.add_argument("--generated", required=True)
    a = ap.parse_args()

    doc, gen = Path(a.doc), Path(a.generated)
    text, blocks = doc.read_text(), sections(gen.read_text())
    used: set[str] = set()
    for marker, prefixes in PLACEMENT.items():
        token = f"<!-- {marker} -->"
        if token not in text:
            print(f"marker {token} not in {doc}", file=sys.stderr)
            return 2
        picked = []
        for pref in prefixes:
            hits = [(h, b) for h, b in blocks if h.startswith(pref)]
            if not hits:
                print(f"no generated section starts with {pref!r}", file=sys.stderr)
                return 2
            for h, b in hits:
                picked.append(b)
                used.add(h)
        # Replace everything BETWEEN the open and close marker, so a re-run refreshes
        # the tables in place instead of stacking copies, and so the hand-written
        # sections outside the markers are never touched.
        close = f"<!-- /{marker} -->"
        if close not in text:
            print(f"closing marker {close} not in {doc}", file=sys.stderr)
            return 2
        pat = re.compile(re.escape(token) + r".*?" + re.escape(close), re.S)
        body = token + "\n\n" + "\n\n".join(picked) + "\n\n" + close
        text, n = pat.subn(lambda _m: body, text, count=1)
        if n != 1:
            print(f"{token} appears after {close} in {doc}", file=sys.stderr)
            return 2

    left = [h for h, _ in blocks if h not in used]
    if left:
        print(f"WARNING: generated sections not placed: {left}", file=sys.stderr)
    doc.write_text(text.rstrip() + "\n")
    print(f"spliced {len(used)} generated sections into {doc}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
