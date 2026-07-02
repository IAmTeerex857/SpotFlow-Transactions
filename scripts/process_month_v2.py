#!/usr/bin/env python3
"""
Month-agnostic pipeline for v2 daily reports.

Detects the month from each raw export's filename (e.g. "Jul 1 - export …",
"July 1 - export …"), then for each export:
  1. Strip down to 11 columns (fragment-aware — reuses process_june_v2)
  2. Anonymize customer emails
  3. Render a v2 HTML report → web/reports/<month>/<abbr>-XX-report.html
  4. Delete the original raw CSV

Run from anywhere:
    python3 scripts/process_month_v2.py

Add --dry-run to preview without writing anything.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import re
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from process_june_v2 import strip_columns  # fragment-aware column stripper

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT          = Path(__file__).parent.parent
ANON_SCRIPT   = Path(__file__).parent / "anonymize_customers.py"
RENDER_SCRIPT = ROOT / "Spotflow transactions data" / "render_html_report_v2.py"
LOGO          = ROOT / "Logo.svg"

# key(3-letter) → (folder, display abbr, month number)
MONTHS = {
    "jan": ("january", "Jan", 1),  "feb": ("february", "Feb", 2),
    "mar": ("march", "Mar", 3),    "apr": ("april", "Apr", 4),
    "may": ("may", "May", 5),      "jun": ("june", "Jun", 6),
    "jul": ("july", "Jul", 7),     "aug": ("august", "Aug", 8),
    "sep": ("september", "Sep", 9),"oct": ("october", "Oct", 10),
    "nov": ("november", "Nov", 11),"dec": ("december", "Dec", 12),
}
MONTH_RE     = re.compile(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+(\d{1,2})\b", re.IGNORECASE)
EXPORT_TS_RE = re.compile(r"(\d{4})-(\d{2})-\d{2}T")


def find_raw_exports() -> list[tuple[str, int, Path]]:
    found = []
    for d in [ROOT, ROOT / "data"]:
        for p in sorted(d.glob("*.csv")):
            if "anonymized" in p.name.lower():
                continue
            m = MONTH_RE.search(p.name)
            if not m:
                continue
            key = m.group(1).lower()
            day = int(m.group(2))
            if key in MONTHS and 1 <= day <= 31:
                found.append((key, day, p))
    found.sort(key=lambda x: (MONTHS[x[0]][2], x[1]))
    return found


def _year_for(key: str, raw: Path) -> int:
    """Infer the data year from the export timestamp in the filename.

    Exports usually happen the day after; if the data month is ahead of the
    export month (Dec data exported in Jan), it belongs to the prior year.
    """
    m = EXPORT_TS_RE.search(raw.name)
    if not m:
        return date.today().year
    year, exp_month = int(m.group(1)), int(m.group(2))
    if MONTHS[key][2] > exp_month:
        year -= 1
    return year


def process(key: str, day: int, raw: Path, dry_run: bool) -> bool:
    folder, abbr, _ = MONTHS[key]
    year       = _year_for(key, raw)
    report_dir = ROOT / "web" / "reports" / folder
    anon_out   = ROOT / "data" / folder / f"{abbr} {day} - export anonymized.csv"
    temp_clean = ROOT / f".{key}{day}_clean_tmp.csv"
    report_out = report_dir / f"{key}-{day:02d}-report.html"
    title      = f"{abbr} {day:02d}, {year} — Spotflow Insights"

    print(f"\n── {abbr} {day:02d} ──────────────────────────────────────")
    print(f"   Raw    : {raw.name}")
    print(f"   Anon   : {anon_out.name}")
    print(f"   Report : {report_out.relative_to(ROOT)}")

    if dry_run:
        print("   [dry-run — skipping]")
        return True

    report_dir.mkdir(parents=True, exist_ok=True)
    anon_out.parent.mkdir(parents=True, exist_ok=True)

    # 1. Strip unwanted columns → temp
    print("   → Stripping columns…", end=" ", flush=True)
    try:
        strip_columns(raw, temp_clean)
    except Exception as e:
        print(f"FAILED — {e}")
        return False
    print("done")

    # 2. Anonymize temp → anon_out
    print("   → Anonymizing…", end=" ", flush=True)
    r = subprocess.run(
        [sys.executable, str(ANON_SCRIPT), str(temp_clean), str(anon_out)],
        capture_output=True, text=True,
    )
    temp_clean.unlink(missing_ok=True)
    if r.returncode != 0:
        print(f"FAILED\n{r.stderr.strip()}")
        return False
    print("done")

    # 3. Delete the original raw CSV (and any sibling JSON)
    for ext in (".csv", ".json"):
        sibling = raw.with_suffix(ext)
        if sibling.exists():
            sibling.unlink()
            print(f"   → Deleted {sibling.name}")

    # 4. Render v2 report
    print("   → Rendering v2 report…", end=" ", flush=True)
    cmd = [sys.executable, str(RENDER_SCRIPT), str(anon_out), str(report_out), "--title", title]
    if LOGO.exists():
        cmd += ["--logo", str(LOGO)]
    r = subprocess.run(cmd, capture_output=True, text=True,
                       cwd=str(ROOT / "Spotflow transactions data"))
    if r.returncode != 0:
        print(f"FAILED\n{r.stderr.strip()}")
        return False
    print("done")
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing anything")
    args = parser.parse_args()

    exports = find_raw_exports()
    if not exports:
        print("No unprocessed exports found in project root or data/.")
        print("Expected filenames like 'Jul 1 - export …'")
        return

    print(f"Found {len(exports)} export(s) to process:")
    for key, day, p in exports:
        print(f"  {MONTHS[key][1]} {day:02d}: {p.name}")

    if args.dry_run:
        print("\n[dry-run — no files will be written]\n")

    ok = fail = 0
    for key, day, p in exports:
        if process(key, day, p, args.dry_run):
            ok += 1
        else:
            fail += 1

    print(f"\n{'─' * 52}")
    print(f"✅  {ok} succeeded" + (f"   ❌  {fail} failed" if fail else ""))


if __name__ == "__main__":
    main()
