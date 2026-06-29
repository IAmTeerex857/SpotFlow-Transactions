#!/usr/bin/env python3
"""
Generate weekly Spotflow reports by aggregating daily anonymized CSVs.

A weekly report is simply N days of data concatenated and rendered through
the existing v2 renderer, so every section (regions, providers, merchants,
retry behaviour) aggregates across the whole week automatically.

Run from anywhere:
    python3 scripts/render_weekly_reports.py
"""

from __future__ import annotations

import csv
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).parent.parent
RENDER_DIR = ROOT / "Spotflow transactions data"
DATA_JUNE = ROOT / "data" / "june"
REPORT_OUT = ROOT / "web" / "reports" / "june"
LOGO = ROOT / "Logo.svg"

sys.path.insert(0, str(RENDER_DIR))
from render_html_report_v2 import render_report_v2  # noqa: E402

# ── Week definitions ─────────────────────────────────────────────────────────
# (slug, human label, list of day numbers)
JUNE_WEEKS = [
    ("week-1", "Jun 1 – 7, 2026 — Weekly Insights", list(range(1, 8))),
    ("week-2", "Jun 8 – 14, 2026 — Weekly Insights", list(range(8, 15))),
    ("week-3", "Jun 15 – 21, 2026 — Weekly Insights", list(range(15, 22))),
    ("week-4", "Jun 22 – 28, 2026 — Weekly Insights", list(range(22, 29))),
]


def daily_csv(day: int) -> Path:
    return DATA_JUNE / f"Jun {day} - export anonymized.csv"


def concat_week(days: list[int], dest: Path) -> int:
    """Concatenate the daily CSVs for `days` into `dest`. Returns row count."""
    header: list[str] | None = None
    rows: list[list[str]] = []
    for day in days:
        src = daily_csv(day)
        if not src.exists():
            print(f"   ! missing {src.name} — skipping")
            continue
        with src.open(newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            h = next(reader, None)
            if h is None:
                continue
            if header is None:
                header = h
            rows.extend(r for r in reader if r)

    if header is None:
        raise ValueError("No daily CSVs found for this week")

    with dest.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    return len(rows)


def main() -> None:
    logo_svg = LOGO.read_text(encoding="utf-8") if LOGO.exists() else None
    REPORT_OUT.mkdir(parents=True, exist_ok=True)

    for slug, title, days in JUNE_WEEKS:
        out = REPORT_OUT / f"jun-{slug}-report.html"
        print(f"\n── {slug} ({title}) ──")
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, dir=str(ROOT)
        ) as tmp:
            tmp_path = Path(tmp.name)
        try:
            n = concat_week(days, tmp_path)
            print(f"   {n:,} transactions across {len(days)} days")
            render_report_v2(tmp_path, out, title, logo_svg)
            print(f"   → {out.relative_to(ROOT)}")
        finally:
            tmp_path.unlink(missing_ok=True)

    print("\n✅  Weekly reports done")


if __name__ == "__main__":
    main()
