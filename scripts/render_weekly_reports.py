#!/usr/bin/env python3
"""
Generate weekly Spotflow reports by aggregating daily anonymized CSVs.

A weekly report is simply 7 days of data concatenated and rendered through
the existing v2 renderer, so every section (regions, providers, merchants,
retry behaviour) aggregates across the whole week automatically.

Only *complete* 7-day weeks are rendered (1-7, 8-14, 15-21, 22-28); the
trailing 1-3 days of a month are not a full week and are skipped.

Run from anywhere:
    python3 scripts/render_weekly_reports.py            # all months with data
    python3 scripts/render_weekly_reports.py july       # one or more months
"""

from __future__ import annotations

import csv
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).parent.parent
RENDER_DIR = ROOT / "Spotflow transactions data"
LOGO = ROOT / "Logo.svg"

sys.path.insert(0, str(RENDER_DIR))
from render_html_report_v2 import render_report_v2  # noqa: E402

# folder → (display abbr, filename prefix)
MONTHS = {
    "january": ("Jan", "jan"),   "february": ("Feb", "feb"),
    "march": ("Mar", "mar"),     "april": ("Apr", "apr"),
    "may": ("May", "may"),       "june": ("Jun", "jun"),
    "july": ("Jul", "jul"),      "august": ("Aug", "aug"),
    "september": ("Sep", "sep"), "october": ("Oct", "oct"),
    "november": ("Nov", "nov"),  "december": ("Dec", "dec"),
}

# (slug, first day, last day) — full weeks only
WEEK_BLOCKS = [
    ("week-1", 1, 7), ("week-2", 8, 14), ("week-3", 15, 21), ("week-4", 22, 28),
]


def daily_csv(folder: str, abbr: str, day: int) -> Path:
    return ROOT / "data" / folder / f"{abbr} {day} - export anonymized.csv"


def year_from(path: Path) -> str:
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            d = (row.get("Payment Date") or "").strip()
            if len(d) >= 4 and d[:4].isdigit():
                return d[:4]
    return ""


def concat(paths: list[Path], dest: Path) -> int:
    header: list[str] | None = None
    rows: list[list[str]] = []
    for src in paths:
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


def process_month(folder: str, logo_svg: str | None) -> None:
    abbr, prefix = MONTHS[folder]
    report_dir = ROOT / "web" / "reports" / folder
    report_dir.mkdir(parents=True, exist_ok=True)

    for slug, d1, d2 in WEEK_BLOCKS:
        paths = [daily_csv(folder, abbr, d) for d in range(d1, d2 + 1)]
        if not all(p.exists() for p in paths):
            continue  # incomplete week — skip
        year = year_from(paths[0])
        title = f"{abbr} {d1} – {d2}, {year} — Weekly Insights"
        out = report_dir / f"{prefix}-{slug}-report.html"
        print(f"\n── {folder} {slug} ({title}) ──")
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, dir=str(ROOT)) as tmp:
            tmp_path = Path(tmp.name)
        try:
            n = concat(paths, tmp_path)
            print(f"   {n:,} transactions across 7 days")
            render_report_v2(tmp_path, out, title, logo_svg)
            print(f"   → {out.relative_to(ROOT)}")
        finally:
            tmp_path.unlink(missing_ok=True)


def main() -> None:
    args = [a.lower() for a in sys.argv[1:]]
    if args:
        months = [m for m in args if m in MONTHS]
        unknown = [m for m in args if m not in MONTHS]
        if unknown:
            print(f"Unknown month(s): {unknown}. Valid: {list(MONTHS)}")
    else:
        # all month folders under data/ that have at least one anonymized CSV
        months = [
            d.name for d in sorted((ROOT / "data").iterdir())
            if d.is_dir() and d.name in MONTHS
            and any(d.glob("*anonymized*.csv"))
        ]

    if not months:
        print("No months to process.")
        return

    logo_svg = LOGO.read_text(encoding="utf-8") if LOGO.exists() else None
    for folder in months:
        process_month(folder, logo_svg)

    print("\n✅  Weekly reports done")


if __name__ == "__main__":
    main()
