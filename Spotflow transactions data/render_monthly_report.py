#!/usr/bin/env python3
"""Aggregate daily exports into a monthly report using the current v2 UI."""

from __future__ import annotations

import argparse
import csv
import math
import re
import sys
from pathlib import Path
from typing import Sequence

from render_html_report_v2 import parse_csvs_v2, render_report_v2


MONTH_NAMES = {
    "Jan": "January", "Feb": "February", "Mar": "March",
    "Apr": "April", "May": "May", "Jun": "June",
    "Jul": "July", "Aug": "August", "Sep": "September",
    "Oct": "October", "Nov": "November", "Dec": "December",
}


def _gather_csvs(
    patterns: Sequence[str],
    explicit: Sequence[Path],
    directory: Path,
    month_slug: str,
    start_day: int,
    end_day: int,
) -> tuple[list[Path], list[int]]:
    files: list[Path] = []
    for pattern in patterns or [f"{month_slug} * anonymized.csv"]:
        files.extend(directory.glob(pattern))
    files.extend(explicit)

    day_pattern = re.compile(rf"\b{re.escape(month_slug)}\s+(\d{{1,2}})\b", re.IGNORECASE)
    by_day: dict[int, Path] = {}
    for path in files:
        match = day_pattern.search(path.name)
        if not match:
            continue
        day = int(match.group(1))
        if not start_day <= day <= end_day:
            continue
        if day in by_day and by_day[day].resolve() != path.resolve():
            raise ValueError(f"Multiple source files found for {month_slug} {day}: {by_day[day]} and {path}")
        by_day[day] = path

    paths = [by_day[day] for day in sorted(by_day)]
    missing = sorted(set(range(start_day, end_day + 1)) - set(by_day))
    return paths, missing


def _source_row_count(paths: Sequence[Path]) -> int:
    row_count = 0
    for path in paths:
        with path.open(newline="", encoding="utf-8") as source:
            reader = csv.reader(source)
            if next(reader, None) is None:
                raise ValueError(f"Source CSV is empty: {path}")
            row_count += sum(bool(row) for row in reader)
    return row_count


def _validate_aggregate(paths: Sequence[Path]) -> tuple[int, int, int, int]:
    transactions, summaries, merchants = parse_csvs_v2(paths)
    aggregate = summaries["Aggregate"]
    total = len(transactions)

    if sum(aggregate.status_counts.values()) != total:
        raise ValueError("Aggregate status counts do not reconcile with parsed transactions")
    if sum(sum(s.status_counts.values()) for name, s in summaries.items() if name != "Aggregate") != total:
        raise ValueError("Regional counts do not reconcile with parsed transactions")
    if sum(merchant.total_txns for merchant in merchants.values()) != total:
        raise ValueError("Merchant counts do not reconcile with parsed transactions")

    successful_volume = sum(t.amount_usd for t in transactions if t.status == "successful")
    if not math.isclose(aggregate.successful_volume_usd, successful_volume, rel_tol=1e-12, abs_tol=0.01):
        raise ValueError("Successful volume does not reconcile with parsed transactions")

    invalid_timestamps = sum(transaction.dt is None for transaction in transactions)
    missing_rates = sum(
        transaction.amount > 0
        and transaction.currency not in {"", "USD"}
        and transaction.rate <= 0
        for transaction in transactions
    )
    missing_merchants = sum(not transaction.merchant for transaction in transactions)
    return total, invalid_timestamps, missing_rates, missing_merchants


def render_monthly_report(
    output_path: Path,
    csv_paths: Sequence[Path],
    title: str,
    logo_path: Path | None,
    coverage_note: str,
) -> tuple[int, int, int, int, int]:
    if not csv_paths:
        raise ValueError("No CSV exports were provided for aggregation")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    logo = logo_path.read_text(encoding="utf-8") if logo_path else None

    source_rows = _source_row_count(csv_paths)
    parsed, invalid_timestamps, missing_rates, missing_merchants = _validate_aggregate(csv_paths)
    notes = [coverage_note]
    if source_rows > parsed:
        notes.append(f"{source_rows - parsed:,} source rows could not be parsed")
    elif parsed > source_rows:
        notes.append(f"{parsed - source_rows:,} additional transactions recovered from fragmented rows")
    if invalid_timestamps:
        notes.append(f"{invalid_timestamps:,} transactions lack a valid timestamp")
    if missing_rates:
        notes.append(f"{missing_rates:,} non-USD transactions lack a conversion rate")
    if missing_merchants:
        notes.append(f"{missing_merchants:,} transactions lack merchant attribution")
    render_report_v2(
        csv_paths,
        output_path,
        title,
        logo,
        include_retry=False,
        web_root="../",
        coverage_note=" · ".join(notes),
    )
    return source_rows, parsed, invalid_timestamps, missing_rates, missing_merchants


def main() -> int:
    parser = argparse.ArgumentParser(description="Render a monthly Spotflow report with the current daily-report UI")
    parser.add_argument("output_path", type=Path)
    parser.add_argument("--pattern", action="append", default=[])
    parser.add_argument("--directory", type=Path, default=Path.cwd())
    parser.add_argument("--csv", action="append", default=[], type=Path)
    parser.add_argument("--month-slug", required=True, choices=MONTH_NAMES)
    parser.add_argument("--start-day", type=int, default=1)
    parser.add_argument("--end-day", type=int, required=True)
    parser.add_argument("--title", default=None)
    parser.add_argument("--logo", type=Path, default=None)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--require-complete-coverage", action="store_true")
    args = parser.parse_args()

    try:
        csv_paths, missing = _gather_csvs(
            args.pattern,
            args.csv,
            args.directory,
            args.month_slug,
            args.start_day,
            args.end_day,
        )
        if not csv_paths:
            parser.error("No CSV files matched the requested month and day range")
        if missing and args.require_complete_coverage:
            parser.error(f"Missing source days: {', '.join(map(str, missing))}")

        month = MONTH_NAMES[args.month_slug]
        coverage = f"Source coverage: {month} {args.start_day}–{args.end_day}, {args.year}"
        if missing:
            coverage += f" (missing {', '.join(map(str, missing))})"
        title = args.title or f"{month} {args.year} Transaction Insights"
        source_rows, parsed, invalid_timestamps, missing_rates, missing_merchants = render_monthly_report(
            args.output_path,
            csv_paths,
            title,
            args.logo,
            coverage,
        )
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Monthly report written to {args.output_path}")
    print(f"Validated {parsed:,} transactions from {source_rows:,} source rows")
    if missing:
        print(f"Warning: missing source days: {', '.join(map(str, missing))}", file=sys.stderr)
    if invalid_timestamps:
        print(f"Warning: {invalid_timestamps:,} transactions lack a valid timestamp", file=sys.stderr)
    if missing_rates:
        print(f"Warning: {missing_rates:,} non-USD transactions lack a conversion rate", file=sys.stderr)
    if missing_merchants:
        print(f"Warning: {missing_merchants:,} transactions lack merchant attribution", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
