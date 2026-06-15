#!/usr/bin/env python3
"""
Anonymize customer emails in a Spotflow export while respecting fragmented rows.

Usage:
    python anonymize_customers.py SOURCE_CSV OUTPUT_CSV
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

PROVIDERS = {"cellulant", "hubtel", "interswitch", "ozow", "paystack", "spotflow_accounts", "tembo_plus", "precium", "hub2", "kashier", "pawapay"}
REGIONS = {"Nigeria", "Ghana", "South Africa", "Kenya", "Tanzania", "Côte d'Ivoire", "Benin", "Togo", "Egypt", "Cameroon"}
STATUSES = {"successful", "failed", "abandoned", "cancelled", "inprogress"}
CHANNELS = {"card", "bank_transfer", "eft", "mobile_money"}
ISO_PREFIXES = ("202", "201")


def _find_next_provider_index(row: List[str], start: int) -> Optional[int]:
    n = len(row)
    for idx in range(start, n - 1):
        provider = row[idx].strip().lower()
        region = row[idx + 1].strip()
        if provider in PROVIDERS and region in REGIONS:
            return idx
    return None


def _split_transactions(row: List[str], default_email_idx: int) -> Iterable[Tuple[int, str]]:
    n = len(row)
    i = 0

    base_email = row[default_email_idx].strip()
    if base_email:
        yield default_email_idx, base_email

    while i < n:
        provider_idx = _find_next_provider_index(row, i)
        if provider_idx is None:
            break
        idx = provider_idx + 2

        while idx < n and row[idx].strip().lower() not in STATUSES:
            idx += 1
        if idx >= n:
            break
        idx += 1

        while idx < n and row[idx].strip().lower() not in CHANNELS:
            idx += 1
        if idx >= n:
            break
        idx += 1

        while idx < n:
            cell = row[idx].strip()
            if not cell:
                idx += 1
                continue
            if "@" in cell:
                yield idx, cell
                break
            if cell.startswith(ISO_PREFIXES):
                break
            idx += 1

        i = idx + 1


def anonymize_email_field(source_csv: Path, target_csv: Path, key_path: Path) -> None:
    with source_csv.open(newline="", encoding="utf-8") as infile:
        reader = csv.reader(infile)
        rows = list(reader)

    if not rows:
        raise ValueError("Source CSV is empty")

    header = rows[0]
    try:
        email_col = header.index("Customer")
    except ValueError as exc:
        raise ValueError("Could not find 'Customer' column in header") from exc

    email_to_id: Dict[str, str] = {}
    counter = 0

    def assign_id(email: str) -> str:
        nonlocal counter
        key = email.lower()
        if key not in email_to_id:
            counter += 1
            email_to_id[key] = f"customer {counter}"
        return email_to_id[key]

    new_rows = [header]
    for row in rows[1:]:
        if not row:
            new_rows.append(row)
            continue

        rewritten_cols = set()
        for col_idx, raw_email in _split_transactions(row, email_col):
            if not raw_email:
                continue
            tag = assign_id(raw_email.strip())
            row[col_idx] = tag
            rewritten_cols.add(col_idx)

        if email_col not in rewritten_cols and row[email_col].strip():
            row[email_col] = assign_id(row[email_col].strip())

        # Fallback: scrub any remaining cells that contain an email string.
        for idx, cell in enumerate(row):
            if "@" in cell:
                row[idx] = assign_id(cell.strip())

        new_rows.append(row)

    with target_csv.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerows(new_rows)

    with key_path.open("w", encoding="utf-8") as mapping_file:
        json.dump(email_to_id, mapping_file, indent=2, sort_keys=True)

    print(f"Anonymized CSV written to: {target_csv}")
    print(f"Customer mapping written to: {key_path}")
    print(f"Total unique customers: {len(email_to_id)}")


def main(args: List[str]) -> int:
    if len(args) != 3:
        print("Usage: python anonymize_customers.py SOURCE_CSV OUTPUT_CSV", file=sys.stderr)
        return 1

    source = Path(args[1]).expanduser()
    target = Path(args[2]).expanduser()
    key_path = target.with_suffix(target.suffix + ".mapping.json")

    anonymize_email_field(source, target, key_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
