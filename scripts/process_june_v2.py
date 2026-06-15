#!/usr/bin/env python3
"""
Pipeline for Jun 1-14 v2 reports.

For each raw Jun export in the project root:
  1. Strip down to 11 columns (drop Reference, Spotflow Reference,
     Account Number, Mode, Retry Count, Attempt References)
  2. Anonymize customer emails
  3. Generate a v2 HTML report → web/reports/june/jun-XX-report.html
  4. Delete the original raw CSV (and temp file)

Run from anywhere:
    python3 scripts/process_june_v2.py

Add --dry-run to preview without writing anything.
"""

from __future__ import annotations

import argparse
import csv
import re
import subprocess
import sys
from pathlib import Path

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT          = Path(__file__).parent.parent          # project root
ANON_SCRIPT   = Path(__file__).parent / "anonymize_customers.py"
RENDER_SCRIPT = ROOT / "Spotflow transactions data" / "render_html_report_v2.py"
REPORT_OUT    = ROOT / "web" / "reports" / "june"
LOGO          = ROOT / "Logo.svg"

REPORT_OUT.mkdir(parents=True, exist_ok=True)

# Columns to keep (in order)
KEEP = [
    "Merchant", "Amount", "Currency", "Rate",
    "Provider", "Region", "Customer", "Payment Status",
    "Payment Channel", "Provider Message", "Payment Date",
]

# Day number from filenames like "Jun 1 - export - …" or "Jun 10 - export …"
DAY_RE = re.compile(r'jun(?:e)?\s+(\d{1,2})', re.IGNORECASE)


def find_raw_exports() -> list[tuple[int, Path]]:
    found = []
    search_dirs = [ROOT, ROOT / "data"]
    for d in search_dirs:
        for p in sorted(d.glob("*.csv")):
            if "anonymized" in p.name.lower():
                continue
            m = DAY_RE.search(p.name)
            if not m:
                continue
            day = int(m.group(1))
            if 1 <= day <= 31:
                found.append((day, p))
    found.sort(key=lambda x: x[0])
    return found


PROVIDERS = {"cellulant", "hubtel", "interswitch", "ozow", "paystack",
             "spotflow_accounts", "tembo_plus", "precium", "hub2", "kashier", "pawapay"}
REGIONS   = {"Nigeria", "Ghana", "South Africa", "Kenya", "Tanzania",
             "Côte d'Ivoire", "Benin", "Togo", "Egypt", "Cameroon"}
STATUSES  = {"successful", "failed", "abandoned", "cancelled", "inprogress"}
CHANNELS  = {"card", "bank_transfer", "mobile_money", "eft"}
ISO_RE    = re.compile(r"^\d{4}-\d{2}-\d{2}T")


def _find_providers(row: list[str]) -> list[int]:
    """Return column indices where a provider+region pair starts."""
    hits = []
    for i in range(len(row) - 1):
        if row[i].strip().lower() in PROVIDERS and row[i + 1].strip() in REGIONS:
            hits.append(i)
    return hits


def _extract_txn(row: list[str], prov_idx: int, header: list[str],
                 raw_header: list[str]) -> list[str]:
    """Extract one KEEP-column row from a transaction starting at prov_idx.

    Layout relative to provider index:
      prov_idx - 4 : Merchant
      prov_idx - 3 : Amount
      prov_idx - 2 : Currency
      prov_idx - 1 : Rate
      prov_idx     : Provider
      prov_idx + 1 : Region
      prov_idx + 2 : Customer
      prov_idx + 3 : Payment Status
      then scan forward for Channel, Message, Date
    """
    n = len(row)

    def safe(idx: int) -> str:
        return row[idx].strip() if 0 <= idx < n else ""

    merchant = safe(prov_idx - 4)
    amount   = safe(prov_idx - 3)
    currency = safe(prov_idx - 2)
    rate     = safe(prov_idx - 1)
    provider = safe(prov_idx)
    region   = safe(prov_idx + 1)
    customer = safe(prov_idx + 2)
    status   = safe(prov_idx + 3)

    # Scan forward from prov_idx + 4 for channel, message, date
    idx = prov_idx + 4
    channel = ""
    message = ""
    date    = ""

    # Skip account number if present (numeric string)
    if idx < n and row[idx].strip().replace("-", "").isdigit() and len(row[idx].strip()) > 4:
        idx += 1

    # Find channel
    while idx < n:
        val = row[idx].strip().lower()
        if val in CHANNELS:
            channel = val
            idx += 1
            break
        idx += 1

    # Skip mode (LIVE/TEST)
    if idx < n and row[idx].strip().lower() in {"live", "test"}:
        idx += 1

    # Collect message parts until we hit a date or another provider
    msg_parts = []
    while idx < n:
        val = row[idx].strip()
        if ISO_RE.match(val):
            date = val
            break
        lower = val.lower()
        if lower in PROVIDERS or val in REGIONS:
            break
        # Check if this could be the start of the next transaction's merchant
        if idx + 4 < n:
            maybe_prov = row[idx + 4].strip().lower()
            maybe_reg  = safe(idx + 5)
            if maybe_prov in PROVIDERS and maybe_reg in REGIONS:
                break
        if val:
            msg_parts.append(val)
        idx += 1

    message = ", ".join(msg_parts) if msg_parts else ""

    return [merchant, amount, currency, rate, provider, region, customer,
            status, channel, message, date]


def strip_columns(raw: Path, dest: Path) -> None:
    """Read raw CSV, keep only KEEP columns, write to dest.

    Uses provider-based scanning to find every transaction in each row,
    including fragmented rows that pack 2+ transactions on one CSV line.
    """
    with open(raw, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        raw_header = [h.strip() for h in next(reader)]
        missing = [c for c in KEEP if c not in raw_header]
        if missing:
            raise ValueError(f"Missing columns in {raw.name}: {missing}")

        # Column indices in the raw header (for the primary/first transaction)
        keep_indices = [raw_header.index(c) for c in KEEP]

        output_rows: list[list[str]] = []
        for row in reader:
            if not row:
                continue

            prov_positions = _find_providers(row)

            if len(prov_positions) <= 1:
                # Simple row: just pick the KEEP columns
                block = row + [""] * max(0, len(raw_header) - len(row))
                stripped = [block[i].strip() for i in keep_indices]
                if any(v for v in stripped):
                    output_rows.append(stripped)
            else:
                # Fragmented row: extract the primary transaction normally,
                # then each additional transaction via provider scanning
                block = row + [""] * max(0, len(raw_header) - len(row))
                stripped = [block[i].strip() for i in keep_indices]
                if any(v for v in stripped):
                    output_rows.append(stripped)

                # Extract additional transactions (skip the first provider hit
                # since that's the primary transaction)
                primary_prov_col = raw_header.index("Provider")
                for prov_idx in prov_positions:
                    if prov_idx == primary_prov_col:
                        continue  # already extracted as primary
                    txn = _extract_txn(row, prov_idx, KEEP, raw_header)
                    if any(v for v in txn):
                        output_rows.append(txn)

    with open(dest, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(KEEP)
        writer.writerows(output_rows)


def process(day: int, raw: Path, dry_run: bool) -> bool:
    anon_out   = ROOT / "data" / "june" / f"Jun {day} - export anonymized.csv"
    temp_clean = ROOT / f".jun{day}_clean_tmp.csv"
    report_out = REPORT_OUT / f"jun-{day:02d}-report.html"
    title      = f"Jun {day:02d}, 2026 — Spotflow Insights"

    print(f"\n── Jun {day:02d} ──────────────────────────────────────")
    print(f"   Raw    : {raw.name}")
    print(f"   Anon   : {anon_out.name}")
    print(f"   Report : {report_out.relative_to(ROOT)}")

    if dry_run:
        print("   [dry-run — skipping]")
        return True

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

    # 4. Generate v2 report
    print("   → Rendering v2 report…", end=" ", flush=True)
    cmd = [
        sys.executable, str(RENDER_SCRIPT),
        str(anon_out),
        str(report_out),
        "--title", title,
    ]
    if LOGO.exists():
        cmd += ["--logo", str(LOGO)]

    r = subprocess.run(
        cmd, capture_output=True, text=True,
        cwd=str(ROOT / "Spotflow transactions data"),
    )
    if r.returncode != 0:
        print(f"FAILED\n{r.stderr.strip()}")
        return False
    print("done")
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without writing anything")
    args = parser.parse_args()

    exports = find_raw_exports()

    if not exports:
        print("No unprocessed Jun exports found in the project root.")
        print("Expected filenames starting with 'Jun N - export …'")
        return

    print(f"Found {len(exports)} export(s) to process:")
    for day, p in exports:
        print(f"  Jun {day:02d}: {p.name}")

    if args.dry_run:
        print("\n[dry-run — no files will be written]\n")

    ok, fail = 0, 0
    for day, p in exports:
        if process(day, p, args.dry_run):
            ok += 1
        else:
            fail += 1

    print(f"\n{'─' * 52}")
    print(f"✅  {ok} succeeded" + (f"   ❌  {fail} failed" if fail else ""))
    if ok and not args.dry_run:
        print(f"\nReports saved to: web/reports/june/")


if __name__ == "__main__":
    main()
