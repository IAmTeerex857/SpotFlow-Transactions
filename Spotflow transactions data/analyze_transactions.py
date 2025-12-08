#!/usr/bin/env python3
"""Spotflow transaction analyzer.

Usage:
    python analyze_transactions.py /path/to/export.csv

The script handles malformed CSV rows where multiple transactions are merged onto
one line and produces:
    * Aggregate totals
    * Per-region summaries for Nigeria, Ghana, South Africa, Kenya, Tanzania
    * Top success and failure reasons overall, per region, and per provider
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import re
from typing import Dict, Iterable, List, Optional, Tuple

REGIONS = ["Nigeria", "Ghana", "South Africa", "Kenya", "Tanzania"]
REGION_SET = set(REGIONS)
PROVIDERS = {"cellulant", "hubtel", "interswitch", "ozow", "paystack", "spotflow_accounts", "tembo_plus"}
STATUSES = {"successful", "failed", "abandoned", "cancelled", "inprogress"}
CHANNELS = {"card", "bank_transfer", "eft", "mobile_money"}
ISO_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T")
DEFAULT_TZ_SUFFIX = "+00:00"
DEFAULT_DATE_PLACEHOLDER = "<missing-date>"
BLANK_MESSAGE = "<blank>"


@dataclass
class ProviderSummary:
    status_counts: Counter = field(default_factory=Counter)
    channel_counts: Counter = field(default_factory=Counter)
    messages_by_status: Dict[str, Counter] = field(default_factory=lambda: defaultdict(Counter))


@dataclass
class RegionSummary:
    times: List[datetime] = field(default_factory=list)
    status_counts: Counter = field(default_factory=Counter)
    channel_counts: Counter = field(default_factory=Counter)
    messages_by_status: Dict[str, Counter] = field(default_factory=lambda: defaultdict(Counter))
    providers: Dict[str, ProviderSummary] = field(default_factory=lambda: defaultdict(ProviderSummary))


def _normalise_message(message: str) -> str:
    """Coerce provider messages into consistent, comparable buckets."""
    if message is None:
        return BLANK_MESSAGE

    cleaned = message.replace("\r\n", "\n").strip()
    if not cleaned or cleaned in {",", ",,", '""', "''"}:
        return BLANK_MESSAGE

    first_line = cleaned.split("\n", 1)[0].strip()
    if first_line.startswith('"') and first_line.endswith('"') and len(first_line) > 1:
        first_line = first_line[1:-1].strip()

    # Remove stray unmatched quotes after stripping leading/trailing ones
    first_line = first_line.strip('"').strip()

    if not first_line or first_line in {",", ",,", ".", "-"}:
        return BLANK_MESSAGE

    first_line = first_line.replace("\xa0", " ")
    first_line = re.sub(r"\s+", " ", first_line).strip()

    # Handle cases where malformed CSV rows append additional fragments such as
    # `",has insufficient funds...` to an otherwise valid message.
    stray_split = re.split(r'"\s*,', first_line, maxsplit=1)
    if len(stray_split) > 1 and stray_split[0].strip():
        first_line = stray_split[0].strip()

    first_line = re.sub(r'[\s,;:."-]+$', '', first_line).strip()

    if "_" in first_line:
        candidate = first_line.replace("_", " ").strip()
        if candidate:
            if first_line.upper() == first_line:
                first_line = candidate.title()
            else:
                first_line = candidate

    first_line_lower = first_line.lower()

    otp_patterns = [
        "kindly enter the otp",
        "please enter the otp",
        "please input the otp",
    ]
    if any(first_line_lower.startswith(pattern) for pattern in otp_patterns):
        first_line = "OTP verification required"
        return first_line

    uppercase_tokens = {
        match.group(0)
        for match in re.finditer(r"\b[A-Z]{2,}\b", first_line)
        if len(match.group(0)) <= 4
    }

    for token in uppercase_tokens:
        first_line_lower = re.sub(
            r"\b" + re.escape(token.lower()) + r"\b", token, first_line_lower
        )

    first_line = first_line_lower[:1].upper() + first_line_lower[1:] if first_line_lower else first_line_lower

    return first_line


def _find_next_provider_index(row: List[str], start: int) -> Optional[int]:
    n = len(row)
    for idx in range(start, n - 1):
        provider = row[idx].strip().lower()
        region = row[idx + 1].strip()
        if provider in PROVIDERS and region in REGION_SET:
            return idx
    return None


def extract_transactions(row: List[str], default_dt: str) -> Iterable[Tuple[str, str, str, str, str, str, str]]:
    """Yield (provider, region, status, channel, message, payment_date, customer)."""
    n = len(row)
    i = 0
    while i < n:
        provider_idx = _find_next_provider_index(row, i)
        if provider_idx is None:
            break
        provider = row[provider_idx].strip().lower()
        region = row[provider_idx + 1].strip()

        idx = provider_idx + 2
        customer_parts: List[str] = []
        while idx < n and row[idx].strip().lower() not in STATUSES:
            token = row[idx].strip()
            if token:
                customer_parts.append(token)
            idx += 1
        if idx >= n:
            i = provider_idx + 1
            continue
        status = row[idx].strip().lower()
        idx += 1

        while idx < n and row[idx].strip().lower() not in CHANNELS:
            idx += 1
        if idx >= n:
            i = provider_idx + 1
            continue
        channel = row[idx].strip().lower()
        idx += 1  # move past channel

        if idx < n and row[idx].strip().lower() in {"live", "test"}:
            idx += 1  # skip mode column if present

        message_parts: List[str] = []
        payment_date = ""
        customer = ""
        for token in customer_parts:
            if "@" in token:
                customer = token
                break
        if not customer and customer_parts:
            customer = customer_parts[0]
        customer = customer.strip().strip('"')

        while idx < n:
            raw = row[idx]
            stripped = raw.strip()
            if ISO_PATTERN.match(stripped):
                payment_date = stripped
                idx += 1
                break
            lower = stripped.lower()
            lookahead = _find_next_provider_index(row, idx)
            if lookahead is not None and lookahead == idx:
                break
            if lower in PROVIDERS or stripped in REGION_SET:
                break
            if "@" in stripped or lower in STATUSES or lower in CHANNELS:
                break
            next_token = row[idx + 1].strip() if idx + 1 < n else ""
            def _is_number(token: str) -> bool:
                if not token:
                    return False
                token = token.replace(",", "")
                try:
                    float(token)
                    return True
                except ValueError:
                    return False
            if next_token and _is_number(next_token):
                break
            message_parts.append(raw)
            idx += 1

        message = ",".join(message_parts).strip()
        if message:
            lower_message = message.lower()
            for provider_token in PROVIDERS:
                marker = f",{provider_token}"
                pos = lower_message.find(marker)
                if pos != -1:
                    message = message[:pos]
                    lower_message = message.lower()
                    break
            for region_token in REGION_SET:
                marker = f",{region_token.lower()}"
                pos = lower_message.find(marker)
                if pos != -1:
                    message = message[:pos]
                    lower_message = message.lower()
                    break
        if message.startswith('"') and message.endswith('"'):
            message = message[1:-1]
        payment_date = payment_date or default_dt

        yield provider, region, status, channel, message, payment_date, customer
        i = idx


def parse_csv(path: Path) -> Tuple[List[Tuple[str, str, str, str, str, datetime, str]], Dict[str, RegionSummary]]:
    transactions: List[Tuple[str, str, str, str, str, datetime, str]] = []
    summaries: Dict[str, RegionSummary] = defaultdict(RegionSummary)
    aggregate = summaries["Aggregate"]

    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header is None:
            raise ValueError("CSV file is empty")
        default_dt = next((col for col in header if ISO_PATTERN.match(col.strip())), None)
        if default_dt:
            default_dt = default_dt.strip()
        else:
            default_dt = DEFAULT_DATE_PLACEHOLDER

        for row in reader:
            if not row:
                continue
            for provider, region, status, channel, message, payment_date, customer in extract_transactions(row, default_dt):
                if region not in REGION_SET and region != "Aggregate":
                    continue
                if status not in STATUSES:
                    continue
                if channel not in CHANNELS:
                    channel = "other"

                try:
                    dt = datetime.fromisoformat(payment_date.replace("Z", DEFAULT_TZ_SUFFIX))
                except ValueError:
                    if payment_date == DEFAULT_DATE_PLACEHOLDER:
                        dt = None
                    else:
                        continue

                norm_message = _normalise_message(message)
                key = (provider, region, status, channel, norm_message, dt, customer)
                transactions.append(key)

                target = summaries[region]
                if dt:
                    target.times.append(dt)
                    aggregate.times.append(dt)

                target.status_counts[status] += 1
                target.channel_counts[channel] += 1
                target.messages_by_status[status][norm_message] += 1

                aggregate.status_counts[status] += 1
                aggregate.channel_counts[channel] += 1
                aggregate.messages_by_status[status][norm_message] += 1

                provider_summary = target.providers[provider]
                provider_summary.status_counts[status] += 1
                provider_summary.channel_counts[channel] += 1
                provider_summary.messages_by_status[status][norm_message] += 1

                aggregate_provider_summary = aggregate.providers[provider]
                aggregate_provider_summary.status_counts[status] += 1
                aggregate_provider_summary.channel_counts[channel] += 1
                aggregate_provider_summary.messages_by_status[status][norm_message] += 1

    return transactions, summaries


def _print_status_block(status_counts: Counter) -> None:
    total = sum(status_counts.values())
    print(f"  Total transactions: {total}")
    for status in ["successful", "failed", "abandoned", "cancelled", "inprogress"]:
        if status in status_counts:
            print(f"    {status.title():<12}: {status_counts[status]}")


def _print_channel_block(channel_counts: Counter) -> None:
    print("  Channels:")
    for channel in ["card", "bank_transfer", "eft", "mobile_money", "other"]:
        count = channel_counts.get(channel, 0)
        if count:
            print(f"    {channel.replace('_', ' ').title():<16}: {count}")


def _print_top_reasons(messages_by_status: Dict[str, Counter], status: str, top_n: int) -> None:
    reasons = messages_by_status.get(status)
    if not reasons:
        return
    label = "success" if status == "successful" else status
    print(f"  Top {label} messages:")
    for message, count in reasons.most_common(top_n):
        print(f"    {count:>4} × {message}")


def print_summary(summaries: Dict[str, RegionSummary], top_n: int) -> None:
    ordered_regions = REGIONS + ["Aggregate"]
    for region in ordered_regions:
        if region not in summaries:
            continue
        data = summaries[region]
        if not data.status_counts:
            continue
        print(f"\n=== {region} ===")
        if data.times:
            times_sorted = sorted(data.times)
            print(
                "  Date/time range: "
                f"{times_sorted[0].isoformat()} to {times_sorted[-1].isoformat()}"
            )
        _print_status_block(data.status_counts)
        _print_channel_block(data.channel_counts)
        _print_top_reasons(data.messages_by_status, "successful", top_n)
        _print_top_reasons(data.messages_by_status, "failed", top_n)

        if data.providers:
            print("  Provider breakdown:")
            for provider, pdata in sorted(data.providers.items()):
                print(f"    - {provider}:")
                total = sum(pdata.status_counts.values())
                print(f"        Total: {total}")
                for status, count in pdata.status_counts.items():
                    print(f"        {status.title():<12}: {count}")
                _print_top_reasons(pdata.messages_by_status, "successful", top_n)
                _print_top_reasons(pdata.messages_by_status, "failed", top_n)



def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze Spotflow transaction exports.")
    parser.add_argument("csv_path", type=Path, help="Path to the CSV export file")
    parser.add_argument(
        "--top",
        type=int,
        default=5,
        help="Number of top success/failure messages to display (default: 5)",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if not args.csv_path.exists():
        parser.error(f"File not found: {args.csv_path}")

    try:
        _, summaries = parse_csv(args.csv_path)
    except Exception as exc:  # noqa: BLE001
        print(f"Error parsing {args.csv_path}: {exc}", file=sys.stderr)
        return 1

    print_summary(summaries, args.top)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
