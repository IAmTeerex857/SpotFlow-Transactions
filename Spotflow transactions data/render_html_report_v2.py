#!/usr/bin/env python3
"""Render a v2 HTML insight report for a Spotflow export.

Features over v1:
  - Dark premium UI (Design.md spec)
  - Financial metrics: GPV, merchant volume, revenue by hour, currency mix
  - Amount-tier success-rate analysis
  - Backward compatible: works with CSVs that lack Currency/Rate columns
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

# ── Re-use core parser pieces ────────────────────────────────────────────────
from analyze_transactions import (
    BLANK_MESSAGE,
    REGIONS,
    REGION_SET,
    PROVIDERS,
    STATUSES,
    CHANNELS,
    ISO_PATTERN,
    DEFAULT_TZ_SUFFIX,
    DEFAULT_DATE_PLACEHOLDER,
    _normalise_message,
    _find_next_provider_index,
)

CURRENCIES = {"USD", "NGN", "GHS", "XAF", "KES", "TZS", "ZAR", "XOF", "EGP", "UGX", "ZMW", "RWF", "SLE", "CDF"}


# ── Enhanced data classes ────────────────────────────────────────────────────
@dataclass
class ProviderSummaryV2:
    status_counts: Counter = field(default_factory=Counter)
    channel_counts: Counter = field(default_factory=Counter)
    messages_by_status: Dict[str, Counter] = field(default_factory=lambda: defaultdict(Counter))
    volume_usd: float = 0.0
    successful_volume_usd: float = 0.0
    volume_local: Dict[str, float] = field(default_factory=lambda: defaultdict(float))


@dataclass
class MerchantSummary:
    total_txns: int = 0
    successful_txns: int = 0
    failed_txns: int = 0
    abandoned_txns: int = 0
    cancelled_txns: int = 0
    refunded_txns: int = 0
    volume_usd: float = 0.0
    successful_volume_usd: float = 0.0
    failed_volume_usd: float = 0.0
    channel_counts: Counter = field(default_factory=Counter)
    currency_counts: Counter = field(default_factory=Counter)
    region_counts: Counter = field(default_factory=Counter)


@dataclass
class RegionSummaryV2:
    times: List[datetime] = field(default_factory=list)
    status_counts: Counter = field(default_factory=Counter)
    channel_counts: Counter = field(default_factory=Counter)
    messages_by_status: Dict[str, Counter] = field(default_factory=lambda: defaultdict(Counter))
    providers: Dict[str, ProviderSummaryV2] = field(default_factory=lambda: defaultdict(ProviderSummaryV2))
    volume_usd: float = 0.0
    successful_volume_usd: float = 0.0
    volume_local: Dict[str, float] = field(default_factory=lambda: defaultdict(float))
    currency_counts: Counter = field(default_factory=Counter)


# ── Transaction record ──────────────────────────────────────────────────────
@dataclass
class Transaction:
    merchant: str
    amount: float
    currency: str
    rate: float
    provider: str
    region: str
    status: str
    channel: str
    message: str
    dt: Optional[datetime]
    customer: str
    amount_usd: float = 0.0

    def __post_init__(self):
        if self.currency == "USD" and self.amount > 0:
            self.amount_usd = self.amount
        elif self.rate and self.rate > 0 and self.amount > 0:
            self.amount_usd = self.amount / self.rate
        else:
            self.amount_usd = 0.0


# ── Parsing ──────────────────────────────────────────────────────────────────
def _safe_float(val: str) -> float:
    try:
        return float(val.replace(",", "").strip())
    except (ValueError, AttributeError):
        return 0.0


def _extract_primary_fields(row: List[str], header: List[str]) -> Tuple[str, float, str, float]:
    """Extract merchant, amount, currency, rate from the primary columns."""
    def _col(name):
        try:
            return header.index(name)
        except ValueError:
            return None

    merchant_idx = _col("merchant")
    amount_idx = _col("amount")
    currency_idx = _col("currency")
    rate_idx = _col("rate")

    merchant = row[merchant_idx].strip() if merchant_idx is not None and merchant_idx < len(row) else ""
    amount = _safe_float(row[amount_idx]) if amount_idx is not None and amount_idx < len(row) else 0.0
    currency = row[currency_idx].strip().upper() if currency_idx is not None and currency_idx < len(row) else ""
    rate = _safe_float(row[rate_idx]) if rate_idx is not None and rate_idx < len(row) else 0.0

    return merchant, amount, currency, rate


def _extract_fragment_financials(row: List[str], provider_idx: int) -> Tuple[str, float, str, float]:
    """For a fragment starting at provider_idx, look backwards to find merchant, amount, currency, rate."""
    # In fragmented rows, the pattern before a provider is:
    # ..., merchant, amount, currency, rate, provider, region, ...
    # So look at provider_idx - 4, -3, -2, -1
    merchant = ""
    amount = 0.0
    currency = ""
    rate = 0.0

    if provider_idx >= 4:
        candidate_merchant = row[provider_idx - 4].strip()
        candidate_amount = row[provider_idx - 3].strip()
        candidate_currency = row[provider_idx - 2].strip().upper()
        candidate_rate = row[provider_idx - 1].strip()

        if candidate_currency in CURRENCIES:
            merchant = candidate_merchant
            amount = _safe_float(candidate_amount)
            currency = candidate_currency
            rate = _safe_float(candidate_rate)

    return merchant, amount, currency, rate


def parse_csv_v2(path: Path) -> Tuple[List[Transaction], Dict[str, RegionSummaryV2], Dict[str, MerchantSummary]]:
    """Parse CSV and return transactions, region summaries, and merchant summaries."""
    transactions: List[Transaction] = []
    summaries: Dict[str, RegionSummaryV2] = defaultdict(RegionSummaryV2)
    merchants: Dict[str, MerchantSummary] = defaultdict(MerchantSummary)
    aggregate = summaries["Aggregate"]

    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        raw_header = next(reader, None)
        if raw_header is None:
            raise ValueError("CSV file is empty")
        header = [c.strip().lower() for c in raw_header]

        has_financials = "currency" in header and "rate" in header

        default_dt = next((col for col in raw_header if ISO_PATTERN.match(col.strip())), None)
        if default_dt:
            default_dt = default_dt.strip()
        else:
            default_dt = DEFAULT_DATE_PLACEHOLDER

        for row in reader:
            if not row:
                continue

            # Extract primary row financials
            if has_financials:
                p_merchant, p_amount, p_currency, p_rate = _extract_primary_fields(row, header)
            else:
                p_merchant, p_amount, p_currency, p_rate = "", 0.0, "", 0.0

            # Find all transactions in this row (primary + fragments)
            n = len(row)
            i = 0
            is_first = True

            while i < n:
                provider_idx = _find_next_provider_index(row, i)
                if provider_idx is None:
                    break

                provider = row[provider_idx].strip().lower()
                region = row[provider_idx + 1].strip()

                # Get financials for this transaction
                if is_first:
                    merchant, amount, currency, rate = p_merchant, p_amount, p_currency, p_rate
                    is_first = False
                else:
                    merchant, amount, currency, rate = _extract_fragment_financials(row, provider_idx)

                # Extract customer
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

                # Find channel
                while idx < n and row[idx].strip().lower() not in CHANNELS:
                    idx += 1
                if idx >= n:
                    i = provider_idx + 1
                    continue
                channel = row[idx].strip().lower()
                idx += 1

                # Skip mode
                if idx < n and row[idx].strip().lower() in {"live", "test"}:
                    idx += 1

                # Extract customer from parts
                customer = ""
                for token in customer_parts:
                    if "@" in token or token.startswith("customer "):
                        customer = token
                        break
                if not customer and customer_parts:
                    customer = customer_parts[0]
                customer = customer.strip().strip('"')

                # Extract message and date
                message_parts: List[str] = []
                payment_date = ""

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
                    if "@" in stripped:
                        break
                    next_token = row[idx + 1].strip() if idx + 1 < n else ""
                    if next_token and next_token.replace(",", "").replace(".", "").isdigit():
                        break
                    message_parts.append(raw)
                    idx += 1

                message = ",".join(message_parts).strip()
                if message:
                    lower_message = message.lower()
                    for pt in PROVIDERS:
                        marker = f",{pt}"
                        pos = lower_message.find(marker)
                        if pos != -1:
                            message = message[:pos]
                            break
                    lower_message = message.lower()
                    for rt in REGION_SET:
                        marker = f",{rt.lower()}"
                        pos = lower_message.find(marker)
                        if pos != -1:
                            message = message[:pos]
                            break
                if message.startswith('"') and message.endswith('"'):
                    message = message[1:-1]
                payment_date = payment_date or default_dt

                # Validate
                if region not in REGION_SET:
                    i = idx
                    continue
                if status not in STATUSES:
                    i = idx
                    continue
                if channel not in CHANNELS:
                    channel = "other"

                try:
                    dt = datetime.fromisoformat(payment_date.replace("Z", DEFAULT_TZ_SUFFIX))
                except ValueError:
                    dt = None if payment_date == DEFAULT_DATE_PLACEHOLDER else None

                norm_message = _normalise_message(message)

                txn = Transaction(
                    merchant=merchant,
                    amount=amount,
                    currency=currency,
                    rate=rate,
                    provider=provider,
                    region=region,
                    status=status,
                    channel=channel,
                    message=norm_message,
                    dt=dt,
                    customer=customer,
                )
                transactions.append(txn)

                # ── Aggregate into summaries ─────────────────────────
                for target_key in [region, "Aggregate"]:
                    target = summaries[target_key]
                    if dt:
                        target.times.append(dt)
                    target.status_counts[status] += 1
                    target.channel_counts[channel] += 1
                    target.messages_by_status[status][norm_message] += 1
                    target.volume_usd += txn.amount_usd
                    if status == "successful":
                        target.successful_volume_usd += txn.amount_usd
                    if currency:
                        target.volume_local[currency] += amount
                        target.currency_counts[currency] += 1

                    prov = target.providers[provider]
                    prov.status_counts[status] += 1
                    prov.channel_counts[channel] += 1
                    prov.messages_by_status[status][norm_message] += 1
                    prov.volume_usd += txn.amount_usd
                    if status == "successful":
                        prov.successful_volume_usd += txn.amount_usd
                    if currency:
                        prov.volume_local[currency] += amount

                # ── Merchant aggregation ─────────────────────────────
                m_key = merchant or "<unknown>"
                ms = merchants[m_key]
                ms.total_txns += 1
                ms.volume_usd += txn.amount_usd
                ms.channel_counts[channel] += 1
                if currency:
                    ms.currency_counts[currency] += 1
                ms.region_counts[region] += 1
                if status == "successful":
                    ms.successful_txns += 1
                    ms.successful_volume_usd += txn.amount_usd
                elif status == "failed":
                    ms.failed_txns += 1
                    ms.failed_volume_usd += txn.amount_usd
                elif status == "abandoned":
                    ms.abandoned_txns += 1
                elif status == "cancelled":
                    ms.cancelled_txns += 1
                elif status == "refunded":
                    ms.refunded_txns += 1

                i = idx

    return transactions, summaries, merchants


def _merge_provider_summary(target: ProviderSummaryV2, source: ProviderSummaryV2) -> None:
    target.status_counts.update(source.status_counts)
    target.channel_counts.update(source.channel_counts)
    for status, messages in source.messages_by_status.items():
        target.messages_by_status[status].update(messages)
    target.volume_usd += source.volume_usd
    target.successful_volume_usd += source.successful_volume_usd
    for currency, amount in source.volume_local.items():
        target.volume_local[currency] += amount


def _merge_region_summary(target: RegionSummaryV2, source: RegionSummaryV2) -> None:
    target.times.extend(source.times)
    target.status_counts.update(source.status_counts)
    target.channel_counts.update(source.channel_counts)
    target.currency_counts.update(source.currency_counts)
    for status, messages in source.messages_by_status.items():
        target.messages_by_status[status].update(messages)
    target.volume_usd += source.volume_usd
    target.successful_volume_usd += source.successful_volume_usd
    for currency, amount in source.volume_local.items():
        target.volume_local[currency] += amount
    for provider, summary in source.providers.items():
        _merge_provider_summary(target.providers[provider], summary)


def _merge_merchant_summary(target: MerchantSummary, source: MerchantSummary) -> None:
    target.total_txns += source.total_txns
    target.successful_txns += source.successful_txns
    target.failed_txns += source.failed_txns
    target.abandoned_txns += source.abandoned_txns
    target.cancelled_txns += source.cancelled_txns
    target.refunded_txns += source.refunded_txns
    target.volume_usd += source.volume_usd
    target.successful_volume_usd += source.successful_volume_usd
    target.failed_volume_usd += source.failed_volume_usd
    target.channel_counts.update(source.channel_counts)
    target.currency_counts.update(source.currency_counts)
    target.region_counts.update(source.region_counts)


def parse_csvs_v2(paths: Sequence[Path]) -> Tuple[List[Transaction], Dict[str, RegionSummaryV2], Dict[str, MerchantSummary]]:
    """Parse daily exports independently, then merge their calculated results."""
    transactions: List[Transaction] = []
    summaries: Dict[str, RegionSummaryV2] = defaultdict(RegionSummaryV2)
    merchants: Dict[str, MerchantSummary] = defaultdict(MerchantSummary)

    for path in paths:
        daily_transactions, daily_summaries, daily_merchants = parse_csv_v2(path)
        transactions.extend(daily_transactions)
        for region, summary in daily_summaries.items():
            _merge_region_summary(summaries[region], summary)
        for merchant, summary in daily_merchants.items():
            _merge_merchant_summary(merchants[merchant], summary)

    return transactions, summaries, merchants


# ── Display helpers ──────────────────────────────────────────────────────────
def _fmt_usd(val: float) -> str:
    if val >= 1_000_000:
        return f"${val:,.0f}"
    if val >= 1_000:
        return f"${val:,.2f}"
    return f"${val:.2f}"


def _fmt_pct(num: int, denom: int) -> str:
    if denom == 0:
        return "0.0%"
    return f"{num / denom * 100:.1f}%"


def _display_message(msg: str) -> str:
    return "No provider message (blank)" if msg == BLANK_MESSAGE else msg


def _list_html(items, css_class: str = "data-list") -> str:
    items = list(items)
    if not items:
        return f"<ul class='{css_class}'><li>—</li></ul>"
    return f"<ul class='{css_class}'>" + "".join(f"<li>{item}</li>" for item in items) + "</ul>"


def _list_html_collapsible(items, css_class: str = "data-list", limit: int = 10) -> str:
    """Render a list with a 'see more' toggle if items exceed the limit."""
    items = list(items)
    if not items:
        return f"<ul class='{css_class}'><li>—</li></ul>"
    if len(items) <= limit:
        return _list_html(items, css_class)
    import uuid
    uid = uuid.uuid4().hex[:8]
    visible = "".join(f"<li>{item}</li>" for item in items[:limit])
    hidden = "".join(f"<li>{item}</li>" for item in items[limit:])
    return (f"<ul class='{css_class}'>{visible}"
            f"<div id='more-{uid}' style='display:none;'>{hidden}</div>"
            f"</ul>"
            f"<button class='btn' style='margin-top:.5rem;' onclick=\"var el=document.getElementById('more-{uid}');if(el.style.display==='none'){{el.style.display='block';this.textContent='Show less'}}else{{el.style.display='none';this.textContent='See all {len(items)} messages'}}\">See all {len(items)} messages</button>")


def _status_lines(counter: Counter) -> List[str]:
    order = ["successful", "failed", "abandoned", "cancelled", "refunded", "inprogress"]
    return [f"{s.title()} — {counter.get(s, 0)}" for s in order if counter.get(s, 0)]


def _channel_lines(counter: Counter) -> List[str]:
    order = ["card", "bank_transfer", "mobile_money", "eft", "other"]
    return [f"{k.replace('_', ' ').title()} — {counter.get(k, 0)}" for k in order if counter.get(k, 0)]


def _message_lines(counter: Counter) -> List[str]:
    return [f"{_display_message(msg)} — {count}" for msg, count in counter.most_common()]


# ── Render ───────────────────────────────────────────────────────────────────
def render_report_v2(
    csv_path: Path | Sequence[Path],
    output_path: Path,
    report_title: str,
    logo_inline: str | None = None,
    *,
    include_retry: bool = True,
    web_root: str = "../../",
    coverage_note: str | None = None,
) -> None:
    if isinstance(csv_path, Path):
        transactions, summaries, merchants = parse_csv_v2(csv_path)
    else:
        transactions, summaries, merchants = parse_csvs_v2(csv_path)
    agg = summaries["Aggregate"]

    total_txns = sum(agg.status_counts.values())
    total_success = agg.status_counts.get("successful", 0)
    total_failed = agg.status_counts.get("failed", 0)
    total_abandoned = agg.status_counts.get("abandoned", 0)
    total_cancelled = agg.status_counts.get("cancelled", 0)
    total_refunded = agg.status_counts.get("refunded", 0)
    gpv_usd = agg.successful_volume_usd
    failed_usd = sum(t.amount_usd for t in transactions if t.status == "failed")
    abandoned_usd = sum(t.amount_usd for t in transactions if t.status == "abandoned")
    cancelled_usd = sum(t.amount_usd for t in transactions if t.status == "cancelled")
    refunded_usd = sum(t.amount_usd for t in transactions if t.status == "refunded")
    success_rate = total_success / total_txns * 100 if total_txns else 0

    # Date range
    dates = sorted(t.dt for t in transactions if t.dt)
    date_range = f"{dates[0].strftime('%b %d, %Y %H:%M')} — {dates[-1].strftime('%b %d, %Y %H:%M')}" if dates else "N/A"
    report_date = dates[0].strftime("%B %d, %Y") if dates else "N/A"

    # ── KPI cards ────────────────────────────────────────────────────────
    kpi_html = f"""
    <div class="kpi-strip">
      <div class="kpi-card">
        <span class="kpi-label">Total Transactions</span>
        <span class="kpi-value">{total_txns:,}</span>
      </div>
      <div class="kpi-card kpi-accent">
        <span class="kpi-label">Success Volume</span>
        <span class="kpi-value"><span class="vol-pill" style="font-size:inherit;padding:.2rem .8rem;">{_fmt_usd(gpv_usd)}</span></span>
        <span class="kpi-sub">{total_success:,} successful transactions</span>
      </div>
      <div class="kpi-card kpi-green">
        <span class="kpi-label">Success Rate</span>
        <span class="kpi-value">{success_rate:.1f}%</span>
        <span class="kpi-sub">{total_success:,} of {total_txns:,} transactions</span>
      </div>
      <div class="kpi-card kpi-red">
        <span class="kpi-label">Failed Volume</span>
        <span class="kpi-value"><span class="vol-pill" style="font-size:inherit;padding:.2rem .8rem;background:rgba(255,122,122,.14);color:var(--red);">{_fmt_usd(failed_usd)}</span></span>
        <span class="kpi-sub">{total_failed:,} failed transactions</span>
      </div>
      <div class="kpi-card">
        <span class="kpi-label">Abandoned Volume</span>
        <span class="kpi-value"><span class="vol-pill" style="font-size:inherit;padding:.2rem .8rem;background:rgba(242,194,97,.14);color:var(--amber);">{_fmt_usd(abandoned_usd)}</span></span>
        <span class="kpi-sub">{total_abandoned:,} abandoned transactions</span>
      </div>
      <div class="kpi-card">
        <span class="kpi-label">Cancelled Volume</span>
        <span class="kpi-value"><span class="vol-pill" style="font-size:inherit;padding:.2rem .8rem;background:rgba(184,154,255,.16);color:#B89AFF;">{_fmt_usd(cancelled_usd)}</span></span>
        <span class="kpi-sub">{total_cancelled:,} cancelled transactions</span>
      </div>
      {f'''<div class="kpi-card">
        <span class="kpi-label">Refunded Volume</span>
        <span class="kpi-value"><span class="vol-pill" style="font-size:inherit;padding:.2rem .8rem;background:rgba(77,182,255,.14);color:var(--accent);">{_fmt_usd(refunded_usd)}</span></span>
        <span class="kpi-sub">{total_refunded:,} refunded transactions</span>
      </div>''' if total_refunded else ''}
    </div>
    """

    # ── Merchant volume table ────────────────────────────────────────────
    sorted_merchants = sorted(merchants.items(), key=lambda x: -x[1].volume_usd)
    merchant_rows = []
    for m_name, ms in sorted_merchants:
        sr = _fmt_pct(ms.successful_txns, ms.total_txns)
        top_channel = ms.channel_counts.most_common(1)[0][0].replace("_", " ").title() if ms.channel_counts else "—"
        top_region = ms.region_counts.most_common(1)[0][0] if ms.region_counts else "—"
        merchant_rows.append(f"""<tr>
          <td>{m_name}</td>
          <td>{ms.total_txns:,}</td>
          <td>{ms.successful_txns:,}</td>
          <td>{ms.failed_txns:,}</td>
          <td><span class='vol-pill'>{_fmt_usd(ms.successful_volume_usd)}</span></td>
          <td><span class='vol-pill' style='background:rgba(255,122,122,.14);color:var(--red);'>{_fmt_usd(ms.failed_volume_usd)}</span></td>
          <td>{sr}</td>
          <td>{top_channel}</td>
          <td>{top_region}</td>
        </tr>""")

    merchant_chart_data = {
        "labels": [m[0] for m in sorted_merchants[:8]],
        "volume": [round(m[1].successful_volume_usd, 2) for m in sorted_merchants[:8]],
        "success": [m[1].successful_txns for m in sorted_merchants[:8]],
        "failed": [m[1].failed_txns for m in sorted_merchants[:8]],
    }

    # ── Provider contribution table ──────────────────────────────────────
    provider_rows = []
    for provider, pdata in sorted(agg.providers.items()):
        p_total = sum(pdata.status_counts.values())
        p_succ = pdata.status_counts.get("successful", 0)
        p_fail = pdata.status_counts.get("failed", 0)
        p_aban = pdata.status_counts.get("abandoned", 0)
        p_canc = pdata.status_counts.get("cancelled", 0)
        p_refund = pdata.status_counts.get("refunded", 0)
        top_success = _list_html(
            [f"{_display_message(msg)} ({c})" for msg, c in pdata.messages_by_status["successful"].most_common(3)],
            "table-list",
        )
        top_failure = _list_html(
            [f"{_display_message(msg)} ({c})" for msg, c in pdata.messages_by_status["failed"].most_common(3)],
            "table-list",
        )
        provider_rows.append(f"""<tr>
          <td>{provider}</td>
          <td>{p_total:,}</td>
          <td>{p_succ:,}</td>
          <td>{p_fail:,}</td>
          <td>{p_aban:,}</td>
          <td>{p_canc:,}</td>
          <td>{p_refund:,}</td>
          <td>{_fmt_pct(p_succ, p_total)}</td>
          <td>{_fmt_pct(p_fail, p_total)}</td>
          <td><span class='vol-pill'>{_fmt_usd(pdata.successful_volume_usd)}</span></td>
          <td>{top_success}</td>
          <td>{top_failure}</td>
        </tr>""")

    # ── Region-level merchant aggregation ──────────────────────────────
    region_merchant_data: Dict[str, Dict[str, MerchantSummary]] = defaultdict(lambda: defaultdict(MerchantSummary))
    for t in transactions:
        m_key = t.merchant or "<unknown>"
        ms = region_merchant_data[t.region][m_key]
        ms.total_txns += 1
        ms.volume_usd += t.amount_usd
        ms.channel_counts[t.channel] += 1
        if t.status == "successful":
            ms.successful_txns += 1
            ms.successful_volume_usd += t.amount_usd
        elif t.status == "failed":
            ms.failed_txns += 1
            ms.failed_volume_usd += t.amount_usd
        elif t.status == "abandoned":
            ms.abandoned_txns += 1
        elif t.status == "cancelled":
            ms.cancelled_txns += 1
        elif t.status == "refunded":
            ms.refunded_txns += 1

    # ── Region sections ──────────────────────────────────────────────────
    region_sections = []
    region_failure_summary = []
    for region in REGIONS:
        summary = summaries.get(region)
        if not summary or not summary.status_counts:
            continue
        r_total = sum(summary.status_counts.values())
        status_html = _list_html(_status_lines(summary.status_counts))
        channel_html = _list_html(_channel_lines(summary.channel_counts))
        success_html = _list_html(_message_lines(summary.messages_by_status["successful"]))
        failure_html = _list_html(_message_lines(summary.messages_by_status["failed"]))

        # Currency breakdown for region
        curr_lines = [f"{cur} — {cnt:,} txns" for cur, cnt in summary.currency_counts.most_common()]
        currency_html = _list_html(curr_lines) if curr_lines else "<p class='muted-text'>—</p>"

        # Provider cards
        provider_cards = []
        for prov_name, pdata in sorted(summary.providers.items()):
            p_total = sum(pdata.status_counts.values())
            stats_html = _list_html(_status_lines(pdata.status_counts), "stat-list")
            prov_succ = [f"{_display_message(msg)} — {c}" for msg, c in pdata.messages_by_status["successful"].most_common()]
            prov_fail = [f"{_display_message(msg)} — {c}" for msg, c in pdata.messages_by_status["failed"].most_common()]
            card = f"""<div class='sub-card'>
              <h4>{prov_name}</h4>
              <p class='muted-text'>Total: {p_total:,} · <span class='vol-pill'>{_fmt_usd(pdata.successful_volume_usd)}</span></p>
              {stats_html}"""
            if prov_succ or prov_fail:
                card += "<h5 class='subtle-heading'>Provider message highlights</h5>"
            if prov_succ:
                card += _list_html(prov_succ, "mini-list")
            if prov_fail:
                card += _list_html(prov_fail, "mini-list")
            card += "</div>"
            provider_cards.append(card)

        provider_block = (
            "<details class='details-panel'><summary>Provider breakdown</summary><div class='details-grid'>"
            + "".join(provider_cards) + "</div></details>"
        ) if provider_cards else ""

        # Merchant cards for this region
        merchant_cards = []
        r_merchants = region_merchant_data.get(region, {})
        for m_name, ms in sorted(r_merchants.items(), key=lambda x: -x[1].volume_usd):
            m_sr = _fmt_pct(ms.successful_txns, ms.total_txns)
            top_ch = ms.channel_counts.most_common(1)[0][0].replace('_', ' ').title() if ms.channel_counts else '—'
            m_status_counter = Counter({"successful": ms.successful_txns, "failed": ms.failed_txns, "abandoned": ms.abandoned_txns, "cancelled": ms.cancelled_txns, "refunded": ms.refunded_txns})
            m_status_html = _list_html(_status_lines(m_status_counter), "stat-list")
            merchant_cards.append(f"""<div class='sub-card'>
              <h4>{m_name}</h4>
              <p class='muted-text'>{ms.total_txns:,} txns · <span class='vol-pill'>{_fmt_usd(ms.successful_volume_usd)}</span> · Success: {m_sr} · Top channel: {top_ch}</p>
              {m_status_html}
            </div>""")

        merchant_block = (
            "<details class='details-panel'><summary>Merchant breakdown</summary><div class='details-grid'>"
            + "".join(merchant_cards) + "</div></details>"
        ) if merchant_cards else ""

        region_sections.append(f"""
    <section class="card anim3">
      <div class="section-header">
        <div class="title-with-total"><h2>{region}</h2><span class="badge">{r_total:,} txns · <span class='vol-pill'>{_fmt_usd(summary.successful_volume_usd)}</span></span></div>
      </div>
      <div class="panel-grid two">
        <div class="metric-card">
          <h3>Status mix</h3>
          {status_html}
          <h3>Channel distribution</h3>
          {channel_html}
        </div>
        <div class="metric-card">
          <h3>Top success reasons</h3>
          {success_html}
          <h3>Top failure reasons</h3>
          {failure_html}
        </div>
      </div>
      {provider_block}
      {merchant_block}
    </section>
""")

        # Failure summary
        r_failed = summary.status_counts.get("failed", 0)
        failure_lines = [
            f"{_display_message(msg)} — {c} ({c / r_failed * 100:.1f}%)"
            for msg, c in summary.messages_by_status["failed"].most_common()
        ] if r_failed else []
        region_failure_summary.append((region, r_failed, failure_lines))

    failure_sections = []
    for region, r_failed, lines in region_failure_summary:
        if r_failed == 0:
            failure_sections.append(f"<div class='sub-card'><h3>{region}</h3><p class='muted-text'>No recorded failures.</p></div>")
        else:
            failure_sections.append(f"""<div class='sub-card'>
  <h3>{region}</h3>
  <p class='muted-text'>Total failed: {r_failed:,}</p>
  {_list_html(lines, 'mini-list')}
</div>""")





    # ── Regional chart data ──────────────────────────────────────────────
    region_chart = {
        "labels": [r for r, _, _ in region_failure_summary],
        "success": [summaries[r].status_counts.get("successful", 0) for r, _, _ in region_failure_summary],
        "failed": [summaries[r].status_counts.get("failed", 0) for r, _, _ in region_failure_summary],
        "volume": [round(summaries[r].volume_usd, 2) for r, _, _ in region_failure_summary],
    }

    # ── Retry analysis ───────────────────────────────────────────────────
    customer_attempts: Dict[str, List[Transaction]] = defaultdict(list)
    for t in transactions:
        key = (t.customer or "").strip().lower() or "<unknown>"
        customer_attempts[key].append(t)

    retry_customers = {k: v for k, v in customer_attempts.items() if len(v) > 1} if include_retry else {}
    total_retry = len(retry_customers)
    succeeded_after = sum(1 for v in retry_customers.values() if any(t.status == "successful" for t in v))
    unresolved = sum(1 for v in retry_customers.values() if v[-1].status != "successful") if retry_customers else 0

    attempt_dist = Counter(len(v) for v in retry_customers.values())
    final_statuses = Counter(sorted(v, key=lambda t: (t.dt is None, t.dt))[-1].status for v in retry_customers.values())

    retry_gaps = []
    for v in retry_customers.values():
        sorted_v = sorted(v, key=lambda t: (t.dt is None, t.dt))
        times = [t.dt for t in sorted_v if t.dt]
        for j in range(1, len(times)):
            gap = (times[j] - times[j - 1]).total_seconds()
            if gap >= 0:
                retry_gaps.append(gap)

    avg_gap_h = sum(retry_gaps) / len(retry_gaps) / 3600 if retry_gaps else 0
    median_gap = sorted(retry_gaps)[len(retry_gaps) // 2] / 3600 if retry_gaps else 0

    retry_summary_lines = []
    if total_retry:
        retry_summary_lines.extend([
            f"Customers who retried — {total_retry}",
            f"Ever completed after retry — {succeeded_after}",
            f"Still unresolved — {unresolved}",
        ])

    attempt_lines = [f"{a} attempts — {c} customer{'s' if c > 1 else ''}" for a, c in sorted(attempt_dist.items())]
    final_lines = [f"{s.title()} — {c}" for s, c in final_statuses.most_common()]
    timing_lines = []
    if retry_gaps:
        timing_lines.append(f"Average gap — {avg_gap_h:.2f}h (~{avg_gap_h * 60:.0f}m)")
        timing_lines.append(f"Median gap — {median_gap:.2f}h (~{median_gap * 60:.0f}m)")

    retry_html = ""
    if retry_summary_lines:
        retry_html = f"""
    <section class="card anim4">
      <div class="section-header">
        <div class="title-with-total"><h2>Customer retry behaviour</h2><span class="badge">{total_retry} retriers</span></div>
      </div>
      <div class="panel-grid two">
        <div class="metric-card">
          <h3>Retry outcomes</h3>
          {_list_html(retry_summary_lines)}
          <h3>Final attempt statuses</h3>
          {_list_html(final_lines)}
        </div>
        <div class="metric-card">
          <h3>Retry depth</h3>
          {_list_html(attempt_lines)}
          <h3>Timing signals</h3>
          {_list_html(timing_lines if timing_lines else ['Insufficient timestamp data'])}
        </div>
      </div>
    </section>
"""

    # ── Aggregate overview ───────────────────────────────────────────────
    agg_status_html = _list_html(_status_lines(agg.status_counts))
    agg_channel_html = _list_html(_channel_lines(agg.channel_counts))
    agg_success_html = _list_html_collapsible(_message_lines(agg.messages_by_status["successful"]))
    agg_failure_html = _list_html_collapsible(_message_lines(agg.messages_by_status["failed"]))

    logo_markup = logo_inline or ""

    # ── Build full HTML ──────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang='en'>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>{report_title}</title>
  <link rel="icon" type="image/svg+xml" href="{web_root}assets/icon.svg">
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=IBM+Plex+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
  <script src='https://cdn.jsdelivr.net/npm/chart.js'></script>
  <style>
    :root {{
      --bg: #0A0C0F;
      --bg-alt: #0C1019;
      --ink: #EBEFF4;
      --muted: #93A0AD;
      --faint: #5A6675;
      --line: rgba(255,255,255,.10);
      --line-soft: rgba(255,255,255,.06);
      --surface: rgba(255,255,255,.035);
      --surface-2: rgba(255,255,255,.06);
      --accent: #4DB6FF;
      --accent-dim: rgba(77,182,255,.14);
      --accent-line: rgba(77,182,255,.40);
      --green: #5FD08A;
      --amber: #F2C261;
      --red: #FF7A7A;
      --magenta: #E26BD6;
      --font-display: 'Space Grotesk', system-ui, sans-serif;
      --font-body: 'IBM Plex Sans', system-ui, sans-serif;
      --font-mono: 'JetBrains Mono', ui-monospace, monospace;
    }}
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: var(--font-body);
      background: radial-gradient(1200px 800px at 82% -8%, rgba(77,182,255,.07), transparent 60%), var(--bg);
      color: var(--ink);
      line-height: 1.5;
      -webkit-font-smoothing: antialiased;
    }}

    /* Header */
    header {{
      padding: 3.5rem 0 2.5rem;
      text-align: center;
      position: relative;
      border-bottom: 1px solid var(--line-soft);
    }}
    header .header-inner {{
      width: min(1400px, 92vw);
      margin: 0 auto;
      padding: 0 1.5rem;
      position: relative;
    }}
    header .logo-wrapper {{
      position: absolute;
      top: 0;
      left: 1.5rem;
    }}
    header svg {{ width: 160px; height: auto; }}
    header svg [fill] {{ fill: var(--ink) !important; }}
    .eyebrow {{
      font-family: var(--font-mono);
      font-size: .75rem;
      letter-spacing: .22em;
      text-transform: uppercase;
      color: var(--accent);
      display: inline-flex;
      align-items: center;
      gap: .8rem;
      margin-bottom: .6rem;
    }}
    .eyebrow::before {{
      content: '';
      width: 30px;
      height: 2px;
      background: var(--accent);
    }}
    header h1 {{
      font-family: var(--font-display);
      font-size: 2.4rem;
      font-weight: 600;
      letter-spacing: -.015em;
      line-height: 1.1;
      color: var(--ink);
      margin: .4rem 0 .3rem;
    }}
    header .date-range {{
      font-family: var(--font-mono);
      font-size: .8rem;
      color: var(--muted);
    }}

    /* Main */
    main {{
      width: min(1400px, 95vw);
      margin: 2rem auto 4rem;
      display: grid;
      gap: 1.6rem;
      padding: 0 1rem;
    }}

    /* KPI Strip */
    .kpi-strip {{
      display: flex;
      gap: 1rem;
      overflow-x: auto;
      scroll-snap-type: x proximity;
      scrollbar-width: none;
      -ms-overflow-style: none;
    }}
    .kpi-strip::-webkit-scrollbar {{ display: none; }}
    .kpi-card {{
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 1.4rem 1.6rem;
      display: flex;
      flex-direction: column;
      gap: .3rem;
      flex: 0 0 220px;
      scroll-snap-align: start;
    }}
    .kpi-label {{
      font-family: var(--font-mono);
      font-size: .7rem;
      letter-spacing: .12em;
      text-transform: uppercase;
      color: var(--muted);
    }}
    .kpi-value {{
      font-family: var(--font-display);
      font-size: 2rem;
      font-weight: 700;
      letter-spacing: -.02em;
      color: var(--ink);
    }}
    .kpi-sub {{
      font-size: .78rem;
      color: var(--faint);
    }}
    .kpi-accent .kpi-value {{ color: var(--accent); }}
    .kpi-green .kpi-value {{ color: var(--green); }}
    .kpi-red .kpi-value {{ color: var(--red); }}

    /* Cards / Sections */
    .card {{
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 2rem 2.2rem;
      display: grid;
      gap: 1.4rem;
    }}
    .section-header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: .6rem;
    }}
    .title-with-total {{
      display: inline-flex;
      align-items: baseline;
      gap: .8rem;
    }}
    h2 {{
      font-family: var(--font-display);
      font-size: 1.35rem;
      font-weight: 600;
      letter-spacing: -.01em;
      color: var(--ink);
      text-transform: capitalize;
    }}
    h3 {{
      font-family: var(--font-display);
      font-size: .95rem;
      font-weight: 600;
      color: var(--ink);
      margin: 1rem 0 .6rem;
    }}
    h4 {{
      font-family: var(--font-display);
      font-size: .9rem;
      font-weight: 600;
      color: var(--ink);
      text-transform: capitalize;
    }}
    h5.subtle-heading {{
      font-family: var(--font-mono);
      font-size: .72rem;
      font-weight: 500;
      color: var(--faint);
      text-transform: uppercase;
      letter-spacing: .1em;
      margin: .6rem 0 .4rem;
    }}
    .badge {{
      font-family: var(--font-mono);
      background: var(--accent-dim);
      color: var(--accent);
      padding: .3rem .8rem;
      border-radius: 999px;
      font-weight: 500;
      font-size: .72rem;
      letter-spacing: .04em;
    }}
    .panel-grid.two {{
      display: grid;
      gap: 1.2rem;
      grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    }}
    .panel-grid.three {{
      display: grid;
      gap: 1.2rem;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    }}
    .metric-card {{
      background: var(--surface-2);
      border: 1px solid var(--line-soft);
      border-radius: 14px;
      padding: 1.2rem 1.4rem;
    }}

    /* Lists */
    ul {{
      list-style: none;
      padding: 0;
      margin: 0 0 .8rem;
    }}
    .data-list li, .stat-list li, .mini-list li, .table-list li {{
      font-size: .85rem;
      color: var(--muted);
      padding: .3rem 0;
      line-height: 1.45;
      border-bottom: 1px solid var(--line-soft);
      word-break: break-word;
    }}
    .data-list li::before {{
      content: '◆';
      font-size: .5rem;
      color: var(--accent);
      margin-right: .6rem;
      vertical-align: middle;
    }}
    .stat-list {{ margin-bottom: .8rem; }}
    .mini-list {{ padding-left: .8rem; }}
    .mini-list li::before {{
      content: '·';
      color: var(--faint);
      margin-right: .5rem;
    }}
    .table-list {{ margin: 0; }}
    .table-list li {{ border: none; padding: .15rem 0; font-size: .78rem; }}
    .table-list li::before {{
      content: '–';
      color: var(--faint);
      margin-right: .4rem;
    }}

    /* Tables */
    .table-container {{
      overflow-x: auto;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: var(--surface);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 900px;
    }}
    th, td {{
      padding: .85rem 1rem;
      text-align: left;
      vertical-align: top;
      font-size: .82rem;
      border-bottom: 1px solid var(--line-soft);
      color: var(--muted);
    }}
    th {{
      font-family: var(--font-mono);
      font-size: .7rem;
      font-weight: 500;
      letter-spacing: .08em;
      text-transform: uppercase;
      color: var(--faint);
      background: var(--surface-2);
    }}
    tbody tr:hover {{ background: var(--accent-dim); }}
    .muted-text {{ color: var(--faint); font-size: .82rem; margin: 0 0 .6rem; }}

    /* Details / Accordion */
    details.details-panel {{
      background: var(--surface-2);
      border: 1px solid var(--line-soft);
      border-radius: 14px;
      padding: .8rem 1.2rem;
    }}
    details summary {{
      cursor: pointer;
      font-family: var(--font-display);
      font-weight: 600;
      font-size: .9rem;
      color: var(--ink);
      list-style: none;
    }}
    details summary::marker {{ display: none; }}
    details summary::after {{
      content: '▾';
      display: inline-block;
      margin-left: .5rem;
      transition: transform .2s ease;
      color: var(--accent);
    }}
    details[open] summary::after {{ transform: rotate(180deg); }}
    .details-grid {{
      display: grid;
      gap: 1rem;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      margin-top: .8rem;
    }}
    .sub-card {{
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 1rem 1.2rem;
    }}
    .sub-card h3 {{ font-size: .88rem; margin: .6rem 0 .4rem; }}

    /* Chart */
    .chart-wrapper {{
      background: var(--surface-2);
      border: 1px solid var(--line-soft);
      border-radius: 14px;
      padding: 1.5rem;
      min-height: 280px;
      position: relative;
    }}
    .vol-pill {{
      display: inline-block;
      font-family: var(--font-mono);
      font-size: .72rem;
      font-weight: 500;
      background: rgba(95,208,138,.14);
      color: var(--green);
      padding: .2rem .6rem;
      border-radius: 999px;
      letter-spacing: .03em;
    }}
    .btn {{
      font-family: var(--font-mono);
      font-size: .72rem;
      padding: .4rem .8rem;
      border-radius: 999px;
      border: 1px solid var(--accent-line);
      background: var(--accent-dim);
      color: var(--accent);
      cursor: pointer;
      letter-spacing: .06em;
      text-transform: uppercase;
      transition: all .15s ease;
    }}
    .btn:hover {{
      background: rgba(77,182,255,.22);
      transform: translateY(-1px);
    }}

    /* Footer */
    footer {{
      text-align: center;
      padding: 2rem 1rem;
      border-top: 1px solid var(--line-soft);
    }}
    footer p {{
      font-family: var(--font-mono);
      font-size: .7rem;
      letter-spacing: .1em;
      text-transform: uppercase;
      color: var(--faint);
    }}

    /* Animations */
    @media (prefers-reduced-motion: no-preference) {{
      .anim  {{ animation: rise .55s cubic-bezier(.2,.7,.2,1) both; }}
      .anim2 {{ animation: rise .55s cubic-bezier(.2,.7,.2,1) .08s both; }}
      .anim3 {{ animation: rise .55s cubic-bezier(.2,.7,.2,1) .16s both; }}
      .anim4 {{ animation: rise .55s cubic-bezier(.2,.7,.2,1) .24s both; }}
      @keyframes rise {{
        from {{ opacity: 0; transform: translateY(16px); }}
        to   {{ opacity: 1; transform: none; }}
      }}
    }}
    @media print {{
      .anim,.anim2,.anim3,.anim4 {{ animation: none !important; opacity: 1 !important; transform: none !important; }}
      body {{ background: #fff; color: #1a1a1a; }}
    }}

    /* Responsive */
    @media (max-width: 768px) {{
      header {{ padding: 2.4rem 0 1.8rem; }}
      header .logo-wrapper {{ position: static; margin-bottom: .8rem; }}
      header h1 {{ font-size: 1.8rem; }}
      .kpi-card {{ flex-basis: 180px; padding: 1.2rem 1.3rem; }}
      .kpi-value {{ font-size: 1.5rem; }}
      .card {{ padding: 1.4rem 1.2rem; }}
    }}

    /* Back nav */
    .back-nav {{
      display: inline-flex;
      align-items: center;
      gap: .4rem;
      padding: .75rem 1.5rem;
      font-family: var(--font-mono);
      font-size: .72rem;
      letter-spacing: .12em;
      text-transform: uppercase;
      color: var(--muted);
      text-decoration: none;
      border-bottom: 1px solid var(--line-soft);
      width: 100%;
      transition: color .15s;
    }}
    .back-nav:hover {{ color: var(--accent); }}
    .back-nav::before {{ content: '←'; font-size: .9rem; letter-spacing: 0; }}
  </style>
</head>
<body>
  <a class="back-nav" href="{web_root}reports.html">All reports</a>
  <header>
    <div class='header-inner'>
      <div class='logo-wrapper'>
        {logo_markup}
      </div>
      <div class="eyebrow">Spotflow Insights</div>
      <h1>{report_title}</h1>
      <p class="date-range">{date_range}</p>
      {f'<p class="date-range">{coverage_note}</p>' if coverage_note else ''}
    </div>
  </header>
  <main>
    {kpi_html}

    <section class="card anim">
      <div class="section-header">
        <div class="title-with-total"><h2>Merchant volume</h2><span class="badge">{len(merchants)} merchants</span></div>
      </div>
      <div class="panel-grid two">
        <div class="chart-wrapper">
          <canvas id="merchantChart"></canvas>
        </div>
        <div class="table-container">
          <table>
            <thead>
              <tr><th>Merchant</th><th>Txns</th><th>Success</th><th>Failed</th><th>Volume</th><th>Failed $</th><th>Rate</th><th>Top Channel</th><th>Top Region</th></tr>
            </thead>
            <tbody>{''.join(merchant_rows)}</tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="card anim2">
      <div class="section-header">
        <div class="title-with-total"><h2>Aggregate overview</h2><span class="badge">{total_txns:,} txns</span></div>
      </div>
      <div class="panel-grid two">
        <div class="metric-card">
          <h3>Status mix</h3>
          {agg_status_html}
          <h3>Channel distribution</h3>
          {agg_channel_html}
        </div>
        <div class="metric-card">
          <h3>Top success messages</h3>
          {agg_success_html}
          <h3>Top failure messages</h3>
          {agg_failure_html}
        </div>
      </div>
    </section>

    <section class="card anim2">
      <div class="section-header">
        <div class="title-with-total"><h2>Provider contribution</h2><span class="badge">{len(provider_rows)} providers</span></div>
      </div>
      <div class="table-container">
        <table>
          <thead>
            <tr><th>Provider</th><th>Total</th><th>Success</th><th>Failed</th><th>Abandoned</th><th>Cancelled</th><th>Refunded</th><th>Success %</th><th>Fail %</th><th>Volume</th><th>Top success</th><th>Top failure</th></tr>
          </thead>
          <tbody>{''.join(provider_rows)}</tbody>
        </table>
      </div>
    </section>

    {''.join(region_sections)}



    <section class="card anim4">
      <div class="section-header">
        <div class="title-with-total"><h2>Failure causes by region</h2><span class="badge">Customer pain points</span></div>
      </div>
      <div class="panel-grid two">
        {''.join(failure_sections)}
      </div>
    </section>

    {retry_html}

    <section class="card anim4">
      <div class="section-header">
        <div class="title-with-total"><h2>Regional success vs failure</h2><span class="badge">Comparison</span></div>
        <button class="btn" id="toggleChart">Toggle view</button>
      </div>
      <div class="chart-wrapper">
        <canvas id="regionChart"></canvas>
      </div>
    </section>
  </main>

  <footer>
    <p>◆ Spotflow &middot; Transaction Intelligence</p>
  </footer>

  <script>
    Chart.defaults.color = '#93A0AD';
    Chart.defaults.borderColor = 'rgba(255,255,255,.06)';
    Chart.defaults.font.family = "'IBM Plex Sans', sans-serif";

    // Merchant chart
    const mData = {json.dumps(merchant_chart_data)};
    new Chart(document.getElementById('merchantChart'), {{
      type: 'bar',
      data: {{
        labels: mData.labels,
        datasets: [
          {{ label: 'Volume (USD)', data: mData.volume, backgroundColor: 'rgba(77,182,255,.5)', borderColor: '#4DB6FF', borderWidth: 1, borderRadius: 6, yAxisID: 'y' }},
        ]
      }},
      options: {{
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: {{ legend: {{ display: false }} }},
        scales: {{
          x: {{ grid: {{ color: 'rgba(255,255,255,.04)' }}, ticks: {{ callback: v => '$' + v.toLocaleString() }} }},
          y: {{ grid: {{ display: false }} }}
        }}
      }}
    }});





    // Region chart
    const rData = {json.dumps(region_chart)};
    const rCtx = document.getElementById('regionChart').getContext('2d');
    let rType = 'bar';
    function createRegionChart(type) {{
      return new Chart(rCtx, {{
        type,
        data: {{
          labels: rData.labels,
          datasets: [
            {{ label: 'Successful', data: rData.success, backgroundColor: '#5FD08A', borderColor: '#5FD08A', borderWidth: 2, borderRadius: 6, tension: .35 }},
            {{ label: 'Failed', data: rData.failed, backgroundColor: '#FF7A7A', borderColor: '#FF7A7A', borderWidth: 2, borderRadius: 6, tension: .35 }}
          ]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true, padding: 14 }} }} }},
          scales: {{
            x: {{ grid: {{ color: 'rgba(255,255,255,.04)' }} }},
            y: {{ beginAtZero: true, grid: {{ color: 'rgba(255,255,255,.04)' }} }}
          }}
        }}
      }});
    }}
    let regionChart = createRegionChart(rType);
    document.getElementById('toggleChart').addEventListener('click', () => {{
      rType = rType === 'bar' ? 'line' : 'bar';
      regionChart.destroy();
      regionChart = createRegionChart(rType);
    }});
  </script>
</body>
</html>
"""

    html = "\n".join(line.rstrip() for line in html.splitlines()) + "\n"
    output_path.write_text(html, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Render v2 HTML report from Spotflow export.")
    parser.add_argument("csv_path", type=Path, help="Path to the CSV export")
    parser.add_argument("output_path", type=Path, help="Destination HTML path")
    parser.add_argument("--title", default="Spotflow Insights", help="Report title")
    parser.add_argument("--logo", type=Path, default=None, help="Inline SVG logo to embed")
    args = parser.parse_args()

    logo_svg = args.logo.read_text(encoding="utf-8") if args.logo else None
    render_report_v2(args.csv_path, args.output_path, args.title, logo_svg)
    print(f"v2 Report written to {args.output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
