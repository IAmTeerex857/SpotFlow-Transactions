#!/usr/bin/env python3
"""Dump reference aggregates from the original Python parser.

The TypeScript port in src/lib/parser must reproduce these numbers exactly.
Output contains merchant volumes, so it is written to .golden/ (gitignored).

    python3 scripts/golden/dump_python.py
"""
from __future__ import annotations

import glob
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "Spotflow transactions data"))

from render_html_report_v2 import parse_csv_v2  # noqa: E402

OUT = Path(__file__).resolve().parents[2] / ".golden" / "python.json"
REGION_ALIASES = {"RWANDA": "Rwanda"}


def region_name(name: str) -> str:
    return REGION_ALIASES.get(name, name)


def r2(value: float) -> float:
    return round(value, 2)


def summarise(path: Path) -> dict:
    transactions, summaries, merchants = parse_csv_v2(path)

    regions = {}
    for name, summary in summaries.items():
        if name == "Aggregate" or not summary.status_counts:
            continue
        regions[region_name(name)] = {
            "status": dict(summary.status_counts),
            "channel": dict(summary.channel_counts),
            "messages": {s: dict(c) for s, c in summary.messages_by_status.items() if c},
            "successUsd": r2(summary.successful_volume_usd),
            "volumeUsd": r2(summary.volume_usd),
            "providers": {
                p: {"status": dict(ps.status_counts), "successUsd": r2(ps.successful_volume_usd)}
                for p, ps in summary.providers.items()
            },
        }

    agg = summaries["Aggregate"]
    providers = {
        p: {
            "status": dict(ps.status_counts),
            "channel": dict(ps.channel_counts),
            "successUsd": r2(ps.successful_volume_usd),
            "messages": {s: dict(c) for s, c in ps.messages_by_status.items() if c},
        }
        for p, ps in agg.providers.items()
    }

    merchant_out = {
        m: {
            "total": ms.total_txns,
            "successful": ms.successful_txns,
            "failed": ms.failed_txns,
            "abandoned": ms.abandoned_txns,
            "cancelled": ms.cancelled_txns,
            "successUsd": r2(ms.successful_volume_usd),
            "failedUsd": r2(ms.failed_volume_usd),
            "volumeUsd": r2(ms.volume_usd),
        }
        for m, ms in merchants.items()
    }

    by_status_usd = defaultdict(float)
    for t in transactions:
        by_status_usd[t.status] += t.amount_usd

    # Retry semantics exactly as the Python report computes them, minus the
    # "unresolved" figure, which reads the oldest attempt (a known bug).
    attempts = defaultdict(list)
    for t in transactions:
        attempts[(t.customer or "").strip().lower() or "<unknown>"].append(t)
    retriers = {k: v for k, v in attempts.items() if len(v) > 1}
    gaps = []
    for v in retriers.values():
        times = [t.dt for t in sorted(v, key=lambda t: (t.dt is None, t.dt)) if t.dt]
        for j in range(1, len(times)):
            gap = (times[j] - times[j - 1]).total_seconds()
            if gap >= 0:
                gaps.append(gap)

    return {
        "total": len(transactions),
        "status": dict(agg.status_counts),
        "channel": dict(agg.channel_counts),
        "missingDates": sum(t.dt is None for t in transactions),
        "statusUsd": {k: r2(v) for k, v in by_status_usd.items()},
        "regions": regions,
        "providers": providers,
        "merchants": merchant_out,
        "retry": {
            "retriers": len(retriers),
            "succeededAfter": sum(any(t.status == "successful" for t in v) for v in retriers.values()),
            "attempts": {str(k): c for k, c in Counter(len(v) for v in retriers.values()).items()},
            "finalStatus": dict(Counter(
                sorted(v, key=lambda t: (t.dt is None, t.dt))[-1].status for v in retriers.values()
            )),
            "gapCount": len(gaps),
            "gapSum": r2(sum(gaps)),
            "gapMedian": sorted(gaps)[len(gaps) // 2] if gaps else 0,
        },
    }


def main() -> int:
    files = sorted(glob.glob(str(ROOT / "data" / "*" / "*anonymized.csv")))
    result = {}
    for f in files:
        rel = str(Path(f).relative_to(ROOT))
        result[rel] = summarise(Path(f))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, ensure_ascii=False, sort_keys=True))
    total = sum(v["total"] for v in result.values())
    print(f"Wrote {len(result)} files, {total:,} transactions → {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
