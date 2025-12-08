#!/usr/bin/env python3
"""Render an HTML insight report for a Spotflow export."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable, List, Tuple

from analyze_transactions import BLANK_MESSAGE, REGIONS, parse_csv


def _display_message(msg: str) -> str:
    return "No provider message (blank)" if msg == BLANK_MESSAGE else msg


def _list_html(items: Iterable[str], css_class: str = "data-list") -> str:
    items = list(items)
    if not items:
        return f"<ul class='{css_class}'><li>—</li></ul>"
    return "<ul class='{cls}'>".format(cls=css_class) + "".join(f"<li>{item}</li>" for item in items) + "</ul>"


def _status_lines(counter: Counter) -> List[str]:
    order = ["successful", "failed", "abandoned", "cancelled", "inprogress"]
    return [f"{status.title()} — {counter.get(status, 0)}" for status in order if counter.get(status, 0)]


def _channel_lines(counter: Counter) -> List[str]:
    order = ["card", "bank_transfer", "mobile_money", "eft", "other"]
    return [f"{key.replace('_', ' ').title()} — {counter.get(key, 0)}" for key in order if counter.get(key, 0)]


def _message_lines(counter: Counter) -> List[str]:
    return [f"{_display_message(msg)} — {count}" for msg, count in counter.most_common()]


def render_report(csv_path: Path, output_path: Path, report_title: str, logo_inline: str | None = None) -> None:
    transactions, summaries = parse_csv(csv_path)
    aggregate = summaries["Aggregate"]

    # aggregate sections
    aggregate_status_html = _list_html(_status_lines(aggregate.status_counts))
    aggregate_channels_html = _list_html(_channel_lines(aggregate.channel_counts))
    aggregate_success_html = _list_html(_message_lines(aggregate.messages_by_status["successful"]))
    aggregate_failure_html = _list_html(_message_lines(aggregate.messages_by_status["failed"]))

    provider_rows: List[str] = []
    for provider, pdata in sorted(aggregate.providers.items()):
        total = sum(pdata.status_counts.values())
        success = pdata.status_counts.get("successful", 0)
        failed = pdata.status_counts.get("failed", 0)
        abandoned = pdata.status_counts.get("abandoned", 0)
        cancelled = pdata.status_counts.get("cancelled", 0)
        success_rate = f"{(success / total * 100):.1f}%" if total else "0.0%"
        failure_rate = f"{(failed / total * 100):.1f}%" if total else "0.0%"
        abandon_rate = f"{(abandoned / total * 100):.1f}%" if total else "0.0%"
        top_success = _list_html(
            [f"{_display_message(msg)} ({count})" for msg, count in pdata.messages_by_status["successful"].most_common()],
            "table-list",
        )
        top_failure = _list_html(
            [f"{_display_message(msg)} ({count})" for msg, count in pdata.messages_by_status["failed"].most_common()],
            "table-list",
        )
        provider_rows.append(
            "".join(
                [
                    "<tr>",
                    f"<td>{provider}</td>",
                    f"<td>{total}</td>",
                    f"<td>{success}</td>",
                    f"<td>{failed}</td>",
                    f"<td>{abandoned}</td>",
                    f"<td>{cancelled}</td>",
                    f"<td>{success_rate}</td>",
                    f"<td>{failure_rate}</td>",
                    f"<td>{abandon_rate}</td>",
                    f"<td>{top_success}</td>",
                    f"<td>{top_failure}</td>",
                    "</tr>",
                ]
            )
        )

    region_sections: List[str] = []
    region_failure_summary: List[Tuple[str, int, List[str]]] = []

    for region in REGIONS:
        summary = summaries.get(region)
        if not summary or not summary.status_counts:
            continue

        status_html = _list_html(_status_lines(summary.status_counts))
        channel_lines = _channel_lines(summary.channel_counts)
        channel_html = ""
        if channel_lines:
            channel_html = "<h3>Channel breakdown</h3>" + _list_html(channel_lines)

        success_html = _list_html(_message_lines(summary.messages_by_status["successful"]))
        failure_html = _list_html(_message_lines(summary.messages_by_status["failed"]))

        provider_cards: List[str] = []
        for provider, pdata in sorted(summary.providers.items()):
            stats_html = _list_html(_status_lines(pdata.status_counts), "stat-list")
            success_lines = [f"{_display_message(msg)} — {count}" for msg, count in pdata.messages_by_status["successful"].most_common()]
            failure_lines = [f"{_display_message(msg)} — {count}" for msg, count in pdata.messages_by_status["failed"].most_common()]
            card = [
                "<div class='sub-card'>",
                f"<h4>{provider}</h4>",
                f"<p class='muted'>Total: {sum(pdata.status_counts.values())}</p>",
                stats_html,
            ]
            if success_lines or failure_lines:
                card.append("<h5 class='subtle-heading'>provider message highlights</h5>")
            if success_lines:
                card.append(_list_html(success_lines, "mini-list"))
            if failure_lines:
                card.append(_list_html(failure_lines, "mini-list"))
            card.append("</div>")
            provider_cards.append("".join(card))

        provider_block = (
            "<details class='details-panel'><summary>Provider breakdown</summary><div class='details-grid'>"
            + "".join(provider_cards)
            + "</div></details>"
        ) if provider_cards else ""

        region_sections.append(
            f"""
<section>
  <div class='section-header'>
    <div class='title-with-total'><h2>{region}</h2><span class='badge'>Total: {sum(summary.status_counts.values())}</span></div>
  </div>
  <div class='panel-grid two'>
    <div class='metric-card'>
      <h3>Status mix</h3>
      {status_html}
      {channel_html}
    </div>
    <div class='metric-card'>
      <h3>Top success reasons</h3>
      {success_html}
      <h3>Top failure reasons</h3>
      {failure_html}
    </div>
  </div>
  {provider_block}
</section>
"""
        )

        total_failed = summary.status_counts.get("failed", 0)
        failure_lines = [
            f"{_display_message(msg)} — {count} ({(count / total_failed * 100):.1f}% of failures)"
            for msg, count in summary.messages_by_status["failed"].most_common()
        ] if total_failed else []
        region_failure_summary.append((region, total_failed, failure_lines))

    failure_sections = []
    for region, total_failed, lines in region_failure_summary:
        if total_failed == 0:
            failure_sections.append(f"<div class='sub-card'><h3>{region}</h3><p class='muted'>No recorded failures.</p></div>")
        else:
            failure_sections.append(
                f"""
<div class='sub-card'>
  <h3>{region}</h3>
  <p class='muted'>Total failed: {total_failed}</p>
  {_list_html(lines, 'mini-list')}
</div>
"""
            )

    region_chart = {
        "labels": [region for region, _, _ in region_failure_summary],
        "success": [summaries[region].status_counts.get("successful", 0) for region, _, _ in region_failure_summary],
        "failed": [summaries[region].status_counts.get("failed", 0) for region, _, _ in region_failure_summary],
    }

    raw_990_count = csv_path.read_text(encoding="utf-8").count("990,NGN")

    customer_attempts = defaultdict(list)
    for provider, region, status, channel, norm_message, dt, customer in transactions:
        key = (customer or "").strip().lower() or "<unknown>"
        customer_attempts[key].append(
            {
                "provider": provider,
                "region": region,
                "status": status,
                "dt": dt,
            }
        )

    retry_customers = []
    attempt_distribution = Counter()
    final_status_counter = Counter()
    provider_presence = Counter()
    region_presence = Counter()
    retry_gaps: List[float] = []
    succeeded_after_retry = 0
    unresolved_after_retry = 0
    mid_success_not_final = 0
    longest_attempt: Tuple[int, bool, str] | None = None

    for attempts_list in customer_attempts.values():
        if len(attempts_list) <= 1:
            continue
        attempts_list.sort(key=lambda item: (item["dt"] is None, item["dt"]))
        statuses = [item["status"] for item in attempts_list]
        providers = {item["provider"] for item in attempts_list}
        regions = {item["region"] for item in attempts_list}
        attempt_count = len(attempts_list)
        final_status = statuses[-1]
        ever_success = any(status == "successful" for status in statuses)

        attempt_distribution[attempt_count] += 1
        final_status_counter[final_status] += 1
        for provider in providers:
            provider_presence[provider] += 1
        for region in regions:
            region_presence[region] += 1

        if ever_success:
            succeeded_after_retry += 1
            if final_status != "successful":
                mid_success_not_final += 1
        else:
            unresolved_after_retry += 1

        for current, nxt in zip(attempts_list, attempts_list[1:]):
            if current["dt"] and nxt["dt"]:
                retry_gaps.append((nxt["dt"] - current["dt"]).total_seconds())

        if longest_attempt is None or attempt_count > longest_attempt[0]:
            longest_attempt = (attempt_count, ever_success, final_status)

        retry_customers.append(attempt_count)

    total_retry_customers = len(retry_customers)

    def _format_customer_count(count: int) -> str:
        return f"{count} customer" if count == 1 else f"{count} customers"

    attempt_distribution_lines = [
        f"{attempts} attempts — {_format_customer_count(count)}" for attempts, count in sorted(attempt_distribution.items())
    ]
    final_status_lines = [f"{status.title()} — {count}" for status, count in final_status_counter.most_common()]
    provider_lines = [
        f"{provider.replace('_', ' ').title()} — {_format_customer_count(count)}"
        for provider, count in provider_presence.most_common()
    ]
    region_lines = [
        f"{region} — {_format_customer_count(count)}" for region, count in region_presence.most_common()
    ]

    summary_lines = []
    if total_retry_customers:
        summary_lines.extend(
            [
                f"Customers who retried — {total_retry_customers}",
                f"Ever completed after retry — {succeeded_after_retry}",
                f"Still unresolved after retries — {unresolved_after_retry}",
            ]
        )
        if mid_success_not_final:
            summary_lines.append(
                f"Succeeded mid-sequence but ended with non-success status — {mid_success_not_final}"
            )
    summary_lines.append(f"Transactions at 990 NGN (including fragmented rows) — {raw_990_count}")

    timing_lines: List[str] = []
    if retry_gaps:
        avg_gap_hours = sum(retry_gaps) / len(retry_gaps) / 3600
        median_sorted = sorted(retry_gaps)
        mid = len(median_sorted) // 2
        if len(median_sorted) % 2:
            median_gap_seconds = median_sorted[mid]
        else:
            median_gap_seconds = (median_sorted[mid - 1] + median_sorted[mid]) / 2
        timing_lines.append(
            f"Average gap between attempts — {avg_gap_hours:.2f} hours (~{avg_gap_hours * 60:.0f} minutes)"
        )
        timing_lines.append(
            f"Median gap between attempts — {median_gap_seconds / 3600:.2f} hours (~{median_gap_seconds / 60:.0f} minutes)"
        )
    if longest_attempt:
        attempts, ever_success, final_status = longest_attempt
        if ever_success and final_status != "successful":
            longest_text = (
                f"A customer attempted {attempts} times, succeeded once midstream, "
                f"but the latest attempt ended in {final_status}."
            )
        elif ever_success:
            longest_text = f"A customer attempted {attempts} times before ending on a successful outcome."
        else:
            longest_text = f"A customer attempted {attempts} times without a success; final status was {final_status}."
        timing_lines.append(longest_text)
    if not timing_lines:
        timing_lines.append("Timestamp data was insufficient to analyse retrial timing.")

    further_insights_html = ""
    if summary_lines or attempt_distribution_lines or provider_lines or region_lines or timing_lines:
        further_insights_html = f"""
    <section>
      <div class='section-header'>
        <div class='title-with-total'><h2>Further insights</h2><span class='badge'>Customer retry behaviour</span></div>
      </div>
      <div class='panel-grid two'>
        <div class='metric-card'>
          <h3>Customer retry outcomes</h3>
          {_list_html(summary_lines)}
          <h3>Final attempt statuses</h3>
          {_list_html(final_status_lines)}
        </div>
        <div class='metric-card'>
          <h3>Retry depth</h3>
          {_list_html(attempt_distribution_lines)}
          <h3>Timing signals</h3>
          {_list_html(timing_lines)}
        </div>
      </div>
      <div class='panel-grid two'>
        <div class='metric-card'>
          <h3>Providers involved</h3>
          {_list_html(provider_lines)}
        </div>
        <div class='metric-card'>
          <h3>Regions involved</h3>
          {_list_html(region_lines)}
        </div>
      </div>
    </section>
"""

    logo_markup = logo_inline or ""

    html = f"""<!DOCTYPE html>
<html lang='en'>
<head>
  <meta charset='utf-8'>
  <title>{report_title}</title>
  <script src='https://cdn.jsdelivr.net/npm/chart.js'></script>
  <style>
    :root {{ color-scheme: light; font-size: 16px; }}
    * {{ box-sizing: border-box; }}
    body {{ font-family: "Inter", "Segoe UI", -apple-system, BlinkMacSystemFont, sans-serif; margin: 0; background: #f4f6fb; color: #1f2937; line-height: 1.65; }}
    header {{ background: #0f172a; color: #f8fafc; padding: 2.8rem 0; margin-bottom: 2.4rem; position: relative; }}
    header .header-inner {{ width: min(1400px, 92vw); margin: 0 auto; padding: 0 1.5rem; text-align: center; position: relative; }}
    header .logo-wrapper {{ position: absolute; top: 1.2rem; left: 1.5rem; }}
    header svg {{ width: 170px; height: auto; }}
    header svg [fill] {{ fill: #f8fafc !important; }}
    header h1 {{ margin: 0; font-size: 2.6rem; letter-spacing: -0.02em; }}
    main {{ width: min(1400px, 95vw); margin: 0 auto 3.5rem; display: grid; gap: 2rem; padding: 0 1rem; }}
    section {{ background: #fff; border-radius: 22px; padding: 2.2rem 2.6rem; box-shadow: 0 22px 40px rgba(15, 23, 42, 0.08); display: grid; gap: 1.6rem; }}
    .section-header {{ display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: .8rem; margin-bottom: .3rem; }}
    .title-with-total {{ display: inline-flex; align-items: baseline; gap: .8rem; }}
    h2 {{ margin: 0; font-size: 1.45rem; letter-spacing: -0.01em; color: #0f172a; text-transform: capitalize; }}
    h3 {{ margin: 1.2rem 0 .8rem; font-size: 1.05rem; color: #0f172a; }}
    h4 {{ margin: 0 0 .4rem; font-size: 1rem; color: #0f172a; text-transform: capitalize; }}
    h5.subtle-heading {{ margin: .6rem 0 .4rem; font-size: .9rem; font-weight: 600; color: #475569; text-transform: none; letter-spacing: .06em; }}
    .badge {{ background: #e0e7ff; color: #3730a3; padding: .35rem .9rem; border-radius: 999px; font-weight: 600; font-size: .82rem; }}
    .btn {{ font-size: .88rem; padding: .45rem .85rem; border-radius: 999px; border: 1px solid transparent; cursor: pointer; transition: transform .15s ease, box-shadow .15s ease; }}
    .btn-secondary {{ background: #eef2ff; color: #3730a3; border-color: #c7d2fe; }}
    .btn-secondary:hover {{ transform: translateY(-1px); box-shadow: 0 8px 16px rgba(79, 70, 229, 0.2); }}
    .panel-grid.two {{ display: grid; gap: 1.6rem; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); }}
    .metric-card {{ background: linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%); border-radius: 18px; padding: 1.4rem 1.6rem; box-shadow: inset 0 0 0 1px #e2e8f0; }}
    ul {{ list-style: disc; padding-left: 1.3rem; margin: 0 0 1.2rem; }}
    .data-list li, .stat-list li, .mini-list li, .table-list li {{ margin-bottom: .45rem; line-height: 1.55; word-break: break-word; }}
    .stat-list {{ margin-bottom: 1rem; }}
    .mini-list {{ list-style: disc inside; margin-bottom: 0; padding-left: 1rem; }}
    .table-container {{ overflow-x: auto; border-radius: 20px; border: 1px solid #dce3ef; background: #f8fafc; padding: 1rem 1.2rem; }}
    .table-container.extra-wide table {{ min-width: 1320px; }}
    table {{ width: 100%; border-collapse: collapse; min-width: 1100px; background: #fff; border-radius: 16px; overflow: hidden; }}
    th, td {{ padding: 1rem 1.15rem; border-bottom: 1px solid #e2e8f0; text-align: left; vertical-align: top; font-size: .95rem; }}
    th {{ background: #eef2ff; font-weight: 600; color: #0f172a; }}
    tbody tr:nth-child(even) {{ background: #f9fbff; }}
    .table-list {{ list-style: disc inside; margin: 0; padding-left: 1rem; }}
    details.details-panel {{ background: #f8fafc; border-radius: 18px; padding: 1rem 1.3rem; box-shadow: inset 0 0 0 1px #e2e8f0; }}
    details summary {{ cursor: pointer; font-weight: 600; font-size: 1.02rem; list-style: none; }}
    details summary::marker {{ display: none; }}
    details summary::after {{ content: '▾'; display: inline-block; margin-left: .6rem; transition: transform .2s ease; }}
    details[open] summary::after {{ transform: rotate(180deg); }}
    .details-grid {{ display: grid; gap: 1.2rem; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); margin-top: 1rem; }}
    .sub-card {{ background: #fff; border-radius: 16px; padding: 1.1rem 1.3rem; box-shadow: inset 0 0 0 1px #e2e8f0; }}
    .muted {{ color: #64748b; margin: 0 0 .8rem; }}
    .chart-wrapper {{ background: #fff; border-radius: 20px; padding: 1.8rem; box-shadow: inset 0 0 0 1px #e2e8f0; min-height: 380px; }}
    canvas {{ width: 100%; height: 100%; }}
    @media (max-width: 768px) {{
      header {{ padding: 2.4rem 0; }}
      header .logo-wrapper {{ position: static; margin-bottom: 1rem; }}
      header svg {{ width: 140px; }}
    }}
  </style>
</head>
<body>
  <header>
    <div class='header-inner'>
      <div class='logo-wrapper'>
        {logo_markup}
      </div>
      <h1>{report_title}</h1>
    </div>
  </header>
  <main>
    <section>
      <div class='section-header'>
        <div class='title-with-total'><h2>Aggregate overview</h2><span class='badge'>Total: {sum(aggregate.status_counts.values())}</span></div>
      </div>
      <div class='panel-grid two'>
        <div class='metric-card'>
          <h3>Status mix</h3>
          {aggregate_status_html}
          <h3>Channel distribution</h3>
          {aggregate_channels_html}
        </div>
        <div class='metric-card'>
          <h3>Top success messages</h3>
          {aggregate_success_html}
          <h3>Top failure messages</h3>
          {aggregate_failure_html}
        </div>
      </div>
    </section>
    <section>
      <div class='section-header'>
        <div class='title-with-total'><h2>Provider contribution</h2><span class='badge'>Providers: {len(provider_rows)}</span></div>
      </div>
      <div class='table-container extra-wide'>
        <table>
          <thead>
            <tr>
              <th>Provider</th><th>Total</th><th>Success</th><th>Failed</th><th>Abandoned</th><th>Cancelled</th><th>Success rate</th><th>Failure rate</th><th>Abandon rate</th><th>Top success reasons</th><th>Top failure reasons</th>
            </tr>
          </thead>
          <tbody>
            {''.join(provider_rows)}
          </tbody>
        </table>
      </div>
    </section>
    {''.join(region_sections)}
    {further_insights_html}
    <section>
      <div class='section-header'>
        <div class='title-with-total'><h2>Highest failure causes by region</h2><span class='badge'>Focused on customer pain</span></div>
      </div>
      <div class='panel-grid two'>
        {''.join(failure_sections)}
      </div>
    </section>
    <section>
      <div class='section-header'>
        <div class='title-with-total'><h2>Regional success vs failure</h2><span class='badge'>Side-by-side comparison</span></div>
        <button class='btn btn-secondary' id='toggleChart'>Toggle view</button>
      </div>
      <div class='chart-wrapper'>
        <canvas id='regionChart'></canvas>
      </div>
    </section>
  </main>
  <script>
    const regionData = {json.dumps(region_chart)};
    const ctx = document.getElementById('regionChart').getContext('2d');
    let chartType = 'bar';
    const baseConfig = {{
      data: {{
        labels: regionData.labels,
        datasets: [
          {{ label: 'Successful', data: regionData.success, backgroundColor: '#10b981', borderColor: '#10b981', borderWidth: 2, borderRadius: 8 }},
          {{ label: 'Failed', data: regionData.failed, backgroundColor: '#ef4444', borderColor: '#ef4444', borderWidth: 2, borderRadius: 8 }}
        ]
      }},
      options: {{
        responsive: true,
        maintainAspectRatio: false,
        plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true, padding: 14 }} }} }},
        scales: {{
          x: {{ stacked: false, ticks: {{ color: '#1f2937' }}, grid: {{ color: '#e2e8f0' }} }},
          y: {{ beginAtZero: true, grid: {{ color: '#e2e8f0' }}, ticks: {{ color: '#1f2937' }} }}
        }}
      }}
    }};

    function createChart(type) {{
      const config = Object.assign({{ type }}, JSON.parse(JSON.stringify(baseConfig)));
      config.data.datasets.forEach(ds => {{
        ds.fill = type !== 'line';
        ds.borderWidth = type === 'line' ? 3 : 2;
        ds.tension = type === 'line' ? 0.35 : 0;
      }});
      return new Chart(ctx, config);
    }}

    let regionChart = createChart(chartType);
    document.getElementById('toggleChart').addEventListener('click', () => {{
      chartType = chartType === 'bar' ? 'line' : 'bar';
      regionChart.destroy();
      regionChart = createChart(chartType);
    }});
  </script>
</body>
</html>
"""

    output_path.write_text(html, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Render HTML report from Spotflow export.")
    parser.add_argument("csv_path", type=Path, help="Path to the CSV export")
    parser.add_argument("output_path", type=Path, help="Destination HTML path")
    parser.add_argument("--title", default="Spotflow Insights", help="Report title")
    parser.add_argument("--logo", type=Path, default=None, help="Inline SVG logo to embed")
    args = parser.parse_args()

    logo_svg = args.logo.read_text(encoding="utf-8") if args.logo else None
    render_report(args.csv_path, args.output_path, args.title, logo_svg)
    print(f"Report written to {args.output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

