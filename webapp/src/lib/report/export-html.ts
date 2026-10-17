// A self-contained, printable HTML report for sharing outside the app.
// Every value comes from uploaded files, so everything is escaped.
import { displayMessage } from "@/lib/parser/normalize";
import { fmtDuration, fmtInt, fmtPct, fmtRange, fmtUsd, label } from "@/lib/format";
import type { Report } from "./build";
import type { ReportFilters } from "./types";

const esc = (value: unknown) =>
  String(value).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]!);

const table = (headers: string[], rows: (string | number)[][], numericFrom = 1) => `
<table><thead><tr>${headers.map((h, i) => `<th class="${i >= numericFrom ? "n" : ""}">${esc(h)}</th>`).join("")}</tr></thead>
<tbody>${rows.map((r) => `<tr>${r.map((c, i) => `<td class="${i >= numericFrom ? "n" : ""}">${esc(c)}</td>`).join("")}</tr>`).join("")}</tbody></table>`;

export function renderReportHtml(report: Report, filters: ReportFilters, meta: { generatedBy: string; generatedAt: string }) {
  const t = report.totals;
  const scope = [
    filters.merchants?.length ? `Merchants: ${filters.merchants.join(", ")}` : null,
    filters.providers?.length ? `Providers: ${filters.providers.join(", ")}` : null,
    filters.regions?.length ? `Regions: ${filters.regions.join(", ")}` : null,
    filters.channels?.length ? `Channels: ${filters.channels.map(label).join(", ")}` : null,
  ].filter(Boolean).join(" · ") || "All merchants, providers, regions and channels";
  const failed = t.status.failed ?? 0;
  const kpis = [
    ["Transactions", fmtInt(t.total)],
    ["Success rate", fmtPct(t.successRate)],
    ["Success volume", fmtUsd(t.usdByStatus.successful ?? 0)],
    ["Failed volume", fmtUsd(t.usdByStatus.failed ?? 0)],
    ["Abandoned volume", fmtUsd(t.usdByStatus.abandoned ?? 0)],
  ];

  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Spotflow Insights · ${esc(fmtRange(filters.from, filters.to))}</title>
<style>
:root{--ink:#0e1116;--muted:#5b6673;--line:#e3e6ea;--accent:#1a8ae6;--ok:#1f9d57;--bad:#d9473f}
*{box-sizing:border-box}body{margin:0;font:14px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif;color:var(--ink);background:#fff}
main{max-width:1100px;margin:0 auto;padding:40px 24px}
.eyebrow{font:600 11px ui-monospace,monospace;letter-spacing:.2em;text-transform:uppercase;color:var(--accent)}
h1{font-size:28px;margin:6px 0 4px;letter-spacing:-.02em}h2{font-size:17px;margin:36px 0 10px}
.muted{color:var(--muted)}.kpis{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin:24px 0}
.kpi{border:1px solid var(--line);border-radius:12px;padding:12px 14px}.kpi b{display:block;font-size:20px;margin-top:4px}
table{width:100%;border-collapse:collapse;font-size:12.5px}th{font-weight:500;color:var(--muted);text-align:left;border-bottom:1px solid var(--line);padding:7px 8px}
td{border-bottom:1px solid var(--line);padding:7px 8px;vertical-align:top}.n{text-align:right;font-variant-numeric:tabular-nums}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:24px}footer{margin-top:40px;font-size:12px;color:var(--muted)}
@media(max-width:720px){.kpis{grid-template-columns:repeat(2,1fr)}.grid{grid-template-columns:1fr}}
@media print{main{padding:0}h2{break-after:avoid}table{break-inside:auto}tr{break-inside:avoid}}
</style></head><body><main>
<div class="eyebrow">Spotflow Insights</div>
<h1>${esc(fmtRange(filters.from, filters.to))}</h1>
<p class="muted">${esc(scope)}</p>
<div class="kpis">${kpis.map(([k, v]) => `<div class="kpi"><span class="muted">${esc(k)}</span><b>${esc(v)}</b></div>`).join("")}</div>

<h2>Daily breakdown</h2>
${table(["Date", "Total", "Successful", "Failed", "Abandoned", "Cancelled", "Success rate", "Success volume"],
    report.daily.map((d) => [d.date, fmtInt(d.total), fmtInt(d.successful), fmtInt(d.failed), fmtInt(d.abandoned), fmtInt(d.cancelled), fmtPct(d.successRate), fmtUsd(d.successUsd)]))}

<h2>Merchants</h2>
${table(["Merchant", "Txns", "Successful", "Failed", "Success volume", "Failed volume", "Success rate", "Top channel", "Top region"],
    report.merchants.map((m) => [m.name === "<unknown>" ? "Unattributed" : m.name, fmtInt(m.total), fmtInt(m.status.successful ?? 0), fmtInt(m.status.failed ?? 0), fmtUsd(m.successUsd), fmtUsd(m.failedUsd), fmtPct(m.successRate), m.topChannel ? label(m.topChannel) : "—", m.topRegion ?? "—"]))}

<h2>Providers</h2>
${table(["Provider", "Txns", "Successful", "Failed", "Abandoned", "Success rate", "Success volume", "Top failure reason"],
    report.providers.map((p) => [p.name, fmtInt(p.total), fmtInt(p.status.successful ?? 0), fmtInt(p.status.failed ?? 0), fmtInt(p.status.abandoned ?? 0), fmtPct(p.successRate), fmtUsd(p.successUsd), p.messages.failed?.[0] ? displayMessage(p.messages.failed[0].message) : "—"]))}

<div class="grid">
<div><h2>Failure reasons</h2>
${table(["Reason", "Count", "Share"], (report.messages.failed ?? []).slice(0, 15).map((m) => [displayMessage(m.message), fmtInt(m.count), fmtPct(m.count / (failed || 1))]))}</div>
<div><h2>Regions</h2>
${table(["Region", "Txns", "Success rate", "Success volume"], report.regions.map((r) => [r.name, fmtInt(r.total), fmtPct(r.successRate), fmtUsd(r.successUsd)]))}</div>
</div>

<h2>Customer retries</h2>
${table(["Measure", "Value"], [
    ["Customers who retried", fmtInt(report.retry.retriers)],
    ["Completed after retrying", fmtInt(report.retry.succeededAfter)],
    ["Still unresolved", fmtInt(report.retry.unresolved)],
    ["Median gap between attempts", fmtDuration(report.retry.gapMedianSeconds)],
  ])}

<footer>Generated ${esc(meta.generatedAt)} by ${esc(meta.generatedBy)}. Volumes are USD equivalents at each export's rate. Customer identities are pseudonymised. Internal: do not share outside Spotflow.</footer>
</main></body></html>`;
}
