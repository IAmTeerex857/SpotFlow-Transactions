// Compact, model-friendly views of a report. Numbers are rounded so the
// assistant quotes them consistently with the dashboard.
import { displayMessage } from "@/lib/parser/normalize";
import type { MessageCount, Report } from "@/lib/report/build";

const r2 = (v: number) => Math.round(v * 100) / 100;
const pct = (v: number) => Math.round(v * 1000) / 10;
const msgs = (list: MessageCount[] | undefined, n: number, total?: number) =>
  (list ?? []).slice(0, n).map((m) => ({
    reason: displayMessage(m.message),
    count: m.count,
    ...(total ? { share_pct: pct(m.count / total) } : {}),
  }));

export function summarizeReport(report: Report, opts: { merchants?: number; reasons?: number } = {}) {
  const t = report.totals;
  const failed = t.status.failed ?? 0;
  return {
    range: report.range,
    totals: {
      transactions: t.total,
      by_status: t.status,
      success_rate_pct: pct(t.successRate),
      usd_by_status: Object.fromEntries(Object.entries(t.usdByStatus).map(([k, v]) => [k, r2(v)])),
      channels: t.channels,
    },
    top_failure_reasons: msgs(report.messages.failed, opts.reasons ?? 8, failed),
    top_abandonment_reasons: msgs(report.messages.abandoned, 3, t.status.abandoned ?? 0),
    merchants: report.merchants.slice(0, opts.merchants ?? 10).map((m) => ({
      merchant: m.name === "<unknown>" ? "Unattributed" : m.name,
      transactions: m.total,
      successful: m.status.successful ?? 0,
      failed: m.status.failed ?? 0,
      success_rate_pct: pct(m.successRate),
      success_usd: r2(m.successUsd),
      failed_usd: r2(m.failedUsd),
      top_channel: m.topChannel,
      top_region: m.topRegion,
    })),
    providers: report.providers.map((p) => ({
      provider: p.name,
      transactions: p.total,
      success_rate_pct: pct(p.successRate),
      failure_rate_pct: pct((p.status.failed ?? 0) / (p.total || 1)),
      success_usd: r2(p.successUsd),
      top_failure_reasons: msgs(p.messages.failed, 3),
    })),
    regions: report.regions.map((r) => ({
      region: r.name,
      transactions: r.total,
      success_rate_pct: pct(r.successRate),
      success_usd: r2(r.successUsd),
      failed: r.status.failed ?? 0,
      top_failure_reasons: msgs(r.messages.failed, 3, r.status.failed ?? 0),
    })),
    retries: {
      customers_who_retried: report.retry.retriers,
      completed_after_retry: report.retry.succeededAfter,
      unresolved: report.retry.unresolved,
      attempts_distribution: report.retry.attempts,
      latest_attempt_status: report.retry.finalStatus,
      median_gap_minutes: Math.round(report.retry.gapMedianSeconds / 60),
      legacy_day_scoped_share_pct: pct(report.retry.legacyShare),
    },
  };
}

export function failureBreakdown(report: Report, by: "region" | "provider" | "merchant", status: string) {
  if (by === "merchant") {
    return { note: "Merchant-level reasons are not tracked separately; showing merchant failure counts and volumes.",
      rows: report.merchants.map((m) => ({ merchant: m.name, failed: m.status.failed ?? 0, failed_usd: r2(m.failedUsd), failure_rate_pct: pct((m.status.failed ?? 0) / (m.total || 1)) })) };
  }
  const rows = by === "region" ? report.regions : report.providers;
  return {
    status,
    rows: rows.map((r) => ({
      [by]: r.name,
      [status]: r.status[status] ?? 0,
      reasons: msgs(r.messages[status], 10, r.status[status] ?? 0),
    })),
  };
}
