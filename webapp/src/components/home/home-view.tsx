"use client";

import { motion } from "motion/react";
import { ArrowDownRight, ArrowUpRight, BarChart3, CalendarRange, MessageSquareText, Upload } from "lucide-react";
import Link from "next/link";
import { useState } from "react";
import { PillBarChart } from "@/components/charts/pill-bar-chart";
import { STATUS_SERIES } from "@/components/charts/series";
import { Card, CardHeader } from "@/components/ui/card";
import { PillTabs } from "@/components/ui/pill-tabs";
import { Delta, StatusPill } from "@/components/ui/status";
import { displayMessage } from "@/lib/parser/normalize";
import { fmtCompact, fmtDate, fmtInt, fmtPct, fmtUsd, fmtUsdCompact, label } from "@/lib/format";
import type { HomeSummary, Kpi } from "@/lib/report/home";
import { cn } from "@/lib/utils";

interface RecentUpload {
  id: string;
  fileName: string;
  status: "processing" | "complete" | "failed";
  transactions: number;
  durationMs: number | null;
  createdAt: string;
}

const KPI_META: Record<Kpi["key"], { label: string; color: string; format: (v: number) => string; delta: (v: number, p: number) => number; deltaFormat: (v: number) => string; goodWhen: "up" | "down" }> = {
  successRate: { label: "Success rate", color: "var(--chart-ok)", format: (v) => fmtPct(v), delta: (v, p) => v - p, deltaFormat: (v) => `${(v * 100).toFixed(1)} pts`, goodWhen: "up" },
  transactions: { label: "Transactions", color: "var(--chart-accent)", format: fmtInt, delta: (v, p) => (p ? (v - p) / p : 0), deltaFormat: (v) => fmtPct(v), goodWhen: "up" },
  successUsd: { label: "Success volume", color: "var(--chart-cancel)", format: (v) => (v >= 100_000 ? fmtUsdCompact(v) : fmtUsd(v)), delta: (v, p) => (p ? (v - p) / p : 0), deltaFormat: (v) => fmtPct(v), goodWhen: "up" },
  failedUsd: { label: "Failed volume", color: "var(--chart-bad)", format: (v) => (v >= 100_000 ? fmtUsdCompact(v) : fmtUsd(v)), delta: (v, p) => (p ? (v - p) / p : 0), deltaFormat: (v) => fmtPct(v), goodWhen: "down" },
};

function QuickAction({ href, icon, title, body, primary, index }: {
  href: string; icon: React.ReactNode; title: string; body: string; primary?: boolean; index: number;
}) {
  return (
    <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.04 * index, duration: 0.4 }}>
      <Link
        href={href}
        className={cn(
          "group flex h-full items-start gap-3.5 rounded-2xl p-4 transition-[transform,box-shadow] duration-200 hover:-translate-y-0.5",
          primary ? "bg-accent text-white shadow-[0_8px_24px_-12px_var(--accent)]" : "bg-raised shadow-[var(--shadow-card)] hover:shadow-[var(--shadow-pop)]",
        )}
      >
        <span className={cn("flex size-9 shrink-0 items-center justify-center rounded-[10px] [&_svg]:size-4", primary ? "bg-white/15" : "border border-line bg-raised text-ink")}>
          {icon}
        </span>
        <span className="min-w-0 flex-1">
          <span className="flex items-center justify-between gap-2 text-[14px] font-medium tracking-[-0.01em]">
            {title}
            <ArrowUpRight className={cn("size-4 transition-transform duration-200 group-hover:-translate-y-0.5 group-hover:translate-x-0.5", primary ? "text-white/80" : "text-faint")} />
          </span>
          <span className={cn("mt-0.5 block text-[12.5px] leading-snug", primary ? "text-white/75" : "text-muted")}>{body}</span>
        </span>
      </Link>
    </motion.div>
  );
}

function KpiCard({ kpi, href, index }: { kpi: Kpi; href: string; index: number }) {
  const meta = KPI_META[kpi.key];
  return (
    <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.05 * index + 0.1, duration: 0.45 }}>
      <Card className="flex h-full flex-col p-5">
        <div className="flex items-center justify-between">
          <span className="text-[13px] text-muted">{meta.label}</span>
          <Link href={href} className="rounded-md p-1 text-faint transition-colors hover:bg-surface-2 hover:text-ink" aria-label={`Open ${meta.label} in reports`}>
            <ArrowUpRight className="size-4" />
          </Link>
        </div>
        <div className="mt-3 text-[30px] font-semibold leading-none tracking-[-0.03em] text-ink">{meta.format(kpi.value)}</div>
        <div className="mt-2 flex items-center gap-2 text-[12px]">
          <Delta value={kpi.previous === null ? null : meta.delta(kpi.value, kpi.previous)} format={meta.deltaFormat} goodWhen={meta.goodWhen} />
          <span className="text-faint">vs previous day</span>
        </div>
        <div className="mt-5 h-8 overflow-hidden rounded-full" style={{ background: `color-mix(in oklab, ${meta.color} 12%, transparent)` }}>
          <motion.div
            className="h-full rounded-full"
            style={{ background: `linear-gradient(90deg, color-mix(in oklab, ${meta.color} 70%, white), ${meta.color})` }}
            initial={{ width: "8%" }}
            animate={{ width: `${Math.max(8, Math.min(100, kpi.fill * 100))}%` }}
            transition={{ duration: 0.9, delay: 0.15 + 0.05 * index, ease: [0.2, 0.7, 0.2, 1] }}
          />
        </div>
        <p className="mt-2.5 text-[11.5px] text-faint">{kpi.caption}</p>
      </Card>
    </motion.div>
  );
}

type Metric = "transactions" | "rate" | "volume";

function DailyCard({ summary }: { summary: HomeSummary }) {
  const [metric, setMetric] = useState<Metric>("transactions");
  const daily = summary.daily;
  const config = {
    transactions: { label: "transactions", value: (i: number) => daily[i].total, format: fmtInt, tick: fmtCompact },
    rate: { label: "success rate", value: (i: number) => daily[i].successRate, format: (v: number) => fmtPct(v), tick: (v: number) => fmtPct(v, 0) },
    volume: { label: "success volume", value: (i: number) => daily[i].successUsd, format: fmtUsd, tick: fmtUsdCompact },
  }[metric];
  const range = daily.length ? `${fmtDate(daily[0].date, { month: "short", day: "numeric" })} – ${fmtDate(daily[daily.length - 1].date, { month: "short", day: "numeric" })}` : "";
  return (
    <Card className="xl:col-span-2">
      <CardHeader
        title="Daily activity"
        subtitle={`Last ${daily.length} days · ${range}`}
        action={
          <PillTabs<Metric>
            size="sm"
            value={metric}
            onChange={setMetric}
            tabs={[{ value: "transactions", label: "Transactions" }, { value: "rate", label: "Success rate" }, { value: "volume", label: "Volume" }]}
          />
        }
      />
      <div className="px-3 pb-3 pt-4">
        <PillBarChart
          label={config.label}
          data={daily.map((d, i) => ({ date: d.date, value: config.value(i) }))}
          format={config.format}
          tickFormat={config.tick}
          details={(i) => [
            ...STATUS_SERIES.map((s) => ({ label: s.label, value: fmtInt(daily[i][s.key]), color: s.color })),
            { label: "success rate", value: fmtPct(daily[i].successRate) },
            { label: "success volume", value: fmtUsd(daily[i].successUsd) },
          ]}
        />
      </div>
    </Card>
  );
}

function WhatChanged({ summary, base }: { summary: HomeSummary; base: string }) {
  return (
    <Card className="flex flex-col">
      <CardHeader title="What changed" subtitle="Success rate on the latest day vs the 7 days before" />
      <ul className="flex-1 space-y-1 p-3">
        {summary.changes.map((c) => {
          const up = c.diff > 0;
          const param = c.kind === "merchant" ? "merchant" : c.kind;
          return (
            <li key={`${c.kind}:${c.name}`}>
              <Link href={`${base}/reports?${param}=${encodeURIComponent(c.name)}`} className="flex items-center gap-3 rounded-xl px-2.5 py-2.5 transition-colors hover:bg-surface-2">
                <span className={cn("flex size-8 shrink-0 items-center justify-center rounded-lg", up ? "bg-ok-dim text-ok" : "bg-bad-dim text-bad")}>
                  {up ? <ArrowUpRight className="size-4" /> : <ArrowDownRight className="size-4" />}
                </span>
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-[13.5px] text-ink">
                    {c.kind === "provider" ? <span>{c.name}</span> : c.name}
                  </span>
                  <span className="block text-[12px] text-faint">{label(c.kind)} · {fmtInt(c.latestCount)} txns</span>
                </span>
                <span className="text-right">
                  <span className={cn("tnum block text-[13px] font-medium", up ? "text-ok" : "text-bad")}>{up ? "+" : "−"}{Math.abs(c.diff * 100).toFixed(1)} pts</span>
                  <span className="tnum block text-[11.5px] text-faint">{fmtPct(c.baselineRate)} → {fmtPct(c.latestRate)}</span>
                </span>
              </Link>
            </li>
          );
        })}
        {!summary.changes.length && (
          <li className="flex h-full flex-col items-center justify-center gap-1 px-4 py-10 text-center">
            <span className="text-[13.5px] text-ink">Nothing unusual</span>
            <span className="text-[12.5px] text-muted">No provider, region or merchant moved more than 3 points.</span>
          </li>
        )}
      </ul>
    </Card>
  );
}

function FailureReasons({ summary }: { summary: HomeSummary }) {
  const failed = summary.latest.totals.status.failed ?? 0;
  return (
    <Card>
      <CardHeader title="Why payments failed" subtitle={`${fmtInt(failed)} failures on the latest day`} />
      <ul className="space-y-2 p-5">
        {summary.reasons.slice(0, 5).map((r) => {
          const segments = 12;
          const filled = Math.max(1, Math.round(r.share * segments));
          return (
            <li key={r.message} className="rounded-xl border border-line px-3.5 py-2.5">
              <div className="flex items-baseline justify-between gap-3">
                <span className="truncate text-[13px] text-ink" title={displayMessage(r.message)}>{displayMessage(r.message)}</span>
                <span className="tnum shrink-0 text-[13px] font-medium text-ink">{fmtInt(r.count)}</span>
              </div>
              <div className="mt-2 flex items-center gap-3">
                <div className="flex flex-1 gap-[3px]" aria-hidden>
                  {Array.from({ length: segments }, (_, i) => (
                    <span key={i} className="h-1 flex-1 rounded-full" style={{ background: i < filled ? "var(--chart-bad)" : "var(--surface-3)" }} />
                  ))}
                </div>
                <span className="tnum w-11 text-right text-[11.5px] text-muted">{fmtPct(r.share, 0)}</span>
              </div>
            </li>
          );
        })}
        {!summary.reasons.length && <li className="py-8 text-center text-[13px] text-muted">No failures on the latest day.</li>}
      </ul>
    </Card>
  );
}

function ProviderHealth({ summary, base }: { summary: HomeSummary; base: string }) {
  return (
    <Card>
      <CardHeader title="Provider health" subtitle="Latest day, by volume" />
      <ul className="p-3">
        {summary.providers.slice(0, 8).map((p) => {
          const bars = 14;
          return (
            <li key={p.name}>
              <Link href={`${base}/reports?provider=${encodeURIComponent(p.name)}`} className="flex items-center gap-3 rounded-xl px-2.5 py-2 transition-colors hover:bg-surface-2">
                <span className="w-[120px] truncate text-[13px] text-ink">{p.name}</span>
                <span className="flex flex-1 items-end gap-[2px]" aria-label={`${fmtPct(p.successRate)} success`}>
                  {Array.from({ length: bars }, (_, i) => {
                    const on = i < Math.round(p.successRate * bars);
                    return <span key={i} className="h-3.5 w-[3px] rounded-full" style={{ background: on ? "var(--chart-ok)" : "var(--surface-3)" }} />;
                  })}
                </span>
                <span className="tnum w-12 text-right text-[12.5px] font-medium text-ink">{fmtPct(p.successRate, 0)}</span>
                <span className="tnum w-12 text-right text-[12px] text-faint">{fmtCompact(p.total)}</span>
              </Link>
            </li>
          );
        })}
      </ul>
    </Card>
  );
}

const UPLOAD_STATUS = {
  complete: { tone: "successful", label: "Complete" },
  processing: { tone: "abandoned", label: "Processing" },
  failed: { tone: "failed", label: "Failed" },
} as const;

function RecentUploads({ uploads, base, demo }: { uploads: RecentUpload[]; base: string; demo: boolean }) {
  return (
    <Card className="flex flex-col">
      <CardHeader
        title="Recent uploads"
        subtitle="Latest exports processed"
        action={!demo && <Link href={`${base}/uploads`} className="text-[12.5px] text-accent hover:underline">View all</Link>}
      />
      <ul className="flex-1 space-y-2 p-5">
        {uploads.map((u) => (
          <li key={u.id}>
            <Link href={demo ? "/login" : `${base}/uploads/${u.id}`} className="flex items-center gap-3 rounded-xl border border-line px-3.5 py-2.5 transition-colors hover:bg-surface">
              <span className="min-w-0 flex-1 truncate text-[13px] text-ink">{u.fileName}</span>
              <span className="tnum hidden text-[12px] text-faint sm:inline">{fmtInt(u.transactions)} txns</span>
              <span className="tnum w-10 text-right text-[12px] text-faint">{u.durationMs ? `${(u.durationMs / 1000).toFixed(1)}s` : "—"}</span>
              <StatusPill status={UPLOAD_STATUS[u.status].tone}>{UPLOAD_STATUS[u.status].label}</StatusPill>
            </Link>
          </li>
        ))}
        {!uploads.length && <li className="py-8 text-center text-[13px] text-muted">No uploads yet.</li>}
      </ul>
    </Card>
  );
}

export function HomeView({ summary, uploads, base, demo = false }: {
  summary: HomeSummary; uploads: RecentUpload[]; base: string; demo?: boolean;
}) {
  const latest = summary.latestDate;
  const latestQuery = latest ? `?from=${latest}&to=${latest}` : "";
  return (
    <div className="space-y-6">
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <QuickAction index={0} primary href={demo ? "/login" : `${base}/upload`} icon={<Upload />} title="Upload export" body="Drop today's CSV. Parsed and checked before saving." />
        <QuickAction index={1} href={`${base}/reports${latestQuery}`} icon={<BarChart3 />} title="Open latest report" body={latest ? `Full breakdown for ${fmtDate(latest, { month: "short", day: "numeric" })}` : "Merchants, providers, regions"} />
        <QuickAction index={2} href={`${base}/reports`} icon={<CalendarRange />} title="Compare the week" body="Last 7 days by provider, region and merchant" />
        <QuickAction index={3} href={demo ? "/login" : `${base}/ask`} icon={<MessageSquareText />} title="Ask a question" body="“Why did Ghana failures jump?”" />
      </div>

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        {summary.kpis.map((k, i) => <KpiCard key={k.key} kpi={k} index={i} href={`${base}/reports${latestQuery}`} />)}
      </div>

      <div className="grid gap-4 xl:grid-cols-3">
        <DailyCard summary={summary} />
        <WhatChanged summary={summary} base={base} />
      </div>

      <div className="grid gap-4 lg:grid-cols-2 xl:grid-cols-3">
        <FailureReasons summary={summary} />
        <ProviderHealth summary={summary} base={base} />
        <RecentUploads uploads={uploads} base={base} demo={demo} />
      </div>
    </div>
  );
}
