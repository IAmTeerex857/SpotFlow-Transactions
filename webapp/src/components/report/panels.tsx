"use client";

import { Filter, MessageSquareText } from "lucide-react";
import { useState } from "react";
import { BarList } from "@/components/charts/bar-list";
import { DailyStatusChart } from "@/components/charts/daily-status-chart";
import { PeakHeatmap } from "@/components/charts/peak-heatmap";
import { RateLineChart } from "@/components/charts/rate-line-chart";
import { StatusBar } from "@/components/charts/status-bar";
import { STATUS_SERIES } from "@/components/charts/series";
import { Card, CardHeader } from "@/components/ui/card";
import { MorphButton } from "@/components/ui/morph-button";
import { displayMessage } from "@/lib/parser/normalize";
import { fmtDuration, fmtInt, fmtPct, fmtUsd, fmtUsdCompact, label } from "@/lib/format";
import type { MessageCount, MerchantRow, ProviderRow, RegionRow, Report, StatusBlock } from "@/lib/report/build";
import { DataTable, RateBars, TrendBars, type Column } from "./data-table";
import { DetailDrawer } from "./detail-drawer";

export type Drill = (field: "merchants" | "providers" | "regions", value: string) => void;

const merchantName = (name: string) => (name === "<unknown>" ? "Unattributed" : name);
const usd = (v: number) => (v >= 1_000_000 ? fmtUsdCompact(v) : fmtUsd(v));

// ── Overview ───────────────────────────────────────────────────────────────
function Sparkline({ values, color }: { values: number[]; color: string }) {
  if (values.length < 2) return null;
  const w = 88;
  const h = 32;
  const max = Math.max(...values);
  const min = Math.min(...values);
  const pts = values.map((v, i) => [(i / (values.length - 1)) * w, h - 3 - ((v - min) / (max - min || 1)) * (h - 6)]);
  const line = pts.map(([x, y], i) => `${i ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`).join("");
  return (
    <svg width={w} height={h} aria-hidden className="overflow-visible">
      <path d={`${line}L${w},${h}L0,${h}Z`} fill={color} opacity={0.1} />
      <path d={line} fill="none" stroke={color} strokeWidth={1.75} strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}

function Stat({ label: name, value, sub, trend, color }: { label: string; value: string; sub: string; trend: number[]; color: string }) {
  return (
    <Card className="p-5">
      <div className="text-[13px] text-muted">{name}</div>
      <div className="mt-3 flex items-end justify-between gap-3">
        <div className="text-[26px] font-semibold leading-none tracking-[-0.03em]">{value}</div>
        <Sparkline values={trend} color={color} />
      </div>
      <div className="mt-3 text-[12px] text-faint">{sub}</div>
    </Card>
  );
}

export function OverviewPanel({ report }: { report: Report }) {
  const t = report.totals;
  const d = report.daily;
  const failed = t.status.failed ?? 0;
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <Stat label="Success rate" value={fmtPct(t.successRate)} sub={`${fmtInt(t.status.successful ?? 0)} of ${fmtInt(t.total)} transactions`} trend={d.map((x) => x.successRate)} color="var(--chart-ok)" />
        <Stat label="Transactions" value={fmtInt(t.total)} sub={`${report.range.days} day${report.range.days === 1 ? "" : "s"}`} trend={d.map((x) => x.total)} color="var(--chart-accent)" />
        <Stat label="Success volume" value={usd(t.usdByStatus.successful ?? 0)} sub="USD equivalent, successful only" trend={d.map((x) => x.successUsd)} color="var(--chart-cancel)" />
        <Stat label="Failed volume" value={usd(t.usdByStatus.failed ?? 0)} sub={`${fmtInt(failed)} failed · ${usd(t.usdByStatus.abandoned ?? 0)} abandoned`} trend={d.map((x) => x.failedUsd)} color="var(--chart-bad)" />
      </div>
      <div className="grid gap-4 xl:grid-cols-3">
        <Card className="xl:col-span-2">
          <CardHeader title="Transactions by day" subtitle="Stacked by status · hover a day for detail" />
          <div className="p-5 pt-4"><DailyStatusChart data={d} /></div>
        </Card>
        <Card>
          <CardHeader title="Status and channels" />
          <div className="space-y-6 p-5">
            <StatusBar status={t.status} height={10} />
            <BarList items={Object.entries(t.channels).sort(([, a], [, b]) => b - a).map(([c, n]) => ({ key: c, label: label(c), value: n, display: `${fmtInt(n)} · ${fmtPct(n / t.total)}` }))} />
          </div>
        </Card>
      </div>
      <div className="grid gap-4 xl:grid-cols-2">
        <Card>
          <CardHeader title="Success rate by day" subtitle="Successful ÷ all transactions" />
          <div className="p-5 pt-4">
            <RateLineChart label="success rate" data={d.map((x) => ({ date: x.date, value: x.successRate, detail: `${fmtInt(x.successful)} of ${fmtInt(x.total)}` }))} />
          </div>
        </Card>
        <Card>
          <CardHeader title="Top failure reasons" subtitle={`Share of ${fmtInt(failed)} failed transactions`} />
          <div className="p-3 pt-2"><Reasons messages={report.messages.failed ?? []} total={failed} limit={5} /></div>
        </Card>
      </div>
    </div>
  );
}

function Reasons({ messages, total, limit = 6, tone = "var(--chart-bad)" }: { messages: MessageCount[]; total: number; limit?: number; tone?: string }) {
  const [all, setAll] = useState(false);
  if (!messages.length) return <p className="px-2.5 py-6 text-[13px] text-faint">None recorded.</p>;
  const shown = all ? messages : messages.slice(0, limit);
  return (
    <div>
      <BarList color={tone} items={shown.map((m) => ({ key: m.message, label: displayMessage(m.message), value: m.count, display: `${fmtInt(m.count)} · ${fmtPct(m.count / (total || 1))}` }))} />
      {messages.length > limit && (
        <button onClick={() => setAll((v) => !v)} className="mt-1 cursor-pointer px-2.5 py-1 text-[12.5px] text-accent hover:underline">
          {all ? "Show fewer" : `Show all ${messages.length}`}
        </button>
      )}
    </div>
  );
}

// ── Entity tables + drawer ─────────────────────────────────────────────────
type Entity =
  | { kind: "merchant"; row: MerchantRow }
  | { kind: "provider"; row: ProviderRow }
  | { kind: "region"; row: RegionRow };

function MiniStats({ row }: { row: StatusBlock }) {
  const items = [
    ["Transactions", fmtInt(row.total)],
    ["Success rate", fmtPct(row.successRate)],
    ["Success volume", usd(row.successUsd)],
    ["Failed volume", usd(row.failedUsd)],
  ];
  return (
    <div className="grid grid-cols-2 gap-2.5">
      {items.map(([k, v]) => (
        <div key={k} className="rounded-xl border border-line px-3.5 py-3">
          <div className="text-[12px] text-faint">{k}</div>
          <div className="mt-1 text-[17px] font-semibold tracking-[-0.02em]">{v}</div>
        </div>
      ))}
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section>
      <h3 className="mb-2.5 text-[12.5px] font-medium text-muted">{title}</h3>
      {children}
    </section>
  );
}

function EntityDrawer({ entity, onClose, onDrill, askBase }: { entity: Entity | null; onClose: () => void; onDrill?: Drill; askBase?: string }) {
  const row = entity?.row;
  const name = entity ? (entity.kind === "merchant" ? merchantName(entity.row.name) : entity.row.name) : "";
  const field = entity ? (`${entity.kind}s` as "merchants" | "providers" | "regions") : "merchants";
  return (
    <DetailDrawer
      open={!!entity}
      onClose={onClose}
      eyebrow={entity ? label(entity.kind) : undefined}
      title={name}
      actions={entity && (
        <>
          {askBase && <MorphButton size="sm" icon={<MessageSquareText />} href={`${askBase}?q=${encodeURIComponent(`Why is ${name} failing?`)}`}>Ask about {entity.kind}</MorphButton>}
          {onDrill && <MorphButton size="sm" variant="primary" icon={<Filter />} onClick={() => { onDrill(field, entity.row.name); onClose(); }}>Filter report</MorphButton>}
        </>
      )}
    >
      {row && entity && (
        <>
          <MiniStats row={row} />
          <Section title="Status mix"><StatusBar status={row.status} height={10} /></Section>
          {"messages" in row && (
            <Section title="Failure reasons"><Reasons messages={row.messages.failed ?? []} total={row.status.failed ?? 0} /></Section>
          )}
          <Section title="Channels">
            <BarList items={Object.entries(row.channels).sort(([, a], [, b]) => b - a).map(([c, n]) => ({ key: c, label: label(c), value: n, display: `${fmtInt(n)} · ${fmtPct(n / (row.total || 1))}` }))} />
          </Section>
          {entity.kind === "region" && (
            <>
              <Section title="Providers">
                <BarList color="var(--chart-ok)" items={entity.row.providers.map((p) => ({ key: p.name, label: p.name, value: p.successRate, display: `${fmtPct(p.successRate)} ok · ${fmtInt(p.total)}` }))} max={1} />
              </Section>
              <Section title="Merchants">
                <BarList items={entity.row.merchants.map((m) => ({ key: m.name, label: merchantName(m.name), value: m.successUsd, display: usd(m.successUsd), sub: `${fmtInt(m.total)} txns · ${fmtPct(m.successRate)} success` }))} />
              </Section>
            </>
          )}
          {entity.kind === "merchant" && (
            <p className="text-[12.5px] text-faint">Top region: {entity.row.topRegion ?? "—"} · Top channel: {entity.row.topChannel ? label(entity.row.topChannel) : "—"}</p>
          )}
        </>
      )}
    </DetailDrawer>
  );
}

export function EntityPanel({ report, kind, onDrill, askBase }: { report: Report; kind: Entity["kind"]; onDrill?: Drill; askBase?: string }) {
  const [open, setOpen] = useState<Entity | null>(null);
  const common = {
    total: { key: "total", header: "Transactions", numeric: true, sortValue: (r: StatusBlock) => r.total, render: (r: StatusBlock) => fmtInt(r.total) },
    rate: { key: "rate", header: "Success rate", sortValue: (r: StatusBlock) => r.successRate, render: (r: StatusBlock) => <RateBars rate={r.successRate} /> },
    vol: { key: "vol", header: "Success volume", numeric: true, sortValue: (r: StatusBlock) => r.successUsd, render: (r: StatusBlock) => <span className="font-medium">{usd(r.successUsd)}</span> },
    fvol: { key: "fvol", header: "Failed volume", numeric: true, sortValue: (r: StatusBlock) => r.failedUsd, render: (r: StatusBlock) => <span className="text-bad">{usd(r.failedUsd)}</span> },
    trend: { key: "trend", header: "Activity", render: (r: StatusBlock) => <TrendBars values={r.trend} /> },
  };

  if (kind === "merchant") {
    const columns: Column<MerchantRow>[] = [
      { key: "name", header: "Merchant", sortValue: (r) => r.name, render: (r) => <span className="font-medium">{merchantName(r.name)}</span> },
      common.total, common.rate, common.vol, common.fvol,
      { key: "region", header: "Top region", render: (r) => <span className="text-muted">{r.topRegion ?? "—"}</span> },
      common.trend,
    ];
    return (
      <Card>
        <DataTable rows={report.merchants} columns={columns} rowKey={(r) => r.name} onRowClick={(row) => setOpen({ kind, row })} initialSort={{ key: "vol", dir: "desc" }} />
        <EntityDrawer entity={open} onClose={() => setOpen(null)} onDrill={onDrill} askBase={askBase} />
      </Card>
    );
  }
  if (kind === "provider") {
    const columns: Column<ProviderRow>[] = [
      { key: "name", header: "Provider", sortValue: (r) => r.name, render: (r) => r.name },
      common.total, common.rate, common.vol,
      { key: "reason", header: "Top failure reason", render: (r) => <span className="block max-w-[280px] truncate text-muted" title={r.messages.failed?.[0] ? displayMessage(r.messages.failed[0].message) : ""}>{r.messages.failed?.[0] ? displayMessage(r.messages.failed[0].message) : "—"}</span> },
      common.trend,
    ];
    return (
      <Card>
        <DataTable rows={report.providers} columns={columns} rowKey={(r) => r.name} onRowClick={(row) => setOpen({ kind, row })} initialSort={{ key: "total", dir: "desc" }} />
        <EntityDrawer entity={open} onClose={() => setOpen(null)} onDrill={onDrill} askBase={askBase} />
      </Card>
    );
  }
  const columns: Column<RegionRow>[] = [
    { key: "name", header: "Region", sortValue: (r) => r.name, render: (r) => <span className="font-medium">{r.name}</span> },
    common.total, common.rate, common.vol, common.fvol,
    { key: "mix", header: "Status mix", render: (r) => <StatusBar status={r.status} showLegend={false} height={6} className="w-28" /> },
    common.trend,
  ];
  return (
    <Card>
      <DataTable rows={report.regions} columns={columns} rowKey={(r) => r.name} onRowClick={(row) => setOpen({ kind, row })} initialSort={{ key: "total", dir: "desc" }} />
      <EntityDrawer entity={open} onClose={() => setOpen(null)} onDrill={onDrill} askBase={askBase} />
    </Card>
  );
}

// ── Failures, retries, peaks ───────────────────────────────────────────────
export function FailuresPanel({ report }: { report: Report }) {
  const failed = report.totals.status.failed ?? 0;
  const abandoned = report.totals.status.abandoned ?? 0;
  return (
    <div className="space-y-4">
      <div className="grid gap-4 xl:grid-cols-2">
        <Card>
          <CardHeader title="Failure reasons" subtitle={`${fmtInt(failed)} failed transactions, grouped by normalised provider message`} />
          <div className="p-3 pt-2"><Reasons messages={report.messages.failed ?? []} total={failed} limit={10} /></div>
        </Card>
        <Card>
          <CardHeader title="Abandonment reasons" subtitle={`${fmtInt(abandoned)} abandoned transactions`} />
          <div className="p-3 pt-2"><Reasons messages={report.messages.abandoned ?? []} total={abandoned} tone="var(--chart-warn)" /></div>
        </Card>
      </div>
      <Card>
        <CardHeader title="Failure causes by region" subtitle="Each region's top reasons as a share of its failures" />
        <div className="grid gap-x-8 gap-y-6 p-5 md:grid-cols-2 xl:grid-cols-3">
          {report.regions.filter((r) => r.status.failed).map((r) => {
            const rf = r.status.failed ?? 0;
            return (
              <div key={r.name}>
                <div className="mb-2 flex items-baseline justify-between border-b border-line pb-2">
                  <h4 className="text-[13.5px] font-medium">{r.name}</h4>
                  <span className="tnum text-[12px] text-faint">{fmtInt(rf)} failed</span>
                </div>
                <ul className="space-y-1.5 text-[12.5px]">
                  {(r.messages.failed ?? []).slice(0, 4).map((m) => (
                    <li key={m.message} className="flex items-baseline justify-between gap-3">
                      <span className="truncate text-muted" title={displayMessage(m.message)}>{displayMessage(m.message)}</span>
                      <span className="tnum shrink-0 text-ink">{fmtPct(m.count / rf)}</span>
                    </li>
                  ))}
                </ul>
              </div>
            );
          })}
        </div>
      </Card>
    </div>
  );
}

export function RetriesPanel({ report }: { report: Report }) {
  const r = report.retry;
  if (!r.retriers) return <Card className="p-12 text-center text-[13px] text-muted">No customer made more than one attempt in this range.</Card>;
  const stats = [
    { label: "Customers who retried", value: fmtInt(r.retriers), sub: "More than one attempt in range" },
    { label: "Completed after retrying", value: fmtPct(r.succeededAfter / r.retriers), sub: `${fmtInt(r.succeededAfter)} customers` },
    { label: "Still unresolved", value: fmtPct(r.unresolved / r.retriers), sub: `${fmtInt(r.unresolved)} customers` },
    { label: "Median gap", value: fmtDuration(r.gapMedianSeconds), sub: "Between attempts" },
  ];
  const attempts = Object.entries(r.attempts).map(([k, v]) => [Number(k), v] as const).sort(([a], [b]) => a - b);
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
        {stats.map((s) => (
          <Card key={s.label} className="p-5">
            <div className="text-[13px] text-muted">{s.label}</div>
            <div className="mt-3 text-[26px] font-semibold leading-none tracking-[-0.03em]">{s.value}</div>
            <div className="mt-3 text-[12px] text-faint">{s.sub}</div>
          </Card>
        ))}
      </div>
      <div className="grid gap-4 xl:grid-cols-2">
        <Card>
          <CardHeader title="Retry depth" subtitle="Customers by number of attempts" />
          <div className="p-3 pt-2"><BarList items={attempts.map(([n, c]) => ({ key: String(n), label: `${n} attempts`, value: c, display: fmtInt(c) }))} /></div>
        </Card>
        <Card>
          <CardHeader title="Outcome of the latest attempt" subtitle="Each retrier's most recent attempt, by payment time" />
          <div className="space-y-5 p-5">
            <StatusBar status={r.finalStatus} height={10} />
            <ul className="space-y-2 text-[13px]">
              {STATUS_SERIES.filter((s) => r.finalStatus[s.key]).map((s) => (
                <li key={s.key} className="flex justify-between border-b border-line-soft pb-2 last:border-0">
                  <span className="flex items-center gap-2 text-muted"><span className="size-2 rounded-[3px]" style={{ background: s.color }} />{s.label}</span>
                  <span className="tnum">{fmtInt(r.finalStatus[s.key])}</span>
                </li>
              ))}
            </ul>
          </div>
        </Card>
      </div>
      {r.legacyShare > 0 && (
        <p className="text-[12.5px] text-faint">{fmtPct(r.legacyShare, 0)} of these retriers come from historical exports, whose pseudonyms were assigned per day, so their retries are only linked within a single day.</p>
      )}
    </div>
  );
}

export function PeaksPanel({ report }: { report: Report }) {
  const { byDay, byHour, grid } = report.peaks;
  const topHours = [...byHour].sort((a, b) => b.successUsd - a.successUsd).slice(0, 6);
  return (
    <div className="space-y-4">
      <Card>
        <CardHeader title="When payments happen" subtitle="Transactions by weekday and hour, UTC" />
        <div className="p-5"><PeakHeatmap grid={grid} /></div>
      </Card>
      <div className="grid gap-4 xl:grid-cols-2">
        <Card>
          <CardHeader title="Success volume by weekday" />
          <div className="p-3 pt-2">
            <BarList color="var(--chart-ok)" items={byDay.map((d) => ({ key: d.label, label: d.label, value: d.successUsd, display: usd(d.successUsd), sub: `${fmtInt(d.total)} transactions · ${fmtPct(d.total ? d.successful / d.total : 0)} success` }))} />
          </div>
        </Card>
        <Card>
          <CardHeader title="Top hours by success volume" subtitle="UTC" />
          <div className="p-3 pt-2">
            <BarList color="var(--chart-ok)" items={topHours.map((h) => ({ key: h.label, label: h.label, value: h.successUsd, display: usd(h.successUsd), sub: `${fmtInt(h.total)} transactions` }))} />
          </div>
        </Card>
      </div>
    </div>
  );
}
