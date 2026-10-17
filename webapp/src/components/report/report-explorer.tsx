"use client";

import { BorderBeam } from "border-beam";
import { X } from "lucide-react";
import { useSyncExternalStore } from "react";
import { ReportView, type ReportTab } from "@/components/report/report-view";
import { UnderlineTabs } from "@/components/ui/pill-tabs";
import { useTheme } from "@/components/ui/theme-toggle";
import { fmtInt, fmtPct, fmtRange, fmtUsd, fmtUsdCompact, label } from "@/lib/format";
import type { Report } from "@/lib/report/build";
import type { FilterOptions } from "@/lib/report/filters";
import type { ReportFilters } from "@/lib/report/types";
import { cn } from "@/lib/utils";
import { PageHeader } from "@/components/shell/page-header";
import { FilterSelect } from "./filter-select";
import { RangeSelect } from "./range-select";

const TABS: ReportTab[] = ["overview", "merchants", "providers", "regions", "failures", "retries", "peaks"];

// The open tab lives in the URL hash (#merchants), so views can be linked and ⌘K can jump to them.
function subscribeTab(callback: () => void) {
  window.addEventListener("hashchange", callback);
  window.addEventListener("sf:tab", callback);
  return () => {
    window.removeEventListener("hashchange", callback);
    window.removeEventListener("sf:tab", callback);
  };
}
const readTab = (): ReportTab => {
  const hash = window.location.hash.slice(1) as ReportTab;
  return TABS.includes(hash) ? hash : "overview";
};
function writeTab(next: ReportTab) {
  const { pathname, search } = window.location;
  window.history.replaceState(window.history.state, "", next === "overview" ? `${pathname}${search}` : `${pathname}${search}#${next}`);
  window.dispatchEvent(new Event("sf:tab"));
}

const merchantLabel = (m: string) => (m === "<unknown>" ? "Unattributed" : m);
const usd = (v: number) => (v >= 1_000_000 ? fmtUsdCompact(v) : fmtUsd(v));

export function ReportExplorer({ report, filters, options, onChange, pending = false, title = "Reports", crumb = "Reports", actions, banner, askBase }: {
  report: Report;
  filters: ReportFilters;
  options: FilterOptions;
  onChange: (next: ReportFilters) => void;
  pending?: boolean;
  title?: string;
  crumb?: string;
  actions?: React.ReactNode;
  banner?: React.ReactNode;
  askBase?: string;
}) {
  const theme = useTheme();
  const tab = useSyncExternalStore(subscribeTab, readTab, () => "overview" as const);
  const setTab = writeTab;
  const set = (patch: Partial<ReportFilters>) => onChange({ ...filters, ...patch });
  const active = (filters.merchants?.length ?? 0) + (filters.providers?.length ?? 0) + (filters.regions?.length ?? 0) + (filters.channels?.length ?? 0);
  const t = report.totals;

  return (
    <div className="space-y-6 pb-16">
      <PageHeader crumb={crumb} title={title} description={fmtRange(report.range.from ?? filters.from, report.range.to ?? filters.to)} actions={actions} />
      {banner}

      <UnderlineTabs<ReportTab>
        value={tab}
        onChange={setTab}
        tabs={[
          { value: "overview", label: "Overview" },
          { value: "merchants", label: "Merchants", count: report.merchants.length },
          { value: "providers", label: "Providers", count: report.providers.length },
          { value: "regions", label: "Regions", count: report.regions.length },
          { value: "failures", label: "Failures" },
          { value: "retries", label: "Retries" },
          { value: "peaks", label: "Peak times" },
        ]}
      />

      <div className="flex flex-wrap items-center gap-2">
        <RangeSelect from={filters.from} to={filters.to} options={options} onChange={(r) => set(r)} />
        <FilterSelect label="Merchant" options={options.merchants} selected={filters.merchants ?? []} onChange={(merchants) => set({ merchants })} format={merchantLabel} />
        <FilterSelect label="Provider" options={options.providers} selected={filters.providers ?? []} onChange={(providers) => set({ providers })} />
        <FilterSelect label="Region" options={options.regions} selected={filters.regions ?? []} onChange={(regions) => set({ regions })} />
        <FilterSelect label="Channel" options={options.channels} selected={filters.channels ?? []} onChange={(channels) => set({ channels })} format={label} />
        {(active > 0 || filters.uploadId) && (
          <button onClick={() => set({ merchants: [], providers: [], regions: [], channels: [], uploadId: null })} className="flex h-8 cursor-pointer items-center gap-1.5 rounded-[10px] px-2.5 text-[13px] text-muted hover:bg-surface-2 hover:text-ink">
            <X className="size-3.5" /> Clear
          </button>
        )}
      </div>

      <div className={cn("relative transition-opacity duration-200", pending && "opacity-60")} aria-busy={pending}>
        {pending && (
          <div className="pointer-events-none absolute inset-x-0 -top-3 z-10">
            <BorderBeam size="line" colorVariant="ocean" theme={theme} active>
              <div className="h-px w-full" />
            </BorderBeam>
          </div>
        )}
        <ReportView
          report={report}
          tab={tab}
          askBase={askBase}
          onDrill={(field, value) => set({ [field]: Array.from(new Set([...(filters[field] ?? []), value])) })}
        />
      </div>

      {/* Totals for everything in view (CRM footer calculations). */}
      <div className="sticky bottom-3 z-20 flex flex-wrap items-center gap-x-6 gap-y-1 rounded-xl bg-raised/90 px-4 py-2.5 text-[12.5px] shadow-[var(--shadow-pop)] backdrop-blur-xl">
        <span><span className="tnum font-medium text-ink">{fmtInt(t.total)}</span> <span className="text-faint">transactions in view</span></span>
        <span><span className="text-faint">Success rate</span> <span className="tnum font-medium text-ink">{fmtPct(t.successRate)}</span></span>
        <span><span className="text-faint">Success volume</span> <span className="tnum font-medium text-ink">{usd(t.usdByStatus.successful ?? 0)}</span></span>
        <span><span className="text-faint">Failed volume</span> <span className="tnum font-medium text-bad">{usd(t.usdByStatus.failed ?? 0)}</span></span>
        {active > 0 && <span className="ml-auto text-faint">{active} filter{active > 1 ? "s" : ""} applied</span>}
      </div>
    </div>
  );
}
