"use client";

import { cn } from "@/lib/utils";

export interface TooltipRow {
  label: string;
  value: string;
  /** CSS colour for the line key; omitted for summary rows. */
  color?: string;
}

/** A chart readout. Values lead, labels follow; series keyed with a short line. */
export function ChartTooltip({ title, rows, x, y, width, className }: {
  title: string; rows: TooltipRow[]; x: number; y: number; width: number; className?: string;
}) {
  const flip = x > width - 200;
  return (
    <div
      role="status"
      className={cn(
        "pointer-events-none absolute z-20 min-w-[168px] rounded-xl border border-line bg-raised/95 px-3 py-2.5 text-[12px] shadow-xl backdrop-blur-md",
        className,
      )}
      style={{ left: flip ? undefined : x + 14, right: flip ? width - x + 14 : undefined, top: Math.max(0, y) }}
    >
      <div className="mb-1.5 text-[12px] font-medium text-ink">{title}</div>
      <div className="space-y-1">
        {rows.map((row) => (
          <div key={row.label} className="flex items-center gap-2.5">
            {row.color ? <span className="h-0.5 w-3 rounded-full" style={{ background: row.color }} /> : <span className="w-3" />}
            <span className="tnum font-semibold text-ink">{row.value}</span>
            <span className="text-muted">{row.label}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
