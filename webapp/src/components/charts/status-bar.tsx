"use client";

import { useState } from "react";
import { fmtInt, fmtPct } from "@/lib/format";
import { cn } from "@/lib/utils";
import { STATUS_SERIES } from "./series";

/** A 100% bar of the status mix with a 2px surface gap between segments. */
export function StatusBar({ status, className, showLegend = true, height = 10 }: {
  status: Record<string, number>; className?: string; showLegend?: boolean; height?: number;
}) {
  const [hover, setHover] = useState<string | null>(null);
  const total = STATUS_SERIES.reduce((s, x) => s + (status[x.key] ?? 0), 0);
  const parts = STATUS_SERIES.map((s) => ({ ...s, value: status[s.key] ?? 0 })).filter((s) => s.value > 0);
  return (
    <div className={cn("space-y-2.5", className)}>
      <div className="flex w-full gap-[2px] overflow-hidden rounded-full" style={{ height }}>
        {parts.map((p) => (
          <div
            key={p.key}
            title={`${p.label}: ${fmtInt(p.value)} (${fmtPct(p.value / total)})`}
            onMouseEnter={() => setHover(p.key)}
            onMouseLeave={() => setHover(null)}
            className="h-full transition-opacity duration-150"
            style={{ flexGrow: p.value, flexBasis: 0, minWidth: 3, background: p.color, opacity: hover && hover !== p.key ? 0.4 : 1 }}
          />
        ))}
      </div>
      {showLegend && (
        <ul className="flex flex-wrap gap-x-4 gap-y-1 text-[12px]">
          {parts.map((p) => (
            <li key={p.key} className={cn("flex items-center gap-1.5 transition-opacity", hover && hover !== p.key && "opacity-50")}>
              <span className="size-2 rounded-[3px]" style={{ background: p.color }} />
              <span className="text-muted">{p.label}</span>
              <span className="tnum font-medium text-ink">{fmtPct(p.value / total)}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
