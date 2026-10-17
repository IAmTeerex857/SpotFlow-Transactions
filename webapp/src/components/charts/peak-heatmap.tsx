"use client";

import { useState } from "react";
import { fmtInt, fmtPct } from "@/lib/format";
import type { PeakCell } from "@/lib/report/build";

const DAYS = [
  { dow: 1, label: "Mon" }, { dow: 2, label: "Tue" }, { dow: 3, label: "Wed" }, { dow: 4, label: "Thu" },
  { dow: 5, label: "Fri" }, { dow: 6, label: "Sat" }, { dow: 0, label: "Sun" },
];

/** Weekday × hour (UTC) volume. Sequential: one hue, light → dark. */
export function PeakHeatmap({ grid }: { grid: PeakCell[] }) {
  const [hover, setHover] = useState<PeakCell | null>(null);
  const lookup = new Map(grid.map((c) => [`${c.dow}:${c.hour}`, c]));
  const max = Math.max(1, ...grid.map((c) => c.total));
  return (
    <div className="space-y-3">
      <div className="overflow-x-auto scroll-quiet">
        <div className="grid min-w-[560px] gap-[3px]" style={{ gridTemplateColumns: "36px repeat(24, minmax(0, 1fr))" }}>
          {DAYS.map((d) => (
            <div key={d.dow} className="contents">
              <div className="flex items-center text-[11px] text-faint">{d.label}</div>
              {Array.from({ length: 24 }, (_, h) => {
                const cell = lookup.get(`${d.dow}:${h}`) ?? { dow: d.dow, hour: h, total: 0, successful: 0 };
                const t = cell.total / max;
                return (
                  <button
                    key={h}
                    onMouseEnter={() => setHover(cell)}
                    onFocus={() => setHover(cell)}
                    onMouseLeave={() => setHover(null)}
                    aria-label={`${d.label} ${h}:00 UTC, ${cell.total} transactions`}
                    className="aspect-square rounded-[4px] outline-offset-1 transition-transform hover:scale-110"
                    style={{ background: t ? `color-mix(in oklab, var(--chart-accent) ${Math.round(12 + t * 88)}%, var(--bg-raised))` : "var(--surface-2)" }}
                  />
                );
              })}
            </div>
          ))}
          <div />
          {Array.from({ length: 24 }, (_, h) => (
            <div key={h} className="text-center tnum text-[10.5px] text-faint">{h % 3 === 0 ? String(h).padStart(2, "0") : ""}</div>
          ))}
        </div>
      </div>
      <div className="flex h-5 items-center justify-between text-[12px] text-muted">
        {hover ? (
          <span>
            <span className="font-medium text-ink">{DAYS.find((d) => d.dow === hover.dow)?.label} {String(hover.hour).padStart(2, "0")}:00 UTC</span>
            {" · "}<span className="tnum text-ink">{fmtInt(hover.total)}</span> transactions
            {" · "}<span className="tnum text-ink">{fmtPct(hover.total ? hover.successful / hover.total : 0)}</span> successful
          </span>
        ) : <span>Hover a cell for detail. Hours are UTC.</span>}
        <span className="flex items-center gap-1.5 text-[11px] text-faint">
          less
          {[0.15, 0.4, 0.65, 0.9].map((t) => (
            <span key={t} className="size-2.5 rounded-[3px]" style={{ background: `color-mix(in oklab, var(--chart-accent) ${Math.round(12 + t * 88)}%, var(--bg-raised))` }} />
          ))}
          more
        </span>
      </div>
    </div>
  );
}
