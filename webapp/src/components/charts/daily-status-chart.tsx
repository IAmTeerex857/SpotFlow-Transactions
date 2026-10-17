"use client";

import { useState } from "react";
import { useWidth } from "@/hooks/use-size";
import { fmtCompact, fmtDate, fmtInt, fmtPct, fmtUsd } from "@/lib/format";
import type { DailyRow } from "@/lib/report/build";
import { Legend } from "./legend";
import { columnPath, niceMax, STATUS_SERIES } from "./series";
import { ChartTooltip } from "./tooltip";

const HEIGHT = 240;
const PAD = { top: 12, right: 8, bottom: 28, left: 44 };
const GAP = 2;

/** Daily transactions stacked by status. Each column is its own hover target. */
export function DailyStatusChart({ data }: { data: DailyRow[] }) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const [hover, setHover] = useState<number | null>(null);

  const plotW = width - PAD.left - PAD.right;
  const plotH = HEIGHT - PAD.top - PAD.bottom;
  const max = niceMax(Math.max(1, ...data.map((d) => d.successful + d.failed + d.cancelled + d.abandoned)));
  const band = plotW / Math.max(1, data.length);
  const barW = Math.max(3, Math.min(24, band * 0.62));
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((t) => t * max);
  const labelEvery = Math.ceil(data.length / Math.max(1, Math.floor(plotW / 64)));
  const y = (v: number) => PAD.top + plotH - (v / max) * plotH;

  const active = hover !== null ? data[hover] : null;

  return (
    <div className="space-y-3">
      <Legend items={STATUS_SERIES.map((s) => ({ label: s.label, color: s.color }))} />
      <div ref={ref} className="relative w-full overflow-hidden" onMouseLeave={() => setHover(null)}>
        <svg width={width} height={HEIGHT} role="img" aria-label="Daily transactions by status">
          {ticks.map((t) => (
            <g key={t}>
              <line x1={PAD.left} x2={width - PAD.right} y1={y(t)} y2={y(t)} stroke="var(--chart-grid)" />
              <text x={PAD.left - 8} y={y(t)} dy="0.32em" textAnchor="end" className="tnum fill-faint text-[11px]">
                {fmtCompact(t)}
              </text>
            </g>
          ))}
          {data.map((d, i) => {
            const cx = PAD.left + band * i + band / 2;
            const x = cx - barW / 2;
            let acc = 0;
            const segments = STATUS_SERIES.map((s) => ({ ...s, value: d[s.key] })).filter((s) => s.value > 0);
            return (
              <g key={d.date} opacity={hover === null || hover === i ? 1 : 0.45} className="transition-opacity duration-150">
                {segments.map((s, si) => {
                  const top = y(acc + s.value);
                  const bottom = y(acc);
                  acc += s.value;
                  const isTop = si === segments.length - 1;
                  const h = Math.max(0, bottom - top - (si > 0 ? GAP : 0));
                  return isTop ? (
                    <path key={s.key} d={columnPath(x, top, barW, h)} fill={s.color} />
                  ) : (
                    <rect key={s.key} x={x} y={top} width={barW} height={h} fill={s.color} />
                  );
                })}
                {i % labelEvery === 0 && (
                  <text x={cx} y={HEIGHT - 8} textAnchor="middle" className="tnum fill-faint text-[11px]">
                    {fmtDate(d.date, { month: "short", day: "numeric" })}
                  </text>
                )}
                <rect
                  x={PAD.left + band * i}
                  y={PAD.top}
                  width={band}
                  height={plotH}
                  fill="transparent"
                  onMouseEnter={() => setHover(i)}
                  onFocus={() => setHover(i)}
                  tabIndex={0}
                  aria-label={`${fmtDate(d.date)}: ${d.total} transactions`}
                />
              </g>
            );
          })}
        </svg>
        {active && hover !== null && (
          <ChartTooltip
            width={width}
            x={PAD.left + band * hover + band / 2}
            y={PAD.top}
            title={fmtDate(active.date, { weekday: "short", month: "short", day: "numeric" })}
            rows={[
              ...STATUS_SERIES.map((s) => ({ label: s.label, value: fmtInt(active[s.key]), color: s.color })),
              { label: "success rate", value: fmtPct(active.successRate) },
              { label: "success volume", value: fmtUsd(active.successUsd) },
            ]}
          />
        )}
      </div>
    </div>
  );
}
