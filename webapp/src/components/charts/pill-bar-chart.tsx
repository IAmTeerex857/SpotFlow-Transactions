"use client";

import { motion } from "motion/react";
import { useState } from "react";
import { useWidth } from "@/hooks/use-size";
import { fmtDate } from "@/lib/format";
import { niceMax } from "./series";
import { ChartTooltip, type TooltipRow } from "./tooltip";

const HEIGHT = 260;
const PAD = { top: 12, right: 4, bottom: 28, left: 44 };

/**
 * Rounded "pill" columns (Usage & Analytics reference). One series, one hue:
 * the hovered day is solid, the rest a soft tint of the same colour.
 */
export function PillBarChart({ data, format, tickFormat, label, details }: {
  data: { date: string; value: number }[];
  format: (v: number) => string;
  tickFormat?: (v: number) => string;
  label: string;
  details?: (index: number) => TooltipRow[];
}) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const [hover, setHover] = useState<number | null>(null);
  const plotW = width - PAD.left - PAD.right;
  const plotH = HEIGHT - PAD.top - PAD.bottom;
  const max = niceMax(Math.max(0, ...data.map((d) => d.value)) * 1.05 || 1);
  const band = plotW / Math.max(1, data.length);
  const barW = Math.max(4, Math.min(28, band * 0.62));
  const y = (v: number) => PAD.top + plotH - (v / max) * plotH;
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((t) => t * max);
  const labelEvery = Math.ceil(data.length / Math.max(1, Math.floor(plotW / 44)));
  const last = data.length - 1;

  return (
    <div ref={ref} className="relative w-full overflow-hidden" onMouseLeave={() => setHover(null)}>
      <svg width={width} height={HEIGHT} role="img" aria-label={label}>
        <defs>
          <linearGradient id="pill-soft" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="var(--chart-accent)" stopOpacity={0.34} />
            <stop offset="100%" stopColor="var(--chart-accent)" stopOpacity={0.12} />
          </linearGradient>
          <linearGradient id="pill-solid" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="var(--chart-accent)" stopOpacity={1} />
            <stop offset="100%" stopColor="var(--chart-accent)" stopOpacity={0.72} />
          </linearGradient>
        </defs>
        {ticks.map((t) => (
          <g key={t}>
            <line x1={PAD.left} x2={width - PAD.right} y1={y(t)} y2={y(t)} stroke="var(--chart-grid)" />
            <text x={PAD.left - 10} y={y(t)} dy="0.32em" textAnchor="end" className="tnum fill-faint text-[11px]">{(tickFormat ?? format)(t)}</text>
          </g>
        ))}
        {data.map((d, i) => {
          const cx = PAD.left + band * i + band / 2;
          const h = Math.max(barW, plotH - (y(d.value) - PAD.top));
          const emphasised = hover === null ? i === last : hover === i;
          return (
            <g key={d.date}>
              <motion.rect
                x={cx - barW / 2}
                width={barW}
                rx={barW / 2}
                initial={{ y: PAD.top + plotH - barW, height: barW }}
                animate={{ y: PAD.top + plotH - h, height: h }}
                transition={{ duration: 0.6, delay: i * 0.012, ease: [0.2, 0.7, 0.2, 1] }}
                fill={emphasised ? "url(#pill-solid)" : "url(#pill-soft)"}
              />
              {i % labelEvery === 0 && (
                <text x={cx} y={HEIGHT - 8} textAnchor="middle" className="tnum fill-faint text-[11px]">
                  {fmtDate(d.date, { day: "numeric" })}
                </text>
              )}
              <rect
                x={PAD.left + band * i} y={PAD.top} width={band} height={plotH} fill="transparent"
                onMouseEnter={() => setHover(i)} onFocus={() => setHover(i)} tabIndex={0}
                aria-label={`${fmtDate(d.date)}: ${format(d.value)}`}
              />
            </g>
          );
        })}
      </svg>
      {hover !== null && (
        <ChartTooltip
          width={width}
          x={PAD.left + band * hover + band / 2}
          y={PAD.top}
          title={fmtDate(data[hover].date, { weekday: "short", month: "short", day: "numeric" })}
          rows={details ? details(hover) : [{ label, value: format(data[hover].value), color: "var(--chart-accent)" }]}
        />
      )}
    </div>
  );
}
