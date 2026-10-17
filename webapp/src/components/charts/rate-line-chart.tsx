"use client";

import { useState } from "react";
import { useWidth } from "@/hooks/use-size";
import { fmtDate, fmtPct } from "@/lib/format";
import { ChartTooltip } from "./tooltip";

const HEIGHT = 248;
const PAD = { top: 16, right: 52, bottom: 26, left: 44 };

/** A single ratio over time, with a snapping crosshair and an end label. */
export function RateLineChart({ data, label, color = "var(--chart-ok)" }: {
  data: { date: string; value: number; detail?: string }[];
  label: string;
  color?: string;
}) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const [hover, setHover] = useState<number | null>(null);
  if (!data.length) return <div ref={ref} className="h-[248px]" />;

  const plotW = width - PAD.left - PAD.right;
  const plotH = HEIGHT - PAD.top - PAD.bottom;
  const hi = Math.max(...data.map((d) => d.value));
  const lo = Math.min(...data.map((d) => d.value));
  const step = [0.01, 0.02, 0.05, 0.1, 0.2, 0.25].find((s) => (hi - lo + 0.04) / s <= 5) ?? 0.25;
  const min = Math.max(0, Math.floor((lo - 0.02) / step) * step);
  const max = Math.min(1, Math.max(min + step, Math.ceil((hi + 0.02) / step) * step));
  const x = (i: number) => PAD.left + (data.length === 1 ? plotW / 2 : (i / (data.length - 1)) * plotW);
  const y = (v: number) => PAD.top + plotH - ((v - min) / (max - min || 1)) * plotH;
  const ticks = Array.from({ length: Math.round((max - min) / step) + 1 }, (_, i) => min + i * step);
  const line = data.map((d, i) => `${i ? "L" : "M"}${x(i)},${y(d.value)}`).join("");
  const area = `${line}L${x(data.length - 1)},${y(min)}L${x(0)},${y(min)}Z`;
  const last = data[data.length - 1];
  const labelEvery = Math.ceil(data.length / Math.max(1, Math.floor(plotW / 64)));

  const onMove = (event: React.MouseEvent<SVGSVGElement>) => {
    const rect = event.currentTarget.getBoundingClientRect();
    const px = event.clientX - rect.left - PAD.left;
    const i = Math.round((px / plotW) * (data.length - 1));
    setHover(Math.min(data.length - 1, Math.max(0, i)));
  };

  return (
    <div ref={ref} className="relative w-full overflow-hidden">
      <svg width={width} height={HEIGHT} onMouseMove={onMove} onMouseLeave={() => setHover(null)} role="img" aria-label={label}>
        {ticks.map((t) => (
          <g key={t}>
            <line x1={PAD.left} x2={width - PAD.right} y1={y(t)} y2={y(t)} stroke="var(--chart-grid)" />
            <text x={PAD.left - 8} y={y(t)} dy="0.32em" textAnchor="end" className="tnum fill-faint text-[11px]">
              {fmtPct(t, 0)}
            </text>
          </g>
        ))}
        <path d={area} fill={color} opacity={0.1} />
        <path d={line} fill="none" stroke={color} strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />
        {data.map((d, i) => i % labelEvery === 0 && (
          <text key={d.date} x={x(i)} y={HEIGHT - 6} textAnchor="middle" className="tnum fill-faint text-[11px]">
            {fmtDate(d.date, { month: "short", day: "numeric" })}
          </text>
        ))}
        <circle cx={x(data.length - 1)} cy={y(last.value)} r={4} fill={color} stroke="var(--bg-raised)" strokeWidth={2} />
        <text x={x(data.length - 1) + 8} y={y(last.value)} dy="0.32em" className="tnum fill-ink text-[11px] font-semibold">
          {fmtPct(last.value)}
        </text>
        {hover !== null && (
          <g>
            <line x1={x(hover)} x2={x(hover)} y1={PAD.top} y2={PAD.top + plotH} stroke="var(--line)" />
            <circle cx={x(hover)} cy={y(data[hover].value)} r={4.5} fill={color} stroke="var(--bg-raised)" strokeWidth={2} />
          </g>
        )}
      </svg>
      {hover !== null && (
        <ChartTooltip
          width={width}
          x={x(hover)}
          y={PAD.top}
          title={fmtDate(data[hover].date, { weekday: "short", month: "short", day: "numeric" })}
          rows={[
            { label, value: fmtPct(data[hover].value), color },
            ...(data[hover].detail ? [{ label: data[hover].detail!, value: "" }] : []),
          ]}
        />
      )}
    </div>
  );
}
