"use client";

import { ArrowDown, ArrowUp } from "lucide-react";
import { useMemo, useState } from "react";
import { cn } from "@/lib/utils";

export interface Column<T> {
  key: string;
  header: string;
  numeric?: boolean;
  sortValue?: (row: T) => number | string;
  render: (row: T) => React.ReactNode;
  className?: string;
}

/** CRM-style table: quiet header, roomy rows, sortable columns. */
export function DataTable<T>({ rows, columns, rowKey, onRowClick, initialSort, empty = "No rows" }: {
  rows: T[];
  columns: Column<T>[];
  rowKey: (row: T) => string;
  onRowClick?: (row: T) => void;
  initialSort?: { key: string; dir: "asc" | "desc" };
  empty?: string;
}) {
  const [sort, setSort] = useState(initialSort ?? null);
  const sorted = useMemo(() => {
    if (!sort) return rows;
    const col = columns.find((c) => c.key === sort.key);
    if (!col?.sortValue) return rows;
    const dir = sort.dir === "asc" ? 1 : -1;
    return [...rows].sort((a, b) => {
      const va = col.sortValue!(a);
      const vb = col.sortValue!(b);
      return (va < vb ? -1 : va > vb ? 1 : 0) * dir;
    });
  }, [rows, columns, sort]);

  if (!rows.length) return <p className="px-5 py-12 text-center text-[13px] text-muted">{empty}</p>;

  return (
    <div className="overflow-x-auto scroll-quiet">
      <table className="w-full min-w-[760px] border-collapse text-[13.5px]">
        <thead>
          <tr>
            {columns.map((col) => {
              const active = sort?.key === col.key;
              return (
                <th key={col.key} scope="col" className={cn("h-11 border-b border-line px-4 text-[12.5px] font-normal text-faint first:pl-5 last:pr-5", col.numeric ? "text-right" : "text-left", col.className)}>
                  {col.sortValue ? (
                    <button
                      className={cn("inline-flex cursor-pointer items-center gap-1 hover:text-ink", active && "text-ink")}
                      onClick={() => setSort(active && sort?.dir === "desc" ? { key: col.key, dir: "asc" } : { key: col.key, dir: "desc" })}
                    >
                      {col.header}
                      {active && (sort?.dir === "desc" ? <ArrowDown className="size-3" /> : <ArrowUp className="size-3" />)}
                    </button>
                  ) : col.header}
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {sorted.map((row) => (
            <tr
              key={rowKey(row)}
              onClick={onRowClick ? () => onRowClick(row) : undefined}
              className={cn("border-b border-line-soft transition-colors last:border-0", onRowClick && "cursor-pointer hover:bg-surface")}
            >
              {columns.map((col) => (
                <td key={col.key} className={cn("h-[52px] px-4 first:pl-5 last:pr-5", col.numeric && "tnum text-right", col.className)}>
                  {col.render(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** Multicolour rate sparkbar (CRM "win probability" column). */
export function RateBars({ rate, bars = 16 }: { rate: number; bars?: number }) {
  const on = Math.round(rate * bars);
  const color = rate >= 0.5 ? "var(--chart-ok)" : rate >= 0.25 ? "var(--chart-warn)" : "var(--chart-bad)";
  return (
    <span className="inline-flex items-center gap-2.5">
      <span className="flex items-end gap-[2px]" aria-hidden>
        {Array.from({ length: bars }, (_, i) => (
          <span key={i} className="h-3.5 w-[3px] rounded-full" style={{ background: i < on ? color : "var(--surface-3)" }} />
        ))}
      </span>
      <span className="tnum w-11 text-right text-ink">{(rate * 100).toFixed(1)}%</span>
    </span>
  );
}

/** Tiny daily-volume bars (CRM "activity trend" column). */
export function TrendBars({ values }: { values?: number[] }) {
  if (!values || values.length < 2) return <span className="text-faint">—</span>;
  const tail = values.slice(-14);
  const max = Math.max(1, ...tail);
  return (
    <span className="inline-flex h-5 items-end gap-[2px]" aria-hidden>
      {tail.map((v, i) => (
        <span key={i} className="w-[3px] rounded-full" style={{ height: `${Math.max(12, (v / max) * 100)}%`, background: i === tail.length - 1 ? "var(--chart-accent)" : "color-mix(in oklab, var(--chart-accent) 35%, transparent)" }} />
      ))}
    </span>
  );
}
