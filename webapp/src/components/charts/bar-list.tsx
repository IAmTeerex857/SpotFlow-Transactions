"use client";

import { motion } from "motion/react";
import { cn } from "@/lib/utils";

interface BarItem {
  key: string;
  label: React.ReactNode;
  value: number;
  display: string;
  sub?: string;
  onClick?: () => void;
}

/** Ranked horizontal bars. One hue: magnitude, not identity. */
export function BarList({ items, color = "var(--chart-accent)", max, className }: {
  items: BarItem[]; color?: string; max?: number; className?: string;
}) {
  const top = max ?? Math.max(1, ...items.map((i) => i.value));
  return (
    <ul className={cn("space-y-1", className)}>
      {items.map((item, idx) => {
        const Row = item.onClick ? "button" : "div";
        return (
          <li key={item.key}>
            <Row
              onClick={item.onClick}
              className={cn(
                "group relative flex w-full items-center gap-3 rounded-lg px-2.5 py-2 text-left",
                item.onClick && "cursor-pointer hover:bg-surface-2",
              )}
            >
              <div className="relative min-w-0 flex-1">
                <div className="flex items-baseline justify-between gap-3">
                  <span className="truncate text-[13px] text-ink">{item.label}</span>
                  <span className="tnum shrink-0 text-[12.5px] text-muted">{item.display}</span>
                </div>
                <div className="mt-1.5 h-1.5 w-full rounded-full bg-surface-2">
                  <motion.div
                    className="h-full rounded-full"
                    style={{ background: color }}
                    initial={{ width: 0 }}
                    whileInView={{ width: `${Math.max(1.5, (item.value / top) * 100)}%` }}
                    viewport={{ once: true }}
                    transition={{ duration: 0.7, delay: idx * 0.03, ease: [0.2, 0.7, 0.2, 1] }}
                  />
                </div>
                {item.sub && <div className="mt-1 text-[11.5px] text-faint">{item.sub}</div>}
              </div>
            </Row>
          </li>
        );
      })}
    </ul>
  );
}
