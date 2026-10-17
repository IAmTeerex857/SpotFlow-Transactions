"use client";

import { motion } from "motion/react";
import { useId } from "react";
import { cn } from "@/lib/utils";

interface PillTab<T extends string> {
  value: T;
  label: string;
  count?: number;
}

/** Segmented control: a raised pill slides across a sunken track. */
export function PillTabs<T extends string>({
  tabs, value, onChange, className, size = "md",
}: { tabs: PillTab<T>[]; value: T; onChange: (value: T) => void; className?: string; size?: "sm" | "md" }) {
  const id = useId();
  return (
    <div role="tablist" className={cn("relative inline-flex max-w-full items-center gap-0.5 overflow-x-auto rounded-[10px] bg-sunken p-[3px] [scrollbar-width:none]", className)}>
      {tabs.map((tab) => {
        const selected = tab.value === value;
        return (
          <button
            key={tab.value}
            role="tab"
            aria-selected={selected}
            onClick={() => onChange(tab.value)}
            className={cn(
              "relative shrink-0 cursor-pointer whitespace-nowrap rounded-[8px] font-medium tracking-[-0.01em] transition-colors duration-150",
              size === "sm" ? "h-7 px-2.5 text-[12.5px]" : "h-8 px-3.5 text-[13px]",
              selected ? "text-ink" : "text-muted hover:text-ink",
            )}
          >
            {selected && (
              <motion.span
                layoutId={`seg-${id}`}
                className="absolute inset-0 rounded-[8px] bg-raised shadow-[0_1px_2px_rgba(16,24,40,.08),0_0_0_1px_var(--line)]"
                transition={{ type: "spring", stiffness: 500, damping: 38 }}
              />
            )}
            <span className="relative flex items-center gap-1.5">
              {tab.label}
              {tab.count !== undefined && <span className="tnum text-[11.5px] text-faint">{tab.count}</span>}
            </span>
          </button>
        );
      })}
    </div>
  );
}

/** Underline tabs, like the CRM reference ("Companies · Deals · Forecast"). */
export function UnderlineTabs<T extends string>({ tabs, value, onChange, className }: {
  tabs: PillTab<T>[]; value: T; onChange: (value: T) => void; className?: string;
}) {
  const id = useId();
  return (
    <div role="tablist" className={cn("flex max-w-full gap-6 overflow-x-auto border-b border-line [scrollbar-width:none]", className)}>
      {tabs.map((tab) => {
        const selected = tab.value === value;
        return (
          <button
            key={tab.value}
            role="tab"
            aria-selected={selected}
            onClick={() => onChange(tab.value)}
            className={cn("relative shrink-0 cursor-pointer pb-3 text-[13.5px] font-medium tracking-[-0.01em] transition-colors", selected ? "text-ink" : "text-muted hover:text-ink")}
          >
            <span className="flex items-center gap-1.5">
              {tab.label}
              {tab.count !== undefined && <span className="tnum rounded-md bg-surface-2 px-1.5 text-[11px] text-muted">{tab.count}</span>}
            </span>
            {selected && <motion.span layoutId={`ul-${id}`} className="absolute inset-x-0 -bottom-px h-[2px] rounded-full bg-ink" transition={{ type: "spring", stiffness: 500, damping: 40 }} />}
          </button>
        );
      })}
    </div>
  );
}
