"use client";

import { AnimatePresence, motion } from "motion/react";
import { useEffect, useRef, useState } from "react";
import { cn } from "@/lib/utils";

/** A click-to-open popover that closes on outside click and Escape. */
export function Popover({ trigger, children, align = "left", className }: {
  trigger: (props: { open: boolean; toggle: () => void }) => React.ReactNode;
  children: (close: () => void) => React.ReactNode;
  align?: "left" | "right";
  className?: string;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const close = (e: MouseEvent) => !ref.current?.contains(e.target as Node) && setOpen(false);
    const esc = (e: KeyboardEvent) => e.key === "Escape" && setOpen(false);
    document.addEventListener("mousedown", close);
    document.addEventListener("keydown", esc);
    return () => {
      document.removeEventListener("mousedown", close);
      document.removeEventListener("keydown", esc);
    };
  }, [open]);
  return (
    <div ref={ref} className="relative">
      {trigger({ open, toggle: () => setOpen((v) => !v) })}
      <AnimatePresence>
        {open && (
          <motion.div
            initial={{ opacity: 0, y: -4, scale: 0.98 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -4, scale: 0.98 }}
            transition={{ duration: 0.14 }}
            className={cn("absolute top-10 z-40 overflow-hidden rounded-xl bg-raised shadow-[var(--shadow-pop)]", align === "right" ? "right-0 origin-top-right" : "left-0 origin-top-left", className)}
          >
            {children(() => setOpen(false))}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

/** The "Label | value ▾" segmented trigger from the CRM reference. */
export function SegmentTrigger({ label, value, open, onClick, active }: {
  label: string; value: string; open: boolean; onClick: () => void; active?: boolean;
}) {
  return (
    <button
      onClick={onClick}
      aria-expanded={open}
      className={cn(
        "flex h-8 cursor-pointer items-stretch overflow-hidden rounded-[10px] bg-raised text-[13px] shadow-[var(--shadow-card)] transition-shadow",
        active && "shadow-[0_0_0_1px_var(--accent-line)]",
      )}
    >
      <span className="flex items-center border-r border-line px-2.5 text-faint">{label}</span>
      <span className="flex items-center gap-1.5 px-2.5 text-ink">
        <span className="max-w-[160px] truncate">{value}</span>
        <svg viewBox="0 0 12 12" className={cn("size-3 text-faint transition-transform", open && "rotate-180")} aria-hidden>
          <path d="M3 4.5 6 7.5 9 4.5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      </span>
    </button>
  );
}
