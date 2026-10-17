"use client";

import { InlineLoader } from "generative-loaders";
import { motion } from "motion/react";
import { cn } from "@/lib/utils";

export type NodeState = "waiting" | "active" | "done" | "error";

const TONES = {
  red: { chip: "bg-bad-dim text-bad", ring: "shadow-[0_0_0_1px_color-mix(in_oklab,var(--bad)_35%,transparent)]" },
  violet: { chip: "bg-violet-dim text-violet", ring: "shadow-[0_0_0_1px_color-mix(in_oklab,var(--violet)_35%,transparent)]" },
  blue: { chip: "bg-accent-dim text-accent", ring: "shadow-[0_0_0_1px_var(--accent-line)]" },
  amber: { chip: "bg-warn-dim text-warn", ring: "shadow-[0_0_0_1px_color-mix(in_oklab,var(--warn)_35%,transparent)]" },
  green: { chip: "bg-ok-dim text-ok", ring: "shadow-[0_0_0_1px_color-mix(in_oklab,var(--ok)_35%,transparent)]" },
} as const;

function Check() {
  return (
    <svg viewBox="0 0 24 24" className="size-4 text-ok" aria-hidden>
      <motion.path d="M5 12.5l4.2 4.2L19 7" fill="none" stroke="currentColor" strokeWidth={2.4} strokeLinecap="round" strokeLinejoin="round" initial={{ pathLength: 0 }} animate={{ pathLength: 1 }} transition={{ duration: 0.35 }} />
    </svg>
  );
}

/** A workflow-builder style node: coloured kind chip, title, detail line. */
export function FlowNode({ kind, tone, title, detail, state, icon, children }: {
  kind: string;
  tone: keyof typeof TONES;
  title: string;
  detail?: string;
  state: NodeState;
  icon: React.ReactNode;
  children?: React.ReactNode;
}) {
  const t = TONES[tone];
  return (
    <motion.div
      layout
      animate={{ opacity: state === "waiting" ? 0.55 : 1, scale: state === "active" ? 1.01 : 1 }}
      transition={{ duration: 0.25 }}
      className={cn("w-full max-w-[360px] rounded-2xl bg-raised p-1 shadow-[var(--shadow-card)] transition-shadow", state === "active" && t.ring)}
    >
      <div className="flex items-center justify-between px-3 pb-2 pt-2.5">
        <span className={cn("inline-flex items-center gap-1.5 rounded-md px-2 py-0.5 text-[12px] font-medium [&_svg]:size-3.5", t.chip)}>
          {icon}
          {kind}
        </span>
        <span className="flex size-5 items-center justify-center">
          {state === "active" && <InlineLoader variant="orbit" size={16} color="var(--accent)" label={title} />}
          {state === "done" && <Check />}
          {state === "error" && <span className="size-2 rounded-full bg-bad" />}
        </span>
      </div>
      <div className="rounded-xl border border-line px-3.5 py-3">
        <div className="text-[14px] font-medium tracking-[-0.01em] text-ink">{title}</div>
        <div className="tnum mt-1 min-h-[18px] text-[12.5px] text-muted">{detail ?? "Waiting"}</div>
        {children}
      </div>
    </motion.div>
  );
}

/** Connector between nodes; a dot travels down it while the next step runs. */
export function FlowEdge({ active, done }: { active: boolean; done: boolean }) {
  return (
    <div className="relative mx-auto h-9 w-px">
      <div className={cn("absolute inset-0 w-px", done ? "bg-ok/50" : "bg-line")} />
      {active && (
        <motion.span
          className="absolute -left-[3px] size-[7px] rounded-full bg-accent"
          initial={{ top: -4 }}
          animate={{ top: 32 }}
          transition={{ duration: 0.7, repeat: Infinity, ease: "easeInOut" }}
        />
      )}
    </div>
  );
}
