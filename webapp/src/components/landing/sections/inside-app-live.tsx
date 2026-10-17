"use client";

// Small looping product vignettes for the bento. Illustrative only: no data.
import { AnimatePresence, motion, useInView, useReducedMotion } from "motion/react";
import { useTicker } from "@/components/landing/motion";
import { ArrowRight, BarChart3, CalendarDays, Check, ChevronDown, Database, Globe2, Loader2, MessageSquareText, Plus, Store } from "lucide-react";
import { useRef, useState } from "react";
import { cn } from "@/lib/utils";

const card = "rounded-xl border border-[#e3e7ef] bg-white shadow-[0_1px_2px_rgba(16,24,40,.04)]";
const pill = "rounded-full border border-[#e3e7ef] bg-white px-2.5 py-0.5 text-[12px]";

// ── Upload history: a new export lands on top, processes, then completes ──
const DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
const TIMES = ["4.2s", "3.8s", "5.1s", "3.6s", "4.4s", "3.9s", "4.7s"];

export function RunsLive() {
  // Two ticks per run: it lands as "Processing", then completes.
  const { ref, tick } = useTicker(1300);
  const run = Math.floor(tick / 2) + 4;
  const rows = Array.from({ length: 4 }, (_, i) => run - i);
  return (
    <div ref={ref} className="relative h-[200px] w-full overflow-hidden [mask-image:linear-gradient(to_bottom,black_75%,transparent)]">
      <AnimatePresence initial={false}>
        {rows.map((id, i) => {
          const d = ((id % DAYS.length) + DAYS.length) % DAYS.length;
          const processing = i === 0 && tick % 2 === 0 && tick > 0;
          return (
            <motion.div
              key={id}
              layout
              initial={{ opacity: 0, y: -24, scale: 0.97 }}
              animate={{ opacity: 1, y: 0, scale: 1 }}
              exit={{ opacity: 0 }}
              transition={{ type: "spring", stiffness: 380, damping: 32 }}
              className={cn(card, "mb-2 flex items-center gap-3 px-3.5 py-2.5 text-[13px]")}
            >
              <span className={cn(pill, "text-[#475569]")}>{DAYS[d]} export</span>
              <span className="ml-auto tnum text-[#94a3b8]">{processing ? "—" : TIMES[d]}</span>
              <span className="flex w-[92px] items-center gap-1.5 text-[#334155]">
                {processing
                  ? <><Loader2 className="size-3 animate-spin text-[#e8a20c]" />Processing</>
                  : <><span className="size-1.5 rounded-full bg-[#12855a]" />Complete</>}
              </span>
            </motion.div>
          );
        })}
      </AnimatePresence>
    </div>
  );
}

// ── Provider registry: tiles light up as exports mention them ─────────────
const CELLS = ["hu", "", "ps", "pw", "", "h2", "oz", "", "tp", "ce", "", "is"];

export function ProvidersLive() {
  const { ref, tick } = useTicker(900);
  const filled = CELLS.map((c, i) => (c ? i : -1)).filter((i) => i >= 0);
  const lit = filled[(tick * 5) % filled.length];
  return (
    <div ref={ref} className="grid grid-cols-4 gap-2.5">
      {CELLS.map((p, i) => (
        <motion.span
          key={i}
          initial={{ opacity: 0, scale: 0.8 }}
          whileInView={{ opacity: 1, scale: 1 }}
          viewport={{ once: true }}
          transition={{ type: "spring", stiffness: 400, damping: 22, delay: i * 0.04 }}
          className={cn(
            "relative flex size-14 items-center justify-center rounded-2xl text-[13px] font-semibold transition-[box-shadow,color] duration-300",
            p ? "bg-white text-[#3366ff] shadow-[0_1px_2px_rgba(16,24,40,.06),0_0_0_1px_#e3e7ef]" : "bg-[#eef1f6]",
            lit === i && "text-white",
          )}
        >
          {lit === i && <motion.span layoutId="prov-lit" className="absolute inset-0 rounded-2xl bg-[#3366ff] shadow-[0_8px_18px_-8px_rgba(51,102,255,.8)]" transition={{ type: "spring", stiffness: 400, damping: 32 }} />}
          <span className="relative">{p}</span>
        </motion.span>
      ))}
    </div>
  );
}

// ── Status chart: stacked bars grow in, hover shows the day ───────────────
const BARS = [[3, 2, 7], [2, 3, 9], [4, 2, 6], [2, 4, 10], [5, 2, 5], [3, 3, 8], [2, 2, 9], [4, 3, 7], [3, 2, 9]];
const STATUS = [
  { key: "Successful", color: "#009688" },
  { key: "Failed", color: "#d93a3f" },
  { key: "Abandoned", color: "#e8a20c" },
];

export function StatusLive() {
  const ref = useRef<HTMLDivElement>(null);
  const inView = useInView(ref, { once: true, margin: "-10% 0px" });
  const [hover, setHover] = useState<number | null>(null);
  return (
    <div ref={ref} className={cn(card, "w-full p-4")}>
      <div className="relative flex h-36 items-end gap-2">
        {BARS.map(([f, a, s], i) => (
          <div key={i} className="relative flex h-full flex-1 cursor-default flex-col justify-end" onPointerEnter={() => setHover(i)} onPointerLeave={() => setHover(null)}>
            <motion.div
              className="flex origin-bottom flex-col justify-end gap-[2px]"
              initial={{ scaleY: 0 }}
              animate={{ scaleY: inView ? 1 : 0, opacity: hover === null || hover === i ? 1 : 0.45 }}
              transition={{ type: "spring", stiffness: 160, damping: 18, delay: inView && hover === null ? i * 0.05 : 0 }}
            >
              <span className="rounded-t-[4px]" style={{ height: s * 6, background: STATUS[0].color }} />
              <span style={{ height: f * 4, background: STATUS[1].color }} />
              <span style={{ height: a * 4, background: STATUS[2].color }} />
            </motion.div>
            <AnimatePresence>
              {hover === i && (
                <motion.div
                  initial={{ opacity: 0, y: 4 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: 4 }}
                  className="absolute bottom-full left-1/2 z-10 mb-2 w-[118px] -translate-x-1/2 rounded-lg border border-[#e3e7ef] bg-white p-2 text-left text-[11px] text-[#334155] shadow-[0_8px_24px_-8px_rgba(16,24,40,.2)]"
                >
                  <div className="mb-1 font-medium text-[#0f172a]">Day {i + 1}</div>
                  {STATUS.map((st, k) => (
                    <div key={st.key} className="flex items-center gap-1.5">
                      <span className="size-1.5 rounded-full" style={{ background: st.color }} />{st.key}
                      <span className="ml-auto tnum">{[s, f, a][k] * 10}</span>
                    </div>
                  ))}
                </motion.div>
              )}
            </AnimatePresence>
          </div>
        ))}
      </div>
      <div className="mt-3 flex justify-center gap-2 text-[11.5px] text-[#475569]">
        {STATUS.map((st) => (
          <span key={st.key} className={cn(pill, "flex items-center gap-1.5")}><span className="size-1.5 rounded-full" style={{ background: st.color }} />{st.key}</span>
        ))}
      </div>
    </div>
  );
}

// ── Filters: the focus ring hops between controls ─────────────────────────
const CHIPS = [
  { icon: Store, label: "Merchant" },
  { icon: BarChart3, label: "Provider" },
  { icon: CalendarDays, label: "Last 7 days" },
  { icon: Globe2, label: "Region" },
  { icon: ChevronDown, label: "Channel" },
];

export function FiltersLive() {
  const { ref, tick } = useTicker(1500);
  const active = [2, 0, 3, 1, 4][tick % 5];
  return (
    <div ref={ref} className="flex max-w-[310px] flex-wrap justify-center gap-2">
      {CHIPS.map((c, i) => (
        <span key={c.label} className="relative flex items-center gap-1.5 rounded-xl border border-[#e3e7ef] bg-white px-3 py-1.5 text-[13px] text-[#334155]">
          {active === i && (
            <motion.span
              layoutId="chip-focus"
              className="absolute -inset-px rounded-xl border border-[#3366ff] shadow-[0_0_0_3px_rgba(51,102,255,.14)]"
              transition={{ type: "spring", stiffness: 420, damping: 34 }}
            />
          )}
          <c.icon className="size-3.5 text-[#94a3b8]" />
          {c.label}
          {i !== 4 && <ChevronDown className="size-3 text-[#cbd2de]" />}
        </span>
      ))}
    </div>
  );
}

// ── Ask: the question types itself, tools run, the answer appears ─────────
const QUESTION = "Why did Ghana dip on Tuesday?";

export function AskLive() {
  const { ref, tick, inView } = useTicker(120);
  const reduced = useReducedMotion();
  // One loop = typing (~30 ticks), tools (2 steps), answer, pause.
  const t = reduced || !inView ? 80 : tick % 90;
  const typed = QUESTION.slice(0, Math.min(QUESTION.length, t));
  const step = t < 34 ? 0 : t < 44 ? 1 : t < 54 ? 2 : 3;
  return (
    <div ref={ref} className="w-full max-w-[300px] text-left">
      <div className={cn(card, "flex items-center gap-3 p-3")}>
        <span className="flex size-9 shrink-0 items-center justify-center rounded-full bg-[#eef2ff] text-[#3366ff]"><MessageSquareText className="size-4" /></span>
        <span className="min-w-0 text-[12px] text-[#94a3b8]">
          You asked
          <span className="block h-[19px] truncate text-[13px] text-[#0f172a]">{typed}{t < QUESTION.length + 4 && <span className="caret ml-px inline-block h-3.5 w-px translate-y-0.5 bg-[#3366ff]" />}</span>
        </span>
      </div>
      <div className="mx-auto h-4 w-px bg-[#c9d6ff]" />
      <div className={cn(card, "p-3 transition-opacity duration-300", step >= 1 ? "opacity-100" : "opacity-40")}>
        <span className="flex items-center gap-2 text-[12px] text-[#94a3b8]"><Database className="size-3.5" />Queries run</span>
        <div className="mt-2 flex gap-1.5">
          {["Compared periods", "Reasons by provider"].map((q, i) => (
            <span key={q} className={cn(pill, "flex items-center gap-1 transition-colors duration-300", step > i + 1 ? "text-[#0f172a]" : "text-[#94a3b8]")}>
              {step > i + 1 ? <Check className="size-3 text-[#12855a]" /> : step === i + 1 ? <Loader2 className="size-3 animate-spin" /> : null}
              {q}
            </span>
          ))}
        </div>
      </div>
      <div className="mx-auto h-4 w-px border-l border-dashed border-[#c9d6ff]" />
      <AnimatePresence mode="wait" initial={false}>
        {step === 3 ? (
          <motion.div key="a" initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }} className={cn(card, "p-3 text-[12.5px] leading-relaxed text-[#334155]")}>
            One provider&apos;s <span className="font-medium text-[#0f172a]">timeouts tripled</span> after 2pm. Everything else held steady.
          </motion.div>
        ) : (
          <motion.div key="b" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="mx-auto flex w-fit items-center gap-1 rounded-xl border border-[#e3e7ef] bg-white px-3 py-1.5 text-[12.5px] text-[#94a3b8]">
            <Plus className="size-3" /> Answer
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

// ── Failure reasons: segmented bars fill in ───────────────────────────────
const REASONS: [string, number, string][] = [["Insufficient funds", 8, "#d93a3f"], ["Authorization timed out", 6, "#e8a20c"], ["Invalid phone number", 3, "#7457e0"]];

export function ReasonsLive() {
  const ref = useRef<HTMLDivElement>(null);
  const inView = useInView(ref, { once: true, margin: "-10% 0px" });
  return (
    <div ref={ref} className="w-full space-y-2.5">
      {REASONS.map(([l, n, c], r) => (
        <div key={l} className={cn(card, "group/r px-3.5 py-2.5 transition-shadow hover:shadow-[0_6px_16px_-8px_rgba(16,24,40,.18)]")}>
          <div className="flex items-center justify-between text-[13px] text-[#0f172a]">
            <span>{l}</span>
            <ArrowRight className="size-3.5 -translate-x-1 text-[#94a3b8] opacity-0 transition-all group-hover/r:translate-x-0 group-hover/r:opacity-100" />
          </div>
          <div className="mt-2 flex gap-1">
            {Array.from({ length: 10 }, (_, i) => (
              <span key={i} className="relative h-1 flex-1 overflow-hidden rounded-full bg-[#eceff4]">
                {i < n && (
                  <motion.span
                    className="absolute inset-0 origin-left rounded-full"
                    style={{ background: c }}
                    initial={{ scaleX: 0 }}
                    animate={{ scaleX: inView ? 1 : 0 }}
                    transition={{ duration: 0.25, delay: r * 0.25 + i * 0.06 }}
                  />
                )}
              </span>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}
