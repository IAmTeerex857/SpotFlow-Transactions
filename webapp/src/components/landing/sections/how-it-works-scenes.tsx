"use client";

// Scripted vignettes for How it works. Illustrative only: every name and number is made up.
import { AnimatePresence, motion } from "motion/react";
import { ArrowDownRight, Check, FileSpreadsheet, KeyRound, Loader2, ScanSearch, ShieldCheck, Sparkles, Upload } from "lucide-react";
import { CountUp, useLoop } from "@/components/landing/motion";
import { cn } from "@/lib/utils";

const card = "rounded-2xl border border-[#e3e7ef] bg-white shadow-[0_1px_2px_rgba(16,24,40,.04),0_12px_32px_-16px_rgba(16,24,40,.16)]";
const spring = { type: "spring", stiffness: 380, damping: 30 } as const;

const STATUS = {
  successful: { label: "Successful", dot: "#009688" },
  failed: { label: "Failed", dot: "#d93a3f" },
  abandoned: { label: "Abandoned", dot: "#e8a20c" },
} as const;

// ── 01 · Export the day: the CSV fills in row by row ──────────────────────
const ROWS: { ref: string; provider: string; status: keyof typeof STATUS; amount: string; merged?: boolean }[] = [
  { ref: "SPF-48213", provider: "Mobile money", status: "successful", amount: "GH₵ 120.00" },
  { ref: "SPF-48214", provider: "Card", status: "failed", amount: "₦ 18,500" },
  { ref: "SPF-48215", provider: "Bank transfer", status: "successful", amount: "KSh 2,400" },
  { ref: "SPF-48216", provider: "Mobile money", status: "abandoned", amount: "GH₵ 45.00", merged: true },
  { ref: "SPF-48217", provider: "Card", status: "successful", amount: "R 310.00" },
  { ref: "SPF-48218", provider: "Mobile money", status: "failed", amount: "RWF 9,000" },
];

export function ExportScene() {
  const { ref, t } = useLoop(520, 15);
  const shown = Math.min(t, ROWS.length);
  return (
    <div ref={ref} className="flex h-full flex-col justify-center gap-4 px-4 sm:px-10">
      <div className={cn(card, "overflow-hidden")}>
        <div className="flex items-center gap-3 border-b border-[#eef1f6] px-4 py-3">
          <span className="flex size-8 items-center justify-center rounded-lg bg-[#e8f6ee] text-[#1f9d55]"><FileSpreadsheet className="size-4" /></span>
          <div className="min-w-0 flex-1">
            <div className="truncate text-[13px] font-medium text-[#0f172a]">transactions_2026-10-15.csv</div>
            <div className="text-[11.5px] text-[#94a3b8]">Exported from Spotflow · 8.4 MB</div>
          </div>
          <span className="rounded-full bg-[#f1f4fa] px-2 py-0.5 text-[11px] font-medium text-[#64748b]">Raw</span>
        </div>
        <div className="grid grid-cols-[1fr_1fr_0.9fr] gap-2 border-b border-[#eef1f6] bg-[#fafbfd] px-4 py-2 text-[11px] font-medium text-[#94a3b8] sm:grid-cols-[1fr_1.1fr_1fr_0.9fr]">
          <span>Reference</span><span className="hidden sm:block">Provider</span><span>Status</span><span className="text-right">Amount</span>
        </div>
        <div className="h-[216px]">
          <AnimatePresence initial={false}>
            {ROWS.slice(0, shown).map((r) => (
              <motion.div
                key={r.ref}
                initial={{ opacity: 0, y: -8, backgroundColor: "rgba(51,102,255,0.08)" }}
                animate={{ opacity: 1, y: 0, backgroundColor: r.merged && t >= 8 ? "rgba(232,162,12,0.10)" : "rgba(51,102,255,0)" }}
                exit={{ opacity: 0 }}
                transition={{ duration: 0.35 }}
                className="grid grid-cols-[1fr_1fr_0.9fr] items-center gap-2 whitespace-nowrap border-b border-[#f3f5f9] px-4 py-2 text-[12px] text-[#334155] sm:grid-cols-[1fr_1.1fr_1fr_0.9fr]"
              >
                <span className="tnum text-[#64748b]">{r.ref}</span>
                <span className="hidden truncate sm:block">{r.provider}</span>
                <span className="flex items-center gap-1.5"><span className="size-1.5 rounded-full" style={{ background: STATUS[r.status].dot }} />{STATUS[r.status].label}</span>
                <span className="tnum text-right">{r.amount}</span>
              </motion.div>
            ))}
          </AnimatePresence>
        </div>
      </div>
      <div className="flex items-center justify-between gap-3">
        <div className={cn(card, "flex items-center gap-3 px-4 py-3")}>
          <span className="text-[12px] text-[#94a3b8]">Rows read</span>
          <CountUp value={Math.round((12408 * shown) / ROWS.length)} format={(v) => Math.round(v).toLocaleString("en-US")} className="tnum text-[18px] font-semibold tracking-[-0.02em] text-[#0f172a]" />
        </div>
        <AnimatePresence>
          {t >= 8 && (
            <motion.div initial={{ opacity: 0, x: 12, scale: 0.96 }} animate={{ opacity: 1, x: 0, scale: 1 }} exit={{ opacity: 0 }} transition={spring}
              className="flex items-center gap-2 rounded-full border border-[#f3dca8] bg-[#fff8ea] px-3 py-1.5 text-[12px] text-[#8a5a00]">
              <Sparkles className="size-3.5" /> 3 merged rows recovered
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}

// ── 02 · Drop it in: the file lands and each check runs ───────────────────
const CHECKS = [
  { icon: ScanSearch, label: "Parse", hint: "12,408 rows · 3 recovered" },
  { icon: KeyRound, label: "Protect", hint: "Emails become pseudonyms" },
  { icon: ShieldCheck, label: "Check", hint: "No duplicate export found" },
  { icon: Upload, label: "Save", hint: "Ready for the team" },
];

export function UploadScene() {
  const { ref, t } = useLoop(650, 13);
  const landed = t >= 2;
  const doneCount = Math.max(0, Math.min(CHECKS.length, t - 2));
  return (
    <div ref={ref} className="flex h-full flex-col justify-center gap-4 px-4 sm:px-10">
      <div className={cn(
        "relative flex h-[150px] flex-col items-center justify-center rounded-2xl border-2 border-dashed transition-colors duration-500",
        landed ? "border-[#3366ff]/50 bg-[#f3f6ff]" : "border-[#d7deeb] bg-[#fafbfd]",
      )}>
        <AnimatePresence mode="wait">
          {!landed ? (
            <motion.div key="idle" exit={{ opacity: 0 }} className="flex flex-col items-center text-[13px] text-[#64748b]">
              <span className="mb-2 flex size-10 items-center justify-center rounded-xl bg-white text-[#3366ff] shadow-[0_1px_2px_rgba(16,24,40,.06),0_0_0_1px_#e3e7ef]"><Upload className="size-4" /></span>
              Drop today&apos;s export here
            </motion.div>
          ) : (
            <motion.div key="file" initial={{ opacity: 0, scale: 0.9 }} animate={{ opacity: 1, scale: 1 }} transition={spring} className={cn(card, "flex items-center gap-3 px-4 py-3")}>
              <span className="flex size-8 items-center justify-center rounded-lg bg-[#e8f6ee] text-[#1f9d55]"><FileSpreadsheet className="size-4" /></span>
              <span className="text-[13px] font-medium text-[#0f172a]">transactions_2026-10-15.csv</span>
              {doneCount === CHECKS.length ? <Check className="size-4 text-[#12855a]" /> : <Loader2 className="size-4 animate-spin text-[#3366ff]" />}
            </motion.div>
          )}
        </AnimatePresence>
        {/* the file flies in from the corner */}
        <AnimatePresence>
          {t === 1 && (
            <motion.div
              initial={{ x: 180, y: -120, rotate: 12, opacity: 0 }}
              animate={{ x: 0, y: 0, rotate: -3, opacity: 1 }}
              exit={{ opacity: 0, scale: 0.9 }}
              transition={{ type: "spring", stiffness: 160, damping: 18 }}
              className={cn(card, "absolute flex items-center gap-2 px-3 py-2 text-[12px] font-medium text-[#0f172a]")}
            >
              <FileSpreadsheet className="size-3.5 text-[#1f9d55]" /> transactions.csv
            </motion.div>
          )}
        </AnimatePresence>
      </div>
      <div className={cn(card, "divide-y divide-[#f1f4f9] px-1")}>
        {CHECKS.map((c, i) => {
          const state = i < doneCount ? "done" : i === doneCount && landed ? "running" : "idle";
          return (
            <div key={c.label} className="flex items-center gap-3 px-3 py-2.5">
              <span className={cn(
                "flex size-8 items-center justify-center rounded-lg transition-colors duration-300",
                state === "done" ? "bg-[#e7f5f3] text-[#009688]" : state === "running" ? "bg-[#eef2ff] text-[#3366ff]" : "bg-[#f4f6fa] text-[#a3adbf]",
              )}>
                <c.icon className="size-4" />
              </span>
              <div className="min-w-0 flex-1">
                <div className={cn("text-[13px] font-medium transition-colors", state === "idle" ? "text-[#94a3b8]" : "text-[#0f172a]")}>{c.label}</div>
                <div className="truncate text-[11.5px] text-[#94a3b8]">{state === "idle" ? "Waiting" : c.hint}</div>
              </div>
              <span className="flex size-5 items-center justify-center">
                {state === "done" && <motion.span initial={{ scale: 0 }} animate={{ scale: 1 }} transition={spring} className="flex size-5 items-center justify-center rounded-full bg-[#009688] text-white"><Check className="size-3" strokeWidth={3} /></motion.span>}
                {state === "running" && <Loader2 className="size-4 animate-spin text-[#3366ff]" />}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── 03 · Read what broke: the summary assembles itself ────────────────────
const REASONS = [
  { label: "Insufficient funds", share: 0.42, color: "#d93a3f" },
  { label: "Authorization timed out", share: 0.27, color: "#e8a20c" },
  { label: "Invalid phone number", share: 0.14, color: "#7457e0" },
];

export function ReportScene() {
  const { ref, t } = useLoop(700, 12);
  const on = t >= 1;
  return (
    <div ref={ref} className="flex h-full flex-col justify-center gap-3.5 px-4 sm:px-10">
      <div className="grid grid-cols-2 gap-3.5">
        <div className={cn(card, "p-4")}>
          <div className="text-[12px] text-[#94a3b8]">Success rate</div>
          <CountUp value={on ? 72.4 : 0} format={(v) => `${v.toFixed(1)}%`} className="tnum mt-1 block text-[26px] font-semibold tracking-[-0.03em] text-[#0f172a]" />
          <div className="mt-3 flex h-2 gap-[2px] overflow-hidden rounded-full">
            {[["#009688", 72.4], ["#d93a3f", 17.1], ["#e8a20c", 10.5]].map(([c, w], i) => (
              <motion.span key={c as string} className="h-full rounded-[2px]" style={{ background: c as string }} initial={{ width: 0 }} animate={{ width: on ? `${w}%` : 0 }} transition={{ duration: 0.8, delay: i * 0.12 }} />
            ))}
          </div>
        </div>
        <div className={cn(card, "p-4")}>
          <div className="text-[12px] text-[#94a3b8]">Failed volume</div>
          <CountUp value={on ? 4742 : 0} format={(v) => `$${Math.round(v).toLocaleString("en-US")}`} className="tnum mt-1 block text-[26px] font-semibold tracking-[-0.03em] text-[#0f172a]" />
          <div className="mt-3 flex items-center gap-1 text-[12px] font-medium text-[#d93a3f]"><ArrowDownRight className="size-3.5" />2.6% vs yesterday</div>
        </div>
      </div>
      <div className={cn(card, "p-4")}>
        <div className="mb-3 text-[13px] font-medium text-[#0f172a]">Why payments failed</div>
        <div className="space-y-3">
          {REASONS.map((r, i) => (
            <div key={r.label}>
              <div className="mb-1.5 flex justify-between text-[12px] text-[#334155]"><span>{r.label}</span><span className="tnum text-[#94a3b8]">{Math.round(r.share * 100)}%</span></div>
              <div className="h-1.5 overflow-hidden rounded-full bg-[#eef1f6]">
                <motion.div className="h-full rounded-full" style={{ background: r.color }} initial={{ width: 0 }} animate={{ width: t >= 2 + i ? `${r.share * 100 * 2}%` : 0 }} transition={{ type: "spring", stiffness: 90, damping: 18 }} />
              </div>
            </div>
          ))}
        </div>
      </div>
      <AnimatePresence>
        {t >= 6 && (
          <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }} transition={spring} className={cn(card, "flex items-center gap-3 px-4 py-3")}>
            <span className="flex size-8 items-center justify-center rounded-lg bg-[#fdecec] text-[#d93a3f]"><ArrowDownRight className="size-4" /></span>
            <div className="min-w-0 flex-1">
              <div className="text-[13px] font-medium text-[#0f172a]">Card payments slipped</div>
              <div className="text-[11.5px] text-[#94a3b8]">What changed · vs the 7 days before</div>
            </div>
            <span className="tnum text-[13px] font-semibold text-[#d93a3f]">−7.8 pts</span>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
