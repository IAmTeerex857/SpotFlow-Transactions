"use client";

import { InlineLoader } from "generative-loaders";
import { AnimatePresence, motion } from "motion/react";
import { ArrowUp, Brain, ChevronDown, Database, RotateCcw, Sparkles, Square } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { PageHeader } from "@/components/shell/page-header";
import { Mark } from "@/components/brand/logo";
import { MorphButton } from "@/components/ui/morph-button";
import { cn } from "@/lib/utils";
import { Markdown } from "./markdown";

interface ToolStep { name: string; input: unknown; ok: boolean; ms: number }
interface Turn { role: "user" | "assistant"; content: string; thinking?: string; tools?: ToolStep[]; error?: string; pending?: boolean }

const TOOL_LABELS: Record<string, string> = {
  list_options: "Listed available merchants, providers and dates",
  get_report_summary: "Summarised the report",
  compare_periods: "Compared two periods",
  get_reason_breakdown: "Broke down reasons",
  get_daily_series: "Pulled the daily series",
  sample_transactions: "Sampled transactions",
};

function describeInput(input: unknown): string {
  if (!input || typeof input !== "object") return "";
  const i = input as Record<string, unknown>;
  const parts: string[] = [];
  if (i.from && i.to) parts.push(`${i.from} → ${i.to}`);
  if (i.a && i.b) parts.push(`${(i.a as { from: string }).from}… vs ${(i.b as { from: string }).from}…`);
  for (const k of ["merchants", "providers", "regions", "channels", "statuses"]) {
    if (Array.isArray(i[k]) && (i[k] as string[]).length) parts.push((i[k] as string[]).join(", "));
  }
  if (i.by) parts.push(`by ${i.by}`);
  return parts.join(" · ");
}

const SUGGESTIONS = [
  "Which provider had the worst failure rate in the last 7 days, and why?",
  "Compare this week's success rate with last week's by region.",
  "What are the top reasons Ghana mobile money payments fail?",
  "How many customers retried yesterday, and how many eventually paid?",
];

export function AskChat({ view, configured, initialQuestion }: { view?: { from: string; to: string; filters?: string }; configured: boolean; initialQuestion?: string }) {
  const [turns, setTurns] = useState<Turn[]>([]);
  const [input, setInput] = useState(initialQuestion ?? "");
  const [busy, setBusy] = useState(false);
  const abort = useRef<AbortController | null>(null);
  const bottom = useRef<HTMLDivElement>(null);

  useEffect(() => bottom.current?.scrollIntoView({ behavior: "smooth", block: "end" }), [turns]);

  const patchLast = (fn: (t: Turn) => Turn) => setTurns((all) => [...all.slice(0, -1), fn(all[all.length - 1])]);

  const ask = async (question: string) => {
    const q = question.trim();
    if (!q || busy) return;
    const history = [...turns.filter((t) => !t.error && t.content), { role: "user" as const, content: q }];
    setTurns([...turns, { role: "user", content: q }, { role: "assistant", content: "", tools: [], pending: true }]);
    setInput("");
    setBusy(true);
    abort.current = new AbortController();
    try {
      const res = await fetch("/api/ask", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ messages: history.map(({ role, content }) => ({ role, content })), view }),
        signal: abort.current.signal,
      });
      if (!res.ok || !res.body) throw new Error((await res.json().catch(() => ({}))).error ?? `Request failed (${res.status})`);
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() ?? "";
        for (const line of lines) {
          if (!line.trim()) continue;
          const event = JSON.parse(line);
          if (event.type === "text") patchLast((t) => ({ ...t, content: t.content + event.text }));
          else if (event.type === "thinking") patchLast((t) => ({ ...t, thinking: (t.thinking ?? "") + event.text }));
          else if (event.type === "tool") patchLast((t) => ({ ...t, tools: [...(t.tools ?? []), event] }));
          else if (event.type === "error") patchLast((t) => ({ ...t, error: event.message }));
        }
      }
    } catch (e) {
      if ((e as Error).name !== "AbortError") patchLast((t) => ({ ...t, error: e instanceof Error ? e.message : "Something went wrong" }));
    } finally {
      patchLast((t) => ({ ...t, pending: false }));
      setBusy(false);
    }
  };

  return (
    <div className="mx-auto flex min-h-[calc(100dvh-7rem)] max-w-3xl flex-col">
      <PageHeader
        crumb="Assist · Ask"
        title="Ask about your payments"
        description="Answers are computed from your uploaded exports. Every number comes from a query you can inspect."
        actions={turns.length > 0 && <MorphButton icon={<RotateCcw />} size="sm" onClick={() => setTurns([])} disabled={busy}>New chat</MorphButton>}
      />

      <div className="flex-1 space-y-6 py-8">
        {!turns.length && (
          <div className="grid gap-2.5 sm:grid-cols-2">
            {SUGGESTIONS.map((s, i) => (
              <motion.button
                key={s}
                initial={{ opacity: 0, y: 8 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.05 * i }}
                whileHover={{ y: -2 }}
                onClick={() => ask(s)}
                disabled={!configured}
                className="cursor-pointer rounded-2xl bg-raised p-4 text-left text-[13.5px] text-ink shadow-[var(--shadow-card)] transition-shadow hover:shadow-[var(--shadow-pop)] disabled:cursor-not-allowed disabled:opacity-50"
              >
                <Sparkles className="mb-2 size-4 text-accent" />
                {s}
              </motion.button>
            ))}
          </div>
        )}
        {turns.map((turn, i) => turn.role === "user" ? (
          <motion.div key={i} initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }} className="flex justify-end">
            <div className="max-w-[85%] rounded-2xl rounded-br-md bg-ink px-4 py-2.5 text-[14px] text-raised">{turn.content}</div>
          </motion.div>
        ) : (
          <motion.div key={i} initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }} className="flex gap-3">
            <span className="mt-0.5 flex size-7 shrink-0 items-center justify-center rounded-[8px] bg-accent text-white"><Mark className="size-4" /></span>
            <div className="min-w-0 flex-1 space-y-2">
              <Trace turn={turn} />
              {turn.content && <Markdown text={turn.content} />}
              {turn.pending && !turn.content && (
                <div className="flex items-center gap-2 text-[13px] text-muted">
                  <InlineLoader variant="signal" size={16} color="var(--accent)" label="Working" />
                  {turn.tools?.length ? "Reading the results…" : "Working out which data to pull…"}
                </div>
              )}
              {turn.error && <p className="rounded-xl border border-bad/30 bg-bad-dim px-3.5 py-2.5 text-[13px] text-bad">{turn.error}</p>}
            </div>
          </motion.div>
        ))}
        <div ref={bottom} />
      </div>

      <form
        onSubmit={(e) => { e.preventDefault(); void ask(input); }}
        className="sticky bottom-4 rounded-2xl bg-raised p-2 shadow-[var(--shadow-pop)]"
      >
        {!configured && <p className="px-2 pb-2 text-[12.5px] text-warn">Ask isn&apos;t configured yet: the Vercel AI Gateway isn&apos;t connected.</p>}
        <div className="flex items-end gap-2">
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); void ask(input); } }}
            rows={1}
            maxLength={2000}
            disabled={!configured}
            placeholder="Ask about failures, providers, merchants, retries…"
            className="max-h-40 min-h-[40px] flex-1 resize-none bg-transparent px-2 py-2 text-[14px] text-ink outline-none placeholder:text-faint [field-sizing:content]"
          />
          {busy ? (
            <button type="button" onClick={() => abort.current?.abort()} className="flex size-10 cursor-pointer items-center justify-center rounded-xl bg-surface-3 text-ink" aria-label="Stop">
              <Square className="size-3.5" fill="currentColor" />
            </button>
          ) : (
            <motion.button whileTap={{ scale: 0.92 }} type="submit" disabled={!input.trim() || !configured} className="flex size-10 cursor-pointer items-center justify-center rounded-xl bg-accent text-accent-ink disabled:opacity-40" aria-label="Send">
              <ArrowUp className="size-4" />
            </motion.button>
          )}
        </div>
      </form>
    </div>
  );
}

function Trace({ turn }: { turn: Turn }) {
  const [open, setOpen] = useState(false);
  const tools = turn.tools ?? [];
  if (!tools.length && !turn.thinking) return null;
  return (
    <div className="rounded-xl border border-line bg-raised">
      <button onClick={() => setOpen((v) => !v)} className="flex w-full cursor-pointer items-center gap-2 px-3 py-2 text-[12.5px] text-muted hover:text-ink">
        <Database className="size-3.5" />
        {tools.length} quer{tools.length === 1 ? "y" : "ies"}
        {turn.thinking && <><span className="text-faint">·</span><Brain className="size-3.5" /> reasoning</>}
        <ChevronDown className={cn("ml-auto size-3.5 transition-transform", open && "rotate-180")} />
      </button>
      <AnimatePresence initial={false}>
        {open && (
          <motion.div initial={{ height: 0 }} animate={{ height: "auto" }} exit={{ height: 0 }} className="overflow-hidden">
            <ol className="space-y-1.5 border-t border-line-soft px-3 py-2.5 text-[12.5px]">
              {tools.map((t, i) => (
                <li key={i} className="flex items-start gap-2">
                  <span className={cn("mt-1.5 size-1.5 shrink-0 rounded-full", t.ok ? "bg-ok" : "bg-bad")} />
                  <span className="text-ink">{TOOL_LABELS[t.name] ?? t.name}</span>
                  <span className="truncate text-faint">{describeInput(t.input)}</span>
                  <span className="ml-auto shrink-0 tnum text-[11px] text-faint">{t.ms}ms</span>
                </li>
              ))}
            </ol>
            {turn.thinking && <p className="border-t border-line-soft px-3 py-2.5 text-[12.5px] italic leading-relaxed text-muted whitespace-pre-wrap">{turn.thinking}</p>}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
