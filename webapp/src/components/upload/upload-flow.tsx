"use client";

import { AnimatePresence, motion } from "motion/react";
import { AlertTriangle, ArrowRight, Check, FileText, Filter, Plus, RotateCcw, ShieldCheck, Upload, Zap } from "lucide-react";
import { useRouter } from "next/navigation";
import { useCallback, useRef, useState } from "react";
import { PageHeader } from "@/components/shell/page-header";
import { useTheme } from "@/components/ui/theme-toggle";
import { BorderBeam } from "border-beam";
import { MorphButton } from "@/components/ui/morph-button";
import { stripBom } from "@/lib/parser/csv";
import { parseExport, type ParseResult } from "@/lib/parser/parse";
import { buildRegistry } from "@/lib/parser/registry";
import { fmtDateTime, fmtInt, fmtPct, fmtRange, fmtUsd } from "@/lib/format";
import { cn } from "@/lib/utils";
import { FlowEdge, FlowNode, type NodeState } from "./flow-node";

type Phase = "idle" | "analysing" | "review" | "saving" | "done" | "error";
type StepId = "parse" | "protect" | "check" | "save";

interface Previous { id: string; created_at: string; uploader_email: string | null; file_name: string }
interface SaveResult { uploadId: string; inserted: number; updated: number; uniqueCustomers: number; stored: boolean; from: string; to: string }

const pause = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function sha256Hex(text: string) {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(stripBom(text)));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function gzip(text: string): Promise<Blob> {
  const stream = new Blob([text]).stream().pipeThrough(new CompressionStream("gzip"));
  return new Response(stream).blob();
}

const INITIAL: Record<StepId, { state: NodeState; detail?: string }> = {
  parse: { state: "waiting" }, protect: { state: "waiting" }, check: { state: "waiting" }, save: { state: "waiting" },
};

export function UploadFlow({ extras }: { extras: { providers: string[]; regions: string[] } }) {
  const router = useRouter();
  const theme = useTheme();
  const input = useRef<HTMLInputElement>(null);
  const [phase, setPhase] = useState<Phase>("idle");
  const [over, setOver] = useState(false);
  const [file, setFile] = useState<{ name: string; size: number; text: string } | null>(null);
  const [steps, setSteps] = useState(INITIAL);
  const [result, setResult] = useState<ParseResult | null>(null);
  const [previous, setPrevious] = useState<Previous | null>(null);
  const [saved, setSaved] = useState<SaveResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [registry, setRegistry] = useState(extras);
  const [adding, setAdding] = useState<string | null>(null);

  const step = (id: StepId, state: NodeState, detail?: string) => setSteps((s) => ({ ...s, [id]: { state, detail: detail ?? s[id].detail } }));

  const analyse = useCallback(async (f: { name: string; size: number; text: string }, extra = registry) => {
    setPhase("analysing");
    setError(null);
    setSteps(INITIAL);
    try {
      const shaPromise = sha256Hex(f.text);
      step("parse", "active", "Reading rows…");
      await pause(450);
      const parsed = parseExport(f.text, buildRegistry({ extraProviders: extra.providers, extraRegions: extra.regions }));
      const { stats } = parsed;
      step("parse", "done", `${fmtInt(stats.transactions)} transactions · ${fmtInt(stats.recoveredFragments)} recovered from merged rows`);

      step("protect", "active", "Finding customer identifiers…");
      await pause(450);
      const customers = new Set(parsed.transactions.map((t) => t.customer.toLowerCase()).filter(Boolean)).size;
      step("protect", "done", `${fmtInt(customers)} customers → pseudonyms on save`);

      step("check", "active", "Checking providers and regions…");
      const prev = await fetch(`/api/uploads/check?sha=${await shaPromise}`).then((r) => (r.ok ? r.json() : { previous: null })).catch(() => ({ previous: null }));
      await pause(350);
      const unknown = Object.keys(stats.unknownProviders).length + Object.keys(stats.unknownRegions).length;
      if (!stats.transactions) throw new Error("No transactions were found. Is this a Spotflow transactions export?");
      step("check", unknown || prev.previous ? "error" : "done", unknown ? `${unknown} unrecognised value${unknown > 1 ? "s" : ""}` : prev.previous ? "Uploaded before" : "All recognised");

      setPrevious(prev.previous);
      setResult(parsed);
      setPhase("review");
    } catch (e) {
      setError(e instanceof Error ? e.message : "The file could not be read");
      setSteps((s) => Object.fromEntries(Object.entries(s).map(([k, v]) => [k, v.state === "active" ? { ...v, state: "error" } : v])) as typeof s);
      setPhase("error");
    }
  }, [registry]);

  const accept = async (f: File | undefined) => {
    if (!f) return;
    if (!/\.csv$/i.test(f.name)) {
      setError(`${f.name} isn't a .csv file`);
      return;
    }
    const text = await f.text();
    const next = { name: f.name, size: f.size, text };
    setFile(next);
    void analyse(next);
  };

  const addToRegistry = async (kind: "provider" | "region", name: string) => {
    setAdding(name);
    const res = await fetch("/api/registry", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ kind, name }) });
    setAdding(null);
    if (!res.ok) return setError((await res.json().catch(() => ({}))).error ?? "Could not update the registry");
    const next = kind === "provider" ? { ...registry, providers: [...registry.providers, name.toLowerCase()] } : { ...registry, regions: [...registry.regions, name] };
    setRegistry(next);
    if (file) void analyse(file, next);
  };

  const save = async () => {
    if (!file) return;
    setPhase("saving");
    setError(null);
    step("save", "active", "Pseudonymising and saving…");
    try {
      const res = await fetch("/api/uploads", {
        method: "POST",
        headers: { "content-type": "application/gzip", "x-file-name": encodeURIComponent(file.name) },
        body: await gzip(file.text),
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(json.error ?? `Upload failed (${res.status})`);
      setSaved(json);
      step("save", "done", `${fmtInt(json.inserted)} new · ${fmtInt(json.updated)} updated`);
      setPhase("done");
      router.refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Upload failed");
      step("save", "error", "Not saved");
      setPhase("review");
    }
  };

  const reset = () => {
    setPhase("idle");
    setFile(null);
    setResult(null);
    setSaved(null);
    setPrevious(null);
    setError(null);
    setSteps(INITIAL);
  };

  const badge = phase === "done" ? { label: "Saved", cls: "bg-ok-dim text-ok" } : phase === "idle" ? null : { label: "Draft", cls: "bg-warn-dim text-warn" };

  return (
    <div className="space-y-6">
      <PageHeader
        crumb="Data · Upload"
        title={<span className="flex items-center gap-3">Upload an export {badge && <span className={cn("rounded-md px-2 py-0.5 text-[12px] font-medium tracking-normal", badge.cls)}>{badge.label}</span>}</span>}
        description="Parsed in your browser first so you can check it. Nothing is stored until you save."
        actions={phase !== "idle" && <MorphButton size="sm" icon={<RotateCcw />} onClick={reset} disabled={phase === "saving"}>Start over</MorphButton>}
      />

      <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_380px]">
        {/* Canvas */}
        <div className="dot-grid relative flex min-h-[620px] flex-col items-center justify-center overflow-hidden rounded-2xl bg-sunken/60 px-4 py-10 shadow-[var(--shadow-card)]">
          <AnimatePresence mode="wait">
            {phase === "idle" ? (
              <motion.div key="drop" initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -8 }} className="w-full max-w-[420px]">
                <BorderBeam size="md" colorVariant="ocean" theme={theme} active={over} borderRadius={18}>
                  <button
                    onClick={() => input.current?.click()}
                    onDragOver={(e) => { e.preventDefault(); setOver(true); }}
                    onDragLeave={() => setOver(false)}
                    onDrop={(e) => { e.preventDefault(); setOver(false); void accept(e.dataTransfer.files[0]); }}
                    className={cn("w-full cursor-pointer rounded-[18px] bg-raised p-1 text-left shadow-[var(--shadow-card)] transition-transform", over && "scale-[1.02]")}
                  >
                    <div className="flex items-center px-3 pb-2 pt-2.5">
                      <span className="inline-flex items-center gap-1.5 rounded-md bg-bad-dim px-2 py-0.5 text-[12px] font-medium text-bad"><Zap className="size-3.5" />Trigger</span>
                    </div>
                    <div className="flex flex-col items-center gap-3 rounded-[14px] border border-dashed border-line px-6 py-10 text-center">
                      <motion.span animate={over ? { y: [-2, -7, -2] } : { y: 0 }} transition={{ duration: 0.9, repeat: over ? Infinity : 0 }} className="flex size-12 items-center justify-center rounded-xl border border-line bg-raised text-accent">
                        <Upload className="size-5" />
                      </motion.span>
                      <span className="text-[15px] font-medium tracking-[-0.01em] text-ink">{over ? "Drop to analyse" : "Drop a Spotflow export"}</span>
                      <span className="text-[12.5px] text-muted">or click to choose a .csv · raw or anonymised · up to 60 MB</span>
                    </div>
                  </button>
                </BorderBeam>
                <input ref={input} type="file" accept=".csv,text/csv" className="hidden" onChange={(e) => accept(e.target.files?.[0])} />
                {error && <p className="mt-3 text-center text-[12.5px] text-bad">{error}</p>}
              </motion.div>
            ) : (
              <motion.div key="flow" initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="flex w-full flex-col items-center">
                <FlowNode kind="Trigger" tone="red" icon={<Zap />} title="File received" detail={file ? `${file.name} · ${(file.size / 1024).toFixed(0)} KB` : undefined} state="done" />
                <FlowEdge active={steps.parse.state === "active"} done={steps.parse.state === "done"} />
                <FlowNode kind="Parse" tone="violet" icon={<FileText />} title="Read rows, recover merged transactions" detail={steps.parse.detail} state={steps.parse.state} />
                <FlowEdge active={steps.protect.state === "active"} done={steps.protect.state === "done"} />
                <FlowNode kind="Protect" tone="blue" icon={<ShieldCheck />} title="Pseudonymise customers" detail={steps.protect.detail} state={steps.protect.state} />
                <FlowEdge active={steps.check.state === "active"} done={steps.check.state !== "waiting" && steps.check.state !== "active"} />
                <FlowNode kind="Check" tone="amber" icon={<Filter />} title="Validate providers and regions" detail={steps.check.detail} state={steps.check.state} />
                <FlowEdge active={steps.save.state === "active"} done={steps.save.state === "done"} />
                <FlowNode kind="Save" tone="green" icon={<Check />} title="Store transactions" detail={steps.save.detail ?? (phase === "review" ? "Ready when you are" : undefined)} state={steps.save.state} />
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* Step details */}
        <aside className="rounded-2xl bg-raised p-5 shadow-[var(--shadow-card)]">
          {phase === "idle" && <IdleDetails />}
          {(phase === "analysing" || phase === "error") && (
            <div className="space-y-3">
              <h3 className="text-[15px] font-semibold tracking-[-0.01em]">Step details</h3>
              <p className="text-[13px] text-muted">{phase === "error" ? error : "Working through the file. This takes a second or two."}</p>
              {phase === "error" && <MorphButton size="sm" icon={<RotateCcw />} onClick={reset}>Try another file</MorphButton>}
            </div>
          )}
          {(phase === "review" || phase === "saving") && result && (
            <Review result={result} previous={previous} adding={adding} onAdd={addToRegistry} error={error} saving={phase === "saving"} onSave={save} />
          )}
          {phase === "done" && saved && (
            <div className="space-y-4">
              <span className="flex size-11 items-center justify-center rounded-full bg-ok-dim text-ok"><Check className="size-5" /></span>
              <div>
                <h3 className="text-[16px] font-semibold tracking-[-0.01em]">Export saved</h3>
                <p className="mt-1 text-[13px] text-muted">{fmtInt(saved.inserted)} new transactions{saved.updated ? `, ${fmtInt(saved.updated)} updated` : ""} · {fmtRange(saved.from, saved.to)}</p>
                {!saved.stored && <p className="mt-2 text-[12px] text-warn">The scrubbed copy couldn&apos;t be stored, so this upload can&apos;t be reprocessed later.</p>}
              </div>
              <div className="flex flex-wrap gap-2">
                <MorphButton icon={<ArrowRight />} variant="primary" onClick={() => router.push(`/app/uploads/${saved.uploadId}`)}>Open report</MorphButton>
                <MorphButton icon={<Plus />} onClick={reset}>Upload another</MorphButton>
              </div>
            </div>
          )}
        </aside>
      </div>
    </div>
  );
}

function IdleDetails() {
  const rows = [
    ["Parse", "Every row, including transactions merged onto one line"],
    ["Protect", "Emails become stable pseudonyms; phone numbers in messages are masked"],
    ["Check", "Unknown providers or regions are flagged, never dropped silently"],
    ["Save", "Re-uploading the same file updates rows instead of double-counting"],
  ];
  return (
    <div className="space-y-4">
      <h3 className="text-[15px] font-semibold tracking-[-0.01em]">What happens to the file</h3>
      <ol className="space-y-3">
        {rows.map(([k, v], i) => (
          <li key={k} className="flex gap-3">
            <span className="tnum flex size-6 shrink-0 items-center justify-center rounded-md bg-surface-2 text-[12px] text-muted">{i + 1}</span>
            <span className="text-[13px] leading-snug"><span className="font-medium text-ink">{k}.</span> <span className="text-muted">{v}</span></span>
          </li>
        ))}
      </ol>
    </div>
  );
}

function Review({ result, previous, adding, onAdd, error, saving, onSave }: {
  result: ParseResult; previous: Previous | null; adding: string | null; error: string | null; saving: boolean;
  onAdd: (kind: "provider" | "region", name: string) => void; onSave: () => void;
}) {
  const { stats, transactions } = result;
  const ok = transactions.filter((t) => t.status === "successful");
  const successUsd = ok.reduce((s, t) => s + t.amountUsd, 0);
  const failedUsd = transactions.filter((t) => t.status === "failed").reduce((s, t) => s + t.amountUsd, 0);
  const unknown = [
    ...Object.entries(stats.unknownProviders).map(([name, n]) => ({ kind: "provider" as const, name, n })),
    ...Object.entries(stats.unknownRegions).map(([name, n]) => ({ kind: "region" as const, name, n })),
  ];
  const period = stats.firstPaidAt && stats.lastPaidAt ? fmtRange(stats.firstPaidAt.slice(0, 10), stats.lastPaidAt.slice(0, 10)) : "No timestamps";
  const tiles = [
    ["Transactions", fmtInt(stats.transactions)],
    ["Success rate", fmtPct(ok.length / stats.transactions)],
    ["Success volume", fmtUsd(successUsd)],
    ["Failed volume", fmtUsd(failedUsd)],
  ];
  const facts: [string, string, boolean?][] = [
    ["Period", period],
    ["Source rows", fmtInt(stats.sourceRows)],
    ["Recovered from merged rows", fmtInt(stats.recoveredFragments)],
    ["Rows with no transaction", fmtInt(stats.rowsWithoutTransaction), stats.rowsWithoutTransaction > 0],
    ["Without a date", fmtInt(stats.missingDates), stats.missingDates > 0],
    ["Financial columns", stats.hasFinancials ? "Present" : "Missing (volumes $0)", !stats.hasFinancials],
  ];
  return (
    <div className="flex h-full flex-col gap-5">
      <h3 className="text-[15px] font-semibold tracking-[-0.01em]">Step details</h3>
      <div className="grid grid-cols-2 gap-2.5">
        {tiles.map(([k, v]) => (
          <div key={k} className="rounded-xl border border-line px-3.5 py-3">
            <div className="text-[12px] text-faint">{k}</div>
            <div className="mt-1 text-[17px] font-semibold tracking-[-0.02em]">{v}</div>
          </div>
        ))}
      </div>
      <dl className="space-y-2 text-[13px]">
        {facts.map(([k, v, warn]) => (
          <div key={k} className="flex items-baseline justify-between gap-4 border-b border-line-soft pb-2 last:border-0">
            <dt className="text-muted">{k}</dt>
            <dd className={cn("tnum text-right", warn ? "text-warn" : "text-ink")}>{v}</dd>
          </div>
        ))}
      </dl>
      {previous && (
        <div className="flex gap-2.5 rounded-xl bg-warn-dim px-3.5 py-3 text-[12.5px] text-ink">
          <AlertTriangle className="mt-0.5 size-4 shrink-0 text-warn" />
          <span>Uploaded {previous.uploader_email ? `by ${previous.uploader_email} ` : ""}on {fmtDateTime(previous.created_at)}. Saving again updates the same rows.</span>
        </div>
      )}
      {unknown.length > 0 && (
        <div className="space-y-2.5 rounded-xl bg-warn-dim px-3.5 py-3">
          <p className="flex gap-2.5 text-[12.5px] text-ink"><AlertTriangle className="mt-0.5 size-4 shrink-0 text-warn" />These rows aren&apos;t counted until their values are recognised. Add them if they&apos;re genuine.</p>
          <ul className="flex flex-wrap gap-1.5">
            {unknown.map((u) => (
              <li key={`${u.kind}:${u.name}`} className="flex items-center gap-1.5 rounded-lg bg-raised py-1 pl-2.5 pr-1 text-[12px] shadow-[var(--shadow-card)]">
                <span className="text-faint">{u.kind}</span><span>{u.name}</span><span className="tnum text-muted">×{u.n}</span>
                <button onClick={() => onAdd(u.kind, u.name)} disabled={adding !== null} className="cursor-pointer rounded-md bg-ink px-2 py-0.5 text-[11.5px] font-medium text-raised disabled:opacity-50">
                  {adding === u.name ? "Adding…" : "Add"}
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}
      {error && <p className="rounded-xl bg-bad-dim px-3.5 py-3 text-[12.5px] text-bad">{error}</p>}
      <div className="mt-auto flex gap-2 pt-2">
        <MorphButton className="flex-1" variant="solid" icon={<Upload />} hoverIcon={<Check />} onClick={onSave} disabled={saving}>
          {saving ? "Saving…" : previous ? "Save again" : "Save"}
        </MorphButton>
      </div>
    </div>
  );
}
