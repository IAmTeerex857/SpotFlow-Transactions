"use client";

import { AnimatePresence, motion } from "motion/react";
import {
  ArrowDown, ArrowUp, BarChart3, Clock3, CornerDownLeft, Globe2, History, Home, Lock, MessageSquareText, Moon, PanelLeft,
  Search, Sparkles, Store, Sun, TriangleAlert, Upload, Waypoints, type LucideIcon,
} from "lucide-react";
import { useRouter } from "next/navigation";
import { useEffect, useMemo, useRef, useState } from "react";
import { setTheme, useTheme } from "@/components/ui/theme-toggle";
import { cn } from "@/lib/utils";

interface Command {
  id: string;
  group: string;
  label: string;
  hint?: string;
  icon: LucideIcon;
  keywords?: string;
  locked?: boolean;
  shortcut?: string;
  run: () => void;
}

const GROUP_ORDER = ["Ask", "Go to", "Report views", "Preferences"];

/** ⌘K: jump to any page or report view, ask a question, switch theme. */
export function CommandMenu({ base, demo }: { base: string; demo: boolean }) {
  const router = useRouter();
  const theme = useTheme();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(0);
  const list = useRef<HTMLDivElement>(null);

  const commands: Command[] = useMemo(() => {
    const go = (path: string, locked = false) => () => {
      router.push(locked ? "/login" : `${base}${path}`);
      // Same-page hash jumps don't fire hashchange under the app router.
      if (path.includes("#")) setTimeout(() => window.dispatchEvent(new Event("sf:tab")), 60);
    };
    const view = (tab: string, label: string, icon: LucideIcon, hint: string): Command => ({
      id: `view-${tab}`, group: "Report views", label, hint, icon, run: go(`/reports#${tab}`), keywords: "report tab",
    });
    return [
      { id: "home", group: "Go to", label: "Home", hint: "Summary and quick actions", icon: Home, run: go("") },
      { id: "reports", group: "Go to", label: "Reports", hint: "Filters, merchants, providers, regions", icon: BarChart3, run: go("/reports") },
      { id: "upload", group: "Go to", label: "Upload an export", hint: "Drop the daily CSV", icon: Upload, locked: demo, run: go("/upload", demo), keywords: "csv import new" },
      { id: "history", group: "Go to", label: "Upload history", hint: "Every export, reprocess or delete", icon: History, locked: demo, run: go("/uploads", demo) },
      { id: "ask", group: "Go to", label: "Ask", hint: "Questions answered from your data", icon: MessageSquareText, locked: demo, run: go("/ask", demo), keywords: "ai question assistant" },
      view("merchants", "Merchants", Store, "Volume and success rate by merchant"),
      view("providers", "Providers", Waypoints, "Which providers convert and which slip"),
      view("regions", "Regions", Globe2, "Every market side by side"),
      view("failures", "Failure reasons", TriangleAlert, "Why payments fail, grouped"),
      view("retries", "Retries", History, "Who tried again and whether they paid"),
      view("peaks", "Peak hours", Clock3, "When failures cluster"),
      {
        id: "theme", group: "Preferences", label: theme === "dark" ? "Switch to light mode" : "Switch to dark mode",
        icon: theme === "dark" ? Sun : Moon, keywords: "theme dark light appearance",
        run: () => setTheme(theme === "dark" ? "light" : "dark"),
      },
      {
        id: "sidebar", group: "Preferences", label: "Toggle sidebar", icon: PanelLeft, shortcut: "⌘B", keywords: "collapse expand menu",
        run: () => window.dispatchEvent(new KeyboardEvent("keydown", { key: "b", metaKey: true })),
      },
    ];
  }, [base, demo, router, theme]);

  const q = query.trim().toLowerCase();
  const filtered = useMemo(() => {
    const matches = commands.filter((c) => !q || `${c.label} ${c.hint ?? ""} ${c.keywords ?? ""}`.toLowerCase().includes(q));
    if (q.length > 2) {
      matches.unshift({
        id: "ask-q", group: "Ask", label: `Ask “${query.trim()}”`, hint: "Answered from your uploaded data", icon: Sparkles, locked: demo,
        run: () => router.push(demo ? "/login" : `${base}/ask?q=${encodeURIComponent(query.trim())}`),
      });
    }
    return matches.sort((a, b) => GROUP_ORDER.indexOf(a.group) - GROUP_ORDER.indexOf(b.group));
  }, [commands, q, query, demo, base, router]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setQuery("");
        setActive(0);
        setOpen((v) => !v);
      }
    };
    const onOpen = () => {
      setQuery("");
      setActive(0);
      setOpen(true);
    };
    window.addEventListener("keydown", onKey);
    window.addEventListener("sf:command-menu", onOpen);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("sf:command-menu", onOpen);
    };
  }, []);

  const run = (c: Command | undefined) => {
    if (!c) return;
    setOpen(false);
    c.run();
  };

  const move = (delta: number) => {
    setActive((a) => {
      const next = Math.max(0, Math.min(filtered.length - 1, a + delta));
      list.current?.querySelector(`[data-index="${next}"]`)?.scrollIntoView({ block: "nearest" });
      return next;
    });
  };

  const groups = GROUP_ORDER.map((g) => ({ g, items: filtered.filter((c) => c.group === g) })).filter((x) => x.items.length);

  return (
    <AnimatePresence>
      {open && (
        <div className="fixed inset-0 z-[60] flex items-start justify-center px-4 pt-[12vh]">
          <motion.div className="absolute inset-0 bg-[rgba(10,12,20,0.28)]" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} onClick={() => setOpen(false)} />
          <motion.div
            role="dialog"
            aria-label="Search and quick actions"
            initial={{ opacity: 0, y: -10, scale: 0.97 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -6, scale: 0.98 }}
            transition={{ type: "spring", stiffness: 500, damping: 36 }}
            className="relative w-full max-w-[580px] overflow-hidden rounded-2xl bg-raised shadow-[0_24px_64px_-16px_rgba(10,14,30,0.35),0_0_0_1px_var(--line)]"
          >
            <div className="flex h-[54px] items-center gap-3 border-b border-line px-4">
              <Search className="size-[18px] shrink-0 text-faint" />
              <input
                autoFocus
                value={query}
                onChange={(e) => { setQuery(e.target.value); setActive(0); }}
                onKeyDown={(e) => {
                  if (e.key === "ArrowDown") { e.preventDefault(); move(1); }
                  if (e.key === "ArrowUp") { e.preventDefault(); move(-1); }
                  if (e.key === "Enter") run(filtered[active]);
                  if (e.key === "Escape") setOpen(false);
                }}
                placeholder="Search pages, report views, or ask a question…"
                className="h-full w-full bg-transparent text-[15px] text-ink outline-none placeholder:text-faint focus-visible:shadow-none"
                aria-label="Search"
                role="combobox"
                aria-expanded
                aria-controls="cmdk-list"
                aria-activedescendant={filtered[active] ? `cmdk-${filtered[active].id}` : undefined}
              />
              <button onClick={() => setOpen(false)} className="shrink-0 cursor-pointer rounded-md border border-line px-1.5 py-0.5 text-[11px] font-medium text-faint hover:text-muted">Esc</button>
            </div>

            <div ref={list} id="cmdk-list" role="listbox" className="scroll-quiet max-h-[min(420px,60vh)] overflow-y-auto p-2">
              {groups.map(({ g, items }) => (
                <div key={g} className="mb-1 last:mb-0">
                  <div className="px-2.5 pb-1 pt-2 text-[11.5px] font-medium text-faint">{g}</div>
                  {items.map((c) => {
                    const i = filtered.indexOf(c);
                    const on = i === active;
                    return (
                      <button
                        key={c.id}
                        id={`cmdk-${c.id}`}
                        role="option"
                        aria-selected={on}
                        data-index={i}
                        onMouseMove={() => active !== i && setActive(i)}
                        onClick={() => run(c)}
                        className="relative flex w-full cursor-pointer items-center gap-3 rounded-[10px] px-2.5 py-2 text-left"
                      >
                        {on && <motion.span layoutId="cmdk-active" className="absolute inset-0 rounded-[10px] bg-surface-2" transition={{ type: "spring", stiffness: 700, damping: 45 }} />}
                        <span className={cn(
                          "relative flex size-8 shrink-0 items-center justify-center rounded-lg border transition-colors",
                          on ? "border-[var(--accent-line)] bg-accent-dim text-accent" : "border-line bg-raised text-muted",
                        )}>
                          <c.icon className="size-4" />
                        </span>
                        <span className="relative min-w-0 flex-1">
                          <span className="block truncate text-[13.5px] font-medium text-ink">{c.label}</span>
                          {c.hint && <span className="block truncate text-[12px] text-faint">{c.hint}</span>}
                        </span>
                        <span className="relative flex shrink-0 items-center gap-2 text-[11.5px] text-faint">
                          {c.locked && <span className="flex items-center gap-1"><Lock className="size-3" />Sign in</span>}
                          {c.shortcut && <kbd className="rounded-[5px] border border-line px-1.5 py-px font-sans text-[11px]">{c.shortcut}</kbd>}
                          {on && <CornerDownLeft className="size-3.5" />}
                        </span>
                      </button>
                    );
                  })}
                </div>
              ))}
              {!filtered.length && (
                <div className="px-3 py-10 text-center">
                  <p className="text-[13.5px] text-ink">No matches for “{query}”</p>
                  <p className="mt-1 text-[12.5px] text-faint">Try a page name like “providers” or “upload”.</p>
                </div>
              )}
            </div>

            <div className="flex items-center gap-4 border-t border-line bg-bg/60 px-4 py-2.5 text-[11.5px] text-faint">
              <span className="flex items-center gap-1.5"><Key><ArrowUp className="size-3" /></Key><Key><ArrowDown className="size-3" /></Key>Navigate</span>
              <span className="flex items-center gap-1.5"><Key><CornerDownLeft className="size-3" /></Key>Open</span>
              <span className="ml-auto flex items-center gap-1.5"><Key>⌘</Key><Key>K</Key>Toggle</span>
            </div>
          </motion.div>
        </div>
      )}
    </AnimatePresence>
  );
}

function Key({ children }: { children: React.ReactNode }) {
  return <kbd className="flex h-[18px] min-w-[18px] items-center justify-center rounded-[5px] border border-line bg-raised px-1 font-sans text-[10.5px] text-muted">{children}</kbd>;
}

export function openCommandMenu() {
  window.dispatchEvent(new Event("sf:command-menu"));
}
