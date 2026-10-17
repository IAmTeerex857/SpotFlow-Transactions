"use client";

import { AnimatePresence, motion } from "motion/react";
import {
  BarChart3, History, Home, Lock, LogOut, Menu, MessageSquareText, PanelLeftClose, PanelLeftOpen, Search, Upload, X, type LucideIcon,
} from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState, useSyncExternalStore } from "react";
import { Mark } from "@/components/brand/logo";
import { MorphButton } from "@/components/ui/morph-button";
import { ThemeToggle } from "@/components/ui/theme-toggle";
import { fmtDate, fmtInt } from "@/lib/format";
import { cn } from "@/lib/utils";
import { CommandMenu, openCommandMenu } from "./command-menu";

interface ShellUser {
  email: string;
  name: string | null;
  avatarUrl: string | null;
}

interface LatestExport {
  fileName: string;
  businessDate: string | null;
  transactions: number;
  createdAt: string;
}

interface NavItem { href: string; label: string; icon: LucideIcon; demoOpen?: boolean }

const SECTIONS: { label: string; items: NavItem[] }[] = [
  { label: "Overview", items: [
    { href: "", label: "Home", icon: Home, demoOpen: true },
    { href: "/reports", label: "Reports", icon: BarChart3, demoOpen: true },
  ] },
  { label: "Data", items: [
    { href: "/upload", label: "Upload export", icon: Upload },
    { href: "/uploads", label: "Upload history", icon: History },
  ] },
  { label: "Assist", items: [{ href: "/ask", label: "Ask", icon: MessageSquareText }] },
];

// ── Collapsed sidebar state: on <html data-sidebar>, saved, applied before paint ──
function subscribe(callback: () => void) {
  const observer = new MutationObserver(callback);
  observer.observe(document.documentElement, { attributes: true, attributeFilter: ["data-sidebar"] });
  return () => observer.disconnect();
}
const getCollapsed = () => document.documentElement.dataset.sidebar === "collapsed";

function useCollapsed() {
  return useSyncExternalStore(subscribe, getCollapsed, () => false);
}

function toggleSidebar() {
  const root = document.documentElement;
  const next = !getCollapsed();
  if (next) root.dataset.sidebar = "collapsed";
  else delete root.dataset.sidebar;
  try {
    localStorage.setItem("sf-sidebar", next ? "collapsed" : "expanded");
  } catch {}
}

function initials(user: ShellUser) {
  const source = user.name ?? user.email;
  return source.split(/[\s.@]+/).filter(Boolean).slice(0, 2).map((s) => s[0]?.toUpperCase()).join("");
}

function timeAgo(iso: string) {
  const mins = Math.max(0, Math.round((Date.now() - Date.parse(iso)) / 60000));
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.round(mins / 60);
  return hours < 48 ? `${hours}h ago` : `${Math.round(hours / 24)}d ago`;
}

/** Hover label for icon-only rail items. */
function RailTip({ show, children }: { show: boolean; children: React.ReactNode }) {
  if (!show) return null;
  return (
    <span className="pointer-events-none absolute left-full top-1/2 z-50 ml-3 -translate-y-1/2 whitespace-nowrap rounded-md bg-ink px-2 py-1 text-[12px] font-medium text-raised opacity-0 shadow-[var(--shadow-pop)] transition-opacity duration-150 group-hover/item:opacity-100">
      {children}
    </span>
  );
}

function Nav({ base, demo, rail, onNavigate }: { base: string; demo: boolean; rail: boolean; onNavigate?: () => void }) {
  const pathname = usePathname();
  return (
    <nav className="space-y-5">
      {SECTIONS.map((section) => (
        <div key={section.label}>
          {rail ? <div className="mx-auto mb-2 h-px w-6 bg-line first:hidden" /> : <div className="px-2.5 pb-1.5 text-[12px] text-faint">{section.label}</div>}
          <ul className="space-y-0.5">
            {section.items.map((item) => {
              const href = `${base}${item.href}`;
              const locked = demo && !item.demoOpen;
              const active = item.href === "" ? pathname === base : pathname.startsWith(href);
              return (
                <li key={item.label}>
                  <Link
                    href={locked ? "/login" : href}
                    onClick={onNavigate}
                    title={locked ? "Sign in to use this" : undefined}
                    aria-label={rail ? item.label : undefined}
                    className={cn(
                      "group/item relative flex items-center gap-2.5 rounded-lg py-[7px] text-[13.5px] transition-colors",
                      rail ? "justify-center px-0" : "px-2.5",
                      active ? "text-ink" : locked ? "text-faint hover:text-muted" : "text-muted hover:bg-surface-2 hover:text-ink",
                    )}
                  >
                    {active && (
                      <motion.span layoutId="nav-active" className="absolute inset-0 rounded-lg bg-surface-2" transition={{ type: "spring", stiffness: 500, damping: 40 }} />
                    )}
                    <item.icon className="relative size-4 shrink-0" />
                    {!rail && <span className="relative flex-1 truncate">{item.label}</span>}
                    {!rail && locked && <Lock className="relative size-3" />}
                    <RailTip show={rail}>{item.label}{locked ? " · sign in" : ""}</RailTip>
                  </Link>
                </li>
              );
            })}
          </ul>
        </div>
      ))}
    </nav>
  );
}

function LatestExportCard({ latest, base, demo }: { latest: LatestExport | null; base: string; demo: boolean }) {
  return (
    <div className="rounded-xl border border-line bg-bg p-3.5">
      <div className="flex items-center gap-2">
        <span className="relative flex size-1.5">
          <span className="absolute inset-0 animate-ping rounded-full bg-ok opacity-60" />
          <span className="relative size-1.5 rounded-full bg-ok" />
        </span>
        <span className="text-[12.5px] font-medium text-ink">{demo ? "Sample data" : "Latest export"}</span>
      </div>
      {latest ? (
        <p className="mt-1.5 text-[12px] leading-relaxed text-muted">
          {latest.businessDate ? fmtDate(latest.businessDate, { month: "short", day: "numeric" }) : latest.fileName} · {fmtInt(latest.transactions)} txns
          <br />
          <span className="text-faint">Uploaded {timeAgo(latest.createdAt)}</span>
        </p>
      ) : (
        <p className="mt-1.5 text-[12px] text-muted">{demo ? "Fictional merchants, real-shaped data." : "Nothing uploaded yet."}</p>
      )}
      <MorphButton
        href={demo ? "/login" : `${base}/upload`}
        variant="solid"
        size="sm"
        full
        className="mt-3"
        icon={<Upload />}
        hoverIcon={<Upload />}
        interaction="slide"
      >
        {demo ? "Sign in to upload" : "Upload new export"}
      </MorphButton>
    </div>
  );
}

function Avatar({ user, size = "size-7" }: { user: ShellUser; size?: string }) {
  return user.avatarUrl ? (
    // eslint-disable-next-line @next/next/no-img-element -- Google avatar, tiny
    <img src={user.avatarUrl} alt="" className={cn(size, "rounded-full")} referrerPolicy="no-referrer" />
  ) : (
    <span className={cn(size, "flex items-center justify-center rounded-full bg-accent-dim text-[11px] font-medium text-accent")}>{initials(user)}</span>
  );
}

/** Avatar button in the top bar with a small account menu. */
function AccountMenu({ user, demo }: { user: ShellUser | null; demo: boolean }) {
  const [open, setOpen] = useState(false);
  useEffect(() => {
    if (!open) return;
    const close = (e: KeyboardEvent) => e.key === "Escape" && setOpen(false);
    window.addEventListener("keydown", close);
    return () => window.removeEventListener("keydown", close);
  }, [open]);
  if (!user) {
    return <MorphButton href="/login" variant="primary" size="sm" shape="pill" textReveal>{demo ? "Sign in" : "Sign in"}</MorphButton>;
  }
  return (
    <div className="relative">
      <button onClick={() => setOpen((v) => !v)} className="flex cursor-pointer items-center gap-2 rounded-full p-0.5 pr-2.5 transition-colors hover:bg-surface-2" aria-haspopup="menu" aria-expanded={open}>
        <Avatar user={user} />
        <span className="hidden max-w-[140px] truncate text-[13px] font-medium text-ink sm:block">{user.name ?? user.email.split("@")[0]}</span>
      </button>
      <AnimatePresence>
        {open && (
          <>
            <div className="fixed inset-0 z-40" onClick={() => setOpen(false)} />
            <motion.div
              role="menu"
              initial={{ opacity: 0, y: -6, scale: 0.98 }}
              animate={{ opacity: 1, y: 0, scale: 1 }}
              exit={{ opacity: 0, y: -6, scale: 0.98 }}
              transition={{ duration: 0.14 }}
              className="absolute right-0 top-full z-50 mt-2 w-[240px] origin-top-right rounded-xl bg-raised p-1.5 shadow-[var(--shadow-pop)]"
            >
              <div className="flex items-center gap-2.5 px-2.5 py-2">
                <Avatar user={user} size="size-8" />
                <div className="min-w-0">
                  <div className="truncate text-[13px] font-medium text-ink">{user.name ?? user.email.split("@")[0]}</div>
                  <div className="truncate text-[11.5px] text-faint">{user.email}</div>
                </div>
              </div>
              <div className="my-1 h-px bg-line" />
              <form action="/api/auth/signout" method="post">
                <button role="menuitem" className="flex w-full cursor-pointer items-center gap-2.5 rounded-lg px-2.5 py-2 text-[13px] text-muted hover:bg-surface-2 hover:text-ink">
                  <LogOut className="size-3.5" /> Sign out
                </button>
              </form>
            </motion.div>
          </>
        )}
      </AnimatePresence>
    </div>
  );
}

function SearchButton({ className }: { className?: string }) {
  return (
    <button
      onClick={openCommandMenu}
      className={cn("group flex h-9 cursor-pointer items-center gap-2 rounded-[10px] border border-line bg-raised px-3 text-[13px] text-faint transition-colors hover:border-[var(--accent-line)] hover:text-muted", className)}
    >
      <Search className="size-3.5 transition-transform duration-200 group-hover:scale-110" />
      <span className="flex-1 text-left">Search or jump to…</span>
      <kbd className="rounded-[5px] border border-line bg-bg px-1.5 py-px text-[10.5px] font-medium text-faint">⌘K</kbd>
    </button>
  );
}

export function AppShell({ user, latest = null, demo = false, children }: {
  user: ShellUser | null; latest?: LatestExport | null; demo?: boolean; children: React.ReactNode;
}) {
  const base = demo ? "/demo" : "/app";
  const [open, setOpen] = useState(false);
  const collapsed = useCollapsed();

  useEffect(() => {
    // ⌘B / Ctrl+B collapses the sidebar, like most editors.
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "b") {
        e.preventDefault();
        toggleSidebar();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const sidebar = (rail: boolean, onNavigate?: () => void) => (
    <div className={cn("flex h-full flex-col gap-5 py-4", rail ? "items-stretch px-2.5" : "px-3")}>
      <Link href={base} onClick={onNavigate} className={cn("flex items-center gap-2.5", rail ? "justify-center" : "px-1.5")} aria-label="Spotflow Insights">
        <span className="flex size-8 shrink-0 items-center justify-center rounded-[9px] bg-accent text-white shadow-[inset_0_1px_0_rgba(255,255,255,.2)]">
          <Mark className="size-[18px]" />
        </span>
        {!rail && (
          <span className="leading-tight">
            <span className="block text-[14px] font-semibold tracking-[-0.01em] text-ink">Spotflow</span>
            <span className="block text-[11.5px] text-faint">Insights{demo ? " · demo" : ""}</span>
          </span>
        )}
      </Link>
      <Nav base={base} demo={demo} rail={rail} onNavigate={onNavigate} />
      <div className="mt-auto space-y-2">
        {rail ? (
          <Link
            href={demo ? "/login" : `${base}/upload`}
            aria-label="Upload new export"
            className="group/item relative mx-auto flex size-9 items-center justify-center rounded-lg bg-ink text-raised transition-opacity hover:opacity-90"
          >
            <Upload className="size-4" />
            <RailTip show>Upload new export</RailTip>
          </Link>
        ) : (
          <LatestExportCard latest={latest} base={base} demo={demo} />
        )}
      </div>
    </div>
  );

  return (
    <div className={cn("app-shell min-h-dvh lg:grid", collapsed && "is-collapsed")}>
      <aside className="sticky top-0 hidden h-dvh overflow-y-auto overflow-x-hidden border-r border-line bg-raised lg:block">
        {sidebar(collapsed)}
      </aside>

      <div className="min-w-0">
        <header className="sticky top-0 z-30 flex h-14 items-center gap-2 border-b border-line bg-bg/80 px-3 backdrop-blur-xl sm:px-5 lg:px-6">
          <Link href={base} className="mr-1 flex items-center gap-2 lg:hidden">
            <span className="flex size-7 items-center justify-center rounded-[8px] bg-accent text-white"><Mark className="size-4" /></span>
          </Link>
          <span className="hidden lg:block">
            <MorphButton
              onClick={toggleSidebar}
              variant="ghost"
              size="sm"
              icon={collapsed ? <PanelLeftOpen /> : <PanelLeftClose />}
              hoverIcon={collapsed ? <PanelLeftClose /> : <PanelLeftOpen />}
              aria-label={collapsed ? "Expand sidebar (⌘B)" : "Collapse sidebar (⌘B)"}
            />
          </span>
          <span className="mx-1 hidden h-5 w-px bg-line lg:block" />
          <SearchButton className="hidden w-[300px] sm:flex" />
          <div className="ml-auto flex items-center gap-1.5">
            <button onClick={openCommandMenu} className="cursor-pointer rounded-lg p-2 text-muted hover:text-ink sm:hidden" aria-label="Search"><Search className="size-4" /></button>
            <ThemeToggle />
            <span className="mx-1 h-5 w-px bg-line" />
            <AccountMenu user={user} demo={demo} />
            <button onClick={() => setOpen(true)} className="cursor-pointer rounded-lg p-2 text-muted hover:text-ink lg:hidden" aria-label="Open menu"><Menu className="size-5" /></button>
          </div>
        </header>

        <main className="min-w-0 px-4 pb-16 pt-6 sm:px-6 lg:px-8 lg:pt-7">
          <div className="max-w-[1680px]">{children}</div>
        </main>
      </div>

      <AnimatePresence>
        {open && (
          <>
            <motion.div className="fixed inset-0 z-40 bg-black/30 lg:hidden" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} onClick={() => setOpen(false)} />
            <motion.aside
              className="fixed inset-y-0 left-0 z-50 w-[280px] border-r border-line bg-raised lg:hidden"
              initial={{ x: -300 }} animate={{ x: 0 }} exit={{ x: -300 }} transition={{ type: "spring", stiffness: 400, damping: 40 }}
            >
              <button onClick={() => setOpen(false)} className="absolute right-3 top-4 z-10 cursor-pointer rounded-lg p-1.5 text-muted hover:text-ink" aria-label="Close menu"><X className="size-5" /></button>
              {sidebar(false, () => setOpen(false))}
            </motion.aside>
          </>
        )}
      </AnimatePresence>
      <CommandMenu base={base} demo={demo} />
    </div>
  );
}
