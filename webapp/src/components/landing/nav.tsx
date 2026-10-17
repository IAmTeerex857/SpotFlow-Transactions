"use client";

import { AnimatePresence, motion } from "motion/react";
import { ArrowRight, Menu, X } from "lucide-react";
import { MorphButton } from "@/components/ui/morph-button";
import Link from "next/link";
import { useEffect, useState } from "react";
import { Wordmark } from "@/components/brand/logo";
import { cn } from "@/lib/utils";

const LINKS = [
  { href: "#how", label: "How it works" },
  { href: "#product", label: "Product" },
];

export function Nav() {
  const [scrolled, setScrolled] = useState(false);
  const [open, setOpen] = useState(false);
  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 8);
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);
  return (
    <header className={cn("sticky top-0 z-50 border-b transition-[background-color,border-color] duration-200", scrolled ? "border-[#e6e9f0] bg-[#f7f8fa]/85 backdrop-blur-xl" : "border-transparent bg-transparent")}>
      <div className="mx-auto grid h-16 max-w-[1240px] grid-cols-[1fr_auto] items-center px-5 sm:px-8 md:grid-cols-[1fr_auto_1fr]">
        <Link href="/" className="text-ink" aria-label="Spotflow Insights home"><Wordmark className="h-[19px] w-auto" /></Link>
        <nav className="hidden items-center gap-1 md:flex">
          {LINKS.map((l) => (
            <a key={l.href} href={l.href} className="group relative rounded-full px-3.5 py-2 text-[14px] text-muted transition-colors hover:text-ink">
              {l.label}
              <span className="absolute inset-x-3.5 bottom-1 h-px origin-left scale-x-0 bg-ink/40 transition-transform duration-300 group-hover:scale-x-100" />
            </a>
          ))}
        </nav>
        <div className="hidden items-center justify-end gap-1.5 md:flex">
          <MorphButton href="/login" shape="pill" icon={<ArrowRight />} hoverIcon={<ArrowRight />} interaction="slide" trailing textReveal>Sign in</MorphButton>
        </div>
        <button onClick={() => setOpen((v) => !v)} className="cursor-pointer justify-self-end rounded-full p-2 text-ink md:hidden" aria-label={open ? "Close menu" : "Open menu"} aria-expanded={open}>
          {open ? <X className="size-5" /> : <Menu className="size-5" />}
        </button>
      </div>
      <AnimatePresence>
        {open && (
          <motion.div initial={{ height: 0, opacity: 0 }} animate={{ height: "auto", opacity: 1 }} exit={{ height: 0, opacity: 0 }} className="overflow-hidden border-t border-line bg-[#f7f8fa] md:hidden">
            <div className="flex flex-col gap-1 px-5 py-4">
              {LINKS.map((l) => <a key={l.href} href={l.href} onClick={() => setOpen(false)} className="rounded-xl px-3 py-2.5 text-[15px] text-ink hover:bg-black/[0.04]">{l.label}</a>)}
              <Link href="/login" className="mt-2 flex h-11 items-center justify-center rounded-full bg-ink text-[14.5px] font-medium text-white">Sign in with Google</Link>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </header>
  );
}
