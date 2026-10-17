import Link from "next/link";
import { Wordmark } from "@/components/brand/logo";
import { container } from "./sections/heading";

export function Footer() {
  return (
    <footer className="border-t border-line">
      <div className={`${container} flex flex-col gap-4 py-10 text-[13px] text-faint sm:flex-row sm:items-center sm:justify-between`}>
        <div className="flex items-center gap-3 text-ink"><Wordmark className="h-4 w-auto" /><span className="text-faint">Insights · internal tool for the Spotflow team</span></div>
        <div className="flex gap-5">
          <a href="#how" className="hover:text-ink">How it works</a>
          <a href="#product" className="hover:text-ink">Product</a>
          <Link href="/login" className="hover:text-ink">Sign in</Link>
        </div>
        <span>© 2026 Spotflow</span>
      </div>
    </footer>
  );
}
