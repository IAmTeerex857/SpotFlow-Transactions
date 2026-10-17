import type { Metadata } from "next";
import Link from "next/link";
import { ShieldCheck } from "lucide-react";
import { GoogleSignIn } from "@/components/auth/google-sign-in";
import { Wordmark } from "@/components/brand/logo";
import { ThemeToggle } from "@/components/ui/theme-toggle";
import { googleEnv } from "@/lib/auth/google";
import { MEMBER_DOMAIN, supabaseEnv } from "@/lib/supabase/env";

export const metadata: Metadata = { title: "Sign in" };

const ERRORS: Record<string, string> = {
  domain: `That account isn't on @${MEMBER_DOMAIN}. Sign in with your Spotflow Google account.`,
  oauth: "Google sign-in didn't complete. Please try again.",
  config: "Sign-in isn't configured yet. An admin needs to connect Supabase and Google.",
};

export default async function LoginPage({ searchParams }: PageProps<"/login">) {
  const query = await searchParams;
  const error = typeof query.error === "string" ? ERRORS[query.error] ?? ERRORS.oauth : null;
  const next = typeof query.next === "string" && query.next.startsWith("/app") ? query.next : "/app";
  const configured = supabaseEnv() !== null && googleEnv() !== null;

  return (
    <div className="relative grid min-h-dvh place-items-center overflow-hidden px-4">
      <div className="hairlines pointer-events-none absolute inset-0 [mask-image:radial-gradient(ellipse_at_center,black_15%,transparent_70%)]" />
      <div className="absolute right-4 top-4"><ThemeToggle /></div>
      <div className="rise relative w-full max-w-[420px]">
        <div className="rounded-3xl bg-raised p-9 shadow-[var(--shadow-pop)]">
          <Link href="/" className="inline-flex text-ink"><Wordmark className="h-5 w-auto" /></Link>
          <h1 className="mt-10 font-display text-[32px] font-semibold leading-[1.05] tracking-[-0.035em] text-ink">Sign in to Insights</h1>
          <p className="mt-3 text-[14px] leading-relaxed text-muted">Use your @{MEMBER_DOMAIN} Google account. Anyone on the team can upload exports and read reports.</p>
          {error && <p role="alert" className="mt-5 rounded-xl bg-bad-dim px-3.5 py-2.5 text-[13px] text-bad">{error}</p>}
          <div className="mt-7">
            {configured ? <GoogleSignIn next={next} /> : <p className="rounded-xl bg-warn-dim px-3.5 py-2.5 text-[13px] text-warn">{ERRORS.config}</p>}
          </div>
          <div className="mt-7 flex items-center gap-2 border-t border-line pt-5 text-[12px] text-faint">
            <ShieldCheck className="size-3.5" /> Other Google accounts are rejected before an account is created.
          </div>
        </div>
        <p className="mt-6 text-center text-[13px] text-muted">
          <Link href="/" className="font-medium text-accent hover:underline">← Back to the overview</Link>
        </p>
      </div>
    </div>
  );
}
