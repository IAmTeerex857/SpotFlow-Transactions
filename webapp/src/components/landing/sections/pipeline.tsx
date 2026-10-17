import { FileSpreadsheet, KeyRound, LayoutDashboard, Upload, Zap } from "lucide-react";
import type { CSSProperties } from "react";
import { InView, Reveal } from "@/components/landing/motion";
import { cn } from "@/lib/utils";
import { Heading, container } from "./heading";

// Dot-grid flow diagram (fit-check reference); a packet travels each connector
// and lights the next step (landing.css .pl-*).
function FlowCard({ icon, color, kicker, title, badge, className, step }: {
  icon: React.ReactNode; color: string; kicker: string; title: string; badge?: string; className?: string; step?: number;
}) {
  return (
    <div
      className={cn("pl-card flex items-center gap-3.5 rounded-2xl border border-[#e3e7ef] bg-white py-3 pl-3 pr-5 shadow-[0_6px_20px_-12px_rgba(16,24,40,.25)]", className)}
      style={step === undefined ? undefined : ({ "--step": step } as CSSProperties)}
    >
      <span className="flex size-11 shrink-0 items-center justify-center rounded-xl text-white [&_svg]:size-5" style={{ background: color }}>{icon}</span>
      <span className="text-left">
        {badge ? <span className="rounded-md bg-[#fde68a] px-1.5 py-0.5 text-[12px] text-[#713f12]">{badge}</span> : <span className="text-[12.5px] text-[#94a3b8]">{kicker}</span>}
        <span className="block text-[14.5px] font-medium text-[#0f172a]">{title}</span>
      </span>
    </div>
  );
}

const CONNECTORS = ["M262 126 C330 126 330 48 398 48", "M647 48 C730 48 720 124 778 124", "M914 160 C914 222 878 232 802 232", "M560 233 L386 234", "M258 236 L190 238"];

export function Pipeline() {
  const people = [["AO", "#3366ff"], ["KB", "#f2735f"], ["TS", "#32b67f"]];
  return (
    <section className="border-y border-line bg-white py-24 sm:py-32">
      <div className={container}>
        <Heading center eyebrow="Under the hood" title="What happens between the CSV and the chart." body="The same pipeline runs for every upload, and you can watch each step on the upload canvas." />
        <Reveal className="dot-grid relative mx-auto mt-14 max-w-[1100px] rounded-[28px] px-4 py-10 sm:py-16">
          <InView className="pl relative mx-auto hidden h-[320px] w-[1068px] lg:block">
            <svg className="absolute inset-0" width="1068" height="320" viewBox="0 0 1068 320" fill="none" aria-hidden>
              <defs><marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="8" markerHeight="8" orient="auto"><path d="M0,0 L8,4 L0,8" fill="none" stroke="#8a94a6" strokeWidth="1.3" /></marker></defs>
              {CONNECTORS.map((d) => (
                <path key={d} d={d} stroke="#8a94a6" strokeWidth="1.3" strokeDasharray="4 5" className="pl-dash" markerEnd="url(#arr)" />
              ))}
            </svg>
            {CONNECTORS.map((d, i) => (
              <span key={d} aria-hidden className="pl-dot absolute left-0 top-0 size-2.5 rounded-full bg-accent shadow-[0_0_0_4px_rgba(51,102,255,.18)]" style={{ offsetPath: `path("${d}")`, "--step": i } as CSSProperties} />
            ))}
            <FlowCard step={0} className="absolute left-[30px] top-[92px] w-[230px]" icon={<Upload />} color="#3366ff" kicker="Upload" title="Drop the daily CSV" />
            <FlowCard step={1} className="absolute left-[405px] top-[14px] w-[240px]" icon={<FileSpreadsheet />} color="#f2735f" kicker="Parse" title="Recover merged rows" />
            <FlowCard step={2} className="absolute left-[784px] top-[90px] w-[260px]" icon={<KeyRound />} color="#8b6cf0" kicker="Protect" title="Pseudonymise customers" />
            <FlowCard step={3} className="absolute left-[560px] top-[198px] w-[236px]" icon={<Zap />} color="#f1b24a" kicker="" badge="Store" title="Row-level security" />
            <div className="pl-card absolute left-[262px] top-[208px] flex items-center rounded-full border border-[#e3e7ef] bg-white p-1 shadow-[0_6px_20px_-12px_rgba(16,24,40,.25)]" style={{ "--step": 4 } as CSSProperties}>
              {people.map(([p, c], i) => (
                <span key={p} className={cn("flex size-11 items-center justify-center rounded-full border-2 border-white text-[12.5px] font-medium text-white", i && "-ml-3")} style={{ background: c }}>{p}</span>
              ))}
            </div>
            <span className="pl-card absolute left-[60px] top-[222px] flex items-center gap-2 rounded-lg border border-[#e3e7ef] bg-white px-2.5 py-1.5 text-[13px] text-[#0f172a]" style={{ "--step": 5 } as CSSProperties}>
              <LayoutDashboard className="size-3.5 text-[#8b6cf0]" />Report ready
            </span>
          </InView>
          <div className="flex flex-col items-center gap-3 lg:hidden">
            {[
              [<Upload key="u" />, "#3366ff", "Upload", "Drop the daily CSV"],
              [<FileSpreadsheet key="f" />, "#f2735f", "Parse", "Recover merged rows"],
              [<KeyRound key="k" />, "#8b6cf0", "Protect", "Pseudonymise customers"],
              [<Zap key="z" />, "#f1b24a", "Store", "Row-level security"],
              [<LayoutDashboard key="l" />, "#32b67f", "Read", "Report ready for the team"],
            ].map(([icon, color, kicker, title], i) => (
              <Reveal key={title as string} delay={i * 0.08}>
                <FlowCard icon={icon} color={color as string} kicker={kicker as string} title={title as string} />
              </Reveal>
            ))}
          </div>
        </Reveal>
      </div>
    </section>
  );
}
