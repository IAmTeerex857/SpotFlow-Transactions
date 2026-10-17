import { HeroCtas } from "./cta";
import { HeroScene } from "./hero-scene";

const LINES = [["Every", "failed", "payment,"], ["explained."]];

export function Hero() {
  let w = 0;
  return (
    <section className="relative h-[900px] overflow-hidden sm:h-[1000px] lg:h-[940px]">
      {/* Chatsheet frame: hairline rails either side of the content column */}
      <div aria-hidden className="pointer-events-none absolute inset-y-0 left-1/2 hidden w-[1240px] -translate-x-1/2 border-x border-[#e9ecf3] xl:block" />
      <HeroScene />
      <div aria-hidden className="pointer-events-none absolute inset-x-0 bottom-0 h-40 bg-gradient-to-b from-transparent to-[#f7f8fa]" />

      <div className="relative z-10 mx-auto flex max-w-[880px] flex-col items-center px-5 pt-14 text-center sm:pt-16 lg:pt-16">
        <a href="#how" className="fade-up group inline-flex items-center gap-2 rounded-full border border-[#e3e7f0] bg-white/80 py-1 pl-1 pr-3 text-[13px] text-muted backdrop-blur transition-colors hover:text-ink">
          <span className="rounded-full bg-accent-dim px-2 py-0.5 text-[12px] font-medium text-accent">Internal</span>
          <span>Payments intelligence<span className="hidden sm:inline"> for the Spotflow team</span></span>
          <span className="transition-transform group-hover:translate-x-0.5">→</span>
        </a>
        <h1 className="mt-7 font-display text-[44px] font-semibold leading-[1.02] tracking-[-0.045em] text-[#0b1533] sm:text-[64px] lg:text-[76px]">
          {LINES.map((line, i) => (
            <span key={i} className="block">
              {line.map((word) => (
                <span key={word} className={`bw ${word === "explained." ? "text-accent" : ""}`} style={{ animationDelay: `${80 + w++ * 70}ms` }}>
                  {word}{" "}
                </span>
              ))}
            </span>
          ))}
        </h1>
        <p className="fade-up mt-6 max-w-[560px] text-[17px] leading-relaxed text-muted sm:text-[18px]" style={{ animationDelay: "420ms" }}>
          Drop in the daily Spotflow export and see which providers, regions and merchants are losing payments, and the exact reason behind each one.
        </p>
        <div className="fade-up" style={{ animationDelay: "520ms" }}>
          <HeroCtas center howItWorks />
        </div>
      </div>
    </section>
  );
}
