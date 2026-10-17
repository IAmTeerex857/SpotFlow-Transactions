import { Mark } from "@/components/brand/logo";
import { HeroCtas } from "@/components/landing/hero/cta";
import { Reveal, RevealWords } from "@/components/landing/motion";
import { container } from "./heading";

/** The Spotflow disc from the hero, seen from above, turning slowly. */
function Rings() {
  return (
    <svg viewBox="-150 -150 300 300" className="size-full" aria-hidden>
      <circle r="146" fill="none" stroke="rgba(255,255,255,0.08)" strokeWidth="2" />
      <g className="cta-spin"><circle r="112" fill="none" stroke="#3d6bff" strokeWidth="18" strokeDasharray="300 39.3" strokeLinecap="round" opacity="0.9" /></g>
      <g className="cta-spin-rev"><circle r="72" fill="none" stroke="#3d6bff" strokeWidth="18" strokeDasharray="190 30 170 50" strokeLinecap="round" opacity="0.7" /></g>
      <circle r="38" fill="#3366ff" />
    </svg>
  );
}

export function FinalCta() {
  return (
    <section className={`${container} pb-24 pt-24 sm:pt-32`}>
      <Reveal className="relative overflow-hidden rounded-[32px] bg-[#0b1533] px-7 py-14 sm:px-14 sm:py-20">
        <div aria-hidden className="absolute inset-0 bg-[radial-gradient(rgba(255,255,255,0.07)_1px,transparent_1px)] [background-size:18px_18px] [mask-image:linear-gradient(to_right,black,transparent_70%)]" />
        <div aria-hidden className="absolute -right-24 top-1/2 hidden size-[440px] -translate-y-1/2 md:block">
          <Rings />
          <span className="absolute inset-0 flex items-center justify-center text-white"><Mark className="size-9" /></span>
        </div>
        <div className="relative max-w-[600px]">
          <h2 className="font-display text-[40px] font-semibold leading-[1.02] tracking-[-0.045em] text-white sm:text-[60px]">
            <RevealWords>Know why it failed before anyone asks.</RevealWords>
          </h2>
          <p className="mt-5 max-w-[460px] text-[16.5px] leading-relaxed text-[#aab6d6]">
            Sign in with your Spotflow Google account, upload this morning&apos;s export, and have the answer ready for stand-up.
          </p>
          <HeroCtas />
          <p className="mt-5 text-[13px] text-[#7d8bb3]">For @spotflow.one accounts. Nothing to install.</p>
        </div>
      </Reveal>
    </section>
  );
}
