import { RevealWords, Reveal } from "@/components/landing/motion";
import { cn } from "@/lib/utils";

export const container = "mx-auto max-w-[1240px] px-5 sm:px-8";
export const ink = "text-[#0b1533]";

/** Section eyebrow, a word-by-word headline with an optional blue tail, and a lede. */
export function Heading({ eyebrow, title, accent, body, center, className }: {
  eyebrow?: string; title: string; accent?: string; body?: string; center?: boolean; className?: string;
}) {
  return (
    <div className={cn("max-w-[760px]", center && "mx-auto text-center", className)}>
      {eyebrow && (
        <Reveal>
          <p className={cn("inline-flex items-center gap-2 text-[13.5px] font-medium text-accent", center && "justify-center")}>
            <span className="h-px w-5 bg-accent/50" />
            {eyebrow}
            {center && <span className="h-px w-5 bg-accent/50" />}
          </p>
        </Reveal>
      )}
      <h2 className={cn("mt-4 font-display text-[36px] font-semibold leading-[1.06] tracking-[-0.04em] sm:text-[52px]", ink)}>
        <RevealWords>
          {title}
          {accent && <span className="text-accent">{` ${accent}`}</span>}
        </RevealWords>
      </h2>
      {body && <Reveal delay={0.15}><p className={cn("mt-5 max-w-[560px] text-[16.5px] leading-relaxed text-muted", center && "mx-auto")}>{body}</p></Reveal>}
    </div>
  );
}
