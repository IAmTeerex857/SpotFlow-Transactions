"use client";

// Shared landing motion: scroll reveals and the amicro interactions (word reveal,
// spotlight, scroll progress) and small loop helpers for scripted vignettes.

import { motion, useInView, useMotionTemplate, useMotionValue, useReducedMotion, useScroll, useSpring, useTransform } from "motion/react";
import { Children, isValidElement, useEffect, useRef, useState } from "react";
import { cn } from "@/lib/utils";

/** Sets data-inview once the element scrolls into view, so CSS animations (landing.css) can start then. */
export function InView({ children, className, once = true, margin = "-12% 0px" }: {
  children: React.ReactNode; className?: string; once?: boolean; margin?: string;
}) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const io = new IntersectionObserver(([e]) => {
      if (e.isIntersecting) {
        el.setAttribute("data-inview", "");
        if (once) io.disconnect();
      } else if (!once) el.removeAttribute("data-inview");
    }, { rootMargin: margin });
    io.observe(el);
    return () => io.disconnect();
  }, [once, margin]);
  return <div ref={ref} className={className}>{children}</div>;
}

/** Fades content up once as it scrolls into view. */
export function Reveal({ children, delay = 0, className }: { children: React.ReactNode; delay?: number; className?: string }) {
  return (
    <motion.div
      className={className}
      initial={{ opacity: 0, y: 18 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true, margin: "-60px" }}
      transition={{ duration: 0.6, delay, ease: [0.2, 0.7, 0.2, 1] }}
    >
      {children}
    </motion.div>
  );
}

/** amicro word-reveal: words fade, unblur and rise in a stagger when scrolled into view. */
export function RevealWords({ children, className, delay = 0 }: { children: React.ReactNode; className?: string; delay?: number }) {
  const reduced = useReducedMotion();
  // Split plain strings into words; keep elements (e.g. an accented span) as one unit.
  const parts: React.ReactNode[] = [];
  Children.forEach(children, (child) => {
    if (child == null || typeof child === "boolean") return;
    if (typeof child === "string") child.split(/(\s+)/).filter(Boolean).forEach((w) => parts.push(w));
    else if (isValidElement<{ children?: React.ReactNode; className?: string }>(child) && typeof child.props.children === "string") {
      child.props.children.split(/(\s+)/).filter(Boolean).forEach((w) => parts.push(/^\s+$/.test(w) ? w : <span className={child.props.className}>{w}</span>));
    } else parts.push(child);
  });
  let n = 0;
  return (
    <motion.span className={cn("inline", className)} initial="hidden" whileInView="shown" viewport={{ once: true, margin: "-60px" }}>
      {parts.map((p, i) =>
        typeof p === "string" && /^\s+$/.test(p) ? " " : (
          <motion.span
            key={i}
            className="inline-block"
            variants={reduced ? undefined : {
              hidden: { opacity: 0, y: 14, filter: "blur(8px)" },
              shown: { opacity: 1, y: 0, filter: "blur(0px)", transition: { duration: 0.6, delay: delay + n++ * 0.045, ease: [0.2, 0.7, 0.2, 1] } },
            }}
          >
            {p}
          </motion.span>
        ),
      )}
    </motion.span>
  );
}

/** amicro spotlight: a soft light follows the cursor across the card. */
export function Spotlight({ children, className, color = "rgba(51,102,255,0.07)" }: { children: React.ReactNode; className?: string; color?: string }) {
  const x = useMotionValue(-400);
  const y = useMotionValue(-400);
  const bg = useMotionTemplate`radial-gradient(360px circle at ${x}px ${y}px, ${color}, transparent 70%)`;
  return (
    <div
      className={cn("group/spot relative", className)}
      onPointerMove={(e) => {
        const r = e.currentTarget.getBoundingClientRect();
        x.set(e.clientX - r.left);
        y.set(e.clientY - r.top);
      }}
    >
      <motion.div aria-hidden className="pointer-events-none absolute inset-0 opacity-0 transition-opacity duration-300 group-hover/spot:opacity-100" style={{ background: bg }} />
      {children}
    </div>
  );
}

/** amicro progress-indicator: a thin bar showing how far down the page you are. */
export function ScrollProgress() {
  const { scrollYProgress } = useScroll();
  const scaleX = useSpring(scrollYProgress, { stiffness: 200, damping: 30 });
  return <motion.div aria-hidden className="fixed inset-x-0 top-0 z-[60] h-[2px] origin-left bg-accent" style={{ scaleX }} />;
}

/** A counter that ticks while its element is on screen (and motion is allowed). */
export function useTicker(ms: number) {
  const ref = useRef<HTMLDivElement>(null);
  const inView = useInView(ref, { margin: "-10% 0px" });
  const reduced = useReducedMotion();
  const [tick, setTick] = useState(0);
  useEffect(() => {
    if (!inView || reduced) return;
    const id = setInterval(() => setTick((t) => t + 1), ms);
    return () => clearInterval(id);
  }, [inView, reduced, ms]);
  return { ref, tick, inView, reduced: !!reduced };
}

/** A looping frame index for scripted vignettes; holds the last frame under reduced motion. */
export function useLoop(ms: number, frames: number) {
  const { ref, tick, reduced } = useTicker(ms);
  return { ref, t: reduced ? frames - 1 : tick % frames };
}

/** A number that springs to its target value. */
export function CountUp({ value, format, className }: { value: number; format: (v: number) => string; className?: string }) {
  const mv = useSpring(0, { stiffness: 70, damping: 20 });
  const text = useTransform(mv, format);
  useEffect(() => {
    mv.set(value);
  }, [mv, value]);
  return <motion.span className={className}>{text}</motion.span>;
}
