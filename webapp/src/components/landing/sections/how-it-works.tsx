"use client";

// Sticky scroll story: the steps scroll on the left while the stage on the right
// plays the matching vignette. On small screens each step carries its own stage.
import { AnimatePresence, motion, useScroll, useSpring } from "motion/react";
import { Check } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { cn } from "@/lib/utils";
import { Heading, container, ink } from "./heading";
import { ExportScene, ReportScene, UploadScene } from "./how-it-works-scenes";

const STEPS = [
  {
    title: "Export the day",
    body: "Pull the daily transactions export from Spotflow, exactly as it comes. Raw or anonymised, merged rows and all.",
    points: ["Any provider, region or currency", "Merged rows are recovered, not dropped"],
    Scene: ExportScene,
  },
  {
    title: "Drop it in",
    body: "Drag the CSV onto the upload canvas. It's read in your browser first, so you can check the numbers before anything is saved.",
    points: ["Customer emails become pseudonyms", "Re-uploading the same day is caught"],
    Scene: UploadScene,
  },
  {
    title: "Read what broke",
    body: "Failure reasons by provider, region and merchant, who retried, peak hours, and what changed since yesterday.",
    points: ["Ranked reasons, not raw provider messages", "Ask follow-up questions in plain English"],
    Scene: ReportScene,
  },
];

function Stage({ index }: { index: number }) {
  const Scene = STEPS[index].Scene;
  return (
    <div className="relative h-[480px] overflow-hidden rounded-[28px] border border-line bg-white shadow-[0_1px_2px_rgba(16,24,40,.04),0_32px_64px_-32px_rgba(16,24,40,.18)] lg:h-full">
      <div aria-hidden className="dot-grid absolute inset-0 opacity-60 [mask-image:radial-gradient(ellipse_at_center,black,transparent_75%)]" />
      <div aria-hidden className="absolute -right-24 -top-24 size-72 rounded-full bg-[radial-gradient(closest-side,rgba(51,102,255,0.10),transparent)]" />
      <div className="absolute inset-x-0 top-0 z-10 flex items-center justify-between px-6 pt-5">
        <span className="text-[12px] font-medium text-faint">Step {index + 1} of {STEPS.length}</span>
        <span className="flex gap-1.5">
          {STEPS.map((_, i) => (
            <span key={i} className="h-1 w-6 overflow-hidden rounded-full bg-[#e8ecf4]">
              <motion.span className="block h-full rounded-full bg-accent" initial={false} animate={{ width: i <= index ? "100%" : "0%" }} transition={{ duration: 0.5 }} />
            </span>
          ))}
        </span>
      </div>
      <AnimatePresence mode="wait" initial={false}>
        <motion.div
          key={index}
          className="absolute inset-0 pt-8"
          initial={{ opacity: 0, y: 24, filter: "blur(8px)" }}
          animate={{ opacity: 1, y: 0, filter: "blur(0px)" }}
          exit={{ opacity: 0, y: -16, filter: "blur(6px)" }}
          transition={{ duration: 0.45, ease: [0.2, 0.7, 0.2, 1] }}
        >
          <Scene />
        </motion.div>
      </AnimatePresence>
    </div>
  );
}

export function HowItWorks() {
  const [active, setActive] = useState(0);
  const list = useRef<HTMLDivElement>(null);
  const { scrollYProgress } = useScroll({ target: list, offset: ["start 55%", "end 55%"] });
  const fill = useSpring(scrollYProgress, { stiffness: 120, damping: 26 });

  useEffect(() => {
    const blocks = list.current?.querySelectorAll<HTMLElement>("[data-step]");
    if (!blocks) return;
    // The step crossing the middle of the viewport is the active one.
    const io = new IntersectionObserver((entries) => {
      for (const e of entries) if (e.isIntersecting) setActive(Number((e.target as HTMLElement).dataset.step));
    }, { rootMargin: "-48% 0px -48% 0px" });
    blocks.forEach((b) => io.observe(b));
    return () => io.disconnect();
  }, []);

  return (
    <section id="how" className="scroll-mt-16 py-24 sm:py-32">
      <div className={container}>
        <Heading center eyebrow="How it works" title="From raw export to answers," accent="in three steps." body="No spreadsheets, no scripts on someone's laptop, no waiting for a report to be emailed round." />

        <div className="mt-16 grid gap-10 lg:mt-20 lg:grid-cols-[minmax(0,0.85fr)_minmax(0,1.15fr)] lg:gap-20">
          <div ref={list} className="relative">
            <div aria-hidden className="absolute bottom-[12vh] left-[19px] top-[12vh] hidden w-px bg-line lg:block" />
            <motion.div aria-hidden style={{ scaleY: fill }} className="absolute bottom-[12vh] left-[19px] top-[12vh] hidden w-px origin-top bg-accent lg:block" />
            {STEPS.map((s, i) => {
              const on = active === i;
              return (
                <div key={s.title} data-step={i} className="relative pb-14 last:pb-0 lg:flex lg:min-h-[72vh] lg:items-center lg:pb-0">
                  <div className={cn("relative transition-opacity duration-500 lg:pl-16", !on && "lg:opacity-35")}>
                    <span className={cn(
                      "mb-5 flex size-10 items-center justify-center rounded-full border text-[13px] font-semibold transition-all duration-500 lg:absolute lg:left-0 lg:top-1 lg:mb-0",
                      on ? "border-accent bg-accent text-white shadow-[0_0_0_6px_rgba(51,102,255,0.12)]" : "border-line bg-white text-faint",
                    )}>
                      0{i + 1}
                    </span>
                    <h3 className={cn("font-display text-[30px] font-semibold leading-tight tracking-[-0.035em] sm:text-[38px]", ink)}>{s.title}</h3>
                    <p className="mt-4 max-w-[440px] text-[16px] leading-relaxed text-muted">{s.body}</p>
                    <ul className="mt-6 space-y-2.5">
                      {s.points.map((p, k) => (
                        <motion.li
                          key={p}
                          className="flex items-center gap-2.5 text-[14px] text-ink"
                          initial={false}
                          animate={{ x: on ? 0 : -4 }}
                          transition={{ delay: on ? 0.1 + k * 0.08 : 0 }}
                        >
                          <span className="flex size-5 items-center justify-center rounded-full bg-accent-dim text-accent"><Check className="size-3" strokeWidth={3} /></span>
                          {p}
                        </motion.li>
                      ))}
                    </ul>
                  </div>
                  <div className="mt-8 lg:hidden"><Stage index={i} /></div>
                </div>
              );
            })}
          </div>

          <div className="hidden lg:block">
            <div className="sticky top-[calc(50vh-280px)] h-[560px]">
              <Stage index={active} />
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
