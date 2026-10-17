import { Reveal, Spotlight } from "@/components/landing/motion";
import { cn } from "@/lib/utils";
import { Heading, container, ink } from "./heading";
import { AskLive, FiltersLive, ProvidersLive, ReasonsLive, RunsLive, StatusLive } from "./inside-app-live";

function CropMarks() {
  const mark = "absolute size-3 border-[#c4cad6]";
  return (
    <>
      <span aria-hidden className={cn(mark, "-left-1.5 -top-1.5 border-l border-t")} />
      <span aria-hidden className={cn(mark, "-right-1.5 -top-1.5 border-r border-t")} />
      <span aria-hidden className={cn(mark, "-bottom-1.5 -left-1.5 border-b border-l")} />
      <span aria-hidden className={cn(mark, "-bottom-1.5 -right-1.5 border-b border-r")} />
    </>
  );
}

function Cell({ caption, line, children, i }: { caption: string; line: string; children: React.ReactNode; i: number }) {
  return (
    <Spotlight className="h-full">
      <Reveal delay={(i % 3) * 0.08} className="relative flex h-full flex-col items-center px-6 pb-10 pt-12 text-center">
        <CropMarks />
        <div className="dot-grid absolute inset-0 opacity-40 [mask-image:radial-gradient(ellipse_at_top,black,transparent_70%)]" aria-hidden />
        <div className="relative flex min-h-[230px] w-full max-w-[340px] items-center justify-center">{children}</div>
        <p className="relative mt-8 text-[12px] font-medium uppercase tracking-[0.08em] text-faint">{caption}</p>
        <p className={cn("relative mt-3 max-w-[300px] font-display text-[19px] font-medium leading-snug tracking-[-0.02em]", ink)}>{line}</p>
      </Reveal>
    </Spotlight>
  );
}

/** Bento of looping product vignettes, with crop marks (execution-insights reference). */
export function InsideApp() {
  return (
    <section className="py-24 sm:py-32">
      <div className={container}>
        <Heading center eyebrow="Inside the app" title="Built for the 9am question:" accent="what broke?" />
        <div className="mt-16 grid gap-px md:grid-cols-3">
          <Cell i={0} caption="Upload history" line="Every export, how long it took, and whether it landed."><RunsLive /></Cell>
          <Cell i={1} caption="Every provider" line="One registry. Unknown providers are flagged, never dropped."><ProvidersLive /></Cell>
          <Cell i={2} caption="Status at a glance" line="Successful, failed and abandoned, day by day."><StatusLive /></Cell>
          <Cell i={3} caption="Filter what matters" line="Slice by merchant, provider, region, channel or day."><FiltersLive /></Cell>
          <Cell i={4} caption="Ask in plain English" line="Questions answered from the data, with the queries shown."><AskLive /></Cell>
          <Cell i={5} caption="Failure visibility" line="The reasons behind failures, ranked, with their weight."><ReasonsLive /></Cell>
        </div>
      </div>
    </section>
  );
}
