import { cn } from "@/lib/utils";
import { label } from "@/lib/format";

const STATUS_TONE: Record<string, { text: string; dot: string; hex: string }> = {
  successful: { text: "text-ok", dot: "bg-ok", hex: "var(--chart-ok)" },
  failed: { text: "text-bad", dot: "bg-bad", hex: "var(--chart-bad)" },
  abandoned: { text: "text-ink", dot: "bg-[var(--chart-warn)]", hex: "var(--chart-warn)" },
  cancelled: { text: "text-violet", dot: "bg-violet", hex: "var(--chart-cancel)" },
  refunded: { text: "text-accent", dot: "bg-accent", hex: "var(--chart-accent)" },
  inprogress: { text: "text-muted", dot: "bg-faint", hex: "var(--faint)" },
};

const fallback = STATUS_TONE.inprogress;

/** Bordered pill with a coloured dot (execution-history reference). */
export function StatusPill({ status, children, className }: { status: string; children?: React.ReactNode; className?: string }) {
  const tone = STATUS_TONE[status] ?? fallback;
  return (
    <span className={cn("inline-flex items-center gap-1.5 rounded-full border border-line bg-raised px-2 py-0.5 text-[12px] text-ink", className)}>
      <span className={cn("size-1.5 rounded-full", tone.dot)} />
      {children ?? label(status)}
    </span>
  );
}

/** Signed change badge: green when the change is good, red when bad. */
export function Delta({ value, format, goodWhen = "up", className }: {
  value: number | null; format: (v: number) => string; goodWhen?: "up" | "down"; className?: string;
}) {
  if (value === null || !Number.isFinite(value)) return <span className={cn("text-[12px] text-faint", className)}>—</span>;
  const flat = Math.abs(value) < 1e-9;
  const good = flat ? null : (value > 0) === (goodWhen === "up");
  return (
    <span className={cn("tnum inline-flex items-center gap-0.5 text-[12px] font-medium", good === null ? "text-muted" : good ? "text-ok" : "text-bad", className)}>
      {flat ? "±" : value > 0 ? "↑" : "↓"} {format(Math.abs(value))}
    </span>
  );
}
