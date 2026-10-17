// Status series in the validated stack order (see --chart-* in globals.css).
export const STATUS_SERIES = [
  { key: "successful", label: "Successful", color: "var(--chart-ok)" },
  { key: "failed", label: "Failed", color: "var(--chart-bad)" },
  { key: "cancelled", label: "Cancelled", color: "var(--chart-cancel)" },
  { key: "abandoned", label: "Abandoned", color: "var(--chart-warn)" },
] as const;

export function niceMax(value: number): number {
  if (value <= 0) return 1;
  const exp = Math.pow(10, Math.floor(Math.log10(value)));
  const f = value / exp;
  const nice = f <= 1 ? 1 : f <= 2 ? 2 : f <= 2.5 ? 2.5 : f <= 5 ? 5 : 10;
  return nice * exp;
}

/** Rounded-top bar path: 4px radius at the data end, square at the baseline. */
export function columnPath(x: number, y: number, w: number, h: number, r = 4): string {
  if (h <= 0) return "";
  const rr = Math.min(r, w / 2, h);
  return `M${x},${y + h}V${y + rr}Q${x},${y} ${x + rr},${y}H${x + w - rr}Q${x + w},${y} ${x + w},${y + rr}V${y + h}Z`;
}
