import type { ReportFilters } from "./types";

export interface FilterOptions {
  merchants: string[];
  providers: string[];
  regions: string[];
  channels: string[];
  minDate: string | null;
  maxDate: string | null;
}

export type Preset = "latest" | "7d" | "30d" | "month" | "all" | "custom";

const DATE = /^\d{4}-\d{2}-\d{2}$/;
const list = (value: string | string[] | undefined) =>
  (Array.isArray(value) ? value.join(",") : value ?? "").split(",").map((v) => v.trim()).filter(Boolean);

export function addDays(date: string, days: number): string {
  const d = new Date(`${date}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

/** Presets are anchored to the latest export, not today: data lands a day late. */
export function presetRange(preset: Exclude<Preset, "custom">, options: Pick<FilterOptions, "minDate" | "maxDate">) {
  const end = options.maxDate ?? new Date().toISOString().slice(0, 10);
  switch (preset) {
    case "latest": return { from: end, to: end };
    case "7d": return { from: addDays(end, -6), to: end };
    case "30d": return { from: addDays(end, -29), to: end };
    case "month": return { from: `${end.slice(0, 7)}-01`, to: end };
    case "all": return { from: options.minDate ?? end, to: end };
  }
}

export function detectPreset(filters: ReportFilters, options: Pick<FilterOptions, "minDate" | "maxDate">): Preset {
  for (const p of ["latest", "7d", "30d", "month", "all"] as const) {
    const r = presetRange(p, options);
    if (r.from === filters.from && r.to === filters.to) return p;
  }
  return "custom";
}

export function parseFilters(params: Record<string, string | string[] | undefined>, options: Pick<FilterOptions, "minDate" | "maxDate">): ReportFilters {
  const one = (k: string) => (Array.isArray(params[k]) ? params[k]![0] : params[k]) as string | undefined;
  const fallback = presetRange("7d", options);
  let from = one("from") && DATE.test(one("from")!) ? one("from")! : fallback.from;
  let to = one("to") && DATE.test(one("to")!) ? one("to")! : fallback.to;
  if (from > to) [from, to] = [to, from];
  const upload = one("upload");
  return {
    from,
    to,
    merchants: list(params.merchant),
    providers: list(params.provider),
    regions: list(params.region),
    channels: list(params.channel),
    uploadId: upload && /^[0-9a-f-]{36}$/i.test(upload) ? upload : null,
  };
}

export function filtersToQuery(filters: ReportFilters): string {
  const q = new URLSearchParams({ from: filters.from, to: filters.to });
  if (filters.merchants?.length) q.set("merchant", filters.merchants.join(","));
  if (filters.providers?.length) q.set("provider", filters.providers.join(","));
  if (filters.regions?.length) q.set("region", filters.regions.join(","));
  if (filters.channels?.length) q.set("channel", filters.channels.join(","));
  if (filters.uploadId) q.set("upload", filters.uploadId);
  return q.toString();
}
