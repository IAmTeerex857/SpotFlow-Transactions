const usd0 = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });
const usd2 = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 2, maximumFractionDigits: 2 });
const usdCompact = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", notation: "compact", maximumFractionDigits: 1 });
const int = new Intl.NumberFormat("en-US");
const compact = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });

/** Matches the Python report: whole dollars from $1M, cents below. */
export function fmtUsd(value: number): string {
  return Math.abs(value) >= 1_000_000 ? usd0.format(value) : usd2.format(value);
}
export const fmtUsdCompact = (value: number) => usdCompact.format(value);
export const fmtInt = (value: number) => int.format(Math.round(value));
export const fmtCompact = (value: number) => compact.format(value);
export const fmtPct = (ratio: number, digits = 1) => `${(ratio * 100).toFixed(digits)}%`;

export function fmtDuration(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86_400) return `${(seconds / 3600).toFixed(1)}h`;
  return `${(seconds / 86_400).toFixed(1)}d`;
}

export function fmtDate(iso: string, opts: Intl.DateTimeFormatOptions = { month: "short", day: "numeric", year: "numeric" }) {
  return new Date(`${iso.slice(0, 10)}T00:00:00Z`).toLocaleDateString("en-US", { timeZone: "UTC", ...opts });
}

export function fmtDateTime(iso: string) {
  return new Date(iso).toLocaleString("en-US", {
    month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", timeZone: "UTC", timeZoneName: "short",
  });
}

export function fmtRange(from: string | null, to: string | null): string {
  if (!from || !to) return "No data";
  if (from === to) return fmtDate(from, { weekday: "short", month: "short", day: "numeric", year: "numeric" });
  const sameYear = from.slice(0, 4) === to.slice(0, 4);
  return `${fmtDate(from, { month: "short", day: "numeric", ...(sameYear ? {} : { year: "numeric" }) })} – ${fmtDate(to)}`;
}

const ACRONYMS: Record<string, string> = { eft: "EFT", usd: "USD", otp: "OTP" };
export const label = (value: string) =>
  ACRONYMS[value.toLowerCase()] ?? value.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());

/** Time-of-day greeting in West Africa Time, where the team works. */
export function greeting(name: string | null, now = new Date()): string {
  const hour = Number(new Intl.DateTimeFormat("en-GB", { hour: "numeric", hour12: false, timeZone: "Africa/Lagos" }).format(now));
  const part = hour < 12 ? "Good morning" : hour < 17 ? "Good afternoon" : "Good evening";
  const first = name?.split(/\s+/)[0];
  return first ? `${part}, ${first}` : part;
}
