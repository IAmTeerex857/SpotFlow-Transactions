// The Home summary: the latest export day against the days before it.
import { buildReport, type DailyRow, type MessageCount, type Report } from "./build";
import type { Cube } from "./types";

export interface Kpi {
  key: "successRate" | "transactions" | "successUsd" | "failedUsd";
  value: number;
  previous: number | null;
  /** 0–1 fill for the progress pill. */
  fill: number;
  caption: string;
}

export interface Change {
  kind: "provider" | "region" | "merchant";
  name: string;
  latestRate: number;
  baselineRate: number;
  /** Percentage-point change in success rate (latest − baseline). */
  diff: number;
  latestCount: number;
}

export interface ReasonShift extends MessageCount {
  share: number;
  /** Share on the latest day minus the 30-day share, in points. */
  shift: number | null;
}

export interface HomeSummary {
  latestDate: string | null;
  previousDate: string | null;
  kpis: Kpi[];
  daily: DailyRow[];
  changes: Change[];
  reasons: ReasonShift[];
  providers: Report["providers"];
  latest: Report;
}

const MIN_LATEST = 25;
const MIN_BASELINE = 60;
const MIN_DIFF = 0.03;
const BASELINE_DAYS = 7;

function rateChanges(cube: Cube, latestDate: string, baselineDates: Set<string>): Change[] {
  const groups = new Map<string, { kind: Change["kind"]; name: string; ln: number; lok: number; bn: number; bok: number }>();
  for (const f of cube.facts) {
    const isLatest = f.d === latestDate;
    if (!isLatest && !baselineDates.has(f.d)) continue;
    for (const [kind, name] of [["provider", f.p], ["region", f.r], ["merchant", f.m]] as const) {
      if (kind === "merchant" && name === "<unknown>") continue;
      const key = `${kind}:${name}`;
      const g = groups.get(key) ?? { kind, name, ln: 0, lok: 0, bn: 0, bok: 0 };
      const ok = f.s === "successful" ? f.n : 0;
      if (isLatest) { g.ln += f.n; g.lok += ok; } else { g.bn += f.n; g.bok += ok; }
      groups.set(key, g);
    }
  }
  return [...groups.values()]
    .filter((g) => g.ln >= MIN_LATEST && g.bn >= MIN_BASELINE)
    .map((g) => {
      const latestRate = g.lok / g.ln;
      const baselineRate = g.bok / g.bn;
      return { kind: g.kind, name: g.name, latestRate, baselineRate, diff: latestRate - baselineRate, latestCount: g.ln };
    })
    .filter((c) => Math.abs(c.diff) >= MIN_DIFF)
    .sort((a, b) => Math.abs(b.diff) - Math.abs(a.diff) || a.name.localeCompare(b.name))
    .slice(0, 5);
}

/**
 * @param trendCube the last ~30 days, including the latest day
 * @param latestCube the latest day only (carries that day's message breakdown)
 */
export function buildHome(trendCube: Cube, latestCube: Cube): HomeSummary {
  const trend = buildReport(trendCube);
  const latest = buildReport(latestCube);
  const daily = trend.daily;
  const latestDate = latest.range.to ?? daily[daily.length - 1]?.date ?? null;
  const latestIdx = daily.findIndex((d) => d.date === latestDate);
  const prev = latestIdx > 0 ? daily[latestIdx - 1] : null;
  const baseline = new Set(daily.slice(Math.max(0, latestIdx - BASELINE_DAYS), Math.max(0, latestIdx)).map((d) => d.date));

  const t = latest.totals;
  const maxTotal = Math.max(1, ...daily.map((d) => d.total));
  const maxSuccessUsd = Math.max(1, ...daily.map((d) => d.successUsd));
  const failedUsd = t.usdByStatus.failed ?? 0;
  const successUsd = t.usdByStatus.successful ?? 0;

  const kpis: Kpi[] = [
    {
      key: "successRate", value: t.successRate, previous: prev?.successRate ?? null, fill: t.successRate,
      caption: `${(t.status.successful ?? 0).toLocaleString()} of ${t.total.toLocaleString()} succeeded`,
    },
    {
      key: "transactions", value: t.total, previous: prev?.total ?? null, fill: t.total / maxTotal,
      caption: "vs the busiest day this month",
    },
    {
      key: "successUsd", value: successUsd, previous: prev?.successUsd ?? null, fill: successUsd / maxSuccessUsd,
      caption: "USD equivalent, successful only",
    },
    {
      key: "failedUsd", value: failedUsd, previous: prev?.failedUsd ?? null, fill: failedUsd / Math.max(1, failedUsd + successUsd),
      caption: "share of attempted volume that failed",
    },
  ];

  const failedLatest = t.status.failed ?? 0;
  const failedTrend = trend.totals.status.failed ?? 0;
  const trendShare = new Map((trend.messages.failed ?? []).map((m) => [m.message, m.count / (failedTrend || 1)]));
  const reasons: ReasonShift[] = (latest.messages.failed ?? []).slice(0, 6).map((m) => {
    const share = m.count / (failedLatest || 1);
    const base = trendShare.get(m.message);
    return { ...m, share, shift: base === undefined || daily.length < 3 ? null : share - base };
  });

  return {
    latestDate,
    previousDate: prev?.date ?? null,
    kpis,
    daily,
    changes: latestDate ? rateChanges(trendCube, latestDate, baseline) : [],
    reasons,
    providers: latest.providers,
    latest,
  };
}
