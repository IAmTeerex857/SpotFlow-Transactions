import { regionOrder } from "@/lib/parser/registry";
import type { Cube, PeakRow, RetryStats } from "./types";

export type Counts = Record<string, number>;
export type MessageCount = { message: string; count: number };

export interface StatusBlock {
  /** Daily transaction counts aligned to Report.daily (filled by buildReport). */
  trend?: number[];
  total: number;
  status: Counts;
  successRate: number;
  successUsd: number;
  failedUsd: number;
  volumeUsd: number;
}

export interface MerchantRow extends StatusBlock {
  name: string;
  topChannel: string | null;
  topRegion: string | null;
  channels: Counts;
}

export interface ProviderRow extends StatusBlock {
  name: string;
  channels: Counts;
  messages: Record<string, MessageCount[]>;
}

export interface RegionRow extends StatusBlock {
  name: string;
  channels: Counts;
  messages: Record<string, MessageCount[]>;
  providers: ProviderRow[];
  merchants: MerchantRow[];
}

export interface DailyRow {
  date: string;
  total: number;
  successful: number;
  failed: number;
  abandoned: number;
  cancelled: number;
  successUsd: number;
  failedUsd: number;
  successRate: number;
}

export interface Report {
  range: { from: string | null; to: string | null; days: number };
  totals: StatusBlock & { usdByStatus: Counts; channels: Counts };
  daily: DailyRow[];
  merchants: MerchantRow[];
  providers: ProviderRow[];
  regions: RegionRow[];
  messages: Record<string, MessageCount[]>;
  retry: RetryStats;
  peaks: { byDay: PeakSummary[]; byHour: PeakSummary[]; grid: PeakCell[] };
}

/** One cell of the weekday × hour grid (UTC). */
export interface PeakCell {
  dow: number;
  hour: number;
  total: number;
  successful: number;
}

export interface PeakSummary {
  key: number;
  label: string;
  total: number;
  successful: number;
  successUsd: number;
}

interface Acc {
  status: Counts;
  channels: Counts;
  regions: Counts;
  usdByStatus: Counts;
  volumeUsd: number;
}

const newAcc = (): Acc => ({ status: {}, channels: {}, regions: {}, usdByStatus: {}, volumeUsd: 0 });

function add(acc: Acc, status: string, channel: string, region: string, n: number, usd: number) {
  acc.status[status] = (acc.status[status] ?? 0) + n;
  acc.channels[channel] = (acc.channels[channel] ?? 0) + n;
  acc.regions[region] = (acc.regions[region] ?? 0) + n;
  acc.usdByStatus[status] = (acc.usdByStatus[status] ?? 0) + usd;
  acc.volumeUsd += usd;
}

function block(acc: Acc): StatusBlock {
  const total = Object.values(acc.status).reduce((s, v) => s + v, 0);
  const ok = acc.status.successful ?? 0;
  return {
    total,
    status: acc.status,
    successRate: total ? ok / total : 0,
    successUsd: acc.usdByStatus.successful ?? 0,
    failedUsd: acc.usdByStatus.failed ?? 0,
    volumeUsd: acc.volumeUsd,
  };
}

// Highest count wins; ties resolve alphabetically so SQL and TypeScript agree.
function topKey(counts: Counts): string | null {
  let best: string | null = null;
  let bestCount = 0;
  for (const [key, count] of Object.entries(counts)) {
    if (count > bestCount || (count === bestCount && best !== null && key < best)) [best, bestCount] = [key, count];
  }
  return best;
}

function sortedMessages(map: Map<string, number>): MessageCount[] {
  return [...map.entries()].map(([message, count]) => ({ message, count }))
    .sort((a, b) => b.count - a.count || (a.message < b.message ? -1 : 1));
}

function group<K>(map: Map<K, Acc>, key: K): Acc {
  let acc = map.get(key);
  if (!acc) {
    acc = newAcc();
    map.set(key, acc);
  }
  return acc;
}

type MsgIndex = Map<string, Map<string, Map<string, number>>>;

function addMessage(index: MsgIndex, key: string, status: string, message: string, n: number) {
  let byStatus = index.get(key);
  if (!byStatus) index.set(key, (byStatus = new Map()));
  let byMessage = byStatus.get(status);
  if (!byMessage) byStatus.set(status, (byMessage = new Map()));
  byMessage.set(message, (byMessage.get(message) ?? 0) + n);
}

function messagesFor(index: MsgIndex, key: string): Record<string, MessageCount[]> {
  const out: Record<string, MessageCount[]> = {};
  for (const [status, map] of index.get(key) ?? []) out[status] = sortedMessages(map);
  return out;
}

const DOW = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

export function buildReport(cube: Cube): Report {
  const total = newAcc();
  const merchants = new Map<string, Acc>();
  const providers = new Map<string, Acc>();
  const regions = new Map<string, Acc>();
  const regionProviders = new Map<string, Acc>();
  const regionMerchants = new Map<string, Acc>();
  const days = new Map<string, Acc>();

  const entityDaily = new Map<string, Map<string, number>>();
  const bump = (key: string, day: string, n: number) => {
    let m = entityDaily.get(key);
    if (!m) entityDaily.set(key, (m = new Map()));
    m.set(day, (m.get(day) ?? 0) + n);
  };
  for (const f of cube.facts) {
    bump(`m\u0000${f.m}`, f.d, f.n);
    bump(`p\u0000${f.p}`, f.d, f.n);
    bump(`r\u0000${f.r}`, f.d, f.n);
    add(total, f.s, f.c, f.r, f.n, f.usd);
    add(group(merchants, f.m), f.s, f.c, f.r, f.n, f.usd);
    add(group(providers, f.p), f.s, f.c, f.r, f.n, f.usd);
    add(group(regions, f.r), f.s, f.c, f.r, f.n, f.usd);
    add(group(regionProviders, `${f.r}\u0000${f.p}`), f.s, f.c, f.r, f.n, f.usd);
    add(group(regionMerchants, `${f.r}\u0000${f.m}`), f.s, f.c, f.r, f.n, f.usd);
    add(group(days, f.d), f.s, f.c, f.r, f.n, f.usd);
  }

  const aggMessages: MsgIndex = new Map();
  const regionMessages: MsgIndex = new Map();
  const providerMessages: MsgIndex = new Map();
  const regionProviderMessages: MsgIndex = new Map();
  for (const m of cube.messages) {
    addMessage(aggMessages, "", m.s, m.msg, m.n);
    addMessage(regionMessages, m.r, m.s, m.msg, m.n);
    addMessage(providerMessages, m.p, m.s, m.msg, m.n);
    addMessage(regionProviderMessages, `${m.r}\u0000${m.p}`, m.s, m.msg, m.n);
  }

  const merchantRow = (name: string, acc: Acc): MerchantRow => ({
    name,
    ...block(acc),
    channels: acc.channels,
    topChannel: topKey(acc.channels),
    topRegion: topKey(acc.regions),
  });
  const byVolume = (a: MerchantRow, b: MerchantRow) =>
    b.successUsd - a.successUsd || b.total - a.total || (a.name < b.name ? -1 : 1);
  const byTotal = (a: ProviderRow, b: ProviderRow) => b.total - a.total || (a.name < b.name ? -1 : 1);

  const regionRows: RegionRow[] = [...regions.entries()]
    .sort(([a], [b]) => regionOrder(a) - regionOrder(b) || a.localeCompare(b))
    .map(([name, acc]) => {
      const prefix = `${name}\u0000`;
      return {
        name,
        ...block(acc),
        channels: acc.channels,
        messages: messagesFor(regionMessages, name),
        providers: [...regionProviders.entries()]
          .filter(([key]) => key.startsWith(prefix))
          .map(([key, pacc]) => ({
            name: key.slice(prefix.length),
            ...block(pacc),
            channels: pacc.channels,
            messages: messagesFor(regionProviderMessages, key),
          }))
          .sort(byTotal),
        merchants: [...regionMerchants.entries()]
          .filter(([key]) => key.startsWith(prefix))
          .map(([key, macc]) => merchantRow(key.slice(prefix.length), macc))
          .sort(byVolume),
      };
    });

  const daily: DailyRow[] = [...days.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, acc]) => {
      const b = block(acc);
      return {
        date,
        total: b.total,
        successful: acc.status.successful ?? 0,
        failed: acc.status.failed ?? 0,
        abandoned: acc.status.abandoned ?? 0,
        cancelled: acc.status.cancelled ?? 0,
        successUsd: b.successUsd,
        failedUsd: b.failedUsd,
        successRate: b.successRate,
      };
    });

  const dates = daily.map((d) => d.date);
  const trendOf = (key: string) => {
    const m = entityDaily.get(key);
    return dates.map((d) => m?.get(d) ?? 0);
  };
  const merchantRows = [...merchants.entries()].map(([name, acc]) => ({ ...merchantRow(name, acc), trend: trendOf(`m\u0000${name}`) })).sort(byVolume);
  const providerRows = [...providers.entries()]
    .map(([name, acc]) => ({ name, ...block(acc), channels: acc.channels, messages: messagesFor(providerMessages, name), trend: trendOf(`p\u0000${name}`) }))
    .sort(byTotal);
  for (const r of regionRows) r.trend = trendOf(`r\u0000${r.name}`);

  return {
    range: { from: daily[0]?.date ?? null, to: daily[daily.length - 1]?.date ?? null, days: daily.length },
    totals: { ...block(total), usdByStatus: total.usdByStatus, channels: total.channels },
    daily,
    merchants: merchantRows,
    providers: providerRows,
    regions: regionRows,
    messages: messagesFor(aggMessages, ""),
    retry: cube.retry,
    peaks: summarisePeaks(cube.peaks),
  };
}

function summarisePeaks(rows: readonly PeakRow[]) {
  const byDay = new Map<number, PeakSummary>();
  const byHour = new Map<number, PeakSummary>();
  for (const row of rows) {
    const day = byDay.get(row.dow) ?? { key: row.dow, label: DOW[row.dow], total: 0, successful: 0, successUsd: 0 };
    day.total += row.n;
    day.successful += row.ok;
    day.successUsd += row.usd;
    byDay.set(row.dow, day);
    const hour = byHour.get(row.hour) ?? {
      key: row.hour, label: `${String(row.hour).padStart(2, "0")}:00`, total: 0, successful: 0, successUsd: 0,
    };
    hour.total += row.n;
    hour.successful += row.ok;
    hour.successUsd += row.usd;
    byHour.set(row.hour, hour);
  }
  const grid: PeakCell[] = rows
    .map((r) => ({ dow: r.dow, hour: r.hour, total: r.n, successful: r.ok }))
    .sort((a, b) => a.dow - b.dow || a.hour - b.hour);
  return {
    grid,
    byDay: [1, 2, 3, 4, 5, 6, 0].map((d) => byDay.get(d) ?? { key: d, label: DOW[d], total: 0, successful: 0, successUsd: 0 }),
    byHour: Array.from({ length: 24 }, (_, h) =>
      byHour.get(h) ?? { key: h, label: `${String(h).padStart(2, "0")}:00`, total: 0, successful: 0, successUsd: 0 },
    ),
  };
}

