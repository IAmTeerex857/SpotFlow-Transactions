import type { Cube, FactRow, MessageRow, PeakRow, RetryStats } from "./types";
import { UNKNOWN_MERCHANT } from "./types";

export interface CubeInput {
  merchant: string;
  provider: string;
  region: string;
  status: string;
  channel: string;
  currency: string;
  amountUsd: number;
  message: string;
  paidAt: string | null;
  /** Stable customer id; empty when unknown. */
  customerId: string;
  businessDate: string;
  /** Insertion order, used to break timestamp ties like the database does. */
  seq?: number;
}

export function cubeFromTransactions(rows: readonly CubeInput[], options: { groupBlankCustomers?: boolean } = {}): Cube {
  const facts = new Map<string, FactRow>();
  const messages = new Map<string, MessageRow>();
  const peaks = new Map<string, PeakRow>();

  for (const t of rows) {
    const m = t.merchant || UNKNOWN_MERCHANT;
    const fk = [t.businessDate, m, t.provider, t.region, t.status, t.channel, t.currency].join("\u0000");
    const fact = facts.get(fk);
    if (fact) {
      fact.n += 1;
      fact.usd += t.amountUsd;
    } else {
      facts.set(fk, {
        d: t.businessDate, m, p: t.provider, r: t.region, s: t.status, c: t.channel, cur: t.currency, n: 1, usd: t.amountUsd,
      });
    }

    const mk = [t.region, t.provider, t.status, t.message].join("\u0000");
    const msg = messages.get(mk);
    if (msg) msg.n += 1;
    else messages.set(mk, { r: t.region, p: t.provider, s: t.status, msg: t.message, n: 1 });

    if (t.paidAt) {
      const date = new Date(t.paidAt);
      const dow = date.getUTCDay();
      const hour = date.getUTCHours();
      const pk = `${dow}:${hour}`;
      const ok = t.status === "successful";
      const peak = peaks.get(pk);
      if (peak) {
        peak.n += 1;
        if (ok) {
          peak.ok += 1;
          peak.usd += t.amountUsd;
        }
      } else {
        peaks.set(pk, { dow, hour, n: 1, ok: ok ? 1 : 0, usd: ok ? t.amountUsd : 0 });
      }
    }
  }

  return {
    facts: [...facts.values()],
    messages: [...messages.values()],
    retry: retryStats(rows, options),
    peaks: [...peaks.values()],
  };
}

// Mirrors the retry block of `report_cube` in the SQL migration.
function retryStats(rows: readonly CubeInput[], options: { groupBlankCustomers?: boolean } = {}): RetryStats {
  const byCustomer = new Map<string, CubeInput[]>();
  rows.forEach((t, i) => {
    let key = t.customerId.trim().toLowerCase();
    if (!key) {
      if (!options.groupBlankCustomers) return;
      key = "<unknown>";
    }
    const list = byCustomer.get(key);
    const item = t.seq === undefined ? { ...t, seq: i } : t;
    if (list) list.push(item);
    else byCustomer.set(key, [item]);
  });

  let retriers = 0;
  let succeededAfter = 0;
  let unresolved = 0;
  let legacy = 0;
  const attempts: Record<string, number> = {};
  const finalStatus: Record<string, number> = {};
  const gaps: number[] = [];

  for (const [key, list] of byCustomer) {
    if (list.length < 2) continue;
    retriers += 1;
    if (key.startsWith("legacy:")) legacy += 1;
    if (list.some((t) => t.status === "successful")) succeededAfter += 1;
    attempts[list.length] = (attempts[list.length] ?? 0) + 1;

    // Undated attempts sort last; ties keep their original order.
    const sorted = [...list].sort((a, b) => {
      if (a.paidAt === null || b.paidAt === null) {
        return a.paidAt === b.paidAt ? a.seq! - b.seq! : a.paidAt === null ? 1 : -1;
      }
      return a.paidAt < b.paidAt ? -1 : a.paidAt > b.paidAt ? 1 : a.seq! - b.seq!;
    });
    const last = sorted[sorted.length - 1].status;
    finalStatus[last] = (finalStatus[last] ?? 0) + 1;
    if (last !== "successful") unresolved += 1;

    const times = sorted.filter((t) => t.paidAt).map((t) => Date.parse(t.paidAt!));
    for (let j = 1; j < times.length; j += 1) {
      const gap = (times[j] - times[j - 1]) / 1000;
      if (gap >= 0) gaps.push(gap);
    }
  }

  gaps.sort((a, b) => a - b);
  return {
    retriers,
    succeededAfter,
    unresolved,
    attempts,
    finalStatus,
    gapCount: gaps.length,
    gapAvgSeconds: gaps.length ? gaps.reduce((s, g) => s + g, 0) / gaps.length : 0,
    gapMedianSeconds: gaps.length ? gaps[Math.floor(gaps.length / 2)] : 0,
    legacyShare: retriers ? legacy / retriers : 0,
  };
}
