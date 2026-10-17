// Parity with the original Python pipeline across every historical export.
// Needs the local data/ folder and `.golden/python.json` (both gitignored):
//   python3 scripts/golden/dump_python.py && npm test
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { cubeFromTransactions } from "../report/cube";
import { buildReport, type MessageCount } from "../report/build";
import { parseExport } from "./parse";
import { LEGACY_REGISTRY } from "./registry";

const ROOT = path.resolve(__dirname, "../../../..");
const GOLDEN = path.resolve(__dirname, "../../../.golden/python.json");

type Golden = Record<string, {
  total: number;
  status: Record<string, number>;
  channel: Record<string, number>;
  missingDates: number;
  statusUsd: Record<string, number>;
  regions: Record<string, {
    status: Record<string, number>;
    channel: Record<string, number>;
    messages: Record<string, Record<string, number>>;
    successUsd: number;
    volumeUsd: number;
    providers: Record<string, { status: Record<string, number>; successUsd: number }>;
  }>;
  providers: Record<string, {
    status: Record<string, number>;
    channel: Record<string, number>;
    successUsd: number;
    messages: Record<string, Record<string, number>>;
  }>;
  merchants: Record<string, {
    total: number; successful: number; failed: number; abandoned: number; cancelled: number;
    successUsd: number; failedUsd: number; volumeUsd: number;
  }>;
  retry: {
    retriers: number; succeededAfter: number; attempts: Record<string, number>;
    finalStatus: Record<string, number>; gapCount: number; gapSum: number; gapMedian: number;
  };
}>;

const toCounts = (list: MessageCount[] = []) => Object.fromEntries(list.map((m) => [m.message, m.count]));
const msgCounts = (messages: Record<string, MessageCount[]>) =>
  Object.fromEntries(Object.entries(messages).map(([s, l]) => [s, toCounts(l)]));
const round = (v: number) => Math.round(v * 100) / 100;

function compareFile(file: string, expected: Golden[string]): string[] {
  const text = readFileSync(path.join(ROOT, file), "utf8");
  const { transactions, stats } = parseExport(text, LEGACY_REGISTRY);
  const cube = cubeFromTransactions(
    transactions.map((t, i) => ({
      ...t,
      customerId: t.customer,
      businessDate: t.paidAt?.slice(0, 10) ?? "undated",
      seq: i,
    })),
    { groupBlankCustomers: true },
  );
  const report = buildReport(cube);
  const errors: string[] = [];
  const check = (label: string, actual: unknown, want: unknown) => {
    const a = JSON.stringify(sortKeys(actual));
    const w = JSON.stringify(sortKeys(want));
    if (a !== w) errors.push(`${label}\n    ts: ${a.slice(0, 400)}\n    py: ${w.slice(0, 400)}`);
  };
  const near = (label: string, actual: number, want: number) => {
    if (Math.abs(round(actual) - want) > 0.011) errors.push(`${label}: ts ${round(actual)} py ${want}`);
  };

  check("total", report.totals.total, expected.total);
  check("status", report.totals.status, expected.status);
  check("channel", report.totals.channels, expected.channel);
  check("missingDates", stats.missingDates, expected.missingDates);
  for (const [s, usd] of Object.entries(expected.statusUsd)) near(`usd.${s}`, report.totals.usdByStatus[s] ?? 0, usd);

  const regions = Object.fromEntries(report.regions.map((r) => [r.name, r]));
  check("region names", Object.keys(regions).sort(), Object.keys(expected.regions).sort());
  for (const [name, want] of Object.entries(expected.regions)) {
    const got = regions[name];
    if (!got) continue;
    check(`${name}.status`, got.status, want.status);
    check(`${name}.channel`, got.channels, want.channel);
    check(`${name}.messages`, msgCounts(got.messages), want.messages);
    near(`${name}.successUsd`, got.successUsd, want.successUsd);
    near(`${name}.volumeUsd`, got.volumeUsd, want.volumeUsd);
    const provs = Object.fromEntries(got.providers.map((p) => [p.name, p]));
    for (const [p, pw] of Object.entries(want.providers)) {
      check(`${name}.${p}.status`, provs[p]?.status, pw.status);
      near(`${name}.${p}.successUsd`, provs[p]?.successUsd ?? 0, pw.successUsd);
    }
  }

  const providers = Object.fromEntries(report.providers.map((p) => [p.name, p]));
  for (const [name, want] of Object.entries(expected.providers)) {
    check(`provider ${name}.status`, providers[name]?.status, want.status);
    check(`provider ${name}.channel`, providers[name]?.channels, want.channel);
    check(`provider ${name}.messages`, msgCounts(providers[name]?.messages ?? {}), want.messages);
    near(`provider ${name}.successUsd`, providers[name]?.successUsd ?? 0, want.successUsd);
  }

  const merchants = Object.fromEntries(report.merchants.map((m) => [m.name, m]));
  for (const [name, want] of Object.entries(expected.merchants)) {
    const got = merchants[name];
    check(`merchant ${name}`, got && {
      total: got.total,
      successful: got.status.successful ?? 0,
      failed: got.status.failed ?? 0,
      abandoned: got.status.abandoned ?? 0,
      cancelled: got.status.cancelled ?? 0,
    }, { total: want.total, successful: want.successful, failed: want.failed, abandoned: want.abandoned, cancelled: want.cancelled });
    near(`merchant ${name}.successUsd`, got?.successUsd ?? 0, want.successUsd);
    near(`merchant ${name}.failedUsd`, got?.failedUsd ?? 0, want.failedUsd);
    near(`merchant ${name}.volumeUsd`, got?.volumeUsd ?? 0, want.volumeUsd);
  }

  const r = report.retry;
  check("retry", {
    retriers: r.retriers, succeededAfter: r.succeededAfter, attempts: r.attempts,
    finalStatus: r.finalStatus, gapCount: r.gapCount, gapMedian: r.gapMedianSeconds,
  }, {
    retriers: expected.retry.retriers, succeededAfter: expected.retry.succeededAfter, attempts: expected.retry.attempts,
    finalStatus: expected.retry.finalStatus, gapCount: expected.retry.gapCount, gapMedian: expected.retry.gapMedian,
  });
  near("retry.gapSum", r.gapAvgSeconds * r.gapCount, expected.retry.gapSum);
  return errors;
}

function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .filter(([, v]) => v !== 0 && v !== undefined)
        .sort(([a], [b]) => (a < b ? -1 : 1))
        .map(([k, v]) => [k, sortKeys(v)]),
    );
  }
  return value;
}

describe.skipIf(!existsSync(GOLDEN))("parity with the Python parser", () => {
  const golden: Golden = existsSync(GOLDEN) ? JSON.parse(readFileSync(GOLDEN, "utf8")) : {};

  it("reproduces every historical export", () => {
    const failures: string[] = [];
    let files = 0;
    for (const [file, expected] of Object.entries(golden)) {
      files += 1;
      const errors = compareFile(file, expected);
      if (errors.length) failures.push(`${file}\n  ${errors.slice(0, 4).join("\n  ")}`);
    }
    if (failures.length) console.log(`${failures.length}/${files} files differ\n\n${failures.slice(0, 8).join("\n\n")}`);
    expect(failures.length).toBe(0);
  }, 120_000);
});
