import { cubeFromTransactions } from "@/lib/report/cube";
import { buildHome } from "@/lib/report/home";
import { sampleTransactions } from "./sample";

/** Home summary over the synthetic sample (demo route only). */
export function sampleHome() {
  const rows = sampleTransactions();
  const latest = rows.reduce((m, r) => (r.businessDate > m ? r.businessDate : m), "");
  return buildHome(cubeFromTransactions(rows), cubeFromTransactions(rows.filter((r) => r.businessDate === latest)));
}

export const SAMPLE_UPLOADS = [
  { id: "d1", fileName: "Oct 15 - export (311).csv", status: "complete" as const, transactions: 1184, durationMs: 3400, createdAt: "2026-10-16T08:12:00Z" },
  { id: "d2", fileName: "Oct 14 - export (310).csv", status: "complete" as const, transactions: 1236, durationMs: 3100, createdAt: "2026-10-15T08:05:00Z" },
  { id: "d3", fileName: "Oct 13 - export (309).csv", status: "complete" as const, transactions: 1097, durationMs: 2900, createdAt: "2026-10-14T08:21:00Z" },
  { id: "d4", fileName: "Oct 12 - export.csv", status: "failed" as const, transactions: 0, durationMs: 400, createdAt: "2026-10-13T08:02:00Z" },
  { id: "d5", fileName: "Oct 12 - export (308).csv", status: "complete" as const, transactions: 1312, durationMs: 3600, createdAt: "2026-10-13T08:04:00Z" },
];
