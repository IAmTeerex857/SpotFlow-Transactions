// The cube is the contract between the database (supabase/migrations, the
// `report_cube` function) and the report UI. Postgres produces it from the
// transactions table; `cubeFromTransactions` produces the identical shape in
// TypeScript for upload previews, the landing demo and the parity tests.

export interface FactRow {
  /** Business date, YYYY-MM-DD. */
  d: string;
  /** Merchant, or "<unknown>" when the export had none. */
  m: string;
  p: string;
  r: string;
  s: string;
  c: string;
  cur: string;
  n: number;
  usd: number;
}

export interface MessageRow {
  r: string;
  p: string;
  s: string;
  msg: string;
  n: number;
}

export interface RetryStats {
  /** Customers with more than one attempt in the range. */
  retriers: number;
  succeededAfter: number;
  /** Retriers whose latest attempt (by time) was not successful. */
  unresolved: number;
  attempts: Record<string, number>;
  finalStatus: Record<string, number>;
  gapCount: number;
  gapAvgSeconds: number;
  gapMedianSeconds: number;
  /** Share of retriers whose customer ids are day-scoped legacy pseudonyms. */
  legacyShare: number;
}

export interface PeakRow {
  /** 0 = Sunday … 6 = Saturday (Postgres `extract(dow)`). */
  dow: number;
  hour: number;
  n: number;
  ok: number;
  usd: number;
}

export interface Cube {
  facts: FactRow[];
  messages: MessageRow[];
  retry: RetryStats;
  peaks: PeakRow[];
}

export interface ReportFilters {
  from: string;
  to: string;
  merchants?: string[];
  providers?: string[];
  regions?: string[];
  channels?: string[];
  uploadId?: string | null;
}

export const UNKNOWN_MERCHANT = "<unknown>";
