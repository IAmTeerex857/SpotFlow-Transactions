import { createHash, createHmac } from "node:crypto";
import { readCsv, stripBom, writeCsv } from "@/lib/parser/csv";
import type { ParseResult, ParsedTransaction } from "@/lib/parser/parse";
import type { Registry } from "@/lib/parser/registry";

export const PARSER_VERSION = "ts-2026.10.1";

/** Shape accepted by the `ingest_transactions` database function. */
export interface IngestRow {
  txn_key: string;
  reference: string | null;
  merchant: string;
  amount: number;
  currency: string;
  rate: number;
  amount_usd: number;
  provider: string;
  region: string;
  status: string;
  channel: string;
  message: string;
  paid_at: string | null;
  business_date: string;
  customer_id: string | null;
  retry_count: number | null;
  fragment: boolean;
}

const PSEUDONYM = /^[0-9a-f]{12}@anon$/;
const LONG_NUMBER = /\+?\d[\d -]{7,}\d/g;
const EMAIL = /[^\s,;:"']+@[^\s,;:"']+\.[a-z]{2,}/gi;

/**
 * Provider messages sometimes echo the customer's phone number or email
 * ("You will receive a prompt on your mobile number 2547…"). Mask them before
 * storage; this also folds those per-customer variants into one bucket.
 */
function maskIdentifiers(message: string): string {
  return message.replace(EMAIL, "[email]").replace(LONG_NUMBER, (m) => (m.replace(/\D/g, "").length >= 9 ? "[number]" : m));
}
const LEGACY_CUSTOMER = /^customer \d+$/i;

export function createPseudonymizer(secret: string) {
  if (!secret || secret.length < 32) {
    throw new Error("PSEUDONYM_SECRET must be set to at least 32 characters");
  }
  const cache = new Map<string, string>();
  return (email: string): string => {
    const key = email.trim().toLowerCase();
    if (PSEUDONYM.test(key)) return key;
    let id = cache.get(key);
    if (!id) {
      id = `${createHmac("sha256", secret).update(key).digest("hex").slice(0, 12)}@anon`;
      cache.set(key, id);
    }
    return id;
  };
}

function customerIdFor(
  raw: string,
  businessDate: string,
  pseudonymize: (email: string) => string,
): string | null {
  const token = raw.trim();
  if (!token) return null;
  if (token.includes("@")) return pseudonymize(token);
  // Historical exports were anonymised per day: "customer 1" on Monday is not
  // "customer 1" on Tuesday, so these ids are scoped to their export date.
  if (LEGACY_CUSTOMER.test(token)) return `legacy:${businessDate}:${token.toLowerCase()}`;
  return `raw:${createHash("sha256").update(token.toLowerCase()).digest("hex").slice(0, 12)}`;
}

export function toIngestRows(
  result: ParseResult,
  options: { pseudonymize: (email: string) => string; fallbackDate: string },
): { rows: IngestRow[]; uniqueCustomers: number } {
  const businessDate = result.stats.businessDate ?? options.fallbackDate;
  const occurrences = new Map<string, number>();
  const customers = new Set<string>();

  const rows = result.transactions.map((t: ParsedTransaction) => {
    const customerId = customerIdFor(t.customer, businessDate, options.pseudonymize);
    if (customerId) customers.add(customerId);
    const base = t.reference
      ? `ref:${t.reference}`
      : `h:${createHash("sha256")
          .update([t.merchant, t.amount, t.currency, t.provider, t.region, customerId ?? "", t.status, t.channel, t.message, t.paidAt ?? businessDate].join("|"))
          .digest("hex")
          .slice(0, 32)}`;
    // Identical rows inside one export are distinct attempts; keep them all.
    const seen = occurrences.get(base) ?? 0;
    occurrences.set(base, seen + 1);

    return {
      txn_key: `${base}:${seen}`,
      reference: t.reference,
      merchant: t.merchant,
      amount: t.amount,
      currency: t.currency,
      rate: t.rate,
      amount_usd: t.amountUsd,
      provider: t.provider,
      region: t.region,
      status: t.status,
      channel: t.channel,
      message: maskIdentifiers(t.message),
      paid_at: t.paidAt,
      business_date: t.paidAt ? t.paidAt.slice(0, 10) : businessDate,
      customer_id: customerId,
      retry_count: t.retryCount,
      fragment: t.fragment,
    };
  });

  return { rows, uniqueCustomers: customers.size };
}

/**
 * A copy of the export that is safe to keep: every email (including those in
 * merged rows) is replaced with its pseudonym and account numbers are removed.
 * Parsing the copy yields the same transactions as parsing the original.
 */
export function scrubExport(text: string, registry: Registry, pseudonymize: (email: string) => string): string {
  const rows = [...readCsv(stripBom(text))];
  if (!rows.length) return "";
  const header = rows[0].map((c) => c.trim().toLowerCase());
  const providerIdx = header.indexOf("provider");
  const accountIdx = header.indexOf("account number");
  const accountOffset = accountIdx !== -1 && providerIdx !== -1 ? accountIdx - providerIdx : null;

  const out = [rows[0]];
  for (const row of rows.slice(1)) {
    const next = row.map((cell) => (cell.includes("@") ? pseudonymize(cell.trim()) : cell));
    if (accountOffset !== null) {
      for (let i = 0; i < next.length - 1; i += 1) {
        if (registry.providers.has(next[i].trim().toLowerCase()) && registry.regions.has(next[i + 1].trim())) {
          const at = i + accountOffset;
          if (at >= 0 && at < next.length && /^[\d\s-]{4,}$/.test(next[at].trim())) next[at] = "";
        }
      }
    }
    out.push(next);
  }
  return writeCsv(out);
}

export function sha256(text: string): string {
  return createHash("sha256").update(text).digest("hex");
}

export function chunk<T>(items: readonly T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}
