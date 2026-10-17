// Port of `parse_csv_v2` (render_html_report_v2.py). Spotflow exports are not
// clean one-row-per-transaction files: retries can stitch several complete
// transactions onto one CSV line without quoting. Each transaction is found
// by scanning for a `provider, region` pair and walking forward from there.
//
// Additions over the Python parser:
//   * raw exports: Reference / Retry Count columns are captured
//   * customer emails are surfaced so the server can pseudonymise them
//   * rows that yield no transaction are counted and their unknown provider /
//     region values reported, instead of disappearing silently

import { readCsv, stripBom } from "./csv";
import { normaliseMessage } from "./normalize";
import { CHANNELS, CURRENCIES, DEFAULT_REGISTRY, STATUSES, type Channel, type Registry, type Status } from "./registry";

export interface ParsedTransaction {
  merchant: string;
  amount: number;
  currency: string;
  rate: number;
  amountUsd: number;
  provider: string;
  region: string;
  status: Status;
  channel: Channel;
  /** Normalised provider-message bucket. */
  message: string;
  /** ISO timestamp (UTC) or null when the export had no valid date. */
  paidAt: string | null;
  /** Raw customer token: an email in raw exports, `customer N` in legacy files. */
  customer: string;
  reference: string | null;
  retryCount: number | null;
  /** True when recovered from a row that held more than one transaction. */
  fragment: boolean;
  /** 0-based index of the data row this came from. */
  row: number;
}

export interface ParseStats {
  sourceRows: number;
  transactions: number;
  recoveredFragments: number;
  rowsWithoutTransaction: number;
  missingDates: number;
  unknownProviders: Record<string, number>;
  unknownRegions: Record<string, number>;
  hasFinancials: boolean;
  hasReference: boolean;
  columns: string[];
  firstPaidAt: string | null;
  lastPaidAt: string | null;
  /** Most common UTC date among timestamps — the export's business date. */
  businessDate: string | null;
}

export interface ParseResult {
  transactions: ParsedTransaction[];
  stats: ParseStats;
}

const STATUS_SET: ReadonlySet<string> = new Set(STATUSES);
const CHANNEL_SET: ReadonlySet<string> = new Set(CHANNELS);
const CURRENCY_SET: ReadonlySet<string> = new Set(CURRENCIES);
const ISO_PATTERN = /^\d{4}-\d{2}-\d{2}T/;
const DEFAULT_DATE_PLACEHOLDER = "<missing-date>";
const MODES = new Set(["live", "test"]);

function toUsd(amount: number, currency: string, rate: number): number {
  if (currency === "USD" && amount > 0) return amount;
  if (rate > 0 && amount > 0) return amount / rate;
  return 0;
}

// Python float(): accepts surrounding whitespace, rejects partial numbers.
function safeFloat(value: string | undefined): number {
  if (value == null) return 0;
  const cleaned = value.replace(/,/g, "").trim();
  if (!cleaned || !/^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/.test(cleaned)) return 0;
  return Number(cleaned);
}

function isPyDigits(value: string): boolean {
  return value.length > 0 && /^\d+$/.test(value);
}

// datetime.fromisoformat (Python 3.9) after replacing Z with +00:00.
function parseIso(value: string): string | null {
  const m = value.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{3}|\d{6}))?)?(Z|[+-]\d{2}:\d{2})?$/);
  if (!m) return null;
  const iso = `${m[1]}-${m[2]}-${m[3]}T${m[4]}:${m[5]}:${m[6] ?? "00"}${m[7] ? `.${m[7].slice(0, 3)}` : ""}${m[8] ?? "Z"}`;
  const time = Date.parse(iso);
  return Number.isNaN(time) ? null : new Date(time).toISOString();
}

export function parseExport(input: string, registry: Registry = DEFAULT_REGISTRY): ParseResult {
  const text = stripBom(input);
  const rows = readCsv(text);
  const first = rows.next();
  if (first.done) throw new Error("CSV file is empty");

  const rawHeader = first.value;
  const header = rawHeader.map((c) => c.trim().toLowerCase());
  const col = (name: string): number | null => {
    const idx = header.indexOf(name);
    return idx === -1 ? null : idx;
  };

  const merchantIdx = col("merchant");
  const amountIdx = col("amount");
  const currencyIdx = col("currency");
  const rateIdx = col("rate");
  const providerIdx = col("provider");
  const regionIdx = col("region");
  const referenceIdx = col("reference");
  const retryIdx = col("retry count");
  const dateIdx = col("payment date");
  const hasFinancials = currencyIdx !== null && rateIdx !== null;

  // Offsets relative to the Provider column, used to locate columns of a
  // fragment that sits mid-row.
  const referenceOffset = referenceIdx !== null && providerIdx !== null ? referenceIdx - providerIdx : null;
  const retryAfterDate = retryIdx !== null && dateIdx !== null && retryIdx > dateIdx ? retryIdx - dateIdx : null;

  const headerDate = rawHeader.find((c) => ISO_PATTERN.test(c.trim()));
  const defaultDt = headerDate ? headerDate.trim() : DEFAULT_DATE_PLACEHOLDER;

  const { providers } = registry;
  const regionOf = (raw: string): string | undefined => registry.regions.get(raw);

  const findNextProvider = (row: string[], start: number): number | null => {
    for (let idx = start; idx < row.length - 1; idx += 1) {
      if (providers.has(row[idx].trim().toLowerCase()) && regionOf(row[idx + 1].trim()) !== undefined) {
        return idx;
      }
    }
    return null;
  };

  const transactions: ParsedTransaction[] = [];
  const unknownProviders: Record<string, number> = {};
  const unknownRegions: Record<string, number> = {};
  let sourceRows = 0;
  let rowsWithoutTransaction = 0;
  let recoveredFragments = 0;

  let rowNumber = -1;
  for (const row of rows) {
    if (row.length === 0) continue;
    rowNumber += 1;
    sourceRows += 1;
    const n = row.length;
    const before = transactions.length;

    let pMerchant = "";
    let pAmount = 0;
    let pCurrency = "";
    let pRate = 0;
    if (hasFinancials) {
      pMerchant = merchantIdx !== null && merchantIdx < n ? row[merchantIdx].trim() : "";
      pAmount = amountIdx !== null && amountIdx < n ? safeFloat(row[amountIdx]) : 0;
      pCurrency = currencyIdx !== null && currencyIdx < n ? row[currencyIdx].trim().toUpperCase() : "";
      pRate = rateIdx !== null && rateIdx < n ? safeFloat(row[rateIdx]) : 0;
    }

    let i = 0;
    let isFirst = true;
    while (i < n) {
      const pIdx = findNextProvider(row, i);
      if (pIdx === null) break;

      const provider = row[pIdx].trim().toLowerCase();
      const region = regionOf(row[pIdx + 1].trim())!;
      const fragment = !isFirst;

      let merchant: string;
      let amount: number;
      let currency: string;
      let rate: number;
      let reference: string | null = null;
      if (isFirst) {
        [merchant, amount, currency, rate] = [pMerchant, pAmount, pCurrency, pRate];
        if (referenceIdx !== null && referenceIdx < n) reference = row[referenceIdx].trim() || null;
        isFirst = false;
      } else {
        [merchant, amount, currency, rate] = ["", 0, "", 0];
        if (pIdx >= 4 && CURRENCY_SET.has(row[pIdx - 2].trim().toUpperCase())) {
          merchant = row[pIdx - 4].trim();
          amount = safeFloat(row[pIdx - 3]);
          currency = row[pIdx - 2].trim().toUpperCase();
          rate = safeFloat(row[pIdx - 1]);
        }
        if (referenceOffset !== null) {
          const refAt = pIdx + referenceOffset;
          if (refAt >= 0 && refAt < n) reference = row[refAt].trim() || null;
        }
      }

      let idx = pIdx + 2;
      const customerParts: string[] = [];
      while (idx < n && !STATUS_SET.has(row[idx].trim().toLowerCase())) {
        const token = row[idx].trim();
        if (token) customerParts.push(token);
        idx += 1;
      }
      if (idx >= n) {
        i = pIdx + 1;
        continue;
      }
      const status = row[idx].trim().toLowerCase() as Status;
      idx += 1;

      while (idx < n && !CHANNEL_SET.has(row[idx].trim().toLowerCase())) idx += 1;
      if (idx >= n) {
        i = pIdx + 1;
        continue;
      }
      let channel = row[idx].trim().toLowerCase() as Channel;
      idx += 1;

      if (idx < n && MODES.has(row[idx].trim().toLowerCase())) idx += 1;

      let customer = customerParts.find((t) => t.includes("@") || t.startsWith("customer ")) ?? customerParts[0] ?? "";
      customer = stripQuotes(customer.trim());

      const messageParts: string[] = [];
      let paymentDate = "";
      let retryCount: number | null = null;
      while (idx < n) {
        const raw = row[idx];
        const stripped = raw.trim();
        if (ISO_PATTERN.test(stripped)) {
          paymentDate = stripped;
          if (retryAfterDate !== null) {
            const retryCell = row[idx + retryAfterDate]?.trim();
            if (retryCell && isPyDigits(retryCell)) retryCount = Number(retryCell);
          }
          idx += 1;
          break;
        }
        const lower = stripped.toLowerCase();
        if (findNextProvider(row, idx) === idx) break;
        if (providers.has(lower) || regionOf(stripped) !== undefined) break;
        if (stripped.includes("@")) break;
        const nextToken = idx + 1 < n ? row[idx + 1].trim() : "";
        if (nextToken && isPyDigits(nextToken.replace(/,/g, "").replace(/\./g, ""))) break;
        messageParts.push(raw);
        idx += 1;
      }

      let message = messageParts.join(",").trim();
      if (message) {
        message = cutAtMarker(message, providers);
        message = cutAtMarker(message, new Set([...registry.regions.keys()].map((r) => r.toLowerCase())));
      }
      if (message.startsWith('"') && message.endsWith('"')) message = message.slice(1, -1);
      paymentDate = paymentDate || defaultDt;

      if (!STATUS_SET.has(status)) {
        i = idx;
        continue;
      }
      if (!CHANNEL_SET.has(channel)) channel = "other";

      if (retryCount === null && retryIdx !== null && !fragment && retryIdx < n) {
        const cell = row[retryIdx].trim();
        if (isPyDigits(cell)) retryCount = Number(cell);
      }

      transactions.push({
        merchant,
        amount,
        currency,
        rate,
        amountUsd: toUsd(amount, currency, rate),
        provider,
        region,
        status,
        channel,
        message: normaliseMessage(message),
        paidAt: parseIso(paymentDate),
        customer,
        reference,
        retryCount,
        fragment,
        row: rowNumber,
      });
      if (fragment) recoveredFragments += 1;
      i = idx;
    }

    if (transactions.length === before) {
      rowsWithoutTransaction += 1;
      if (providerIdx !== null && providerIdx < n) {
        const p = row[providerIdx].trim().toLowerCase();
        if (p && !providers.has(p)) unknownProviders[p] = (unknownProviders[p] ?? 0) + 1;
      }
      if (regionIdx !== null && regionIdx < n) {
        const r = row[regionIdx].trim();
        if (r && regionOf(r) === undefined) unknownRegions[r] = (unknownRegions[r] ?? 0) + 1;
      }
    }
  }

  const dated = transactions.map((t) => t.paidAt).filter((d): d is string => d !== null).sort();
  const dayCounts = new Map<string, number>();
  for (const d of dated) dayCounts.set(d.slice(0, 10), (dayCounts.get(d.slice(0, 10)) ?? 0) + 1);
  let businessDate: string | null = null;
  let best = 0;
  for (const [day, count] of dayCounts) {
    if (count > best) [businessDate, best] = [day, count];
  }

  return {
    transactions,
    stats: {
      sourceRows,
      transactions: transactions.length,
      recoveredFragments,
      rowsWithoutTransaction,
      missingDates: transactions.length - dated.length,
      unknownProviders,
      unknownRegions,
      hasFinancials,
      hasReference: referenceIdx !== null,
      columns: rawHeader.map((c) => c.trim()),
      firstPaidAt: dated[0] ?? null,
      lastPaidAt: dated[dated.length - 1] ?? null,
      businessDate,
    },
  };
}

function stripQuotes(value: string): string {
  let start = 0;
  let end = value.length;
  while (start < end && value[start] === '"') start += 1;
  while (end > start && value[end - 1] === '"') end -= 1;
  return value.slice(start, end);
}

// Merged rows can leave `,provider` or `,region` tails inside a message.
function cutAtMarker(message: string, tokens: ReadonlySet<string>): string {
  const lower = message.toLowerCase();
  for (const token of tokens) {
    const pos = lower.indexOf(`,${token}`);
    if (pos !== -1) return message.slice(0, pos);
  }
  return message;
}
