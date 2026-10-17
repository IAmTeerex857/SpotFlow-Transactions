import "server-only";
import type { SupabaseClient } from "@supabase/supabase-js";
import { tool } from "ai";
import { z } from "zod";
import { getFilterOptions, getReport } from "@/lib/server/report-data";
import type { Report } from "@/lib/report/build";
import type { ReportFilters } from "@/lib/report/types";
import { failureBreakdown, summarizeReport } from "./summarize";

const date = z.string().regex(/^\d{4}-\d{2}-\d{2}$/).describe("YYYY-MM-DD (UTC business date)");
const filterShape = {
  merchants: z.array(z.string()).optional().describe("Exact merchant names; use 'Unattributed' rows via '<unknown>'"),
  providers: z.array(z.string()).optional().describe("Provider ids, e.g. hubtel, paystack"),
  regions: z.array(z.string()).optional().describe("Country names, e.g. Ghana, Nigeria"),
  channels: z.array(z.enum(["card", "bank_transfer", "mobile_money", "eft", "other"])).optional(),
};
const rangeSchema = z.object({ from: date, to: date, ...filterShape });

type Range = z.infer<typeof rangeSchema>;
const toFilters = (r: Range): ReportFilters => ({ ...r, from: r.from <= r.to ? r.from : r.to, to: r.from <= r.to ? r.to : r.from });
const json = (value: unknown) => JSON.stringify(value);

type ToolEvent = { name: string; input: unknown; ok: boolean; ms: number };

/** Read-only analytics tools, bound to the signed-in member's Supabase session (RLS applies). */
export function createAskTools(supabase: SupabaseClient, onEvent: (event: ToolEvent) => void) {
  const cache = new Map<string, Promise<Report>>();
  const report = (r: Range) => {
    const f = toFilters(r);
    const key = json(f);
    if (!cache.has(key)) cache.set(key, getReport(supabase, f));
    return cache.get(key)!;
  };
  const track = <I,>(name: string, run: (input: I) => Promise<string>) => async (input: I) => {
    const started = Date.now();
    try {
      const out = await run(input);
      onEvent({ name, input, ok: true, ms: Date.now() - started });
      return out;
    } catch (e) {
      onEvent({ name, input, ok: false, ms: Date.now() - started });
      return json({ error: e instanceof Error ? e.message : "Query failed" });
    }
  };

  return {
    list_options: tool({
      description: "List the merchants, providers, regions and channels present in the data, plus the earliest and latest available dates. Call this first if you need exact names or the date range.",
      inputSchema: z.object({}),
      execute: track("list_options", async () => json(await getFilterOptions(supabase))),
    }),
    get_report_summary: tool({
      description: "Totals, status mix, success/failed/abandoned USD volume, top failure reasons, merchants, providers, regions and retry behaviour for a date range and optional filters.",
      inputSchema: rangeSchema.extend({ top_merchants: z.number().int().min(1).max(50).optional() }),
      execute: track("get_report_summary", async (input) => json(summarizeReport(await report(input), { merchants: input.top_merchants }))),
    }),
    compare_periods: tool({
      description: "Compare two date ranges with the same filters. Returns a summary of each so you can compute changes.",
      inputSchema: z.object({ a: z.object({ from: date, to: date }), b: z.object({ from: date, to: date }), ...filterShape }),
      execute: track("compare_periods", async ({ a, b, ...filters }) => {
        const [ra, rb] = await Promise.all([report({ ...a, ...filters }), report({ ...b, ...filters })]);
        return json({ a: summarizeReport(ra, { merchants: 8, reasons: 6 }), b: summarizeReport(rb, { merchants: 8, reasons: 6 }) });
      }),
    }),
    get_reason_breakdown: tool({
      description: "Provider-message reasons for one status (default failed), broken down by region or provider, with counts and shares.",
      inputSchema: rangeSchema.extend({
        by: z.enum(["region", "provider", "merchant"]),
        status: z.enum(["failed", "abandoned", "cancelled", "successful"]).optional(),
      }),
      execute: track("get_reason_breakdown", async (input) => json(failureBreakdown(await report(input), input.by, input.status ?? "failed"))),
    }),
    get_daily_series: tool({
      description: "Per-day totals, status counts, success rate and USD volumes for a range. Use for trends, spikes and day-of-week questions.",
      inputSchema: rangeSchema,
      execute: track("get_daily_series", async (input) => {
        const r = await report(input);
        return json({
          days: r.daily.map((d) => ({ ...d, successRate: Math.round(d.successRate * 1000) / 10, successUsd: Math.round(d.successUsd * 100) / 100, failedUsd: Math.round(d.failedUsd * 100) / 100 })),
          by_weekday_utc: r.peaks.byDay,
        });
      }),
    }),
    sample_transactions: tool({
      description: "Up to 50 example transactions matching filters, newest first. Use for concrete examples, not for counting.",
      inputSchema: rangeSchema.extend({
        statuses: z.array(z.enum(["successful", "failed", "abandoned", "cancelled", "refunded", "inprogress"])).optional(),
        message_contains: z.string().max(80).optional(),
        limit: z.number().int().min(1).max(50).optional(),
      }),
      execute: track("sample_transactions", async (input) => {
        const f = toFilters(input);
        const { data, error } = await supabase.rpc("sample_transactions", {
          p_from: f.from, p_to: f.to,
          p_merchants: f.merchants?.length ? f.merchants : null,
          p_providers: f.providers?.length ? f.providers : null,
          p_regions: f.regions?.length ? f.regions : null,
          p_statuses: input.statuses?.length ? input.statuses : null,
          p_message: input.message_contains ?? null,
          p_limit: input.limit ?? 20,
        });
        if (error) throw new Error(error.message);
        return json(data);
      }),
    }),
  };
}
