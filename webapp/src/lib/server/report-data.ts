import "server-only";
import type { SupabaseClient } from "@supabase/supabase-js";
import { buildReport, type Report } from "@/lib/report/build";
import type { FilterOptions } from "@/lib/report/filters";
import type { Cube, ReportFilters } from "@/lib/report/types";

const orNull = (values?: string[]) => (values && values.length ? values : null);

export async function getFilterOptions(supabase: SupabaseClient): Promise<FilterOptions> {
  const { data, error } = await supabase.rpc("filter_options");
  if (error) throw new Error(`filter_options failed: ${error.message}`);
  return data as FilterOptions;
}

export async function getCube(supabase: SupabaseClient, filters: ReportFilters): Promise<Cube> {
  const { data, error } = await supabase.rpc("report_cube", {
    p_from: filters.from,
    p_to: filters.to,
    p_merchants: orNull(filters.merchants),
    p_providers: orNull(filters.providers),
    p_regions: orNull(filters.regions),
    p_channels: orNull(filters.channels),
    p_upload: filters.uploadId ?? null,
  });
  if (error) throw new Error(`report_cube failed: ${error.message}`);
  return data as Cube;
}

export async function getReport(supabase: SupabaseClient, filters: ReportFilters): Promise<Report> {
  return buildReport(await getCube(supabase, filters));
}
