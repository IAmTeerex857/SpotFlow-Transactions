import "server-only";
import type { SupabaseClient } from "@supabase/supabase-js";

export interface UploadRecord {
  id: string;
  uploaded_by: string | null;
  uploader_email: string | null;
  source: "upload" | "legacy_import" | "reprocess";
  status: "processing" | "complete" | "failed";
  file_name: string;
  file_bytes: number;
  parser_version: string;
  business_date: string | null;
  period_start: string | null;
  period_end: string | null;
  source_rows: number;
  parsed_count: number;
  recovered_fragments: number;
  rows_without_transaction: number;
  missing_dates: number;
  inserted_count: number;
  updated_count: number;
  unique_customers: number;
  unknown_providers: Record<string, number>;
  unknown_regions: Record<string, number>;
  storage_path: string | null;
  error: string | null;
  duration_ms: number | null;
  created_at: string;
}

export async function listUploads(supabase: SupabaseClient, limit = 200): Promise<UploadRecord[]> {
  const { data, error } = await supabase.from("uploads").select("*").order("created_at", { ascending: false }).limit(limit);
  if (error) throw new Error(`Could not load uploads: ${error.message}`);
  return data as UploadRecord[];
}

export async function getUpload(supabase: SupabaseClient, id: string): Promise<UploadRecord | null> {
  if (!/^[0-9a-f-]{36}$/i.test(id)) return null;
  const { data } = await supabase.from("uploads").select("*").eq("id", id).maybeSingle();
  return (data as UploadRecord) ?? null;
}
