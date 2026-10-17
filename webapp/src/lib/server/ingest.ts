import "server-only";
import type { SupabaseClient } from "@supabase/supabase-js";
import { chunk, createPseudonymizer, PARSER_VERSION, scrubExport, sha256, toIngestRows } from "@/lib/ingest/prepare";
import { stripBom } from "@/lib/parser/csv";
import { parseExport, type ParseStats } from "@/lib/parser/parse";
import { getRegistry } from "./registry";

export const MAX_EXPORT_BYTES = 60 * 1024 * 1024;
const BUCKET = "exports";

interface IngestResult {
  uploadId: string;
  stats: ParseStats;
  inserted: number;
  updated: number;
  uniqueCustomers: number;
  stored: boolean;
  from: string;
  to: string;
}

export class IngestError extends Error {
  constructor(message: string, readonly status = 400, readonly detail?: unknown) {
    super(message);
  }
}

function pseudonymizer() {
  const secret = process.env.PSEUDONYM_SECRET;
  if (!secret) throw new IngestError("The server is missing PSEUDONYM_SECRET, so customer emails can't be pseudonymised. Ask an admin to set it.", 500);
  return createPseudonymizer(secret);
}

async function writeRows(supabase: SupabaseClient, uploadId: string, rows: ReturnType<typeof toIngestRows>["rows"]) {
  let inserted = 0;
  let updated = 0;
  for (const part of chunk(rows, 1000)) {
    const { data, error } = await supabase.rpc("ingest_transactions", { p_upload: uploadId, p_rows: part });
    if (error) throw new IngestError(`Saving transactions failed: ${error.message}`, 500);
    inserted += data.inserted;
    updated += data.updated;
  }
  return { inserted, updated };
}

/**
 * Parse an export, store its transactions and a scrubbed copy, and record the
 * upload. The raw text never leaves this function: emails are pseudonymised
 * before anything is written.
 */
export async function ingestExport(
  supabase: SupabaseClient,
  input: { text: string; fileName: string; bytes: number; userId: string | null; source?: "upload" | "legacy_import" },
): Promise<IngestResult> {
  const started = Date.now();
  const registry = await getRegistry(supabase);
  const pseudonymize = pseudonymizer();

  let parsed;
  try {
    parsed = parseExport(input.text, registry);
  } catch (e) {
    throw new IngestError(e instanceof Error ? e.message : "The file could not be read as CSV");
  }
  const { stats } = parsed;
  if (!stats.transactions) {
    throw new IngestError("No transactions were found. Is this a Spotflow transactions export?", 422, stats);
  }

  const fallbackDate = stats.businessDate ?? new Date().toISOString().slice(0, 10);
  const { rows, uniqueCustomers } = toIngestRows(parsed, { pseudonymize, fallbackDate });
  const dates = rows.map((r) => r.business_date).sort();

  const { data: upload, error: uploadError } = await supabase
    .from("uploads")
    .insert({
      uploaded_by: input.userId,
      source: input.source ?? "upload",
      file_name: input.fileName.slice(0, 255),
      file_sha256: sha256(stripBom(input.text)),
      file_bytes: input.bytes,
      parser_version: PARSER_VERSION,
      business_date: stats.businessDate,
      period_start: stats.firstPaidAt,
      period_end: stats.lastPaidAt,
      source_rows: stats.sourceRows,
      parsed_count: stats.transactions,
      recovered_fragments: stats.recoveredFragments,
      rows_without_transaction: stats.rowsWithoutTransaction,
      missing_dates: stats.missingDates,
      unique_customers: uniqueCustomers,
      unknown_providers: stats.unknownProviders,
      unknown_regions: stats.unknownRegions,
      columns: stats.columns,
    })
    .select("id")
    .single();
  if (uploadError) throw new IngestError(`Could not record the upload: ${uploadError.message}`, 500);
  const uploadId = upload.id as string;

  try {
    const { inserted, updated } = await writeRows(supabase, uploadId, rows);

    let storagePath: string | null = `${uploadId}.csv`;
    const scrubbed = scrubExport(input.text, registry, pseudonymize);
    const { error: storageError } = await supabase.storage.from(BUCKET).upload(storagePath, scrubbed, { contentType: "text/csv", upsert: true });
    if (storageError) {
      console.warn("scrubbed copy not stored", storageError.message);
      storagePath = null;
    }

    await supabase.from("uploads").update({
      status: "complete",
      inserted_count: inserted,
      updated_count: updated,
      storage_path: storagePath,
      duration_ms: Date.now() - started,
      completed_at: new Date().toISOString(),
    }).eq("id", uploadId);

    return { uploadId, stats, inserted, updated, uniqueCustomers, stored: storagePath !== null, from: dates[0], to: dates[dates.length - 1] };
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    await supabase.from("uploads").update({ status: "failed", error: message.slice(0, 500), duration_ms: Date.now() - started }).eq("id", uploadId);
    throw e instanceof IngestError ? e : new IngestError(message, 500);
  }
}

/** Re-run the current parser over the stored scrubbed copy of an upload. */
export async function reprocessUpload(supabase: SupabaseClient, uploadId: string) {
  const { data: upload, error } = await supabase.from("uploads").select("id, storage_path").eq("id", uploadId).single();
  if (error || !upload) throw new IngestError("Upload not found", 404);
  if (!upload.storage_path) throw new IngestError("This upload has no stored copy to reprocess. Upload the file again instead.", 409);

  const { data: blob, error: downloadError } = await supabase.storage.from(BUCKET).download(upload.storage_path);
  if (downloadError || !blob) throw new IngestError(`Could not read the stored copy: ${downloadError?.message}`, 500);
  const text = await blob.text();

  const started = Date.now();
  const registry = await getRegistry(supabase);
  const parsed = parseExport(text, registry);
  const { rows, uniqueCustomers } = toIngestRows(parsed, {
    pseudonymize: pseudonymizer(),
    fallbackDate: parsed.stats.businessDate ?? new Date().toISOString().slice(0, 10),
  });
  const { inserted, updated } = await writeRows(supabase, uploadId, rows);
  const { stats } = parsed;
  await supabase.from("uploads").update({
    parser_version: PARSER_VERSION,
    parsed_count: stats.transactions,
    recovered_fragments: stats.recoveredFragments,
    rows_without_transaction: stats.rowsWithoutTransaction,
    missing_dates: stats.missingDates,
    unique_customers: uniqueCustomers,
    unknown_providers: stats.unknownProviders,
    unknown_regions: stats.unknownRegions,
    inserted_count: inserted,
    updated_count: updated,
    duration_ms: Date.now() - started,
    status: "complete",
    error: null,
  }).eq("id", uploadId);
  return { stats, inserted, updated };
}
