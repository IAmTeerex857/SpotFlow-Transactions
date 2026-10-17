import { notFound } from "next/navigation";
import { AlertTriangle } from "lucide-react";
import { LiveExplorer } from "@/components/report/live-explorer";
import { UploadActions } from "@/components/upload/upload-actions";
import { requireMember } from "@/lib/server/auth";
import { getFilterOptions, getReport } from "@/lib/server/report-data";
import { fmtDateTime, fmtInt } from "@/lib/format";
import { parseFilters } from "@/lib/report/filters";
import { getUpload } from "@/lib/server/uploads";

export default async function UploadDetailPage({ params, searchParams }: PageProps<"/app/uploads/[id]">) {
  const { id } = await params;
  const { supabase, member } = await requireMember();
  const upload = await getUpload(supabase, id);
  if (!upload) notFound();

  const options = await getFilterOptions(supabase);
  const query = await searchParams;
  const start = upload.period_start?.slice(0, 10) ?? upload.business_date ?? options.maxDate ?? "";
  const end = upload.period_end?.slice(0, 10) ?? upload.business_date ?? start;
  const filters = { ...parseFilters({ from: start, to: end, ...query }, options), uploadId: upload.id };
  const report = await getReport(supabase, filters);

  const unknown = [...Object.entries(upload.unknown_providers), ...Object.entries(upload.unknown_regions)];
  const meta = [
    `${fmtInt(upload.parsed_count)} transactions`,
    `${fmtInt(upload.recovered_fragments)} recovered from merged rows`,
    `${fmtInt(upload.unique_customers)} customers`,
    upload.source === "legacy_import" ? "Historical import" : `Uploaded by ${upload.uploader_email ?? "—"}`,
    fmtDateTime(upload.created_at),
  ];

  return (
    <LiveExplorer
      report={report}
      filters={filters}
      options={options}
      basePath={`/app/uploads/${upload.id}`}
      title={upload.file_name}
      crumb="Data · History"
      actions={<UploadActions id={upload.id} canDelete={upload.uploaded_by === member.id} canReprocess={!!upload.storage_path} />}
      banner={
        <div className="space-y-2">
          <p className="text-[13px] text-muted">{meta.join(" · ")}</p>
          {upload.status === "failed" && (
            <div className="flex items-start gap-2.5 rounded-xl border border-bad/30 bg-bad-dim px-4 py-3 text-[13px] text-bad">
              <AlertTriangle className="mt-0.5 size-4 shrink-0" /> This upload failed: {upload.error}
            </div>
          )}
          {(unknown.length > 0 || upload.rows_without_transaction > 0) && (
            <div className="flex items-start gap-2.5 rounded-xl border border-warn/30 bg-warn-dim px-4 py-3 text-[13px] text-ink">
              <AlertTriangle className="mt-0.5 size-4 shrink-0 text-warn" />
              <span>
                {fmtInt(upload.rows_without_transaction)} rows produced no transaction
                {unknown.length > 0 && <> (unrecognised: {unknown.map(([k, n]) => `${k} ×${n}`).join(", ")})</>}.
                {upload.storage_path && " Add them from the Upload page, then reprocess this export."}
              </span>
            </div>
          )}
        </div>
      }
    />
  );
}
