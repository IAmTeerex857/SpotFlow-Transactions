"use client";

import { useRouter } from "next/navigation";
import { DataTable, type Column } from "@/components/report/data-table";
import { StatusPill } from "@/components/ui/status";
import { fmtDate, fmtDateTime, fmtInt } from "@/lib/format";
import type { UploadRecord } from "@/lib/server/uploads";

const STATUS: Record<UploadRecord["status"], { tone: string; label: string }> = {
  complete: { tone: "successful", label: "Complete" },
  processing: { tone: "abandoned", label: "Processing" },
  failed: { tone: "failed", label: "Failed" },
};

export function UploadTable({ uploads }: { uploads: UploadRecord[] }) {
  const router = useRouter();
  const columns: Column<UploadRecord>[] = [
    {
      key: "file", header: "Export", sortValue: (u) => u.file_name,
      render: (u) => (
        <div className="min-w-0">
          <div className="max-w-[320px] truncate font-medium text-ink">{u.file_name}</div>
          <div className="text-[12px] text-faint">{u.source === "legacy_import" ? "Historical import" : u.uploader_email ?? "—"}</div>
        </div>
      ),
    },
    { key: "date", header: "Export date", sortValue: (u) => u.business_date ?? "", render: (u) => (u.business_date ? fmtDate(u.business_date) : "—") },
    { key: "txns", header: "Transactions", numeric: true, sortValue: (u) => u.parsed_count, render: (u) => fmtInt(u.parsed_count) },
    { key: "frag", header: "Recovered", numeric: true, sortValue: (u) => u.recovered_fragments, render: (u) => fmtInt(u.recovered_fragments) },
    {
      key: "issues", header: "Warnings", numeric: true,
      sortValue: (u) => u.rows_without_transaction + Object.keys(u.unknown_providers).length,
      render: (u) => {
        const unknown = Object.keys(u.unknown_providers).length + Object.keys(u.unknown_regions).length;
        return unknown || u.rows_without_transaction
          ? <span className="text-warn">{unknown ? `${unknown} unknown` : `${fmtInt(u.rows_without_transaction)} rows`}</span>
          : <span className="text-faint">—</span>;
      },
    },
    { key: "duration", header: "Took", numeric: true, sortValue: (u) => u.duration_ms ?? 0, render: (u) => (u.duration_ms ? `${(u.duration_ms / 1000).toFixed(1)}s` : "—") },
    { key: "status", header: "Status", render: (u) => <StatusPill status={STATUS[u.status].tone}>{STATUS[u.status].label}</StatusPill> },
    { key: "when", header: "Uploaded", sortValue: (u) => u.created_at, render: (u) => <span className="text-muted">{fmtDateTime(u.created_at)}</span> },
  ];
  return (
    <DataTable
      rows={uploads}
      columns={columns}
      rowKey={(u) => u.id}
      onRowClick={(u) => router.push(`/app/uploads/${u.id}`)}
      empty="No exports have been uploaded yet."
    />
  );
}
